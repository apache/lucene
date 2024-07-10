/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene912;

import static org.apache.lucene.codecs.lucene912.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.SKIP_TOTAL_SIZE;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.VERSION_START;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that reads docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * @lucene.experimental
 */
public final class Lucene912PostingsReader extends PostingsReaderBase {

  private final IndexInput docIn;
  private final IndexInput posIn;
  private final IndexInput payIn;

  private final int version;

  /** Sole constructor. */
  public Lucene912PostingsReader(SegmentReadState state) throws IOException {
    boolean success = false;
    IndexInput docIn = null;
    IndexInput posIn = null;
    IndexInput payIn = null;

    // NOTE: these data files are too costly to verify checksum against all the bytes on open,
    // but for now we at least verify proper structure of the checksum footer: which looks
    // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
    // such as file truncation.

    String docName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.DOC_EXTENSION);
    try {
      // Postings have a forward-only access pattern, so pass ReadAdvice.NORMAL to perform
      // readahead.
      docIn = state.directory.openInput(docName, state.context.withReadAdvice(ReadAdvice.NORMAL));
      version =
          CodecUtil.checkIndexHeader(
              docIn,
              DOC_CODEC,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      CodecUtil.retrieveChecksum(docIn);

      if (state.fieldInfos.hasProx()) {
        String proxName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.POS_EXTENSION);
        posIn = state.directory.openInput(proxName, state.context);
        CodecUtil.checkIndexHeader(
            posIn, POS_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
        CodecUtil.retrieveChecksum(posIn);

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene912PostingsFormat.PAY_EXTENSION);
          payIn = state.directory.openInput(payName, state.context);
          CodecUtil.checkIndexHeader(
              payIn, PAY_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
          CodecUtil.retrieveChecksum(payIn);
        }
      }

      this.docIn = docIn;
      this.posIn = posIn;
      this.payIn = payIn;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docIn, posIn, payIn);
      }
    }
  }

  @Override
  public void init(IndexInput termsIn, SegmentReadState state) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkIndexHeader(
        termsIn,
        TERMS_CODEC,
        VERSION_START,
        VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != BLOCK_SIZE) {
      throw new IllegalStateException(
          "index-time BLOCK_SIZE ("
              + indexBlockSize
              + ") != read-time BLOCK_SIZE ("
              + BLOCK_SIZE
              + ")");
    }
  }

  static void prefixSum(long[] buffer, int count, long base) {
    buffer[0] += base;
    for (int i = 1; i < count; ++i) {
      buffer[i] += buffer[i - 1];
    }
  }

  static int findFirstGreater(long[] buffer, int target, int from) {
    for (int i = from; i < BLOCK_SIZE; ++i) {
      if (buffer[i] >= target) {
        return i;
      }
    }
    return BLOCK_SIZE;
  }

  @Override
  public BlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docIn, posIn, payIn);
  }

  @Override
  public void decodeTerm(
      DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
      throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;
    final boolean fieldHasPositions =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean fieldHasOffsets =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
            >= 0;
    final boolean fieldHasPayloads = fieldInfo.hasPayloads();

    if (absolute) {
      termState.docStartFP = 0;
      termState.posStartFP = 0;
      termState.payStartFP = 0;
    }

    final long l = in.readVLong();
    if ((l & 0x01) == 0) {
      termState.docStartFP += l >>> 1;
      if (termState.docFreq == 1) {
        termState.singletonDocID = in.readVInt();
      } else {
        termState.singletonDocID = -1;
      }
    } else {
      assert absolute == false;
      assert termState.singletonDocID != -1;
      termState.singletonDocID += BitUtil.zigZagDecode(l >>> 1);
    }

    if (fieldHasPositions) {
      termState.posStartFP += in.readVLong();
      if (fieldHasOffsets || fieldHasPayloads) {
        termState.payStartFP += in.readVLong();
      }
      if (termState.totalTermFreq > BLOCK_SIZE) {
        termState.lastPosBlockOffset = in.readVLong();
      } else {
        termState.lastPosBlockOffset = -1;
      }
    }

    if (termState.docFreq > SKIP_TOTAL_SIZE) {
      in.skipBytes(in.readVInt());
      //termState.globalImpacts = readImpacts();
    } else {
      termState.globalImpacts = null;
    }
  }

  @Override
  public PostingsEnum postings(
      FieldInfo fieldInfo, BlockTermState termState, PostingsEnum reuse, int flags)
      throws IOException {

    boolean indexHasPositions =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;

    if (indexHasPositions == false
        || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false) {
      BlockDocsEnum docsEnum;
      if (reuse instanceof BlockDocsEnum) {
        docsEnum = (BlockDocsEnum) reuse;
        if (!docsEnum.canReuse(docIn, fieldInfo)) {
          docsEnum = new BlockDocsEnum(fieldInfo);
        }
      } else {
        docsEnum = new BlockDocsEnum(fieldInfo);
      }
      return docsEnum.reset((IntBlockTermState) termState, flags);
    } else {
      EverythingEnum everythingEnum;
      if (reuse instanceof EverythingEnum) {
        everythingEnum = (EverythingEnum) reuse;
        if (!everythingEnum.canReuse(docIn, fieldInfo)) {
          everythingEnum = new EverythingEnum(fieldInfo);
        }
      } else {
        everythingEnum = new EverythingEnum(fieldInfo);
      }
      return everythingEnum.reset((IntBlockTermState) termState, flags);
    }
  }

  @Override
  public ImpactsEnum impacts(FieldInfo fieldInfo, BlockTermState state, int flags)
      throws IOException {
    //if (state.docFreq <= BLOCK_SIZE) {
      // no skip data
      return new SlowImpactsEnum(postings(fieldInfo, state, null, flags));
    //}

    /*final boolean indexHasPositions =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean indexHasOffsets =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
            >= 0;
    final boolean indexHasPayloads = fieldInfo.hasPayloads();

    if (indexHasPositions == false
        || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false) {
      return new BlockImpactsDocsEnum(fieldInfo, (IntBlockTermState) state);
    }

    if (indexHasPositions
        && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)
        && (indexHasOffsets == false
            || PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS) == false)
        && (indexHasPayloads == false
            || PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS) == false)) {
      return new BlockImpactsPostingsEnum(fieldInfo, (IntBlockTermState) state);
    }

    return new BlockImpactsEverythingEnum(fieldInfo, (IntBlockTermState) state, flags);*/
  }

  final class BlockDocsEnum extends PostingsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil(forUtil);
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE];

    private int docBufferUpto;

    final IndexInput startDocIn;

    IndexInput docIn;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq; // number of docs in this posting list
    private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)
    private int blockUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long accum; // accumulator for doc deltas

    private int lastDocInBlock;
    private int nextSkipDoc;
    private long nextSkipOffset;
    private int nextSkipBlockUpto;

    private boolean needsFreq; // true if the caller actually needs frequencies
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

    public BlockDocsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene912PostingsReader.this.docIn;
      this.docIn = null;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos =
          fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets =
          fieldInfo
                  .getIndexOptions()
                  .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
              >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
      // We set the last element of docBuffer to NO_MORE_DOCS, it helps save conditionals in
      // advance()
      docBuffer[BLOCK_SIZE] = NO_MORE_DOCS;
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn
          && indexHasFreq
              == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0)
          && indexHasPos
              == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
                  >= 0)
          && indexHasPayloads == fieldInfo.hasPayloads();
    }

    public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      totalTermFreq = indexHasFreq ? termState.totalTermFreq : docFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1 || true) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        seekAndPrefetchPostings(docIn, termState);
      }

      doc = -1;
      this.needsFreq = PostingsEnum.featureRequested(flags, PostingsEnum.FREQS);
      if (indexHasFreq == false || needsFreq == false) {
        // Filling this buffer may not be cheap when doing primary key lookups, so we make sure to
        // not fill more than `docFreq` entries.
        Arrays.fill(freqBuffer, 0, Math.min(ForUtil.BLOCK_SIZE, docFreq), 1);
      }
      accum = -1;
      blockUpto = 0;
      lastDocInBlock = -1;
      nextSkipDoc = -1;
      nextSkipOffset = termState.docStartFP;
      nextSkipBlockUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      return this;
    }

    @Override
    public int freq() throws IOException {
      return (int) freqBuffer[docBufferUpto - 1];
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - blockUpto;
      assert left >= 0;

      if (left >= BLOCK_SIZE) {
        forDeltaUtil.decodeAndPrefixSum(docIn, accum, docBuffer);

        if (indexHasFreq) {
          if (needsFreq == false) {
            pforUtil.skip(docIn); // skip over freqBuffer if we don't need them at all
          } else {
            pforUtil.decode(docIn, freqBuffer);
          }
        }
        blockUpto += BLOCK_SIZE;
      } else if (docFreq == 1) {
        docBuffer[0] = singletonDocID;
        freqBuffer[0] = totalTermFreq;
        docBuffer[1] = NO_MORE_DOCS;
        blockUpto++;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, needsFreq);
        prefixSum(docBuffer, left, accum);
        docBuffer[left] = NO_MORE_DOCS;
        blockUpto += left;
      }
      accum = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target > lastDocInBlock) {

        if (target > nextSkipDoc) {

          while (true) {
            accum = nextSkipDoc;
            lastDocInBlock = nextSkipDoc;
            docIn.seek(nextSkipOffset);
            blockUpto = nextSkipBlockUpto;
            nextSkipBlockUpto += SKIP_TOTAL_SIZE;

            if (docFreq - blockUpto < SKIP_TOTAL_SIZE) {
              nextSkipDoc = DocIdSetIterator.NO_MORE_DOCS;
              break;
            }

            nextSkipDoc += docIn.readVInt();
            long impactLength = docIn.readVLong();
            long skipLength = docIn.readVLong();
            nextSkipOffset = docIn.getFilePointer() + impactLength + skipLength;

            if (nextSkipDoc >= target) {
              // skip impacts
              docIn.skipBytes(impactLength);
              break;
            }
          }

        }

        while (true) {
          accum = lastDocInBlock;
          if (docFreq - blockUpto >= BLOCK_SIZE) {
            lastDocInBlock += docIn.readVInt();

            if (target <= lastDocInBlock) {
              if (indexHasPos) {
                docIn.readVLong(); // block ttf
              }
              docIn.readVLong(); // block length
              docIn.skipBytes(docIn.readVLong()); // skip impacts
              break;
            }
            // skip block
            if (indexHasPos) {
              docIn.readVLong(); // block ttf
            }
            docIn.skipBytes(docIn.readVLong());
            blockUpto += BLOCK_SIZE;
          } else {
            lastDocInBlock = DocIdSetIterator.NO_MORE_DOCS;
            break;
          }
        }

        refillDocs();
      }

      // Now scan
      long doc;
      do {
        doc = docBuffer[docBufferUpto++];
      } while (doc < target);
      return this.doc = (int) doc;
    }

    @Override
    public long cost() {
      return docFreq;
    }
  }

  final class EverythingEnum extends PostingsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil(forUtil);
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE];
    private final long[] posDeltaBuffer = new long[BLOCK_SIZE];

    private final long[] payloadLengthBuffer;
    private final long[] offsetStartDeltaBuffer;
    private final long[] offsetLengthBuffer;

    private byte[] payloadBytes;
    private int payloadByteUpto;
    private int payloadLength;

    private int lastStartOffset;
    private int startOffset;
    private int endOffset;

    private int docBufferUpto;
    private int posBufferUpto;

    final IndexInput startDocIn;

    IndexInput docIn;
    final IndexInput posIn;
    final IndexInput payIn;
    final BytesRef payload;

    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq; // number of docs in this posting list
    private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)
    private int blockUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long accum; // accumulator for doc deltas
    private int freq; // freq we last read
    private int position; // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private long posPendingCount;

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    private long posPendingFP;

    // Lazy pay seek: if != -1 then we must seek to this FP
    // before reading payloads/offsets:
    private long payPendingFP;

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    private int lastDocInBlock;
    private int nextSkipDoc;
    private long nextSkipOffset;
    private int nextSkipBlockUpto;

    private boolean needsOffsets; // true if we actually need offsets
    private boolean needsPayloads; // true if we actually need payloads

    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

    public EverythingEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene912PostingsReader.this.docIn;
      this.docIn = null;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos =
          fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets =
          fieldInfo
                  .getIndexOptions()
                  .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
              >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();

      this.posIn = Lucene912PostingsReader.this.posIn.clone();
      if (indexHasOffsets || indexHasPayloads) {
        this.payIn = Lucene912PostingsReader.this.payIn.clone();
      } else {
        this.payIn = null;
      }
      if (indexHasOffsets) {
        offsetStartDeltaBuffer = new long[BLOCK_SIZE];
        offsetLengthBuffer = new long[BLOCK_SIZE];
      } else {
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        startOffset = -1;
        endOffset = -1;
      }

      if (indexHasPayloads) {
        payloadLengthBuffer = new long[BLOCK_SIZE];
        payloadBytes = new byte[128];
        payload = new BytesRef();
      } else {
        payloadLengthBuffer = null;
        payloadBytes = null;
        payload = null;
      }

      // We set the last element of docBuffer to NO_MORE_DOCS, it helps save conditionals in
      // advance()
      docBuffer[BLOCK_SIZE] = NO_MORE_DOCS;
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn
          && indexHasFreq
              == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0)
          && indexHasPos
              == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
                  >= 0)
          && indexHasPayloads == fieldInfo.hasPayloads();
    }

    public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1 || true) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        seekAndPrefetchPostings(docIn, termState);
      }
      posPendingFP = posTermStartFP;
      payPendingFP = payTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      this.needsOffsets = PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS);
      this.needsPayloads = PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS);

      doc = -1;
      accum = -1;
      blockUpto = 0;
      lastDocInBlock = -1;
      nextSkipDoc = -1;
      nextSkipOffset = termState.docStartFP;
      nextSkipBlockUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      return this;
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - blockUpto;
      assert left >= 0;

      if (left >= BLOCK_SIZE) {
        forDeltaUtil.decodeAndPrefixSum(docIn, accum, docBuffer);
        pforUtil.decode(docIn, freqBuffer);
        blockUpto += BLOCK_SIZE;
      } else if (docFreq == 1) {
        docBuffer[0] = singletonDocID;
        freqBuffer[0] = totalTermFreq;
        docBuffer[1] = NO_MORE_DOCS;
        blockUpto++;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, true);
        prefixSum(docBuffer, left, accum);
        docBuffer[left] = NO_MORE_DOCS;
        blockUpto += left;
      }
      accum = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target > lastDocInBlock) {

        if (target > nextSkipDoc) {

          while (true) {
            accum = nextSkipDoc;
            lastDocInBlock = nextSkipDoc;
            docIn.seek(nextSkipOffset);
            blockUpto = nextSkipBlockUpto;
            nextSkipBlockUpto += SKIP_TOTAL_SIZE;

            if (docFreq - blockUpto < SKIP_TOTAL_SIZE) {
              nextSkipDoc = DocIdSetIterator.NO_MORE_DOCS;
              break;
            }

            nextSkipDoc += docIn.readVInt();
            long impactLength = docIn.readVLong();
            long skipLength = docIn.readVLong();
            nextSkipOffset = docIn.getFilePointer() + impactLength + skipLength;

            if (nextSkipDoc >= target) {
              // skip impacts
              docIn.skipBytes(impactLength);
              break;
            }
          }

        }

        while (true) {
          accum = lastDocInBlock;
          if (docFreq - blockUpto >= BLOCK_SIZE) {
            lastDocInBlock += docIn.readVInt();

            if (target <= lastDocInBlock) {
              docIn.readVLong(); // block ttf
              docIn.readVLong(); // block length
              docIn.skipBytes(docIn.readVLong()); // impacts
              break;
            }
            // skip block
            posPendingCount += docIn.readVLong();
            docIn.skipBytes(docIn.readVLong());
            blockUpto += BLOCK_SIZE;
          } else {
            lastDocInBlock = DocIdSetIterator.NO_MORE_DOCS;
            break;
          }
        }

        refillDocs();
      }

      // Now scan
      long doc;
      do {
        doc = docBuffer[docBufferUpto++];
      } while (doc < target);

      freq = (int) freqBuffer[docBufferUpto - 1];
      posPendingCount += freq;
      return this.doc = (int) doc;
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      long toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        long end = posBufferUpto + toSkip;
        while (posBufferUpto < end) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
      } else {
        toSkip -= leftInBlock;
        while (toSkip >= BLOCK_SIZE) {
          assert posIn.getFilePointer() != lastPosBlockFP;
          pforUtil.skip(posIn);

          if (indexHasPayloads) {
            // Skip payloadLength block:
            pforUtil.skip(payIn);

            // Skip payloadBytes block:
            int numBytes = payIn.readVInt();
            payIn.seek(payIn.getFilePointer() + numBytes);
          }

          if (indexHasOffsets) {
            pforUtil.skip(payIn);
            pforUtil.skip(payIn);
          }
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        payloadByteUpto = 0;
        posBufferUpto = 0;
        while (posBufferUpto < toSkip) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
      }

      position = 0;
      lastStartOffset = 0;
    }

    private void refillPositions() throws IOException {
      if (posIn.getFilePointer() == lastPosBlockFP) {
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        int offsetLength = 0;
        payloadByteUpto = 0;
        for (int i = 0; i < count; i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            payloadLengthBuffer[i] = payloadLength;
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              if (payloadByteUpto + payloadLength > payloadBytes.length) {
                payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payloadLength);
              }
              posIn.readBytes(payloadBytes, payloadByteUpto, payloadLength);
              payloadByteUpto += payloadLength;
            }
          } else {
            posDeltaBuffer[i] = code;
          }

          if (indexHasOffsets) {
            int deltaCode = posIn.readVInt();
            if ((deltaCode & 1) != 0) {
              offsetLength = posIn.readVInt();
            }
            offsetStartDeltaBuffer[i] = deltaCode >>> 1;
            offsetLengthBuffer[i] = offsetLength;
          }
        }
        payloadByteUpto = 0;
      } else {
        pforUtil.decode(posIn, posDeltaBuffer);

        if (indexHasPayloads) {
          if (needsPayloads) {
            pforUtil.decode(payIn, payloadLengthBuffer);
            int numBytes = payIn.readVInt();

            if (numBytes > payloadBytes.length) {
              payloadBytes = ArrayUtil.growNoCopy(payloadBytes, numBytes);
            }
            payIn.readBytes(payloadBytes, 0, numBytes);
          } else {
            // this works, because when writing a vint block we always force the first length to be
            // written
            pforUtil.skip(payIn); // skip over lengths
            int numBytes = payIn.readVInt(); // read length of payloadBytes
            payIn.seek(payIn.getFilePointer() + numBytes); // skip over payloadBytes
          }
          payloadByteUpto = 0;
        }

        if (indexHasOffsets) {
          if (needsOffsets) {
            pforUtil.decode(payIn, offsetStartDeltaBuffer);
            pforUtil.decode(payIn, offsetLengthBuffer);
          } else {
            // this works, because when writing a vint block we always force the first length to be
            // written
            pforUtil.skip(payIn); // skip over starts
            pforUtil.skip(payIn); // skip over lengths
          }
        }
      }
    }

    @Override
    public int nextPosition() throws IOException {
      assert posPendingCount > 0;

      if (posPendingFP != -1) {
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        if (payPendingFP != -1 && payIn != null) {
          payIn.seek(payPendingFP);
          payPendingFP = -1;
        }

        // Force buffer refill:
        posBufferUpto = BLOCK_SIZE;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto];

      if (indexHasPayloads) {
        payloadLength = (int) payloadLengthBuffer[posBufferUpto];
        payload.bytes = payloadBytes;
        payload.offset = payloadByteUpto;
        payload.length = payloadLength;
        payloadByteUpto += payloadLength;
      }

      if (indexHasOffsets) {
        startOffset = lastStartOffset + (int) offsetStartDeltaBuffer[posBufferUpto];
        endOffset = startOffset + (int) offsetLengthBuffer[posBufferUpto];
        lastStartOffset = startOffset;
      }

      posBufferUpto++;
      posPendingCount--;
      return position;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }

    @Override
    public int endOffset() {
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (payloadLength == 0) {
        return null;
      } else {
        return payload;
      }
    }

    @Override
    public long cost() {
      return docFreq;
    }
  }

  private void seekAndPrefetchPostings(IndexInput docIn, IntBlockTermState state)
      throws IOException {
    if (docIn.getFilePointer() != state.docStartFP) {
      // Don't prefetch if the input is already positioned at the right offset, which suggests that
      // the caller is streaming the entire inverted index (e.g. for merging), let the read-ahead
      // logic do its work instead. Note that this heuristic doesn't work for terms that have skip
      // data, since skip data is stored after the last term, but handling all terms that have <128
      // docs is a good start already.
      docIn.seek(state.docStartFP);
      docIn.prefetch(state.docStartFP, 1);
    }
    // Note: we don't prefetch positions or offsets, which are less likely to be needed.
  }

  @Override
  public void checkIntegrity() throws IOException {
    if (docIn != null) {
      CodecUtil.checksumEntireFile(docIn);
    }
    if (posIn != null) {
      CodecUtil.checksumEntireFile(posIn);
    }
    if (payIn != null) {
      CodecUtil.checksumEntireFile(payIn);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(positions="
        + (posIn != null)
        + ",payloads="
        + (payIn != null)
        + ")";
  }
}
