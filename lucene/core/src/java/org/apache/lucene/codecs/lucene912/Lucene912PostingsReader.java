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
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.LEVEL1_NUM_DOCS;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.META_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.VERSION_START;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that reads docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * @lucene.experimental
 */
public final class Lucene912PostingsReader extends PostingsReaderBase {

  static final VectorizationProvider VECTORIZATION_PROVIDER = VectorizationProvider.getInstance();

  private final IndexInput docIn;
  private final IndexInput posIn;
  private final IndexInput payIn;

  private final int maxNumImpactsAtLevel0;
  private final int maxImpactNumBytesAtLevel0;
  private final int maxNumImpactsAtLevel1;
  private final int maxImpactNumBytesAtLevel1;

  private final int version;

  /** Sole constructor. */
  public Lucene912PostingsReader(SegmentReadState state) throws IOException {
    String metaName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.META_EXTENSION);
    final long expectedDocFileLength, expectedPosFileLength, expectedPayFileLength;
    ChecksumIndexInput metaIn = null;
    boolean success = false;
    try {
      metaIn = state.directory.openChecksumInput(metaName);
      version =
          CodecUtil.checkIndexHeader(
              metaIn,
              META_CODEC,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      maxNumImpactsAtLevel0 = metaIn.readInt();
      maxImpactNumBytesAtLevel0 = metaIn.readInt();
      maxNumImpactsAtLevel1 = metaIn.readInt();
      maxImpactNumBytesAtLevel1 = metaIn.readInt();
      expectedDocFileLength = metaIn.readLong();
      if (state.fieldInfos.hasProx()) {
        expectedPosFileLength = metaIn.readLong();
        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          expectedPayFileLength = metaIn.readLong();
        } else {
          expectedPayFileLength = -1;
        }
      } else {
        expectedPosFileLength = -1;
        expectedPayFileLength = -1;
      }
      CodecUtil.checkFooter(metaIn, null);
      success = true;
    } catch (Throwable t) {
      if (metaIn != null) {
        CodecUtil.checkFooter(metaIn, t);
        throw new AssertionError("unreachable");
      } else {
        throw t;
      }
    } finally {
      if (success) {
        metaIn.close();
      } else {
        IOUtils.closeWhileHandlingException(metaIn);
      }
    }

    success = false;
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
      CodecUtil.checkIndexHeader(
          docIn, DOC_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.retrieveChecksum(docIn, expectedDocFileLength);

      if (state.fieldInfos.hasProx()) {
        String proxName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.POS_EXTENSION);
        posIn = state.directory.openInput(proxName, state.context);
        CodecUtil.checkIndexHeader(
            posIn, POS_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
        CodecUtil.retrieveChecksum(posIn, expectedPosFileLength);

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene912PostingsFormat.PAY_EXTENSION);
          payIn = state.directory.openInput(payName, state.context);
          CodecUtil.checkIndexHeader(
              payIn, PAY_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
          CodecUtil.retrieveChecksum(payIn, expectedPayFileLength);
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
    final boolean indexHasFreqs =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    final boolean indexHasPositions =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;

    if (state.docFreq >= BLOCK_SIZE
        && indexHasFreqs
        && (indexHasPositions == false
            || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false)) {
      return new BlockImpactsDocsEnum(fieldInfo, (IntBlockTermState) state);
    }

    final boolean indexHasOffsets =
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
            >= 0;
    final boolean indexHasPayloads = fieldInfo.hasPayloads();

    if (state.docFreq >= BLOCK_SIZE
        && indexHasPositions
        && (indexHasOffsets == false
            || PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS) == false)
        && (indexHasPayloads == false
            || PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS) == false)) {
      return new BlockImpactsPostingsEnum(fieldInfo, (IntBlockTermState) state);
    }

    return new SlowImpactsEnum(postings(fieldInfo, state, null, flags));
  }

  final class BlockDocsEnum extends PostingsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil();
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE];

    private int docBufferUpto;

    final IndexInput startDocIn;

    IndexInput docIn;
    PostingDecodingUtil docInUtil;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsetsOrPayloads;

    private int docFreq; // number of docs in this posting list
    private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)
    private int docCountUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long prevDocID; // last doc ID of the previous block

    // level 0 skip data
    private int level0LastDocID;
    // level 1 skip data
    private int level1LastDocID;
    private long level1DocEndFP;
    private int level1DocCountUpto;

    private boolean needsFreq; // true if the caller actually needs frequencies
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1
    private long freqFP;

    public BlockDocsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene912PostingsReader.this.docIn;
      this.docIn = null;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos =
          fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsetsOrPayloads =
          fieldInfo
                      .getIndexOptions()
                      .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                  >= 0
              || fieldInfo.hasPayloads();
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
          && indexHasOffsetsOrPayloads
              == (fieldInfo
                          .getIndexOptions()
                          .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                      >= 0
                  || fieldInfo.hasPayloads());
    }

    public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      totalTermFreq = indexHasFreq ? termState.totalTermFreq : docFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
          docInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(docIn);
        }
        prefetchPostings(docIn, termState);
      }

      doc = -1;
      this.needsFreq = PostingsEnum.featureRequested(flags, PostingsEnum.FREQS);
      if (indexHasFreq == false || needsFreq == false) {
        // Filling this buffer may not be cheap when doing primary key lookups, so we make sure to
        // not fill more than `docFreq` entries.
        Arrays.fill(freqBuffer, 0, Math.min(ForUtil.BLOCK_SIZE, docFreq), 1);
      }
      prevDocID = -1;
      docCountUpto = 0;
      level0LastDocID = -1;
      if (docFreq < LEVEL1_NUM_DOCS) {
        level1LastDocID = NO_MORE_DOCS;
        if (docFreq > 1) {
          docIn.seek(termState.docStartFP);
        }
      } else {
        level1LastDocID = -1;
        level1DocEndFP = termState.docStartFP;
      }
      level1DocCountUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      freqFP = -1;
      return this;
    }

    @Override
    public int freq() throws IOException {
      if (freqFP != -1) {
        docIn.seek(freqFP);
        pforUtil.decode(docInUtil, freqBuffer);
        freqFP = -1;
      }

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

    private void refillFullBlock() throws IOException {
      assert docFreq - docCountUpto >= BLOCK_SIZE;

      forDeltaUtil.decodeAndPrefixSum(docInUtil, prevDocID, docBuffer);

      if (indexHasFreq) {
        if (needsFreq) {
          freqFP = docIn.getFilePointer();
        }
        pforUtil.skip(docIn);
      }
      docCountUpto += BLOCK_SIZE;
      prevDocID = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    private void refillRemainder() throws IOException {
      final int left = docFreq - docCountUpto;
      assert left >= 0;
      assert left < BLOCK_SIZE;

      if (docFreq == 1) {
        docBuffer[0] = singletonDocID;
        freqBuffer[0] = totalTermFreq;
        docBuffer[1] = NO_MORE_DOCS;
        docCountUpto++;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, needsFreq);
        prefixSum(docBuffer, left, prevDocID);
        docBuffer[left] = NO_MORE_DOCS;
        docCountUpto += left;
      }
      docBufferUpto = 0;
      freqFP = -1;
    }

    private void skipLevel1To(int target) throws IOException {
      while (true) {
        prevDocID = level1LastDocID;
        level0LastDocID = level1LastDocID;
        docIn.seek(level1DocEndFP);
        docCountUpto = level1DocCountUpto;
        level1DocCountUpto += LEVEL1_NUM_DOCS;

        if (docFreq - docCountUpto < LEVEL1_NUM_DOCS) {
          level1LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }

        level1LastDocID += docIn.readVInt();
        level1DocEndFP = docIn.readVLong() + docIn.getFilePointer();

        if (level1LastDocID >= target) {
          if (indexHasFreq) {
            // skip impacts and pos skip data
            docIn.skipBytes(docIn.readShort());
          }
          break;
        }
      }
    }

    private void skipLevel0To(int target) throws IOException {
      while (true) {
        prevDocID = level0LastDocID;
        if (docFreq - docCountUpto >= BLOCK_SIZE) {
          long skip0NumBytes = docIn.readVLong();
          // end offset of skip data (before the actual data starts)
          long skip0EndFP = docIn.getFilePointer() + skip0NumBytes;
          int docDelta = readVInt15(docIn);
          level0LastDocID += docDelta;

          if (target <= level0LastDocID) {
            docIn.seek(skip0EndFP);
            break;
          }

          // skip block
          docIn.skipBytes(readVLong15(docIn));
          docCountUpto += BLOCK_SIZE;
        } else {
          level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }
      }
    }

    private void moveToNextLevel0Block() throws IOException {
      if (doc == level1LastDocID) { // advance skip data on level 1
        skipLevel1To(doc + 1);
      }

      prevDocID = level0LastDocID;
      if (docFreq - docCountUpto >= BLOCK_SIZE) {
        docIn.skipBytes(docIn.readVLong());
        refillFullBlock();
        level0LastDocID = (int) docBuffer[BLOCK_SIZE - 1];
      } else {
        level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
        refillRemainder();
      }
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc == level0LastDocID) { // advance skip data on level 0
        moveToNextLevel0Block();
      }

      return this.doc = (int) docBuffer[docBufferUpto++];
    }

    @Override
    public int advance(int target) throws IOException {
      if (target > level0LastDocID) { // advance skip data on level 0

        if (target > level1LastDocID) { // advance skip data on level 1
          skipLevel1To(target);
        }

        skipLevel0To(target);

        if (docFreq - docCountUpto >= BLOCK_SIZE) {
          refillFullBlock();
        } else {
          refillRemainder();
        }
      }

      int next = findFirstGreater(docBuffer, target, docBufferUpto);
      this.doc = (int) docBuffer[next];
      docBufferUpto = next + 1;
      return doc;
    }

    @Override
    public long cost() {
      return docFreq;
    }
  }

  final class EverythingEnum extends PostingsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil();
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE + 1];
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
    PostingDecodingUtil docInUtil;
    final IndexInput posIn;
    final PostingDecodingUtil posInUtil;
    final IndexInput payIn;
    final PostingDecodingUtil payInUtil;
    final BytesRef payload;

    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;
    final boolean indexHasOffsetsOrPayloads;

    private int docFreq; // number of docs in this posting list
    private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)
    private int docCountUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long prevDocID; // last doc ID of the previous block
    private int freq; // freq we last read
    private int position; // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private long posPendingCount;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // level 0 skip data
    private int level0LastDocID;
    private long level0PosEndFP;
    private int level0BlockPosUpto;
    private long level0PayEndFP;
    private int level0BlockPayUpto;
    // level 1 skip data
    private int level1LastDocID;
    private long level1DocEndFP;
    private int level1DocCountUpto;
    private long level1PosEndFP;
    private int level1BlockPosUpto;
    private long level1PayEndFP;
    private int level1BlockPayUpto;

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
      indexHasOffsetsOrPayloads = indexHasOffsets || indexHasPayloads;

      this.posIn = Lucene912PostingsReader.this.posIn.clone();
      posInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(posIn);
      if (indexHasOffsetsOrPayloads) {
        this.payIn = Lucene912PostingsReader.this.payIn.clone();
        payInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(payIn);
      } else {
        this.payIn = null;
        payInUtil = null;
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
          && indexHasOffsets
              == (fieldInfo
                      .getIndexOptions()
                      .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                  >= 0)
          && indexHasPayloads == fieldInfo.hasPayloads();
    }

    public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
          docInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(docIn);
        }
        prefetchPostings(docIn, termState);
      }
      posIn.seek(posTermStartFP);
      if (indexHasOffsetsOrPayloads) {
        payIn.seek(payTermStartFP);
      }
      level1PosEndFP = posTermStartFP;
      level1PayEndFP = payTermStartFP;
      level0PosEndFP = posTermStartFP;
      level0PayEndFP = payTermStartFP;
      posPendingCount = 0;
      payloadByteUpto = 0;
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
      prevDocID = -1;
      docCountUpto = 0;
      level0LastDocID = -1;
      if (docFreq < LEVEL1_NUM_DOCS) {
        level1LastDocID = NO_MORE_DOCS;
        if (docFreq > 1) {
          docIn.seek(termState.docStartFP);
        }
      } else {
        level1LastDocID = -1;
        level1DocEndFP = termState.docStartFP;
      }
      level1DocCountUpto = 0;
      level1BlockPosUpto = 0;
      level1BlockPayUpto = 0;
      level0BlockPosUpto = 0;
      level0BlockPayUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      posBufferUpto = BLOCK_SIZE;
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
      final int left = docFreq - docCountUpto;
      assert left >= 0;

      if (left >= BLOCK_SIZE) {
        forDeltaUtil.decodeAndPrefixSum(docInUtil, prevDocID, docBuffer);
        pforUtil.decode(docInUtil, freqBuffer);
        docCountUpto += BLOCK_SIZE;
      } else if (docFreq == 1) {
        docBuffer[0] = singletonDocID;
        freqBuffer[0] = totalTermFreq;
        docBuffer[1] = NO_MORE_DOCS;
        docCountUpto++;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, true);
        prefixSum(docBuffer, left, prevDocID);
        docBuffer[left] = NO_MORE_DOCS;
        docCountUpto += left;
      }
      prevDocID = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    private void skipLevel1To(int target) throws IOException {
      while (true) {
        prevDocID = level1LastDocID;
        level0LastDocID = level1LastDocID;
        docIn.seek(level1DocEndFP);
        level0PosEndFP = level1PosEndFP;
        level0BlockPosUpto = level1BlockPosUpto;
        if (indexHasOffsetsOrPayloads) {
          level0PayEndFP = level1PayEndFP;
          level0BlockPayUpto = level1BlockPayUpto;
        }
        docCountUpto = level1DocCountUpto;
        level1DocCountUpto += LEVEL1_NUM_DOCS;

        if (docFreq - docCountUpto < LEVEL1_NUM_DOCS) {
          level1LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }

        level1LastDocID += docIn.readVInt();
        long delta = docIn.readVLong();
        level1DocEndFP = delta + docIn.getFilePointer();

        long skip1EndFP = docIn.readShort() + docIn.getFilePointer();
        docIn.skipBytes(docIn.readShort()); // impacts
        level1PosEndFP += docIn.readVLong();
        level1BlockPosUpto = docIn.readByte();
        if (indexHasOffsetsOrPayloads) {
          level1PayEndFP += docIn.readVLong();
          level1BlockPayUpto = docIn.readVInt();
        }
        assert docIn.getFilePointer() == skip1EndFP;

        if (level1LastDocID >= target) {
          break;
        }
      }
    }

    private void moveToNextLevel0Block() throws IOException {
      if (doc == level1LastDocID) { // advance level 1 skip data
        skipLevel1To(doc + 1);
      }

      // Now advance level 0 skip data
      prevDocID = level0LastDocID;

      assert docBufferUpto == BLOCK_SIZE;
      if (level0PosEndFP >= posIn.getFilePointer()) {
        posIn.seek(level0PosEndFP);
        posPendingCount = level0BlockPosUpto;
        if (indexHasOffsetsOrPayloads) {
          assert level0PayEndFP >= payIn.getFilePointer();
          payIn.seek(level0PayEndFP);
          payloadByteUpto = level0BlockPayUpto;
        }
        posBufferUpto = BLOCK_SIZE;
      }

      if (docFreq - docCountUpto >= BLOCK_SIZE) {
        docIn.readVLong(); // skip0 num bytes
        int docDelta = readVInt15(docIn);
        level0LastDocID += docDelta;
        readVLong15(docIn); // block length
        docIn.skipBytes(docIn.readVLong()); // impacts

        level0PosEndFP += docIn.readVLong();
        level0BlockPosUpto = docIn.readByte();
        if (indexHasOffsetsOrPayloads) {
          level0PayEndFP += docIn.readVLong();
          level0BlockPayUpto = docIn.readVInt();
        }
      } else {
        level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
      }

      refillDocs();
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc == level0LastDocID) { // advance level 0 skip data
        moveToNextLevel0Block();
      }

      this.doc = (int) docBuffer[docBufferUpto];
      this.freq = (int) freqBuffer[docBufferUpto];
      docBufferUpto++;
      posPendingCount += freq;
      position = 0;
      lastStartOffset = 0;
      return doc;
    }

    private void skipLevel0To(int target) throws IOException {
      while (true) {
        prevDocID = level0LastDocID;

        // If nextBlockPosFP is less than the current FP, it means that the block of positions for
        // the first docs of the next block are already decoded. In this case we just accumulate
        // frequencies into posPendingCount instead of seeking backwards and decoding the same pos
        // block again.
        if (level0PosEndFP >= posIn.getFilePointer()) {
          posIn.seek(level0PosEndFP);
          posPendingCount = level0BlockPosUpto;
          if (indexHasOffsetsOrPayloads) {
            assert level0PayEndFP >= payIn.getFilePointer();
            payIn.seek(level0PayEndFP);
            payloadByteUpto = level0BlockPayUpto;
          }
          posBufferUpto = BLOCK_SIZE;
        } else {
          for (int i = docBufferUpto; i < BLOCK_SIZE; ++i) {
            posPendingCount += freqBuffer[i];
          }
        }

        if (docFreq - docCountUpto >= BLOCK_SIZE) {
          docIn.readVLong(); // skip0 num bytes
          int docDelta = readVInt15(docIn);
          level0LastDocID += docDelta;

          long blockLength = readVLong15(docIn);
          long blockEndFP = docIn.getFilePointer() + blockLength;
          docIn.skipBytes(docIn.readVLong()); // impacts

          level0PosEndFP += docIn.readVLong();
          level0BlockPosUpto = docIn.readByte();
          if (indexHasOffsetsOrPayloads) {
            level0PayEndFP += docIn.readVLong();
            level0BlockPayUpto = docIn.readVInt();
          }

          if (target <= level0LastDocID) {
            break;
          }

          docIn.seek(blockEndFP);
          docCountUpto += BLOCK_SIZE;
        } else {
          level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (target > level0LastDocID) { // advance level 0 skip data

        if (target > level1LastDocID) { // advance level 1 skip data
          skipLevel1To(target);
        }

        skipLevel0To(target);

        refillDocs();
      }

      int next = findFirstGreater(docBuffer, target, docBufferUpto);
      for (int i = docBufferUpto; i <= next; ++i) {
        posPendingCount += freqBuffer[i];
      }
      this.freq = (int) freqBuffer[next];
      this.docBufferUpto = next + 1;
      position = 0;
      lastStartOffset = 0;

      return this.doc = (int) docBuffer[next];
    }

    private void skipPositions() throws IOException {
      // Skip positions now:
      long toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        int end = (int) (posBufferUpto + toSkip);
        if (indexHasPayloads) {
          for (int i = posBufferUpto; i < end; ++i) {
            payloadByteUpto += payloadLengthBuffer[i];
          }
        }
        posBufferUpto = end;
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
        final int toSkipInt = (int) toSkip;
        if (indexHasPayloads) {
          for (int i = 0; i < toSkipInt; ++i) {
            payloadByteUpto += payloadLengthBuffer[i];
          }
        }
        posBufferUpto = toSkipInt;
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
        pforUtil.decode(posInUtil, posDeltaBuffer);

        if (indexHasPayloads) {
          if (needsPayloads) {
            pforUtil.decode(payInUtil, payloadLengthBuffer);
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
            pforUtil.decode(payInUtil, offsetStartDeltaBuffer);
            pforUtil.decode(payInUtil, offsetLengthBuffer);
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

  final class BlockImpactsDocsEnum extends ImpactsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil();
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE];

    private int docBufferUpto;

    final IndexInput startDocIn;

    final IndexInput docIn;
    final PostingDecodingUtil docInUtil;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsetsOrPayloads;

    private int docFreq; // number of docs in this posting list
    private int docCountUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long prevDocID; // last doc ID of the previous block
    private long freqFP;

    // true if we shallow-advanced to a new block that we have not decoded yet
    private boolean needsRefilling;

    // level 0 skip data
    private int level0LastDocID;
    private long level0DocEndFP;
    private final BytesRef level0SerializedImpacts;
    private final ByteArrayDataInput level0SerializedImpactsIn = new ByteArrayDataInput();
    private final MutableImpactList level0Impacts;
    // level 1 skip data
    private int level1LastDocID;
    private long level1DocEndFP;
    private int level1DocCountUpto;
    private final BytesRef level1SerializedImpacts;
    private final ByteArrayDataInput level1SerializedImpactsIn = new ByteArrayDataInput();
    private final MutableImpactList level1Impacts;

    public BlockImpactsDocsEnum(FieldInfo fieldInfo, IntBlockTermState termState)
        throws IOException {
      this.startDocIn = Lucene912PostingsReader.this.docIn;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos =
          fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsetsOrPayloads =
          fieldInfo
                      .getIndexOptions()
                      .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                  >= 0
              || fieldInfo.hasPayloads();
      // We set the last element of docBuffer to NO_MORE_DOCS, it helps save conditionals in
      // advance()
      docBuffer[BLOCK_SIZE] = NO_MORE_DOCS;

      docFreq = termState.docFreq;
      if (docFreq > 1) {
        docIn = startDocIn.clone();
        docInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(docIn);
        prefetchPostings(docIn, termState);
      } else {
        docIn = null;
        docInUtil = null;
      }

      doc = -1;
      if (indexHasFreq == false) {
        // Filling this buffer may not be cheap when doing primary key lookups, so we make sure to
        // not fill more than `docFreq` entries.
        Arrays.fill(freqBuffer, 0, Math.min(ForUtil.BLOCK_SIZE, docFreq), 1);
      }
      prevDocID = -1;
      docCountUpto = 0;
      level0LastDocID = -1;
      if (docFreq < LEVEL1_NUM_DOCS) {
        level1LastDocID = NO_MORE_DOCS;
        if (docFreq > 1) {
          docIn.seek(termState.docStartFP);
        }
      } else {
        level1LastDocID = -1;
        level1DocEndFP = termState.docStartFP;
      }
      level1DocCountUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      freqFP = -1;
      level0SerializedImpacts = new BytesRef(maxImpactNumBytesAtLevel0);
      level1SerializedImpacts = new BytesRef(maxImpactNumBytesAtLevel1);
      level0Impacts = new MutableImpactList(maxNumImpactsAtLevel0);
      level1Impacts = new MutableImpactList(maxNumImpactsAtLevel1);
    }

    @Override
    public int freq() throws IOException {
      if (freqFP != -1) {
        docIn.seek(freqFP);
        pforUtil.decode(docInUtil, freqBuffer);
        freqFP = -1;
      }
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
      final int left = docFreq - docCountUpto;
      assert left >= 0;

      if (left >= BLOCK_SIZE) {
        forDeltaUtil.decodeAndPrefixSum(docInUtil, prevDocID, docBuffer);

        if (indexHasFreq) {
          freqFP = docIn.getFilePointer();
          pforUtil.skip(docIn);
        }
        docCountUpto += BLOCK_SIZE;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, true);
        prefixSum(docBuffer, left, prevDocID);
        docBuffer[left] = NO_MORE_DOCS;
        freqFP = -1;
        docCountUpto += left;
      }
      prevDocID = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    private void skipLevel1To(int target) throws IOException {
      while (true) {
        prevDocID = level1LastDocID;
        level0LastDocID = level1LastDocID;
        docIn.seek(level1DocEndFP);
        docCountUpto = level1DocCountUpto;
        level1DocCountUpto += LEVEL1_NUM_DOCS;

        if (docFreq - docCountUpto < LEVEL1_NUM_DOCS) {
          level1LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }

        level1LastDocID += docIn.readVInt();
        level1DocEndFP = docIn.readVLong() + docIn.getFilePointer();

        if (level1LastDocID >= target) {
          long skip1EndFP = docIn.readShort() + docIn.getFilePointer();
          int numImpactBytes = docIn.readShort();
          docIn.readBytes(level1SerializedImpacts.bytes, 0, numImpactBytes);
          level1SerializedImpacts.length = numImpactBytes;
          assert indexHasPos || docIn.getFilePointer() == skip1EndFP;
          docIn.seek(skip1EndFP);
          break;
        }
      }
    }

    private void skipLevel0To(int target) throws IOException {
      while (true) {
        prevDocID = level0LastDocID;
        if (docFreq - docCountUpto >= BLOCK_SIZE) {
          long skip0NumBytes = docIn.readVLong();
          // end offset of skip data (before the actual data starts)
          long skip0End = docIn.getFilePointer() + skip0NumBytes;
          int docDelta = readVInt15(docIn);
          long blockLength = readVLong15(docIn);

          level0LastDocID += docDelta;

          if (target <= level0LastDocID) {
            level0DocEndFP = docIn.getFilePointer() + blockLength;
            int numImpactBytes = docIn.readVInt();
            docIn.readBytes(level0SerializedImpacts.bytes, 0, numImpactBytes);
            level0SerializedImpacts.length = numImpactBytes;
            docIn.seek(skip0End);
            break;
          }

          // skip block
          docIn.skipBytes(blockLength);
          docCountUpto += BLOCK_SIZE;
        } else {
          level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }
      }
    }

    @Override
    public void advanceShallow(int target) throws IOException {
      if (target > level0LastDocID) { // advance skip data on level 0
        if (target > level1LastDocID) { // advance skip data on level 1
          skipLevel1To(target);
        } else if (needsRefilling) {
          docIn.seek(level0DocEndFP);
          docCountUpto += BLOCK_SIZE;
        }

        skipLevel0To(target);

        needsRefilling = true;
      }
    }

    private void moveToNextLevel0Block() throws IOException {
      if (doc == level1LastDocID) {
        skipLevel1To(doc + 1);
      } else if (needsRefilling) {
        docIn.seek(level0DocEndFP);
        docCountUpto += BLOCK_SIZE;
      }

      prevDocID = level0LastDocID;
      if (docFreq - docCountUpto >= BLOCK_SIZE) {
        final long skip0Len = docIn.readVLong(); // skip len
        final long skip0End = docIn.getFilePointer() + skip0Len;
        final int docDelta = readVInt15(docIn);
        final long blockLength = readVLong15(docIn);
        level0LastDocID += docDelta;
        level0DocEndFP = docIn.getFilePointer() + blockLength;
        final int numImpactBytes = docIn.readVInt();
        docIn.readBytes(level0SerializedImpacts.bytes, 0, numImpactBytes);
        level0SerializedImpacts.length = numImpactBytes;
        docIn.seek(skip0End);
      } else {
        level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
      }

      refillDocs();
      needsRefilling = false;
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc == level0LastDocID) {
        moveToNextLevel0Block();
      } else if (needsRefilling) {
        refillDocs();
        needsRefilling = false;
      }

      return this.doc = (int) docBuffer[docBufferUpto++];
    }

    @Override
    public int advance(int target) throws IOException {
      if (target > level0LastDocID || needsRefilling) {
        advanceShallow(target);
        refillDocs();
        needsRefilling = false;
      }

      int next = findFirstGreater(docBuffer, target, docBufferUpto);
      this.doc = (int) docBuffer[next];
      docBufferUpto = next + 1;
      return doc;
    }

    @Override
    public Impacts getImpacts() throws IOException {
      return new Impacts() {

        @Override
        public int numLevels() {
          int numLevels = 0;
          if (level0LastDocID != NO_MORE_DOCS) {
            numLevels++;
          }
          if (level1LastDocID != NO_MORE_DOCS) {
            numLevels++;
          }
          if (numLevels == 0) {
            numLevels++;
          }
          return numLevels;
        }

        @Override
        public int getDocIdUpTo(int level) {
          if (level0LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              return level0LastDocID;
            }
            level--;
          }

          if (level1LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              return level1LastDocID;
            }
            level--;
          }

          return NO_MORE_DOCS;
        }

        @Override
        public List<Impact> getImpacts(int level) {
          if (level0LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              level0SerializedImpactsIn.reset(
                  level0SerializedImpacts.bytes, 0, level0SerializedImpacts.length);
              readImpacts(level0SerializedImpactsIn, level0Impacts);
              return level0Impacts;
            }
            level--;
          }

          if (level1LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              level1SerializedImpactsIn.reset(
                  level1SerializedImpacts.bytes, 0, level1SerializedImpacts.length);
              readImpacts(level1SerializedImpactsIn, level1Impacts);
              return level1Impacts;
            }
            level--;
          }

          return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
        }
      };
    }

    @Override
    public long cost() {
      return docFreq;
    }
  }

  final class BlockImpactsPostingsEnum extends ImpactsEnum {

    final ForUtil forUtil = new ForUtil();
    final ForDeltaUtil forDeltaUtil = new ForDeltaUtil();
    final PForUtil pforUtil = new PForUtil(forUtil);

    private final long[] docBuffer = new long[BLOCK_SIZE + 1];
    private final long[] freqBuffer = new long[BLOCK_SIZE];
    private final long[] posDeltaBuffer = new long[BLOCK_SIZE];

    private int docBufferUpto;
    private int posBufferUpto;

    final IndexInput startDocIn;

    final IndexInput docIn;
    final PostingDecodingUtil docInUtil;
    final IndexInput posIn;
    final PostingDecodingUtil posInUtil;

    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;
    final boolean indexHasOffsetsOrPayloads;

    private int docFreq; // number of docs in this posting list
    private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)
    private int docCountUpto; // number of docs in or before the current block
    private int doc; // doc we last read
    private long prevDocID; // last doc ID of the previous block
    private int freq; // freq we last read
    private int position; // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private long posPendingCount;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // true if we shallow-advanced to a new block that we have not decoded yet
    private boolean needsRefilling;

    // level 0 skip data
    private int level0LastDocID;
    private long level0DocEndFP;
    private long level0PosEndFP;
    private int level0BlockPosUpto;
    private final BytesRefBuilder level0SerializedImpacts = new BytesRefBuilder();
    private final ByteArrayDataInput level0SerializedImpactsIn = new ByteArrayDataInput();
    private final MutableImpactList level0Impacts;
    // level 1 skip data
    private int level1LastDocID;
    private long level1DocEndFP;
    private int level1DocCountUpto;
    private long level1PosEndFP;
    private int level1BlockPosUpto;
    private final BytesRefBuilder level1SerializedImpacts = new BytesRefBuilder();
    private final ByteArrayDataInput level1SerializedImpactsIn = new ByteArrayDataInput();
    private final MutableImpactList level1Impacts;

    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

    public BlockImpactsPostingsEnum(FieldInfo fieldInfo, IntBlockTermState termState)
        throws IOException {
      this.startDocIn = Lucene912PostingsReader.this.docIn;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos =
          fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets =
          fieldInfo
                  .getIndexOptions()
                  .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
              >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
      indexHasOffsetsOrPayloads = indexHasOffsets || indexHasPayloads;

      this.posIn = Lucene912PostingsReader.this.posIn.clone();
      posInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(posIn);

      // We set the last element of docBuffer to NO_MORE_DOCS, it helps save conditionals in
      // advance()
      docBuffer[BLOCK_SIZE] = NO_MORE_DOCS;

      docFreq = termState.docFreq;
      posTermStartFP = termState.posStartFP;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        docIn = startDocIn.clone();
        docInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(docIn);
        prefetchPostings(docIn, termState);
      } else {
        docIn = null;
        docInUtil = null;
      }
      posIn.seek(posTermStartFP);
      level1PosEndFP = posTermStartFP;
      level0PosEndFP = posTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      doc = -1;
      prevDocID = -1;
      docCountUpto = 0;
      level0LastDocID = -1;
      if (docFreq < LEVEL1_NUM_DOCS) {
        level1LastDocID = NO_MORE_DOCS;
        if (docFreq > 1) {
          docIn.seek(termState.docStartFP);
        }
      } else {
        level1LastDocID = -1;
        level1DocEndFP = termState.docStartFP;
      }
      level1DocCountUpto = 0;
      level1BlockPosUpto = 0;
      docBufferUpto = BLOCK_SIZE;
      posBufferUpto = BLOCK_SIZE;
      level0SerializedImpacts.growNoCopy(maxImpactNumBytesAtLevel0);
      level1SerializedImpacts.growNoCopy(maxImpactNumBytesAtLevel1);
      level0Impacts = new MutableImpactList(maxNumImpactsAtLevel0);
      level1Impacts = new MutableImpactList(maxNumImpactsAtLevel1);
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
      final int left = docFreq - docCountUpto;
      assert left >= 0;

      if (left >= BLOCK_SIZE) {
        forDeltaUtil.decodeAndPrefixSum(docInUtil, prevDocID, docBuffer);
        pforUtil.decode(docInUtil, freqBuffer);
        docCountUpto += BLOCK_SIZE;
      } else if (docFreq == 1) {
        docBuffer[0] = singletonDocID;
        freqBuffer[0] = totalTermFreq;
        docBuffer[1] = NO_MORE_DOCS;
        docCountUpto++;
      } else {
        // Read vInts:
        PostingsUtil.readVIntBlock(docIn, docBuffer, freqBuffer, left, indexHasFreq, true);
        prefixSum(docBuffer, left, prevDocID);
        docBuffer[left] = NO_MORE_DOCS;
        docCountUpto += left;
      }
      prevDocID = docBuffer[BLOCK_SIZE - 1];
      docBufferUpto = 0;
      assert docBuffer[BLOCK_SIZE] == NO_MORE_DOCS;
    }

    private void skipLevel1To(int target) throws IOException {
      while (true) {
        prevDocID = level1LastDocID;
        level0LastDocID = level1LastDocID;
        docIn.seek(level1DocEndFP);
        level0PosEndFP = level1PosEndFP;
        level0BlockPosUpto = level1BlockPosUpto;
        docCountUpto = level1DocCountUpto;
        level1DocCountUpto += LEVEL1_NUM_DOCS;

        if (docFreq - docCountUpto < LEVEL1_NUM_DOCS) {
          level1LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }

        level1LastDocID += docIn.readVInt();
        level1DocEndFP = docIn.readVLong() + docIn.getFilePointer();

        long skip1EndFP = docIn.readShort() + docIn.getFilePointer();
        int numImpactBytes = docIn.readShort();
        if (level1LastDocID >= target) {
          docIn.readBytes(level1SerializedImpacts.bytes(), 0, numImpactBytes);
          level1SerializedImpacts.setLength(numImpactBytes);
        } else {
          docIn.skipBytes(numImpactBytes);
        }
        level1PosEndFP += docIn.readVLong();
        level1BlockPosUpto = docIn.readByte();
        assert indexHasOffsetsOrPayloads || docIn.getFilePointer() == skip1EndFP;

        if (level1LastDocID >= target) {
          docIn.seek(skip1EndFP);
          break;
        }
      }
    }

    private void skipLevel0To(int target) throws IOException {
      while (true) {
        prevDocID = level0LastDocID;

        // If nextBlockPosFP is less than the current FP, it means that the block of positions for
        // the first docs of the next block are already decoded. In this case we just accumulate
        // frequencies into posPendingCount instead of seeking backwards and decoding the same pos
        // block again.
        if (level0PosEndFP >= posIn.getFilePointer()) {
          posIn.seek(level0PosEndFP);
          posPendingCount = level0BlockPosUpto;
          posBufferUpto = BLOCK_SIZE;
        } else {
          for (int i = docBufferUpto; i < BLOCK_SIZE; ++i) {
            posPendingCount += freqBuffer[i];
          }
        }

        if (docFreq - docCountUpto >= BLOCK_SIZE) {
          docIn.readVLong(); // skip0 num bytes
          int docDelta = readVInt15(docIn);
          long blockLength = readVLong15(docIn);
          level0DocEndFP = docIn.getFilePointer() + blockLength;

          level0LastDocID += docDelta;

          if (target <= level0LastDocID) {
            int numImpactBytes = docIn.readVInt();
            docIn.readBytes(level0SerializedImpacts.bytes(), 0, numImpactBytes);
            level0SerializedImpacts.setLength(numImpactBytes);
            level0PosEndFP += docIn.readVLong();
            level0BlockPosUpto = docIn.readByte();
            if (indexHasOffsetsOrPayloads) {
              docIn.readVLong(); // pay fp delta
              docIn.readVInt(); // pay upto
            }
            break;
          }
          // skip block
          docIn.skipBytes(docIn.readVLong()); // impacts
          level0PosEndFP += docIn.readVLong();
          level0BlockPosUpto = docIn.readVInt();
          docIn.seek(level0DocEndFP);
          docCountUpto += BLOCK_SIZE;
        } else {
          level0LastDocID = DocIdSetIterator.NO_MORE_DOCS;
          break;
        }
      }
    }

    @Override
    public void advanceShallow(int target) throws IOException {
      if (target > level0LastDocID) { // advance level 0 skip data

        if (target > level1LastDocID) { // advance skip data on level 1
          skipLevel1To(target);
        } else if (needsRefilling) {
          docIn.seek(level0DocEndFP);
          docCountUpto += BLOCK_SIZE;
        }

        skipLevel0To(target);

        needsRefilling = true;
      }
    }

    @Override
    public Impacts getImpacts() throws IOException {
      return new Impacts() {

        @Override
        public int numLevels() {
          int numLevels = 0;
          if (level0LastDocID != NO_MORE_DOCS) {
            numLevels++;
          }
          if (level1LastDocID != NO_MORE_DOCS) {
            numLevels++;
          }
          if (numLevels == 0) {
            numLevels++;
          }
          return numLevels;
        }

        @Override
        public int getDocIdUpTo(int level) {
          if (level0LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              return level0LastDocID;
            }
            level--;
          }

          if (level1LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              return level1LastDocID;
            }
            level--;
          }

          return NO_MORE_DOCS;
        }

        @Override
        public List<Impact> getImpacts(int level) {
          if (level0LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              level0SerializedImpactsIn.reset(
                  level0SerializedImpacts.bytes(), 0, level0SerializedImpacts.length());
              readImpacts(level0SerializedImpactsIn, level0Impacts);
              return level0Impacts;
            }
            level--;
          }

          if (level1LastDocID != NO_MORE_DOCS) {
            if (level == 0) {
              level1SerializedImpactsIn.reset(
                  level1SerializedImpacts.bytes(), 0, level1SerializedImpacts.length());
              readImpacts(level1SerializedImpactsIn, level1Impacts);
              return level1Impacts;
            }
            level--;
          }

          return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
        }
      };
    }

    @Override
    public int nextDoc() throws IOException {
      advanceShallow(doc + 1);
      if (needsRefilling) {
        refillDocs();
        needsRefilling = false;
      }

      doc = (int) docBuffer[docBufferUpto];
      freq = (int) freqBuffer[docBufferUpto];
      posPendingCount += freq;
      docBufferUpto++;
      position = 0;
      return this.doc;
    }

    @Override
    public int advance(int target) throws IOException {
      advanceShallow(target);
      if (needsRefilling) {
        refillDocs();
        needsRefilling = false;
      }

      int next = findFirstGreater(docBuffer, target, docBufferUpto);
      for (int i = docBufferUpto; i <= next; ++i) {
        posPendingCount += freqBuffer[i];
      }
      freq = (int) freqBuffer[next];
      docBufferUpto = next + 1;
      position = 0;
      return this.doc = (int) docBuffer[next];
    }

    private void skipPositions() throws IOException {
      // Skip positions now:
      long toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        posBufferUpto += toSkip;
      } else {
        toSkip -= leftInBlock;
        while (toSkip >= BLOCK_SIZE) {
          assert posIn.getFilePointer() != lastPosBlockFP;
          pforUtil.skip(posIn);
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        posBufferUpto = (int) toSkip;
      }

      position = 0;
    }

    private void refillPositions() throws IOException {
      if (posIn.getFilePointer() == lastPosBlockFP) {
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        for (int i = 0; i < count; i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              posIn.skipBytes(payloadLength);
            }
          } else {
            posDeltaBuffer[i] = code;
          }

          if (indexHasOffsets) {
            int deltaCode = posIn.readVInt();
            if ((deltaCode & 1) != 0) {
              posIn.readVInt(); // offset length
            }
          }
        }
      } else {
        pforUtil.decode(posInUtil, posDeltaBuffer);
      }
    }

    @Override
    public int nextPosition() throws IOException {
      assert posPendingCount > 0;

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto];

      posBufferUpto++;
      posPendingCount--;
      return position;
    }

    @Override
    public int startOffset() {
      return -1;
    }

    @Override
    public int endOffset() {
      return -1;
    }

    @Override
    public BytesRef getPayload() {
      return null;
    }

    @Override
    public long cost() {
      return docFreq;
    }
  }

  /**
   * @see Lucene912PostingsWriter#writeVInt15(org.apache.lucene.store.DataOutput, int)
   */
  static int readVInt15(DataInput in) throws IOException {
    short s = in.readShort();
    if (s >= 0) {
      return s;
    } else {
      return (s & 0x7FFF) | (in.readVInt() << 15);
    }
  }

  /**
   * @see Lucene912PostingsWriter#writeVLong15(org.apache.lucene.store.DataOutput, long)
   */
  static long readVLong15(DataInput in) throws IOException {
    short s = in.readShort();
    if (s >= 0) {
      return s;
    } else {
      return (s & 0x7FFFL) | (in.readVLong() << 15);
    }
  }

  private void prefetchPostings(IndexInput docIn, IntBlockTermState state) throws IOException {
    assert state.docFreq > 1; // Singletons are inlined in the terms dict, nothing to prefetch
    if (docIn.getFilePointer() != state.docStartFP) {
      // Don't prefetch if the input is already positioned at the right offset, which suggests that
      // the caller is streaming the entire inverted index (e.g. for merging), let the read-ahead
      // logic do its work instead. Note that this heuristic doesn't work for terms that have skip
      // data, since skip data is stored after the last term, but handling all terms that have <128
      // docs is a good start already.
      docIn.prefetch(state.docStartFP, 1);
    }
    // Note: we don't prefetch positions or offsets, which are less likely to be needed.
  }

  static class MutableImpactList extends AbstractList<Impact> implements RandomAccess {
    int length;
    final Impact[] impacts;

    MutableImpactList(int capacity) {
      impacts = new Impact[capacity];
      for (int i = 0; i < capacity; ++i) {
        impacts[i] = new Impact(Integer.MAX_VALUE, 1L);
      }
    }

    @Override
    public Impact get(int index) {
      return impacts[index];
    }

    @Override
    public int size() {
      return length;
    }
  }

  static MutableImpactList readImpacts(ByteArrayDataInput in, MutableImpactList reuse) {
    int freq = 0;
    long norm = 0;
    int length = 0;
    while (in.getPosition() < in.length()) {
      int freqDelta = in.readVInt();
      if ((freqDelta & 0x01) != 0) {
        freq += 1 + (freqDelta >>> 1);
        try {
          norm += 1 + in.readZLong();
        } catch (IOException e) {
          throw new RuntimeException(e); // cannot happen on a BADI
        }
      } else {
        freq += 1 + (freqDelta >>> 1);
        norm++;
      }
      Impact impact = reuse.impacts[length];
      impact.freq = freq;
      impact.norm = norm;
      length++;
    }
    reuse.length = length;
    return reuse;
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
