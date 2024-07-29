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

import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.*;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class Lucene912PostingsWriter extends PushPostingsWriterBase {

  static final IntBlockTermState EMPTY_STATE = new IntBlockTermState();

  IndexOutput metaOut;
  IndexOutput docOut;
  IndexOutput posOut;
  IndexOutput payOut;

  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;

  final long[] docDeltaBuffer;
  final long[] freqBuffer;
  private int docBufferUpto;

  final long[] posDeltaBuffer;
  final long[] payloadLengthBuffer;
  final long[] offsetStartDeltaBuffer;
  final long[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;
  private long lastBlockPosFP;
  private long lastBlockPayFP;

  private int lastSkipDocID;
  private long lastSkipPosFP;
  private long lastSkipPayFP;

  private int docID;
  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount;

  private final PForUtil pforUtil;
  private final ForDeltaUtil forDeltaUtil;

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();
  private final CompetitiveImpactAccumulator skipCompetitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  private int maxImpactNumBytesAtLevel0;
  private int maxImpactNumBytesAtLevel1;

  /** Spare output that we use to be able to prepend the encoded length, e.g. impacts. */
  private final ByteBuffersDataOutput spareOutput = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Output for a single block. This is useful to be able to prepend skip data before each block,
   * which can only be computed once the block is encoded. The content is then typically copied to
   * {@link #skipOutput}.
   */
  private final ByteBuffersDataOutput blockOutput = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Output for groups of 32 blocks. This is useful to prepend skip data for these 32 blocks, which
   * can only be done once we have encoded these 32 blocks. The content is then typically copied to
   * {@link #docCount}.
   */
  private final ByteBuffersDataOutput skipOutput = ByteBuffersDataOutput.newResettableInstance();

  public Lucene912PostingsWriter(SegmentWriteState state) throws IOException {
    String metaFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.META_EXTENSION);
    String docFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.DOC_EXTENSION);
    metaOut = state.directory.createOutput(metaFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      docOut = state.directory.createOutput(docFileName, state.context);
      CodecUtil.writeIndexHeader(
          metaOut, META_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      final ForUtil forUtil = new ForUtil();
      forDeltaUtil = new ForDeltaUtil(forUtil);
      pforUtil = new PForUtil(forUtil);
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new long[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene912PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene912PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaOut, docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    lastState = EMPTY_STATE;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  @Override
  public void startTerm(NumericDocValues norms) {
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      lastSkipPosFP = lastBlockPosFP = posStartFP;
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
        lastSkipPayFP = lastBlockPayFP = payStartFP;
      }
    }
    lastDocID = -1;
    lastBlockDocID = -1;
    lastSkipDocID = -1;
    this.norms = norms;
    if (writeFreqs) {
      competitiveFreqNormAccumulator.clear();
    }
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    if (docBufferUpto == BLOCK_SIZE) {
      flushDocBlock(false);
      docBufferUpto = 0;
    }

    final int docDelta = docID - lastDocID;

    if (docID < 0 || docDelta <= 0) {
      throw new CorruptIndexException(
          "docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    this.docID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    if (writeFreqs) {
      long norm;
      if (fieldHasNorms) {
        boolean found = norms.advanceExact(docID);
        if (found == false) {
          // This can happen if indexing hits a problem after adding a doc to the
          // postings but before buffering the norm. Such documents are written
          // deleted and will go away on the first merge.
          norm = 1L;
        } else {
          norm = norms.longValue();
          assert norm != 0 : docID;
        }
      } else {
        norm = 1L;
      }

      competitiveFreqNormAccumulator.add(termDocFreq, norm);
    }
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException(
          "position="
              + position
              + " is too large (> IndexWriter.MAX_POSITION="
              + IndexWriter.MAX_POSITION
              + ")",
          docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    if (writePayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(
            payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {
      pforUtil.encode(posDeltaBuffer, posOut);

      if (writePayloads) {
        pforUtil.encode(payloadLengthBuffer, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    docBufferUpto++;
    docCount++;

    lastDocID = docID;
  }

  /**
   * Special vints that are encoded on 2 bytes if they require 15 bits or less.
   * VInt becomes especially slow when the number of bytes is variable, so this special layout helps in the case when the number likely requires 15 bits or less
   */
  static void writeVInt15(DataOutput out, int v) throws IOException {
    assert v >= 0;
    writeVLong15(out, v);
  }

  /**
   * @see #writeVInt15(DataOutput, int)
   */
  static void writeVLong15(DataOutput out, long v) throws IOException {
    assert v >= 0;
    if ((v & ~0x7FFFL) == 0) {
      out.writeShort((short) v);
    } else {
      out.writeShort((short) (0x8000 | (v & 0x7FFF)));
      out.writeVLong(v >> 15);
    }
  }

  private void flushDocBlock(boolean finishTerm) throws IOException {
    assert docBufferUpto != 0;

    if (docBufferUpto < BLOCK_SIZE) {
      assert finishTerm;
      PostingsUtil.writeVIntBlock(
          blockOutput, docDeltaBuffer, freqBuffer, docBufferUpto, writeFreqs);
    } else {
      if (writeFreqs) {
        writeImpacts(competitiveFreqNormAccumulator.getCompetitiveFreqNormPairs(), spareOutput);
        assert blockOutput.size() == 0;
        if (spareOutput.size() > maxImpactNumBytesAtLevel0) {
          maxImpactNumBytesAtLevel0 = Math.toIntExact(spareOutput.size());
        }
        blockOutput.writeVLong(spareOutput.size());
        spareOutput.copyTo(blockOutput);
        spareOutput.reset();
        if (writePositions) {
          blockOutput.writeVLong(posOut.getFilePointer() - lastBlockPosFP);
          blockOutput.writeByte((byte) posBufferUpto);
          lastBlockPosFP = posOut.getFilePointer();

          if (writeOffsets || writePayloads) {
            blockOutput.writeVLong(payOut.getFilePointer() - lastBlockPayFP);
            blockOutput.writeVInt(payloadByteUpto);
            lastBlockPayFP = payOut.getFilePointer();
          }
        }
      }
      long numSkipBytes = blockOutput.size();
      forDeltaUtil.encodeDeltas(docDeltaBuffer, blockOutput);
      if (writeFreqs) {
        pforUtil.encode(freqBuffer, blockOutput);
      }

      // docID - lastBlockDocID is at least 128, so it can never fit a single byte with a vint
      // Even if we subtracted 128, only extremely dense blocks would be eligible to a single byte so let's go with 2 bytes right away
      writeVInt15(spareOutput, docID - lastBlockDocID);
      writeVLong15(spareOutput, blockOutput.size());
      numSkipBytes += spareOutput.size();
      skipOutput.writeVLong(numSkipBytes);
      spareOutput.copyTo(skipOutput);
      spareOutput.reset();
    }

    blockOutput.copyTo(skipOutput);
    blockOutput.reset();
    lastBlockDocID = docID;
    if (writeFreqs) {
      skipCompetitiveFreqNormAccumulator.addAll(competitiveFreqNormAccumulator);
      competitiveFreqNormAccumulator.clear();
    }

    if ((docCount & SKIP_MASK) == 0) { // true every 32 blocks (4,096 docs)
      docOut.writeVInt(docID - lastSkipDocID);
      long numImpactBytes = spareOutput.size();
      final long level1End;
      if (writeFreqs) {
        writeImpacts(skipCompetitiveFreqNormAccumulator.getCompetitiveFreqNormPairs(), spareOutput);
        numImpactBytes = spareOutput.size();
        if (numImpactBytes > maxImpactNumBytesAtLevel1) {
          maxImpactNumBytesAtLevel1 = Math.toIntExact(numImpactBytes);
        }
        if (writePositions) {
          spareOutput.writeVLong(posOut.getFilePointer() - lastSkipPosFP);
          spareOutput.writeByte((byte) posBufferUpto);
          lastSkipPosFP = posOut.getFilePointer();
          if (writeOffsets || writePayloads) {
            spareOutput.writeVLong(payOut.getFilePointer() - lastSkipPayFP);
            spareOutput.writeVInt(payloadByteUpto);
            lastSkipPayFP = payOut.getFilePointer();
          }
        }
        final long level1Len = 2 * Short.BYTES + spareOutput.size() + skipOutput.size();
        docOut.writeVLong(level1Len);
        level1End = docOut.getFilePointer() + level1Len;
        // There are at most 128 impacts, that require at most 2 bytes each
        assert numImpactBytes <= Short.MAX_VALUE;
        // Like impacts plus a few vlongs, still way under the max short value
        assert spareOutput.size() + Short.BYTES <= Short.MAX_VALUE;
        docOut.writeShort((short) (spareOutput.size() + Short.BYTES));
        docOut.writeShort((short) numImpactBytes);
        spareOutput.copyTo(docOut);
        spareOutput.reset();
      } else {
        docOut.writeVLong(skipOutput.size());
        level1End = docOut.getFilePointer() + skipOutput.size();
      }
      skipOutput.copyTo(docOut);
      skipOutput.reset();
      assert docOut.getFilePointer() == level1End : docOut.getFilePointer() + " " + level1End;
      lastSkipDocID = docID;
      skipCompetitiveFreqNormAccumulator.clear();
    } else if (finishTerm) {
      skipOutput.copyTo(docOut);
      skipOutput.reset();
      skipCompetitiveFreqNormAccumulator.clear();
    }
  }

  private void writeImpacts(Collection<Impact> impacts, DataOutput out) throws IOException {
    Impact previous = new Impact(0, 0);
    for (Impact impact : impacts) {
      assert impact.freq > previous.freq;
      assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
      int freqDelta = impact.freq - previous.freq - 1;
      long normDelta = impact.norm - previous.norm - 1;
      if (normDelta == 0) {
        // most of time, norm only increases by 1, so we can fold everything in a single byte
        out.writeVInt(freqDelta << 1);
      } else {
        out.writeVInt((freqDelta << 1) | 1);
        out.writeZLong(normDelta);
      }
      previous = impact;
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it.
    final int singletonDocID;
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = (int) docDeltaBuffer[0] - 1;
    } else {
      singletonDocID = -1;
      flushDocBlock(true);
    }

    final long lastPosBlockOffset;

    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        assert posBufferUpto < BLOCK_SIZE;
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1; // force first payload length to be written
        int lastOffsetLength = -1; // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for (int i = 0; i < posBufferUpto; i++) {
          final int posDelta = (int) posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta << 1) | 1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta << 1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (writeOffsets) {
            int delta = (int) offsetStartDeltaBuffer[i];
            int length = (int) offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }

    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;

    state.lastPosBlockOffset = lastPosBlockOffset;
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = -1;
    docCount = 0;
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {
      lastState = EMPTY_STATE;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      if (metaOut != null) {
        metaOut.writeInt(maxImpactNumBytesAtLevel0);
        metaOut.writeInt(maxImpactNumBytesAtLevel1);
        metaOut.writeLong(docOut.getFilePointer());
        if (posOut != null) {
          metaOut.writeLong(posOut.getFilePointer());
          if (payOut != null) {
            metaOut.writeLong(payOut.getFilePointer());
          }
        }
        CodecUtil.writeFooter(metaOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(metaOut, docOut, posOut, payOut);
      }
      metaOut = docOut = posOut = payOut = null;
    }
  }
}
