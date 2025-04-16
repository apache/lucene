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
package org.apache.lucene.backward_codecs.lucene101;

import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.*;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.DOC_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.LEVEL1_MASK;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.META_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.PAY_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.POS_CODEC;
import static org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.TERMS_CODEC;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat.IntBlockTermState;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/** Writer for {@link Lucene101PostingsFormat}. */
public class Lucene101PostingsWriter extends PushPostingsWriterBase {

  static final IntBlockTermState EMPTY_STATE = new IntBlockTermState();

  private final int version;

  IndexOutput metaOut;
  IndexOutput docOut;
  IndexOutput posOut;
  IndexOutput payOut;

  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;

  final int[] docDeltaBuffer;
  final int[] freqBuffer;
  private int docBufferUpto;

  final int[] posDeltaBuffer;
  final int[] payloadLengthBuffer;
  final int[] offsetStartDeltaBuffer;
  final int[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int level0LastDocID;
  private long level0LastPosFP;
  private long level0LastPayFP;

  private int level1LastDocID;
  private long level1LastPosFP;
  private long level1LastPayFP;

  private int docID;
  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount;

  private final PForUtil pforUtil;
  private final ForDeltaUtil forDeltaUtil;

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator level0FreqNormAccumulator =
      new CompetitiveImpactAccumulator();
  private final CompetitiveImpactAccumulator level1CompetitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  private int maxNumImpactsAtLevel0;
  private int maxImpactNumBytesAtLevel0;
  private int maxNumImpactsAtLevel1;
  private int maxImpactNumBytesAtLevel1;

  /** Scratch output that we use to be able to prepend the encoded length, e.g. impacts. */
  private final ByteBuffersDataOutput scratchOutput = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Output for a single block. This is useful to be able to prepend skip data before each block,
   * which can only be computed once the block is encoded. The content is then typically copied to
   * {@link #level1Output}.
   */
  private final ByteBuffersDataOutput level0Output = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Output for groups of 32 blocks. This is useful to prepend skip data for these 32 blocks, which
   * can only be done once we have encoded these 32 blocks. The content is then typically copied to
   * {@link #docCount}.
   */
  private final ByteBuffersDataOutput level1Output = ByteBuffersDataOutput.newResettableInstance();

  /**
   * Reusable FixedBitSet, for dense blocks that are more efficiently stored by storing them as a
   * bit set than as packed deltas.
   */
  // Since we use a bit set when it's more storage-efficient, the bit set cannot have more than
  // BLOCK_SIZE*32 bits, which is the maximum possible storage requirement with FOR.
  private final FixedBitSet spareBitSet = new FixedBitSet(BLOCK_SIZE * Integer.SIZE);

  /** Sole public constructor. */
  public Lucene101PostingsWriter(SegmentWriteState state) throws IOException {
    this(state, Lucene101PostingsFormat.VERSION_CURRENT);
  }

  /** Constructor that takes a version. */
  Lucene101PostingsWriter(SegmentWriteState state, int version) throws IOException {
    this.version = version;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.META_EXTENSION);
    String docFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.DOC_EXTENSION);
    metaOut = state.directory.createOutput(metaFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      docOut = state.directory.createOutput(docFileName, state.context);
      CodecUtil.writeIndexHeader(
          metaOut, META_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
      forDeltaUtil = new ForDeltaUtil();
      pforUtil = new PForUtil();
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new int[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new int[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new int[BLOCK_SIZE];
          offsetLengthBuffer = new int[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene101PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
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

    docDeltaBuffer = new int[BLOCK_SIZE];
    freqBuffer = new int[BLOCK_SIZE];
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, version, state.segmentInfo.getId(), state.segmentSuffix);
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
      level1LastPosFP = level0LastPosFP = posStartFP;
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
        level1LastPayFP = level0LastPayFP = payStartFP;
      }
    }
    lastDocID = -1;
    level0LastDocID = -1;
    level1LastDocID = -1;
    this.norms = norms;
    if (writeFreqs) {
      level0FreqNormAccumulator.clear();
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

      level0FreqNormAccumulator.add(termDocFreq, norm);
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
  public void finishDoc() {
    docBufferUpto++;
    docCount++;

    lastDocID = docID;
  }

  /**
   * Special vints that are encoded on 2 bytes if they require 15 bits or less. VInt becomes
   * especially slow when the number of bytes is variable, so this special layout helps in the case
   * when the number likely requires 15 bits or less
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
          level0Output, docDeltaBuffer, freqBuffer, docBufferUpto, writeFreqs);
    } else {
      if (writeFreqs) {
        List<Impact> impacts = level0FreqNormAccumulator.getCompetitiveFreqNormPairs();
        if (impacts.size() > maxNumImpactsAtLevel0) {
          maxNumImpactsAtLevel0 = impacts.size();
        }
        writeImpacts(impacts, scratchOutput);
        assert level0Output.size() == 0;
        if (scratchOutput.size() > maxImpactNumBytesAtLevel0) {
          maxImpactNumBytesAtLevel0 = Math.toIntExact(scratchOutput.size());
        }
        level0Output.writeVLong(scratchOutput.size());
        scratchOutput.copyTo(level0Output);
        scratchOutput.reset();
        if (writePositions) {
          level0Output.writeVLong(posOut.getFilePointer() - level0LastPosFP);
          level0Output.writeByte((byte) posBufferUpto);
          level0LastPosFP = posOut.getFilePointer();

          if (writeOffsets || writePayloads) {
            level0Output.writeVLong(payOut.getFilePointer() - level0LastPayFP);
            level0Output.writeVInt(payloadByteUpto);
            level0LastPayFP = payOut.getFilePointer();
          }
        }
      }
      long numSkipBytes = level0Output.size();
      // Now we need to decide whether to encode block deltas as packed integers (FOR) or unary
      // codes (bit set). FOR makes #nextDoc() a bit faster while the bit set approach makes
      // #advance() usually faster and #intoBitSet() much faster. In the end, we make the decision
      // based on storage requirements, picking the bit set approach whenever it's more
      // storage-efficient than the next number of bits per value (which effectively slightly biases
      // towards the bit set approach).
      int bitsPerValue = forDeltaUtil.bitsRequired(docDeltaBuffer);
      int sum = Math.toIntExact(Arrays.stream(docDeltaBuffer).sum());
      int numBitSetLongs = FixedBitSet.bits2words(sum);
      int numBitsNextBitsPerValue = Math.min(Integer.SIZE, bitsPerValue + 1) * BLOCK_SIZE;
      if (sum == BLOCK_SIZE) {
        level0Output.writeByte((byte) 0);
      } else if (version < VERSION_DENSE_BLOCKS_AS_BITSETS || numBitsNextBitsPerValue <= sum) {
        level0Output.writeByte((byte) bitsPerValue);
        forDeltaUtil.encodeDeltas(bitsPerValue, docDeltaBuffer, level0Output);
      } else {
        // Storing doc deltas is more efficient using unary coding (ie. storing doc IDs as a bit
        // set)
        spareBitSet.clear(0, numBitSetLongs << 6);
        int s = -1;
        for (int i : docDeltaBuffer) {
          s += i;
          spareBitSet.set(s);
        }
        // We never use the bit set encoding when it requires more than Integer.SIZE=32 bits per
        // value. So the bit set cannot have more than BLOCK_SIZE * Integer.SIZE / Long.SIZE = 64
        // longs, which fits on a byte.
        assert numBitSetLongs <= BLOCK_SIZE / 2;
        level0Output.writeByte((byte) -numBitSetLongs);
        for (int i = 0; i < numBitSetLongs; ++i) {
          level0Output.writeLong(spareBitSet.getBits()[i]);
        }
      }

      if (writeFreqs) {
        pforUtil.encode(freqBuffer, level0Output);
      }

      // docID - lastBlockDocID is at least 128, so it can never fit a single byte with a vint
      // Even if we subtracted 128, only extremely dense blocks would be eligible to a single byte
      // so let's go with 2 bytes right away
      writeVInt15(scratchOutput, docID - level0LastDocID);
      writeVLong15(scratchOutput, level0Output.size());
      numSkipBytes += scratchOutput.size();
      level1Output.writeVLong(numSkipBytes);
      scratchOutput.copyTo(level1Output);
      scratchOutput.reset();
    }

    level0Output.copyTo(level1Output);
    level0Output.reset();
    level0LastDocID = docID;
    if (writeFreqs) {
      level1CompetitiveFreqNormAccumulator.addAll(level0FreqNormAccumulator);
      level0FreqNormAccumulator.clear();
    }

    if ((docCount & LEVEL1_MASK) == 0) { // true every 32 blocks (4,096 docs)
      writeLevel1SkipData();
      level1LastDocID = docID;
      level1CompetitiveFreqNormAccumulator.clear();
    } else if (finishTerm) {
      level1Output.copyTo(docOut);
      level1Output.reset();
      level1CompetitiveFreqNormAccumulator.clear();
    }
  }

  private void writeLevel1SkipData() throws IOException {
    docOut.writeVInt(docID - level1LastDocID);
    final long level1End;
    if (writeFreqs) {
      List<Impact> impacts = level1CompetitiveFreqNormAccumulator.getCompetitiveFreqNormPairs();
      if (impacts.size() > maxNumImpactsAtLevel1) {
        maxNumImpactsAtLevel1 = impacts.size();
      }
      writeImpacts(impacts, scratchOutput);
      long numImpactBytes = scratchOutput.size();
      if (numImpactBytes > maxImpactNumBytesAtLevel1) {
        maxImpactNumBytesAtLevel1 = Math.toIntExact(numImpactBytes);
      }
      if (writePositions) {
        scratchOutput.writeVLong(posOut.getFilePointer() - level1LastPosFP);
        scratchOutput.writeByte((byte) posBufferUpto);
        level1LastPosFP = posOut.getFilePointer();
        if (writeOffsets || writePayloads) {
          scratchOutput.writeVLong(payOut.getFilePointer() - level1LastPayFP);
          scratchOutput.writeVInt(payloadByteUpto);
          level1LastPayFP = payOut.getFilePointer();
        }
      }
      final long level1Len = 2 * Short.BYTES + scratchOutput.size() + level1Output.size();
      docOut.writeVLong(level1Len);
      level1End = docOut.getFilePointer() + level1Len;
      // There are at most 128 impacts, that require at most 2 bytes each
      assert numImpactBytes <= Short.MAX_VALUE;
      // Like impacts plus a few vlongs, still way under the max short value
      assert scratchOutput.size() + Short.BYTES <= Short.MAX_VALUE;
      docOut.writeShort((short) (scratchOutput.size() + Short.BYTES));
      docOut.writeShort((short) numImpactBytes);
      scratchOutput.copyTo(docOut);
      scratchOutput.reset();
    } else {
      docOut.writeVLong(level1Output.size());
      level1End = docOut.getFilePointer() + level1Output.size();
    }
    level1Output.copyTo(docOut);
    level1Output.reset();
    assert docOut.getFilePointer() == level1End : docOut.getFilePointer() + " " + level1End;
  }

  static void writeImpacts(Collection<Impact> impacts, DataOutput out) throws IOException {
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
      singletonDocID = docDeltaBuffer[0] - 1;
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
          final int posDelta = posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = payloadLengthBuffer[i];
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
            int delta = offsetStartDeltaBuffer[i];
            int length = offsetLengthBuffer[i];
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
        metaOut.writeInt(maxNumImpactsAtLevel0);
        metaOut.writeInt(maxImpactNumBytesAtLevel0);
        metaOut.writeInt(maxNumImpactsAtLevel1);
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
