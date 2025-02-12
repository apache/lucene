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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;

/** Public for jmh benchmark. */
public final class DocIdsWriter {

  private static final byte CONTINUOUS_IDS = (byte) -2;
  private static final byte BITSET_IDS = (byte) -1;
  private static final byte DELTA_BPV_16 = (byte) 16;
  private static final byte BPV_24 = (byte) 24;
  private static final byte BPV_32 = (byte) 32;
  // These signs are legacy, should no longer be used in the writing side.
  private static final byte LEGACY_DELTA_VINT = (byte) 0;

  private static final int[] BATCHES = new int[] {512, 128};

  private final int[] scratch;

  private final LongsRef scratchLongs = new LongsRef();

  /**
   * IntsRef to be used to iterate over the scratch buffer. A single instance is reused to avoid
   * re-allocating the object. The ints and length fields need to be reset each use.
   *
   * <p>The main reason for existing is to be able to call the {@link
   * IntersectVisitor#visit(IntsRef)} method rather than the {@link IntersectVisitor#visit(int)}
   * method. This seems to make a difference in performance, probably due to fewer virtual calls
   * then happening (once per read call rather than once per doc).
   */
  private final IntsRef scratchIntsRef = new IntsRef();

  private final int version;

  {
    // This is here to not rely on the default constructor of IntsRef to set offset to 0
    scratchIntsRef.offset = 0;
  }

  public DocIdsWriter(int maxPointsInLeaf, int version) {
    this.scratch = new int[maxPointsInLeaf];
    this.version = version;
  }

  public void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean strictlySorted = true;
    int min = docIds[0];
    int max = docIds[0];
    for (int i = 1; i < count; ++i) {
      int last = docIds[start + i - 1];
      int current = docIds[start + i];
      if (last >= current) {
        strictlySorted = false;
      }
      min = Math.min(min, current);
      max = Math.max(max, current);
    }

    int min2max = max - min + 1;
    if (strictlySorted) {
      if (min2max == count) {
        // continuous ids, typically happens when segment is sorted
        out.writeByte(CONTINUOUS_IDS);
        out.writeVInt(docIds[start]);
        return;
      } else if (min2max <= (count << 4)) {
        assert min2max > count : "min2max: " + min2max + ", count: " + count;
        // Only trigger bitset optimization when max - min + 1 <= 16 * count in order to avoid
        // expanding too much storage.
        // A field with lower cardinality will have higher probability to trigger this optimization.
        out.writeByte(BITSET_IDS);
        writeIdsAsBitSet(docIds, start, count, out);
        return;
      }
    }

    if (min2max <= 0xFFFF) {
      out.writeByte(DELTA_BPV_16);
      for (int i = 0; i < count; i++) {
        scratch[i] = docIds[start + i] - min;
      }
      out.writeVInt(min);
      if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
        writeInts16(0, scratch, count, out);
      } else {
        int k = 0;
        for (int batchSize : BATCHES) {
          for (int bound = count - batchSize + 1; k < bound; k += batchSize) {
            writeInts16(k, scratch, batchSize, out);
          }
        }
        for (; k < count; k++) {
          out.writeShort((short) scratch[k]);
        }
      }
    } else {
      if (max <= 0xFFFFFF) {
        out.writeByte(BPV_24);
        if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
          writeScalarInts24(docIds, start, count, out);
        } else {
          int k = 0;
          for (int batchSize : BATCHES) {
            for (int bound = count - batchSize + 1; k < bound; k += batchSize) {
              final int quarterLen = batchSize >>> 2;
              final int quarterLen3 = quarterLen * 3;
              for (int i = k; i < k + quarterLen3; i++) {
                scratch[i] = docIds[i + start] << 8;
              }
              for (int i = k; i < k + quarterLen; i++) {
                final int longIdx = i + quarterLen3 + start;
                scratch[i] |= docIds[longIdx] >>> 16;
                scratch[i + quarterLen] |= (docIds[longIdx] >>> 8) & 0xFF;
                scratch[i + quarterLen * 2] |= docIds[longIdx] & 0xFF;
              }
              for (int i = k; i < k + quarterLen3; i++) {
                out.writeInt(scratch[i]);
              }
            }
          }
          writeScalarInts24(docIds, start + k, count - k, out);
        }
      } else {
        out.writeByte(BPV_32);
        for (int i = 0; i < count; i++) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }

  private static void writeInts16(int k, int[] scratch, int count, DataOutput out)
      throws IOException {
    final int halfLen = count >> 1;
    for (int i = k; i < k + halfLen; i++) {
      scratch[i] = scratch[halfLen + i] | (scratch[i] << 16);
    }
    for (int i = k; i < k + halfLen; i++) {
      out.writeInt(scratch[i]);
    }
    if ((count & 1) == 1) {
      out.writeShort((short) scratch[k + count - 1]);
    }
  }

  private static void writeScalarInts24(int[] docIds, int start, int count, DataOutput out)
      throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      int doc1 = docIds[start + i];
      int doc2 = docIds[start + i + 1];
      int doc3 = docIds[start + i + 2];
      int doc4 = docIds[start + i + 3];
      int doc5 = docIds[start + i + 4];
      int doc6 = docIds[start + i + 5];
      int doc7 = docIds[start + i + 6];
      int doc8 = docIds[start + i + 7];
      long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
      long l2 =
          (doc3 & 0xffL) << 56
              | (doc4 & 0xffffffL) << 32
              | (doc5 & 0xffffffL) << 8
              | ((doc6 >> 16) & 0xffL);
      long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
      out.writeLong(l1);
      out.writeLong(l2);
      out.writeLong(l3);
    }
    for (; i < count; ++i) {
      out.writeShort((short) (docIds[start + i] >>> 8));
      out.writeByte((byte) docIds[start + i]);
    }
  }

  private static void writeIdsAsBitSet(int[] docIds, int start, int count, DataOutput out)
      throws IOException {
    int min = docIds[start];
    int max = docIds[start + count - 1];

    final int offsetWords = min >> 6;
    final int offsetBits = offsetWords << 6;
    final int totalWordCount = FixedBitSet.bits2words(max - offsetBits + 1);
    long currentWord = 0;
    int currentWordIndex = 0;

    out.writeVInt(offsetWords);
    out.writeVInt(totalWordCount);
    // build bit set streaming
    for (int i = 0; i < count; i++) {
      final int index = docIds[start + i] - offsetBits;
      final int nextWordIndex = index >> 6;
      assert currentWordIndex <= nextWordIndex;
      if (currentWordIndex < nextWordIndex) {
        out.writeLong(currentWord);
        currentWord = 0L;
        currentWordIndex++;
        while (currentWordIndex < nextWordIndex) {
          currentWordIndex++;
          out.writeLong(0L);
        }
      }
      currentWord |= 1L << index;
    }
    out.writeLong(currentWord);
    assert currentWordIndex + 1 == totalWordCount;
  }

  /** Read {@code count} integers into {@code docIDs}. */
  public void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case CONTINUOUS_IDS:
        readContinuousIds(in, count, docIDs);
        break;
      case BITSET_IDS:
        readBitSet(in, count, docIDs);
        break;
      case DELTA_BPV_16:
        if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
          readDelta16Legacy(in, count, docIDs);
        } else {
          readDelta16(in, count, docIDs);
        }
        break;
      case BPV_24:
        if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
          readScalarInts24(in, count, docIDs, 0);
        } else {
          readInts24(in, count, docIDs);
        }
        break;
      case BPV_32:
        readInts32(in, count, docIDs);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
    int offsetWords = in.readVInt();
    int longLen = in.readVInt();
    scratchLongs.longs = ArrayUtil.growNoCopy(scratchLongs.longs, longLen);
    in.readLongs(scratchLongs.longs, 0, longLen);
    // make ghost bits clear for FixedBitSet.
    if (longLen < scratchLongs.length) {
      Arrays.fill(scratchLongs.longs, longLen, scratchLongs.longs.length, 0);
    }
    scratchLongs.length = longLen;
    FixedBitSet bitSet = new FixedBitSet(scratchLongs.longs, longLen << 6);
    return new DocBaseBitSetIterator(bitSet, count, offsetWords << 6);
  }

  private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
    int start = in.readVInt();
    for (int i = 0; i < count; i++) {
      docIDs[i] = start + i;
    }
  }

  private static void readLegacyDeltaVInts(IndexInput in, int count, int[] docIDs)
      throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  private void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
    DocIdSetIterator iterator = readBitSetIterator(in, count);
    int docId, pos = 0;
    while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      docIDs[pos++] = docId;
    }
    assert pos == count : "pos: " + pos + ", count: " + count;
  }

  private static void readDelta16Legacy(IndexInput in, int count, int[] docIds) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >> 1;
    in.readInts(docIds, 0, halfLen);
    for (int i = 0; i < halfLen; ++i) {
      int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  private static void readDelta16(IndexInput in, int count, int[] docIds) throws IOException {
    final int min = in.readVInt();
    int k = 0;
    for (int bound = count - 511; k < bound; k += 512) {
      in.readInts(docIds, k, 256);
      // Can be inlined to make offsets consistent so that loop get auto-vectorized.
      inner16(k, docIds, 256, min);
    }
    for (int bound = count - 127; k < bound; k += 128) {
      in.readInts(docIds, k, 64);
      inner16(k, docIds, 64, min);
    }
    for (; k < count; k++) {
      docIds[k] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  private static void inner16(int k, int[] docIds, int half, int min) {
    for (int i = k, to = k + half; i < to; ++i) {
      final int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[i + half] = (l & 0xFFFF) + min;
    }
  }

  private void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int k = 0;
    for (int bound = count - 511; k < bound; k += 512) {
      in.readInts(scratch, k, 384);
      shift(k, docIDs, scratch, 384);
      // Can be inlined to make offsets consistent so that loop get auto-vectorized.
      remainder24(k, docIDs, scratch, 128, 256, 384);
    }
    for (int bound = count - 127; k < bound; k += 128) {
      in.readInts(scratch, k, 96);
      shift(k, docIDs, scratch, 96);
      remainder24(k, docIDs, scratch, 32, 64, 96);
    }
    readScalarInts24(in, count - k, docIDs, k);
  }

  private static void shift(int k, int[] docIds, int[] scratch, int halfAndQuarter) {
    for (int i = k, to = k + halfAndQuarter; i < to; i++) {
      docIds[i] = scratch[i] >>> 8;
    }
  }

  private static void remainder24(
      int k, int[] docIds, int[] scratch, int quarter, int half, int halfAndQuarter) {
    for (int i = k, to = k + quarter; i < to; i++) {
      docIds[i + halfAndQuarter] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarter] & 0xFF) << 8)
              | (scratch[i + half] & 0xFF);
    }
  }

  private static void readScalarInts24(IndexInput in, int count, int[] docIDs, int offset)
      throws IOException {
    int i;
    for (i = offset; i < offset + count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i] = (int) (l1 >>> 40);
      docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
      docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
      docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
      docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
      docIDs[i + 7] = (int) l3 & 0xffffff;
    }
    for (; i < offset + count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    in.readInts(docIDs, 0, count);
  }

  /**
   * Read {@code count} integers and feed the result directly to {@link
   * IntersectVisitor#visit(int)}.
   */
  public void readInts(IndexInput in, int count, IntersectVisitor visitor, int[] buffer)
      throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case CONTINUOUS_IDS:
        readContinuousIds(in, count, visitor);
        break;
      case BITSET_IDS:
        readBitSet(in, count, visitor);
        break;
      case DELTA_BPV_16:
        if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
          readLegacyDelta16(in, count, visitor);
        } else {
          readDelta16(in, count, visitor);
        }
        break;
      case BPV_24:
        if (version < BKDWriter.VERSION_VECTORIZED_DOCID) {
          readScalarInts24(in, count, visitor);
        } else {
          readInts24(in, count, visitor, buffer);
        }
        break;
      case BPV_32:
        readInts32(in, count, visitor);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private void readBitSet(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    DocIdSetIterator bitSetIterator = readBitSetIterator(in, count);
    visitor.visit(bitSetIterator);
  }

  private static void readContinuousIds(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    int start = in.readVInt();
    int extra = start & 63;
    int offset = start - extra;
    int numBits = count + extra;
    FixedBitSet bitSet = new FixedBitSet(numBits);
    bitSet.set(extra, numBits);
    visitor.visit(new DocBaseBitSetIterator(bitSet, count, offset));
  }

  private static void readLegacyDeltaVInts(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private void readDelta16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    readDelta16(in, count, scratch);
    scratchIntsRef.ints = scratch;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }

  private void readLegacyDelta16(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    readDelta16Legacy(in, count, scratch);
    scratchIntsRef.ints = scratch;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }

  private void readInts24(IndexInput in, int count, IntersectVisitor visitor, int[] buffer)
      throws IOException {
    readInts24(in, count, buffer);
    scratchIntsRef.ints = buffer;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }

  private void readScalarInts24(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    readScalarInts24(in, count, scratch, 0);
    scratchIntsRef.ints = scratch;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }

  private void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    in.readInts(scratch, 0, count);
    scratchIntsRef.ints = scratch;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }
}
