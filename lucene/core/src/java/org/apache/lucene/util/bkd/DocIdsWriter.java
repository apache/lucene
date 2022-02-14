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
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;

final class DocIdsWriter {

  private static final byte CONTINUOUS_IDS = (byte) -2;
  private static final byte BITSET_IDS = (byte) -1;
  private static final byte DELTA_FOR_UTIL = (byte) 32 + 16;
  private static final byte BPV_24 = (byte) 32 + 24;
  private static final byte BPV_32 = (byte) 32;
  // These signs are legacy, should no longer be used in the writing side.
  private static final byte LEGACY_DELTA_VINT = (byte) 0;
  private static final byte LEGACY_BPV_24 = (byte) 24;

  private final BKDForUtil forUtil;
  private final int[] scratch;

  DocIdsWriter(int maxPointsInLeaf) {
    scratch = new int[maxPointsInLeaf];
    forUtil = new BKDForUtil(maxPointsInLeaf);
  }

  void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
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
      out.writeByte(DELTA_FOR_UTIL);
      for (int i = 0; i < count; i++) {
        scratch[i] = docIds[start + i] - min;
      }
      out.writeVInt(min);
      forUtil.encode16(count, scratch, out);
    } else {
      if (max <= 0xFFFFFF) {
        out.writeByte(BPV_24);
        forUtil.encode24(start, count, docIds, out);
      } else {
        out.writeByte(BPV_32);
        forUtil.encode32(start, count, docIds, out);
      }
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
  void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case CONTINUOUS_IDS:
        readContinuousIds(in, count, docIDs);
        break;
      case BITSET_IDS:
        readBitSet(in, count, docIDs);
        break;
      case DELTA_FOR_UTIL:
        readDelta16(in, count, docIDs);
        break;
      case BPV_24:
        readInts24(in, count, docIDs);
        break;
      case BPV_32:
        readInts32(in, count, docIDs);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, docIDs);
        break;
      case LEGACY_BPV_24:
        readLegacyInts24(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private void readDelta16(IndexInput in, int count, int[] docIDs) throws IOException {
    final int min = in.readVInt();
    forUtil.decode16(in, docIDs, count, min);
  }

  private void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    forUtil.decode24(in, docIDs, count);
  }

  private void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    in.readInts(docIDs, 0, count);
  }

  private static DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
    int offsetWords = in.readVInt();
    int longLen = in.readVInt();
    long[] bits = new long[longLen];
    in.readLongs(bits, 0, longLen);
    FixedBitSet bitSet = new FixedBitSet(bits, longLen << 6);
    return new DocBaseBitSetIterator(bitSet, count, offsetWords << 6);
  }

  private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
    int start = in.readVInt();
    for (int i = 0; i < count; i++) {
      docIDs[i] = start + i;
    }
  }

  private static void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
    DocIdSetIterator iterator = readBitSetIterator(in, count);
    int docId, pos = 0;
    while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      docIDs[pos++] = docId;
    }
    assert pos == count : "pos: " + pos + "count: " + count;
  }

  private static void readLegacyDeltaVInts(IndexInput in, int count, int[] docIDs)
      throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  private static void readLegacyInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
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
    for (; i < count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  /**
   * Read {@code count} integers and feed the result directly to {@link
   * IntersectVisitor#visit(int)}.
   */
  void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case CONTINUOUS_IDS:
        readContinuousIds(in, count, visitor);
        break;
      case BITSET_IDS:
        readBitSet(in, count, visitor);
        break;
      case DELTA_FOR_UTIL:
        readDelta16(in, count, visitor);
        break;
      case BPV_24:
        readInts24(in, count, visitor);
        break;
      case BPV_32:
        readInts32(in, count, visitor);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, visitor);
        break;
      case LEGACY_BPV_24:
        readLegacyInts24(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readLegacyDeltaVInts(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private static void readLegacyInts24(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      visitor.visit((int) (l1 >>> 40));
      visitor.visit((int) (l1 >>> 16) & 0xffffff);
      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit((int) (l2 >>> 32) & 0xffffff);
      visitor.visit((int) (l2 >>> 8) & 0xffffff);
      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit((int) (l3 >>> 24) & 0xffffff);
      visitor.visit((int) l3 & 0xffffff);
    }
    for (; i < count; ++i) {
      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
    }
  }

  private static void readBitSet(IndexInput in, int count, IntersectVisitor visitor)
      throws IOException {
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

  private void readDelta16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final int min = in.readVInt();
    forUtil.decode16(in, scratch, count, min);
    for (int i = 0; i < count; i++) {
      visitor.visit(scratch[i]);
    }
  }

  private void readInts24(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    forUtil.decode24(in, scratch, count);
    for (int i = 0; i < count; i++) {
      visitor.visit(scratch[i]);
    }
  }

  private void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    in.readInts(scratch, 0, count);
    for (int i = 0; i < count; i++) {
      visitor.visit(scratch[i]);
    }
  }
}
