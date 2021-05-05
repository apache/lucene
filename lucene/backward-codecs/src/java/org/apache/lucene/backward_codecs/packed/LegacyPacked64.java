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
package org.apache.lucene.backward_codecs.packed;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Immutable version of {@code Packed64} which is constructed from am existing {@link DataInput}.
 */
class LegacyPacked64 extends PackedInts.Reader {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  /** Values are stores contiguously in the blocks array. */
  private final long[] blocks;
  /** A right-aligned mask of width BitsPerValue used by {@link #get(int)}. */
  private final long maskRight;
  /** Optimization: Saves one lookup in {@link #get(int)}. */
  private final int bpvMinusBlockSize;
  /** number of values */
  protected final int valueCount;
  /** bits per value. */
  protected final int bitsPerValue;

  /**
   * Creates an array with content retrieved from the given DataInput.
   *
   * @param in a DataInput, positioned at the start of Packed64-content.
   * @param valueCount the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @throws IOException if the values for the backing array could not be retrieved.
   */
  public LegacyPacked64(int packedIntsVersion, DataInput in, int valueCount, int bitsPerValue)
      throws IOException {
    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    final PackedInts.Format format = PackedInts.Format.PACKED;
    final long byteCount =
        format.byteCount(packedIntsVersion, valueCount, bitsPerValue); // to know how much to read
    final int longCount =
        format.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue); // to size the array
    blocks = new long[longCount];
    // read as many longs as we can
    for (int i = 0; i < byteCount / 8; ++i) {
      blocks[i] = in.readLong();
    }
    final int remaining = (int) (byteCount % 8);
    if (remaining != 0) {
      // read the last bytes
      long lastLong = 0;
      for (int i = 0; i < remaining; ++i) {
        lastLong |= (in.readByte() & 0xFFL) << (56 - i * 8);
      }
      blocks[blocks.length - 1] = lastLong;
    }
    maskRight = ~0L << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  @Override
  public final int size() {
    return valueCount;
  }

  @Override
  public long get(final int index) {
    // The abstract index in a bit stream
    final long majorBitPos = (long) index * bitsPerValue;
    // The index in the backing long-array
    final int elementPos = (int) (majorBitPos >>> BLOCK_BITS);
    // The number of value-bits in the second long
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
      return (blocks[elementPos] >>> -endBits) & maskRight;
    }
    // Two blocks
    return ((blocks[elementPos] << endBits) | (blocks[elementPos + 1] >>> (BLOCK_SIZE - endBits)))
        & maskRight;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;
    final PackedInts.Decoder decoder =
        PackedInts.getDecoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerValue);

    // go to the next block where the value does not span across two blocks
    final int offsetInBlocks = index % decoder.longValueCount();
    if (offsetInBlocks != 0) {
      for (int i = offsetInBlocks; i < decoder.longValueCount() && len > 0; ++i) {
        arr[off++] = get(index++);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk get
    assert index % decoder.longValueCount() == 0;
    int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
    assert (((long) index * bitsPerValue) & MOD_MASK) == 0;
    final int iterations = len / decoder.longValueCount();
    decoder.decode(blocks, blockIndex, arr, off, iterations);
    final int gotValues = iterations * decoder.longValueCount();
    index += gotValues;
    len -= gotValues;
    assert len >= 0;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to get
      assert index == originalIndex;
      return super.get(index, arr, off, len);
    }
  }

  @Override
  public String toString() {
    return "LegacyPacked64(bitsPerValue="
        + bitsPerValue
        + ",size="
        + size()
        + ",blocks="
        + blocks.length
        + ")";
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                + 3 * Integer.BYTES // bpvMinusBlockSize,valueCount,bitsPerValue
                + Long.BYTES // maskRight
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }
}
