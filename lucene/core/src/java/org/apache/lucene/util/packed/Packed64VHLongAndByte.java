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
package org.apache.lucene.util.packed;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Space optimized random access capable array of values with a fixed number of bits/value. Values
 * are packed contiguously.
 *
 * <p>The implementation strives to perform as fast as possible under the constraint of contiguous
 * bits, by avoiding expensive operations. This comes at the cost of code clarity.
 *
 * <p>Technical details: This implementation is a refinement of a non-branching version. The
 * non-branching get and set methods meant that 2 or 4 atomics in the underlying array were always
 * accessed, even for the cases where only 1 or 2 were needed. Even with caching, this had a
 * detrimental effect on performance. Related to this issue, the old implementation used lookup
 * tables for shifts and masks, which also proved to be a bit slower than calculating the shifts and
 * masks on the fly. See https://issues.apache.org/jira/browse/LUCENE-4062 for details.
 */
class Packed64VHLongAndByte extends PackedInts.MutableImpl {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BYTE_BITS = 3;
  static final int BYTE_BLOCK_SIZE = 8;
  static final int BYTE_BLOCK_MOD_MASK = BYTE_BLOCK_SIZE - 1; // x % BYTE_BLOCK

  /** Values are stores contiguously in the blocks array. */
  private final byte[] blocks;
  /** A right-aligned mask of width BitsPerValue used by {@link #get(int)}. */
  private final long maskRight;
  /** Optimization: Saves one lookup in {@link #get(int)}. */
  private final int bpvMinusBlockSize;

  /**
   * Creates an array with the internal structures adjusted for the given limits and initialized to
   * 0.
   *
   * @param valueCount the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public Packed64VHLongAndByte(int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    final PackedInts.Format format = PackedInts.Format.PACKED;
    final long byteCount = format.byteCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue);
    if (bitsPerValue > 56) {
      throw new IllegalArgumentException("Only bitsPerValue up to 56 allowed");
    }
    if (byteCount > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("Too many values/bits to store");
    }
    this.blocks = new byte[(int) byteCount + Long.BYTES];
    maskRight = ~0L << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  /**
   * @param index the position of the value.
   * @return the value at the given index.
   */
  @Override
  public long get(final int index) {
    // The abstract index in a bit stream
    final long majorBitPos = (long) index * bitsPerValue;
    // The index in the backing byte-array
    final int elementPos = (int) (majorBitPos >>> BYTE_BITS); // BYTE_BLOCK
    // The number of bits already occupied from the previous position
    final int maskOffset = (int) majorBitPos & BYTE_BLOCK_MOD_MASK;
//    final int endBits = maskOffset + bpvMinusBlockSize;
    // Read the main long
    //final long packed = ((long) BitUtil.VH_LE_LONG.get(blocks, elementPos) >>> maskOffset) & maskRight;
    return ((long) BitUtil.VH_LE_LONG.get(blocks, elementPos) >>> maskOffset) & maskRight;

//    if (endBits <= 0) { // Single block
//      return packed;
//    }
//    // compute next mask
//    return packed | (blocks[elementPos + BYTE_BLOCK_SIZE] & ~(~0L << endBits)) << (BLOCK_SIZE - maskOffset);
  }

  @Override
  public void set(final int index, final long value) {
    // The abstract index in a contiguous bit stream
    final long majorBitPos = (long) index * bitsPerValue;
    // The index in the backing byte-array
    final int elementPos = (int) (majorBitPos >>> BYTE_BITS); // / BYTE_BLOCK
    // The number of bits already occupied from the previous position
    final int maskOffset = (int) majorBitPos & BYTE_BLOCK_MOD_MASK;
    final int endBits = maskOffset + bpvMinusBlockSize;
    // Read the main long
    final long packed = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos);
    BitUtil.VH_LE_LONG.set(blocks, elementPos, packed & ~(maskRight << maskOffset) | (value << maskOffset));

//    if (endBits <= 0) { // Fits in single block, bail out
//      return;
//    }
//
//    // Extra read for the overflow bits
//    blocks[elementPos + BYTE_BLOCK_SIZE] = (byte) (blocks[elementPos + BYTE_BLOCK_SIZE] & (~0L << endBits) | (value >>> bitsPerValue - endBits));
  }

  @Override
  public String toString() {
    return "Packed64(bitsPerValue="
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

  private static int gcd(int a, int b) {
    if (a < b) {
      return gcd(b, a);
    } else if (b == 0) {
      return a;
    } else {
      return gcd(b, a % b);
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, (byte) 0);
  }
}
