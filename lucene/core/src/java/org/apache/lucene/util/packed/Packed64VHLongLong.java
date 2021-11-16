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

import java.sql.Array;
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
class Packed64VHLongLong extends PackedInts.MutableImpl {
    static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
    static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
    static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

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
    public Packed64VHLongLong(int valueCount, int bitsPerValue) {
        super(valueCount, bitsPerValue);
        final PackedInts.Format format = PackedInts.Format.PACKED;
        final long byteCount = format.byteCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue);
        if (byteCount > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("Not yet supported");
        }
        this.blocks = new byte[(int) byteCount + 8];
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
        final int elementPos = (int) (majorBitPos >>> BLOCK_BITS) * 8;
        // read entry as a long
        long packed;

        // The number of value-bits in the second long
        final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

        if (endBits <= 0) { // Single block
            packed = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos);
            return (packed >>> -endBits) & maskRight;
        }
        // Two blocks
        packed = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos);
        long next = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos + 8);
        return ((packed << endBits) | (next >>> (BLOCK_SIZE - endBits)))
                & maskRight;
    }

    @Override
    public void set(final int index, final long value) {
        // The abstract index in a contiguous bit stream
        final long majorBitPos = (long) index * bitsPerValue;
        // The index in the backing byte-array
        final int elementPos = (int) (majorBitPos >>> BLOCK_BITS) * 8; // / BLOCK_SIZE
        // The number of value-bits in the last block
        final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

        long packed;
        if (endBits <= 0) { // Single block
            packed = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos);
            packed = packed & ~(maskRight << -endBits) | (value << -endBits);
            BitUtil.VH_LE_LONG.set(blocks, elementPos, packed);
            return;
        }
        // Two blocks
        packed = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos);
        long newPacked = packed & ~(maskRight >>> endBits) | (value >>> endBits);
        BitUtil.VH_LE_LONG.set(blocks, elementPos, newPacked);

        long packed2 = (long) BitUtil.VH_LE_LONG.get(blocks, elementPos + 8);
        long newPacked2 = packed2 & (~0L >>> endBits) | (value << (BLOCK_SIZE - endBits));
        BitUtil.VH_LE_LONG.set(blocks, elementPos + 8, newPacked2);
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
