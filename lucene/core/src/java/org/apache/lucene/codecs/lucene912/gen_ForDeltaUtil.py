#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from math import gcd

"""Code generation for ForDeltaUtil.java"""

# 9 is included because it has faster prefix sum than 10 as the prefix sum can be performed on 16-bit integeres
FAST_BITS_PER_VALUE = [1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 16, 20, 24, 32]
OUTPUT_FILE = "ForDeltaUtil.java"
PRIMITIVE_SIZE = [16, 32]
HEADER = """// This file has been automatically generated, DO NOT EDIT

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

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 9 then we pack 4 ints per long
// else we pack 2 ints per long
// This allows computing a prefix sum without overflowing.
public final class ForDeltaUtil {

  static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_LOG2 = 7;
  private static final int ONE_BLOCK_SIZE_FOURTH = BLOCK_SIZE / 4;
  private static final int TWO_BLOCK_SIZE_FOURTHS = BLOCK_SIZE / 2;
  private static final int THREE_BLOCK_SIZE_FOURTHS = 3 * BLOCK_SIZE / 4;

  // IDENTITY_PLUS_ONE[i] == i+1
  private static final long[] IDENTITY_PLUS_ONE = new long[ForUtil.BLOCK_SIZE];

  static {
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      IDENTITY_PLUS_ONE[i] = i + 1;
    }
  }

  private static void prefixSumOfOnes(long[] arr, long base) {
    System.arraycopy(IDENTITY_PLUS_ONE, 0, arr, 0, ForUtil.BLOCK_SIZE);
    // This loop gets auto-vectorized
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      arr[i] += base;
    }
  }

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  private static void expand16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[32 + i] = (l >>> 32) & 0xFFFFL;
      arr[64 + i] = (l >>> 16) & 0xFFFFL;
      arr[96 + i] = l & 0xFFFFL;
    }
  }

  private static void collapse16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] = (arr[i] << 48) | (arr[32 + i] << 32) | (arr[64 + i] << 16) | arr[96 + i];
    }
  }

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64 + i];
    }
  }

  private static void prefixSum16(long[] arr, long base) {
    // When the number of bits per value is 9 or less, we can sum up all values in a block without
    // risking overflowing a 16-bits integer. This allows computing the prefix sum by summing up 4
    // values at once.
    innerPrefixSum16(arr);
    expand16(arr);
    final long l0 = base;
    final long l1 = l0 + arr[ONE_BLOCK_SIZE_FOURTH - 1];
    final long l2 = l1 + arr[TWO_BLOCK_SIZE_FOURTHS - 1];
    final long l3 = l2 + arr[THREE_BLOCK_SIZE_FOURTHS - 1];

    for (int i = 0; i < ONE_BLOCK_SIZE_FOURTH; ++i) {
      arr[i] += l0;
      arr[ONE_BLOCK_SIZE_FOURTH + i] += l1;
      arr[TWO_BLOCK_SIZE_FOURTHS + i] += l2;
      arr[THREE_BLOCK_SIZE_FOURTHS + i] += l3;
    }
  }

  private static void prefixSum32(long[] arr, long base) {
    arr[0] += base << 32;
    innerPrefixSum32(arr);
    expand32(arr);
    final long l = arr[BLOCK_SIZE/2-1];
    for (int i = BLOCK_SIZE/2; i < BLOCK_SIZE; ++i) {
      arr[i] += l;
    }
  }

  // For some reason, unrolling seems to help
  private static void innerPrefixSum16(long[] arr) {
    arr[1] += arr[0];
    arr[2] += arr[1];
    arr[3] += arr[2];
    arr[4] += arr[3];
    arr[5] += arr[4];
    arr[6] += arr[5];
    arr[7] += arr[6];
    arr[8] += arr[7];
    arr[9] += arr[8];
    arr[10] += arr[9];
    arr[11] += arr[10];
    arr[12] += arr[11];
    arr[13] += arr[12];
    arr[14] += arr[13];
    arr[15] += arr[14];
    arr[16] += arr[15];
    arr[17] += arr[16];
    arr[18] += arr[17];
    arr[19] += arr[18];
    arr[20] += arr[19];
    arr[21] += arr[20];
    arr[22] += arr[21];
    arr[23] += arr[22];
    arr[24] += arr[23];
    arr[25] += arr[24];
    arr[26] += arr[25];
    arr[27] += arr[26];
    arr[28] += arr[27];
    arr[29] += arr[28];
    arr[30] += arr[29];
    arr[31] += arr[30];
  }

  // For some reason unrolling seems to help
  private static void innerPrefixSum32(long[] arr) {
    arr[1] += arr[0];
    arr[2] += arr[1];
    arr[3] += arr[2];
    arr[4] += arr[3];
    arr[5] += arr[4];
    arr[6] += arr[5];
    arr[7] += arr[6];
    arr[8] += arr[7];
    arr[9] += arr[8];
    arr[10] += arr[9];
    arr[11] += arr[10];
    arr[12] += arr[11];
    arr[13] += arr[12];
    arr[14] += arr[13];
    arr[15] += arr[14];
    arr[16] += arr[15];
    arr[17] += arr[16];
    arr[18] += arr[17];
    arr[19] += arr[18];
    arr[20] += arr[19];
    arr[21] += arr[20];
    arr[22] += arr[21];
    arr[23] += arr[22];
    arr[24] += arr[23];
    arr[25] += arr[24];
    arr[26] += arr[25];
    arr[27] += arr[26];
    arr[28] += arr[27];
    arr[29] += arr[28];
    arr[30] += arr[29];
    arr[31] += arr[30];
    arr[32] += arr[31];
    arr[33] += arr[32];
    arr[34] += arr[33];
    arr[35] += arr[34];
    arr[36] += arr[35];
    arr[37] += arr[36];
    arr[38] += arr[37];
    arr[39] += arr[38];
    arr[40] += arr[39];
    arr[41] += arr[40];
    arr[42] += arr[41];
    arr[43] += arr[42];
    arr[44] += arr[43];
    arr[45] += arr[44];
    arr[46] += arr[45];
    arr[47] += arr[46];
    arr[48] += arr[47];
    arr[49] += arr[48];
    arr[50] += arr[49];
    arr[51] += arr[50];
    arr[52] += arr[51];
    arr[53] += arr[52];
    arr[54] += arr[53];
    arr[55] += arr[54];
    arr[56] += arr[55];
    arr[57] += arr[56];
    arr[58] += arr[57];
    arr[59] += arr[58];
    arr[60] += arr[59];
    arr[61] += arr[60];
    arr[62] += arr[61];
    arr[63] += arr[62];
  }

  /**
   * Compute the number of bits per value that should be used when {@code bitsPerValue} bits are
   * required.
   */
  static int computeActualBitsPerValue(int bitsPerValue) {
    int i = Arrays.binarySearch(FAST_BITS_PER_VALUE, bitsPerValue);
    if (i < 0) {
      i = -1 - i;
    }
    return FAST_BITS_PER_VALUE[i];
  }

  private final long[] tmp = new long[BLOCK_SIZE / 2];

  /** Encode 128 integers from {@code longs} into {@code out}. */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    assert Arrays.binarySearch(FAST_BITS_PER_VALUE, bitsPerValue) >= 0 : bitsPerValue;
    final int nextPrimitive;
    final int numLongs;
    if (bitsPerValue <= 9) {
      nextPrimitive = 16;
      numLongs = BLOCK_SIZE / 4;
      collapse16(longs);
    } else {
      nextPrimitive = 32;
      numLongs = BLOCK_SIZE / 2;
      collapse32(longs);
    }

    final int numLongsPerShift = bitsPerValue * 2;
    int idx = 0;
    int shift = nextPrimitive - bitsPerValue;
    for (int i = 0; i < numLongsPerShift; ++i) {
      tmp[i] = longs[idx++] << shift;
    }
    for (shift = shift - bitsPerValue; shift >= 0; shift -= bitsPerValue) {
      for (int i = 0; i < numLongsPerShift; ++i) {
        tmp[i] |= longs[idx++] << shift;
      }
    }

    final int remainingBitsPerLong = shift + bitsPerValue;
    final long maskRemainingBitsPerLong;
    if (nextPrimitive == 16) {
      maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];
    } else {
      maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];
    }

    int tmpIdx = 0;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < numLongs) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;
        if (nextPrimitive == 16) {
          mask1 = MASKS16[remainingBitsPerValue];
          mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        } else {
          mask1 = MASKS32[remainingBitsPerValue];
          mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        }
        tmp[tmpIdx] |= (longs[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & mask2;
      }
    }

    for (int i = 0; i < numLongsPerShift; ++i) {
      out.writeLong(tmp[i]);
    }
  }

  /** Number of bytes required to encode 128 integers of {@code bitsPerValue} bits per value. */
  int numBytes(int bitsPerValue) {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2 compiler, which
   * generates SIMD instructions for it in order to shift multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi + i] = (a[i] >>> shift) & mask;
    }
  }

  /**
   * Encode deltas of a strictly monotonically increasing sequence of integers. The provided {@code
   * longs} are expected to be deltas between consecutive values.
   */
  void encodeDeltas(long[] longs, DataOutput out) throws IOException {
    if (longs[0] == 1 && PForUtil.allEqual(longs)) { // happens with very dense postings
      out.writeByte((byte) 0);
    } else {
      long or = 0;
      for (long l : longs) {
        or |= l;
      }
      assert or != 0;
      int bitsPerValue = PackedInts.bitsRequired(or);
      bitsPerValue = computeActualBitsPerValue(bitsPerValue);
      out.writeByte((byte) bitsPerValue);
      encode(longs, bitsPerValue, out);
    }
  }

  /** Decode deltas, compute the prefix sum and add {@code base} to all decoded longs. */
  void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue == 0) {
      prefixSumOfOnes(longs, base);
    } else {
      decodeAndPrefixSum(bitsPerValue, in, base, longs);
    }
  }

  void skip(DataInput in) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    in.skipBytes(numBytes(bitsPerValue));
  }

"""

def writeRemainderWithSIMDOptimize(bpv, next_primitive, remaining_bits_per_long, o, num_values, f):
  iteration = 1
  num_longs = bpv * num_values / remaining_bits_per_long
  while num_longs % 2 == 0 and num_values % 2 == 0:
    num_longs /= 2
    num_values /= 2
    iteration *= 2

  f.write('    shiftLongs(tmp, %d, tmp, 0, 0, MASK%d_%d);\n' % (iteration * num_longs, next_primitive, remaining_bits_per_long))
  f.write('    for (int iter = 0, tmpIdx = 0, longsIdx = %d; iter < %d; ++iter, tmpIdx += %d, longsIdx += %d) {\n' %(o, iteration, num_longs, num_values))
  tmp_idx = 0
  b = bpv
  b -= remaining_bits_per_long
  f.write('      long l0 = tmp[tmpIdx + %d] << %d;\n' %(tmp_idx, b))
  tmp_idx += 1
  while b >= remaining_bits_per_long:
    b -= remaining_bits_per_long
    f.write('      l0 |= tmp[tmpIdx + %d] << %d;\n' %(tmp_idx, b))
    tmp_idx += 1
  f.write('      longs[longsIdx + 0] = l0;\n')
  f.write('    }\n')
  

def writeRemainder(bpv, next_primitive, remaining_bits_per_long, o, num_values, f):
  iteration = 1
  num_longs = bpv * num_values / remaining_bits_per_long
  while num_longs % 2 == 0 and num_values % 2 == 0:
    num_longs /= 2
    num_values /= 2
    iteration *= 2
  f.write('    for (int iter = 0, tmpIdx = 0, longsIdx = %d; iter < %d; ++iter, tmpIdx += %d, longsIdx += %d) {\n' %(o, iteration, num_longs, num_values))
  i = 0
  remaining_bits = 0
  tmp_idx = 0
  for i in range(int(num_values)):
    b = bpv
    if remaining_bits == 0:
      b -= remaining_bits_per_long
      f.write('      long l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits_per_long, b))
    else:
      b -= remaining_bits
      f.write('      long l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits, b))
    tmp_idx += 1
    while b >= remaining_bits_per_long:
      b -= remaining_bits_per_long
      f.write('      l%d |= (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits_per_long, b))
      tmp_idx += 1
    if b > 0:
      f.write('      l%d |= (tmp[tmpIdx + %d] >>> %d) & MASK%d_%d;\n' %(i, tmp_idx, remaining_bits_per_long-b, next_primitive, b))
      remaining_bits = remaining_bits_per_long-b
    f.write('      longs[longsIdx + %d] = l%d;\n' %(i, i))
  f.write('    }\n')
  

def writeDecode(bpv, f):
  next_primitive = 32
  if bpv <= 9:
    next_primitive = 16
  f.write('  private static void decode%d(DataInput in, long[] tmp, long[] longs) throws IOException {\n' %bpv)
  num_values_per_long = 64 / next_primitive
  f.write('    in.readLongs(tmp, 0, %d);\n' %(bpv*2))
  if bpv != next_primitive:
    shift = next_primitive - bpv
    o = 0
    while shift >= 0:
      f.write('    shiftLongs(tmp, %d, longs, %d, %d, MASK%d_%d);\n' %(bpv*2, o, shift, next_primitive, bpv))
      o += bpv*2
      shift -= bpv
    if shift + bpv > 0:
      if bpv % (next_primitive % bpv) == 0:
        writeRemainderWithSIMDOptimize(bpv, next_primitive, shift + bpv, o, 128/num_values_per_long - o, f)
      else:
        writeRemainder(bpv, next_primitive, shift + bpv, o, 128/num_values_per_long - o, f)
  f.write('  }\n')


if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write("""  private static final int[] FAST_BITS_PER_VALUE = new int[] {
      %s
  };\n""" %",\n      ".join(map(str, FAST_BITS_PER_VALUE)))
  for primitive_size in PRIMITIVE_SIZE:
    f.write('  private static final long[] MASKS%d = new long[%d];\n' %(primitive_size, primitive_size))
  f.write('\n')
  f.write('  static {\n')
  for primitive_size in PRIMITIVE_SIZE:
    f.write('    for (int i = 0; i < %d; ++i) {\n' %primitive_size)
    f.write('      MASKS%d[i] = mask%d(i);\n' %(primitive_size, primitive_size))
    f.write('    }\n')
  f.write('  }')
  f.write("""
  // mark values in array as final longs to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
""")
  for primitive_size in PRIMITIVE_SIZE:
    for bpv in range(33):
      if bpv < primitive_size:
        f.write('  private static final long MASK%d_%d = MASKS%d[%d];\n' %(primitive_size, bpv, primitive_size, bpv))

  f.write("""
  // public for benchmarking
  /**
   * Delta-decode 128 integers into {@code longs}.
   */
  public void decodeAndPrefixSum(int bitsPerValue, DataInput in, long base, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in FAST_BITS_PER_VALUE:
    next_primitive = 32
    if bpv <= 9:
      next_primitive = 16
    f.write('    case %d:\n' %bpv)
    f.write('      decode%d(in, tmp, longs);\n' %bpv)
    f.write('      prefixSum%d(longs, base);\n' %next_primitive)
    f.write('      break;\n')
  f.write('    default:\n')
  f.write('      throw new UnsupportedOperationException(\"bpv=\" + bitsPerValue);\n')
  f.write('    }\n')
  f.write('  }\n')

  f.write('\n')
  for i in FAST_BITS_PER_VALUE:
    writeDecode(i, f)
    f.write('\n')

  f.write('}\n')
