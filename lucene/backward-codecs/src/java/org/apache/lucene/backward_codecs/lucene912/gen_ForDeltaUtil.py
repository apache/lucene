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

MAX_SPECIALIZED_BITS_PER_VALUE = 24
OUTPUT_FILE = "ForDeltaUtil.java"
PRIMITIVE_SIZE = [8, 16, 32]
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
package org.apache.lucene.backward_codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.backward_codecs.lucene912.ForUtil.*;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/
 * Encodes multiple integers in a long to get SIMD-like speedups.
 * If bitsPerValue &lt;= 4 then we pack 8 ints per long
 * else if bitsPerValue &lt;= 11 we pack 4 ints per long
 * else we pack 2 ints per long
 */
final class ForDeltaUtil {

  private static final int ONE_BLOCK_SIZE_FOURTH = BLOCK_SIZE / 4;
  private static final int TWO_BLOCK_SIZE_FOURTHS = BLOCK_SIZE / 2;
  private static final int THREE_BLOCK_SIZE_FOURTHS = 3 * BLOCK_SIZE / 4;

  private static final int ONE_BLOCK_SIZE_EIGHT = BLOCK_SIZE / 8;
  private static final int TWO_BLOCK_SIZE_EIGHTS = BLOCK_SIZE / 4;
  private static final int THREE_BLOCK_SIZE_EIGHTS = 3 * BLOCK_SIZE / 8;
  private static final int FOUR_BLOCK_SIZE_EIGHTS = BLOCK_SIZE / 2;
  private static final int FIVE_BLOCK_SIZE_EIGHTS = 5 * BLOCK_SIZE / 8;
  private static final int SIX_BLOCK_SIZE_EIGHTS = 3 * BLOCK_SIZE / 4;
  private static final int SEVEN_BLOCK_SIZE_EIGHTS = 7 * BLOCK_SIZE / 8;

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

  private static void prefixSum8(long[] arr, long base) {
    // When the number of bits per value is 4 or less, we can sum up all values in a block without
    // risking overflowing a 8-bits integer. This allows computing the prefix sum by summing up 8
    // values at once.
    innerPrefixSum8(arr);
    expand8(arr);
    final long l0 = base;
    final long l1 = l0 + arr[ONE_BLOCK_SIZE_EIGHT - 1];
    final long l2 = l1 + arr[TWO_BLOCK_SIZE_EIGHTS - 1];
    final long l3 = l2 + arr[THREE_BLOCK_SIZE_EIGHTS - 1];
    final long l4 = l3 + arr[FOUR_BLOCK_SIZE_EIGHTS - 1];
    final long l5 = l4 + arr[FIVE_BLOCK_SIZE_EIGHTS - 1];
    final long l6 = l5 + arr[SIX_BLOCK_SIZE_EIGHTS - 1];
    final long l7 = l6 + arr[SEVEN_BLOCK_SIZE_EIGHTS - 1];

    for (int i = 0; i < ONE_BLOCK_SIZE_EIGHT; ++i) {
      arr[i] += l0;
      arr[ONE_BLOCK_SIZE_EIGHT + i] += l1;
      arr[TWO_BLOCK_SIZE_EIGHTS + i] += l2;
      arr[THREE_BLOCK_SIZE_EIGHTS + i] += l3;
      arr[FOUR_BLOCK_SIZE_EIGHTS + i] += l4;
      arr[FIVE_BLOCK_SIZE_EIGHTS + i] += l5;
      arr[SIX_BLOCK_SIZE_EIGHTS + i] += l6;
      arr[SEVEN_BLOCK_SIZE_EIGHTS + i] += l7;
    }
  }

  private static void prefixSum16(long[] arr, long base) {
    // When the number of bits per value is 11 or less, we can sum up all values in a block without
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

  // For some reason unrolling seems to help
  private static void innerPrefixSum8(long[] arr) {
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
  }

  // For some reason unrolling seems to help
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

  private final long[] tmp = new long[BLOCK_SIZE / 2];

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
      final int bitsPerValue = PackedInts.bitsRequired(or);
      out.writeByte((byte) bitsPerValue);

      final int primitiveSize;
      if (bitsPerValue <= 4) {
        primitiveSize = 8;
        collapse8(longs);
      } else if (bitsPerValue <= 11) {
        primitiveSize = 16;
        collapse16(longs);
      } else {
        primitiveSize = 32;
        collapse32(longs);
      }
      encode(longs, bitsPerValue, primitiveSize, out, tmp);
    }
  }

  /** Decode deltas, compute the prefix sum and add {@code base} to all decoded longs. */
  void decodeAndPrefixSum(IndexInput in, long base, long[] longs) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue == 0) {
      prefixSumOfOnes(longs, base);
    } else {
      decodeAndPrefixSum(bitsPerValue, in, base, longs);
    }
  }

"""

def primitive_size_for_bpv(bpv):
  if bpv <= 4:
    # If we have 4 bits per value or less then we can compute the prefix sum of 16 longs that store 8 4-bit values each without overflowing.
    return 8
  elif bpv <= 11:
    # If we have 11 bits per value or less then we can compute the prefix sum of 32 longs that store 4 16-bit values each without overflowing.
    return 16
  else:
    # No risk of overflow with 32 bits per value
    return 32

def next_primitive(bpv):
  if bpv <= 8:
    return 8
  elif bpv <= 16:
    return 16
  else:
    return 32

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
      f.write('      long l%d = tmp[tmpIdx + %d] << %d;\n' %(i, tmp_idx, b))
    else:
      b -= remaining_bits
      f.write('      long l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits, b))
    tmp_idx += 1
    while b >= remaining_bits_per_long:
      b -= remaining_bits_per_long
      f.write('      l%d |= tmp[tmpIdx + %d] << %d;\n' %(i, tmp_idx, b))
      tmp_idx += 1
    if b > 0:
      f.write('      l%d |= (tmp[tmpIdx + %d] >>> %d) & MASK%d_%d;\n' %(i, tmp_idx, remaining_bits_per_long-b, next_primitive, b))
      remaining_bits = remaining_bits_per_long-b
    f.write('      longs[longsIdx + %d] = l%d;\n' %(i, i))
  f.write('    }\n')

def writeDecode(bpv, f):
  next_primitive = primitive_size_for_bpv(bpv)
  if next_primitive % bpv == 0:
    f.write('  private static void decode%dTo%d(IndexInput in, long[] longs) throws IOException {\n' %(bpv, next_primitive))
  else:
    f.write('  private static void decode%dTo%d(IndexInput in, long[] tmp, long[] longs) throws IOException {\n' %(bpv, next_primitive))
  if bpv == next_primitive:
    f.write('    in.readLongs(longs, 0, %d);\n' %(bpv*2))
  else:
    num_values_per_long = 64 / next_primitive
    remaining_bits = next_primitive % bpv
    num_iters = (next_primitive - 1) // bpv
    o = 2 * bpv * num_iters
    if remaining_bits == 0:
      f.write('    splitLongs(in, %d, longs, %d, %d, MASK%d_%d, longs, %d, MASK%d_%d);\n' %(bpv*2, next_primitive - bpv, bpv, next_primitive, bpv, o, next_primitive, next_primitive - num_iters * bpv))
    else:
      f.write('    splitLongs(in, %d, longs, %d, %d, MASK%d_%d, tmp, 0, MASK%d_%d);\n' %(bpv*2, next_primitive - bpv, bpv, next_primitive, bpv, next_primitive, next_primitive - num_iters * bpv))
      writeRemainder(bpv, next_primitive, remaining_bits, o, 128/num_values_per_long - o, f)
  f.write('  }\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write("""
  /**
   * Delta-decode 128 integers into {@code longs}.
   */
  void decodeAndPrefixSum(int bitsPerValue, IndexInput in, long base, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    primitive_size = primitive_size_for_bpv(bpv)
    f.write('    case %d:\n' %bpv)
    if next_primitive(bpv) == primitive_size:
      if primitive_size % bpv == 0:
        f.write('        decode%d(in, longs);\n' %bpv)
      else:
        f.write('        decode%d(in, tmp, longs);\n' %bpv)
    else:
      if primitive_size % bpv == 0:
        f.write('        decode%dTo%d(in, longs);\n' %(bpv, primitive_size))
      else:
        f.write('        decode%dTo%d(in, tmp, longs);\n' %(bpv, primitive_size))
    f.write('      prefixSum%d(longs, base);\n' %primitive_size)
    f.write('      break;\n')
  f.write('    default:\n')
  f.write('      decodeSlow(bitsPerValue, in, tmp, longs);\n')
  f.write('      prefixSum32(longs, base);\n')
  f.write('      break;\n')
  f.write('    }\n')
  f.write('  }\n')

  f.write('\n')
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    if next_primitive(bpv) != primitive_size_for_bpv(bpv):
      writeDecode(bpv, f)
      if bpv < MAX_SPECIALIZED_BITS_PER_VALUE:
        f.write('\n')

  f.write('}\n')
