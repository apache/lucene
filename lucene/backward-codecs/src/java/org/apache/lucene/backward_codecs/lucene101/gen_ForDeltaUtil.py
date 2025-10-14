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

MAX_SPECIALIZED_BITS_PER_VALUE = 16
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
package org.apache.lucene.backward_codecs.lucene101;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.backward_codecs.lucene101.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_1;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_2;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_4;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_5;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_6;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_7;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK16_8;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_1;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_10;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_11;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_12;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_13;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_14;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_15;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_16;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_2;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_3;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_4;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_5;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_6;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_7;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_8;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.MASK32_9;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.collapse16;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.collapse8;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decode1;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decode10;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decode2;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decode3;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decode9;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.decodeSlow;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.encode;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.expand16;
import static org.apache.lucene.backward_codecs.lucene101.ForUtil.expand8;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/
 * Encodes multiple integers in a Java int to get SIMD-like speedups.
 * If bitsPerValue &lt;= 4 then we pack 4 ints per Java int
 * else if bitsPerValue &lt;= 11 we pack 2 ints per Java int
 * else we use scalar operations.
 */
public final class ForDeltaUtil {

  private static final int HALF_BLOCK_SIZE = BLOCK_SIZE / 2;
  private static final int ONE_BLOCK_SIZE_FOURTH = BLOCK_SIZE / 4;
  private static final int TWO_BLOCK_SIZE_FOURTHS = BLOCK_SIZE / 2;
  private static final int THREE_BLOCK_SIZE_FOURTHS = 3 * BLOCK_SIZE / 4;

  private static void prefixSum8(int[] arr, int base) {
    // When the number of bits per value is 4 or less, we can sum up all values in a block without
    // risking overflowing an 8-bits integer. This allows computing the prefix sum by summing up 4
    // values at once.
    innerPrefixSum8(arr);
    expand8(arr);
    final int l0 = base;
    final int l1 = l0 + arr[ONE_BLOCK_SIZE_FOURTH - 1];
    final int l2 = l1 + arr[TWO_BLOCK_SIZE_FOURTHS - 1];
    final int l3 = l2 + arr[THREE_BLOCK_SIZE_FOURTHS - 1];

    for (int i = 0; i < ONE_BLOCK_SIZE_FOURTH; ++i) {
      arr[i] += l0;
      arr[ONE_BLOCK_SIZE_FOURTH + i] += l1;
      arr[TWO_BLOCK_SIZE_FOURTHS + i] += l2;
      arr[THREE_BLOCK_SIZE_FOURTHS + i] += l3;
    }
  }

  private static void prefixSum16(int[] arr, int base) {
    // When the number of bits per value is 11 or less, we can sum up all values in a block without
    // risking overflowing an 16-bits integer. This allows computing the prefix sum by summing up 2
    // values at once.
    innerPrefixSum16(arr);
    expand16(arr);
    final int l0 = base;
    final int l1 = base + arr[HALF_BLOCK_SIZE - 1];
    for (int i = 0; i < HALF_BLOCK_SIZE; ++i) {
      arr[i] += l0;
      arr[HALF_BLOCK_SIZE + i] += l1;
    }
  }

  private static void prefixSum32(int[] arr, int base) {
    arr[0] += base;
    for (int i = 1; i < BLOCK_SIZE; ++i) {
      arr[i] += arr[i-1];
    }
  }

  // For some reason unrolling seems to help
  private static void innerPrefixSum8(int[] arr) {
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
  private static void innerPrefixSum16(int[] arr) {
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

  private final int[] tmp = new int[BLOCK_SIZE];

  /** Return the number of bits per value required to store the given array containing strictly positive numbers. */
  int bitsRequired(int[] ints) {
    int or = 0;
    for (int l : ints) {
      or |= l;
    }
    // Deltas should be strictly positive since the delta between consecutive doc IDs is at least 1
    assert or != 0;
    return PackedInts.bitsRequired(or);
  }

  /**
   * Encode deltas of a strictly monotonically increasing sequence of integers. The provided {@code
   * ints} are expected to be deltas between consecutive values.
   */
  void encodeDeltas(int bitsPerValue, int[] ints, DataOutput out) throws IOException {
    final int primitiveSize;
    if (bitsPerValue <= 3) {
      primitiveSize = 8;
      collapse8(ints);
    } else if (bitsPerValue <= 10) {
      primitiveSize = 16;
      collapse16(ints);
    } else {
      primitiveSize = 32;
    }
    encode(ints, bitsPerValue, primitiveSize, out, tmp);
  }

"""

def primitive_size_for_bpv(bpv):
  if bpv <= 3:
    # If we have 4 bits per value or less then we can compute the prefix sum of 32 ints that store 4 8-bit values each without overflowing.
    return 8
  elif bpv <= 10:
    # If we have 10 bits per value or less then we can compute the prefix sum of 64 ints that store 2 16-bit values each without overflowing.
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

def writeRemainder(bpv, next_primitive, remaining_bits_per_int, o, num_values, f):
  iteration = 1
  num_ints = bpv * num_values / remaining_bits_per_int
  while num_ints % 2 == 0 and num_values % 2 == 0:
    num_ints /= 2
    num_values /= 2
    iteration *= 2
  f.write('    for (int iter = 0, tmpIdx = 0, intsIdx = %d; iter < %d; ++iter, tmpIdx += %d, intsIdx += %d) {\n' %(o, iteration, num_ints, num_values))
  i = 0
  remaining_bits = 0
  tmp_idx = 0
  for i in range(int(num_values)):
    b = bpv
    if remaining_bits == 0:
      b -= remaining_bits_per_int
      f.write('      int l%d = tmp[tmpIdx + %d] << %d;\n' %(i, tmp_idx, b))
    else:
      b -= remaining_bits
      f.write('      int l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits, b))
    tmp_idx += 1
    while b >= remaining_bits_per_int:
      b -= remaining_bits_per_int
      f.write('      l%d |= tmp[tmpIdx + %d] << %d;\n' %(i, tmp_idx, b))
      tmp_idx += 1
    if b > 0:
      f.write('      l%d |= (tmp[tmpIdx + %d] >>> %d) & MASK%d_%d;\n' %(i, tmp_idx, remaining_bits_per_int-b, next_primitive, b))
      remaining_bits = remaining_bits_per_int-b
    f.write('      ints[intsIdx + %d] = l%d;\n' %(i, i))
  f.write('    }\n')

def writeDecode(bpv, f):
  next_primitive = primitive_size_for_bpv(bpv)
  if next_primitive % bpv == 0:
    f.write('  private static void decode%dTo%d(PostingDecodingUtil pdu, int[] ints) throws IOException {\n' %(bpv, next_primitive))
  else:
    f.write('  private static void decode%dTo%d(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {\n' %(bpv, next_primitive))
  if bpv == next_primitive:
    f.write('    pdu.in.readInts(ints, 0, %d);\n' %(bpv*4))
  else:
    num_values_per_int = 32 / next_primitive
    remaining_bits = next_primitive % bpv
    num_iters = (next_primitive - 1) // bpv
    o = 4 * bpv * num_iters
    if remaining_bits == 0:
      f.write('    pdu.splitInts(%d, ints, %d, %d, MASK%d_%d, ints, %d, MASK%d_%d);\n' %(bpv*4, next_primitive - bpv, bpv, next_primitive, bpv, o, next_primitive, next_primitive - num_iters * bpv))
    else:
      f.write('    pdu.splitInts(%d, ints, %d, %d, MASK%d_%d, tmp, 0, MASK%d_%d);\n' %(bpv*4, next_primitive - bpv, bpv, next_primitive, bpv, next_primitive, next_primitive - num_iters * bpv))
      writeRemainder(bpv, next_primitive, remaining_bits, o, 128/num_values_per_int - o, f)
  f.write('  }\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write("""
  /**
   * Delta-decode 128 integers into {@code ints}.
   */
  void decodeAndPrefixSum(int bitsPerValue, PostingDecodingUtil pdu, int base, int[] ints) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    primitive_size = primitive_size_for_bpv(bpv)
    f.write('    case %d:\n' %bpv)
    if next_primitive(bpv) == primitive_size:
      if primitive_size % bpv == 0:
        f.write('        decode%d(pdu, ints);\n' %bpv)
      else:
        f.write('        decode%d(pdu, tmp, ints);\n' %bpv)
    else:
      if primitive_size % bpv == 0:
        f.write('        decode%dTo%d(pdu, ints);\n' %(bpv, primitive_size))
      else:
        f.write('        decode%dTo%d(pdu, tmp, ints);\n' %(bpv, primitive_size))
    f.write('      prefixSum%d(ints, base);\n' %primitive_size)
    f.write('      break;\n')
  f.write('    default:\n')
  f.write('        if (bitsPerValue < 1 || bitsPerValue > Integer.SIZE) {\n')
  f.write('          throw new IllegalStateException("Illegal number of bits per value: " + bitsPerValue);\n')
  f.write('        }\n')
  f.write('      decodeSlow(bitsPerValue, pdu, tmp, ints);\n')
  f.write('      prefixSum32(ints, base);\n')
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
