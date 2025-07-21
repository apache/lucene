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

"""Code generation for ForUtil.java"""

MAX_SPECIALIZED_BITS_PER_VALUE = 16
OUTPUT_FILE = "ForUtil.java"
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

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/
 * Encodes multiple integers in one to get SIMD-like speedups.
 * If bitsPerValue &lt;= 8 then we pack 4 ints per Java int
 * else if bitsPerValue &lt;= 16 we pack 2 ints per Java int
 * else we do scalar operations.
 */
public final class ForUtil {

  static final int BLOCK_SIZE = 128;
  static final int BLOCK_SIZE_LOG2 = 7;

  static int expandMask16(int mask16) {
    return mask16 | (mask16 << 16);
  }

  static int expandMask8(int mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  static int mask32(int bitsPerValue) {
    return (1 << bitsPerValue) - 1;
  }

  static int mask16(int bitsPerValue) {
    return expandMask16((1 << bitsPerValue) - 1);
  }

  static int mask8(int bitsPerValue) {
    return expandMask8((1 << bitsPerValue) - 1);
  }

  static void expand8(int[] arr) {
    for (int i = 0; i < 32; ++i) {
      int l = arr[i];
      arr[i] = (l >>> 24) & 0xFF;
      arr[32 + i] = (l >>> 16) & 0xFF;
      arr[64 + i] = (l >>> 8) & 0xFF;
      arr[96 + i] = l & 0xFF;
    }
  }

  static void collapse8(int[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] =
          (arr[i] << 24)
              | (arr[32 + i] << 16)
              | (arr[64 + i] << 8)
              | arr[96 + i];
    }
  }

  static void expand16(int[] arr) {
    for (int i = 0; i < 64; ++i) {
      int l = arr[i];
      arr[i] = (l >>> 16) & 0xFFFF;
      arr[64 + i] = l & 0xFFFF;
    }
  }

  static void collapse16(int[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 16) | arr[64 + i];
    }
  }

  private final int[] tmp = new int[BLOCK_SIZE];

  /** Encode 128 integers from {@code ints} into {@code out}. */
  void encode(int[] ints, int bitsPerValue, DataOutput out) throws IOException {
    final int nextPrimitive;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      collapse8(ints);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      collapse16(ints);
    } else {
      nextPrimitive = 32;
    }
    encode(ints, bitsPerValue, nextPrimitive, out, tmp);
  }

  static void encode(int[] ints, int bitsPerValue, int primitiveSize, DataOutput out, int[] tmp) throws IOException {
    final int numInts = BLOCK_SIZE * primitiveSize / Integer.SIZE;

    final int numIntsPerShift = bitsPerValue * 4;
    int idx = 0;
    int shift = primitiveSize - bitsPerValue;
    for (int i = 0; i < numIntsPerShift; ++i) {
      tmp[i] = ints[idx++] << shift;
    }
    for (shift = shift - bitsPerValue; shift >= 0; shift -= bitsPerValue) {
      for (int i = 0; i < numIntsPerShift; ++i) {
        tmp[i] |= ints[idx++] << shift;
      }
    }

    final int remainingBitsPerInt = shift + bitsPerValue;
    final int maskRemainingBitsPerInt;
    if (primitiveSize == 8) {
      maskRemainingBitsPerInt = MASKS8[remainingBitsPerInt];
    } else if (primitiveSize == 16) {
      maskRemainingBitsPerInt = MASKS16[remainingBitsPerInt];
    } else {
      maskRemainingBitsPerInt = MASKS32[remainingBitsPerInt];
    }

    int tmpIdx = 0;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < numInts) {
      if (remainingBitsPerValue >= remainingBitsPerInt) {
        remainingBitsPerValue -= remainingBitsPerInt;
        tmp[tmpIdx++] |= (ints[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerInt;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final int mask1, mask2;
        if (primitiveSize == 8) {
          mask1 = MASKS8[remainingBitsPerValue];
          mask2 = MASKS8[remainingBitsPerInt - remainingBitsPerValue];
        } else if (primitiveSize == 16) {
          mask1 = MASKS16[remainingBitsPerValue];
          mask2 = MASKS16[remainingBitsPerInt - remainingBitsPerValue];
        } else {
          mask1 = MASKS32[remainingBitsPerValue];
          mask2 = MASKS32[remainingBitsPerInt - remainingBitsPerValue];
        }
        tmp[tmpIdx] |= (ints[idx++] & mask1) << (remainingBitsPerInt - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerInt + remainingBitsPerValue;
        tmp[tmpIdx++] |= (ints[idx] >>> remainingBitsPerValue) & mask2;
      }
    }

    for (int i = 0; i < numIntsPerShift; ++i) {
      out.writeInt(tmp[i]);
    }
  }

  /** Number of bytes required to encode 128 integers of {@code bitsPerValue} bits per value. */
  static int numBytes(int bitsPerValue) {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  static void decodeSlow(int bitsPerValue, PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    final int numInts = bitsPerValue << 2;
    final int mask = MASKS32[bitsPerValue];
    pdu.splitInts(numInts, ints, 32 - bitsPerValue, 32, mask, tmp, 0, -1);
    final int remainingBitsPerInt = 32 - bitsPerValue;
    final int mask32RemainingBitsPerInt = MASKS32[remainingBitsPerInt];
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerInt;
    for (int intsIdx = numInts; intsIdx < BLOCK_SIZE; ++intsIdx) {
      int b = bitsPerValue - remainingBits;
      int l = (tmp[tmpIdx++] & MASKS32[remainingBits]) << b;
      while (b >= remainingBitsPerInt) {
        b -= remainingBitsPerInt;
        l |= (tmp[tmpIdx++] & mask32RemainingBitsPerInt) << b;
      }
      if (b > 0) {
        l |= (tmp[tmpIdx] >>> (remainingBitsPerInt - b)) & MASKS32[b];
        remainingBits = remainingBitsPerInt - b;
      } else {
        remainingBits = remainingBitsPerInt;
      }
      ints[intsIdx] = l;
    }
  }

"""

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
  next_primitive = 32
  if bpv <= 8:
    next_primitive = 8
  elif bpv <= 16:
    next_primitive = 16
  if bpv == next_primitive:
    f.write('  static void decode%d(PostingDecodingUtil pdu, int[] ints) throws IOException {\n' %bpv)
    f.write('    pdu.in.readInts(ints, 0, %d);\n' %(bpv*4))
  else:
    num_values_per_int = 32 / next_primitive
    remaining_bits = next_primitive % bpv
    num_iters = (next_primitive - 1) // bpv
    o = 4 * bpv * num_iters
    if remaining_bits == 0:
      f.write('  static void decode%d(PostingDecodingUtil pdu, int[] ints) throws IOException {\n' %bpv)
      f.write('    pdu.splitInts(%d, ints, %d, %d, MASK%d_%d, ints, %d, MASK%d_%d);\n' %(bpv*4, next_primitive - bpv, bpv, next_primitive, bpv, o, next_primitive, next_primitive - num_iters * bpv))
    else:
      f.write('  static void decode%d(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {\n' %bpv)
      f.write('    pdu.splitInts(%d, ints, %d, %d, MASK%d_%d, tmp, 0, MASK%d_%d);\n' %(bpv*4, next_primitive - bpv, bpv, next_primitive, bpv, next_primitive, next_primitive - num_iters * bpv))
      writeRemainder(bpv, next_primitive, remaining_bits, o, 128/num_values_per_int - o, f)
  f.write('  }\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  for primitive_size in PRIMITIVE_SIZE:
    f.write('  static final int[] MASKS%d = new int[%d];\n' %(primitive_size, primitive_size))
  f.write('\n')
  f.write('  static {\n')
  for primitive_size in PRIMITIVE_SIZE:
    f.write('    for (int i = 0; i < %d; ++i) {\n' %primitive_size)
    f.write('      MASKS%d[i] = mask%d(i);\n' %(primitive_size, primitive_size))
    f.write('    }\n')
  f.write('  }')
  f.write("""
  // mark values in array as final ints to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
""")
  for primitive_size in PRIMITIVE_SIZE:
    for bpv in range(1, min(MAX_SPECIALIZED_BITS_PER_VALUE + 1, primitive_size)):
      f.write('  static final int MASK%d_%d = MASKS%d[%d];\n' %(primitive_size, bpv, primitive_size, bpv))

  f.write("""
  /** Decode 128 integers into {@code ints}. */
  void decode(int bitsPerValue, PostingDecodingUtil pdu, int[] ints) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    next_primitive = 32
    if bpv <= 8:
      next_primitive = 8
    elif bpv <= 16:
      next_primitive = 16
    f.write('      case %d:\n' %bpv)
    if next_primitive % bpv == 0:
      f.write('        decode%d(pdu, ints);\n' %bpv)
    else:
      f.write('        decode%d(pdu, tmp, ints);\n' %bpv)
    if next_primitive != 32:
      f.write('        expand%d(ints);\n' %next_primitive)
    f.write('        break;\n')
  f.write('      default:\n')
  f.write('        decodeSlow(bitsPerValue, pdu, tmp, ints);\n')
  f.write('        break;\n')
  f.write('    }\n')
  f.write('  }\n')

  for i in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    writeDecode(i, f)
    if i < MAX_SPECIALIZED_BITS_PER_VALUE:
      f.write('\n')

  f.write('}\n')
