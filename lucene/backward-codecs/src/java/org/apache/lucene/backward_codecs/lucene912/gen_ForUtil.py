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

MAX_SPECIALIZED_BITS_PER_VALUE = 24
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
package org.apache.lucene.backward_codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/
 * Encodes multiple integers in a long to get SIMD-like speedups.
 * If bitsPerValue &lt;= 8 then we pack 8 ints per long
 * else if bitsPerValue &lt;= 16 we pack 4 ints per long
 * else we pack 2 ints per long
 */
final class ForUtil {

  public static final int BLOCK_SIZE = 128;
  static final int BLOCK_SIZE_LOG2 = 7;

  static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  static long expandMask8(long mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  static long mask8(int bitsPerValue) {
    return expandMask8((1L << bitsPerValue) - 1);
  }

  static void expand8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 56) & 0xFFL;
      arr[16 + i] = (l >>> 48) & 0xFFL;
      arr[32 + i] = (l >>> 40) & 0xFFL;
      arr[48 + i] = (l >>> 32) & 0xFFL;
      arr[64 + i] = (l >>> 24) & 0xFFL;
      arr[80 + i] = (l >>> 16) & 0xFFL;
      arr[96 + i] = (l >>> 8) & 0xFFL;
      arr[112 + i] = l & 0xFFL;
    }
  }

  static void collapse8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      arr[i] =
          (arr[i] << 56)
              | (arr[16 + i] << 48)
              | (arr[32 + i] << 40)
              | (arr[48 + i] << 32)
              | (arr[64 + i] << 24)
              | (arr[80 + i] << 16)
              | (arr[96 + i] << 8)
              | arr[112 + i];
    }
  }

  static void expand16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[32 + i] = (l >>> 32) & 0xFFFFL;
      arr[64 + i] = (l >>> 16) & 0xFFFFL;
      arr[96 + i] = l & 0xFFFFL;
    }
  }

  static void collapse16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] = (arr[i] << 48) | (arr[32 + i] << 32) | (arr[64 + i] << 16) | arr[96 + i];
    }
  }

  static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }

  static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64 + i];
    }
  }

  private final long[] tmp = new long[BLOCK_SIZE / 2];

  /** Encode 128 integers from {@code longs} into {@code out}. */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    final int nextPrimitive;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      collapse8(longs);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      collapse16(longs);
    } else {
      nextPrimitive = 32;
      collapse32(longs);
    }
    encode(longs, bitsPerValue, nextPrimitive, out, tmp);
  }

  static void encode(long[] longs, int bitsPerValue, int primitiveSize, DataOutput out, long[] tmp) throws IOException {
    final int numLongs = BLOCK_SIZE * primitiveSize / Long.SIZE;

    final int numLongsPerShift = bitsPerValue * 2;
    int idx = 0;
    int shift = primitiveSize - bitsPerValue;
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
    if (primitiveSize == 8) {
      maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];
    } else if (primitiveSize == 16) {
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
        if (primitiveSize == 8) {
          mask1 = MASKS8[remainingBitsPerValue];
          mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        } else if (primitiveSize == 16) {
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
  static int numBytes(int bitsPerValue) {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  static void decodeSlow(int bitsPerValue, IndexInput in, long[] tmp, long[] longs)
      throws IOException {
    final int numLongs = bitsPerValue << 1;
    final long mask = MASKS32[bitsPerValue];
    splitLongs(in, numLongs, longs, 32 - bitsPerValue, 32, mask, tmp, 0, -1L);
    final int remainingBitsPerLong = 32 - bitsPerValue;
    final long mask32RemainingBitsPerLong = MASKS32[remainingBitsPerLong];
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerLong;
    for (int longsIdx = numLongs; longsIdx < BLOCK_SIZE / 2; ++longsIdx) {
      int b = bitsPerValue - remainingBits;
      long l = (tmp[tmpIdx++] & MASKS32[remainingBits]) << b;
      while (b >= remainingBitsPerLong) {
        b -= remainingBitsPerLong;
        l |= (tmp[tmpIdx++] & mask32RemainingBitsPerLong) << b;
      }
      if (b > 0) {
        l |= (tmp[tmpIdx] >>> (remainingBitsPerLong - b)) & MASKS32[b];
        remainingBits = remainingBitsPerLong - b;
      } else {
        remainingBits = remainingBitsPerLong;
      }
      longs[longsIdx] = l;
    }
  }

  static void splitLongs(
      IndexInput in, int count, long[] b, int bShift, int dec, long bMask, long[] c, int cIndex, long cMask)
      throws IOException {
    // takes advantage of the C2 compiler's loop unrolling and auto-vectorization.
    in.readLongs(c, cIndex, count);
    int maxIter = (bShift - 1) / dec;
    for (int i = 0; i < count; ++i) {
      for (int j = 0; j <= maxIter; ++j) {
        b[count * j + i] = (c[cIndex + i] >>> (bShift - j * dec)) & bMask;
      }
      c[cIndex + i] &= cMask;
    }
  }

"""

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
  next_primitive = 32
  if bpv <= 8:
    next_primitive = 8
  elif bpv <= 16:
    next_primitive = 16
  if bpv == next_primitive:
    f.write('  static void decode%d(IndexInput in, long[] longs) throws IOException {\n' %bpv)
    f.write('    in.readLongs(longs, 0, %d);\n' %(bpv*2))
  else:
    num_values_per_long = 64 / next_primitive
    remaining_bits = next_primitive % bpv
    num_iters = (next_primitive - 1) // bpv
    o = 2 * bpv * num_iters
    if remaining_bits == 0:
      f.write('  static void decode%d(IndexInput in, long[] longs) throws IOException {\n' %bpv)
      f.write('    splitLongs(in, %d, longs, %d, %d, MASK%d_%d, longs, %d, MASK%d_%d);\n' %(bpv*2, next_primitive - bpv, bpv, next_primitive, bpv, o, next_primitive, next_primitive - num_iters * bpv))
    else:
      f.write('  static void decode%d(IndexInput in, long[] tmp, long[] longs) throws IOException {\n' %bpv)
      f.write('    splitLongs(in, %d, longs, %d, %d, MASK%d_%d, tmp, 0, MASK%d_%d);\n' %(bpv*2, next_primitive - bpv, bpv, next_primitive, bpv, next_primitive, next_primitive - num_iters * bpv))
      writeRemainder(bpv, next_primitive, remaining_bits, o, 128/num_values_per_long - o, f)
  f.write('  }\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  for primitive_size in PRIMITIVE_SIZE:
    f.write('  static final long[] MASKS%d = new long[%d];\n' %(primitive_size, primitive_size))
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
    for bpv in range(1, min(MAX_SPECIALIZED_BITS_PER_VALUE + 1, primitive_size)):
      f.write('  static final long MASK%d_%d = MASKS%d[%d];\n' %(primitive_size, bpv, primitive_size, bpv))

  f.write("""
  /** Decode 128 integers into {@code longs}. */
  void decode(int bitsPerValue, IndexInput in, long[] longs) throws IOException {
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
      f.write('        decode%d(in, longs);\n' %bpv)
    else:
      f.write('        decode%d(in, tmp, longs);\n' %bpv)
    f.write('        expand%d(longs);\n' %next_primitive)
    f.write('        break;\n')
  f.write('      default:\n')
  f.write('        decodeSlow(bitsPerValue, in, tmp, longs);\n')
  f.write('        expand32(longs);\n')
  f.write('        break;\n')
  f.write('    }\n')
  f.write('  }\n')

  for i in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    writeDecode(i, f)
    if i < MAX_SPECIALIZED_BITS_PER_VALUE:
      f.write('\n')

  f.write('}\n')
