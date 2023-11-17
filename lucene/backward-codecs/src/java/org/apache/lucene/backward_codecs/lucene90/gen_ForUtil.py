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
package org.apache.lucene.backward_codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
final class ForUtil {

  static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_LOG2 = 7;

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  private static long expandMask8(long mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  private static long mask8(int bitsPerValue) {
    return expandMask8((1L << bitsPerValue) - 1);
  }

  private static void expand8(long[] arr) {
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

  private static void expand8To32(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 24) & 0x000000FF000000FFL;
      arr[16 + i] = (l >>> 16) & 0x000000FF000000FFL;
      arr[32 + i] = (l >>> 8) & 0x000000FF000000FFL;
      arr[48 + i] = l & 0x000000FF000000FFL;
    }
  }

  private static void collapse8(long[] arr) {
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

  private static void expand16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[32 + i] = (l >>> 32) & 0xFFFFL;
      arr[64 + i] = (l >>> 16) & 0xFFFFL;
      arr[96 + i] = l & 0xFFFFL;
    }
  }

  private static void expand16To32(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 16) & 0x0000FFFF0000FFFFL;
      arr[32 + i] = l & 0x0000FFFF0000FFFFL;
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

  private final long[] tmp = new long[BLOCK_SIZE / 2];

  /** Encode 128 integers from {@code longs} into {@code out}. */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    final int nextPrimitive;
    final int numLongs;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      numLongs = BLOCK_SIZE / 8;
      collapse8(longs);
    } else if (bitsPerValue <= 16) {
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
    if (nextPrimitive == 8) {
      maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];
    } else if (nextPrimitive == 16) {
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
        if (nextPrimitive == 8) {
          mask1 = MASKS8[remainingBitsPerValue];
          mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        } else if (nextPrimitive == 16) {
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

  private static void decodeSlow(int bitsPerValue, DataInput in, long[] tmp, long[] longs)
      throws IOException {
    final int numLongs = bitsPerValue << 1;
    in.readLongs(tmp, 0, numLongs);
    final long mask = MASKS32[bitsPerValue];
    int longsIdx = 0;
    int shift = 32 - bitsPerValue;
    for (; shift >= 0; shift -= bitsPerValue) {
      shiftLongs(tmp, numLongs, longs, longsIdx, shift, mask);
      longsIdx += numLongs;
    }
    final int remainingBitsPerLong = shift + bitsPerValue;
    final long mask32RemainingBitsPerLong = MASKS32[remainingBitsPerLong];
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerLong;
    for (; longsIdx < BLOCK_SIZE / 2; ++longsIdx) {
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

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2 compiler, which
   * generates SIMD instructions for it in order to shift multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi + i] = (a[i] >>> shift) & mask;
    }
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
  if bpv <= 8:
    next_primitive = 8
  elif bpv <= 16:
    next_primitive = 16
  f.write('  private static void decode%d(DataInput in, long[] tmp, long[] longs) throws IOException {\n' %bpv)
  num_values_per_long = 64 / next_primitive
  if bpv == next_primitive:
    f.write('    in.readLongs(longs, 0, %d);\n' %(bpv*2))
  else:
    f.write('    in.readLongs(tmp, 0, %d);\n' %(bpv*2))
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
    for bpv in range(1, min(MAX_SPECIALIZED_BITS_PER_VALUE + 1, primitive_size)):
      if bpv * 2 != primitive_size or primitive_size == 8:
        f.write('  private static final long MASK%d_%d = MASKS%d[%d];\n' %(primitive_size, bpv, primitive_size, bpv))

  f.write("""
  /** Decode 128 integers into {@code longs}. */
  void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    next_primitive = 32
    if bpv <= 8:
      next_primitive = 8
    elif bpv <= 16:
      next_primitive = 16
    f.write('      case %d:\n' %bpv)
    f.write('        decode%d(in, tmp, longs);\n' %bpv)
    f.write('        expand%d(longs);\n' %next_primitive)
    f.write('        break;\n')
  f.write('      default:\n')
  f.write('        decodeSlow(bitsPerValue, in, tmp, longs);\n')
  f.write('        expand32(longs);\n')
  f.write('        break;\n')
  f.write('    }\n')
  f.write('  }\n')

  f.write("""
  /**
   * Decodes 128 integers into 64 {@code longs} such that each long contains two values, each
   * represented with 32 bits. Values [0..63] are encoded in the high-order bits of {@code longs}
   * [0..63], and values [64..127] are encoded in the low-order bits of {@code longs} [0..63]. This
   * representation may allow subsequent operations to be performed on two values at a time.
   */
  void decodeTo32(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
  for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    next_primitive = 32
    if bpv <= 8:
      next_primitive = 8
    elif bpv <= 16:
      next_primitive = 16
    f.write('      case %d:\n' %bpv)
    f.write('        decode%d(in, tmp, longs);\n' %bpv)
    if next_primitive <= 16:
      f.write('        expand%dTo32(longs);\n' %next_primitive)
    f.write('        break;\n')
  f.write('      default:\n')
  f.write('        decodeSlow(bitsPerValue, in, tmp, longs);\n')
  f.write('        break;\n')
  f.write('    }\n')
  f.write('  }\n')

  f.write('\n')
  for i in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    writeDecode(i, f)
    if i < MAX_SPECIALIZED_BITS_PER_VALUE:
      f.write('\n')

  f.write('}\n')
