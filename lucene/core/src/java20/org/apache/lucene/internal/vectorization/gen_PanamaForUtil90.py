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


"""Code generation for PanamaForUtil90.java"""

MAX_SPECIALIZED_BITS_PER_VALUE = 24
OUTPUT_FILE = "PanamaForUtil90.java"
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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
final class PanamaForUtil90 implements ForUtil90 {

  private static final VectorSpecies<Long> LANESIZE_2 = LongVector.SPECIES_128;

  private static final VectorSpecies<Long> LANESIZE_4 = LongVector.SPECIES_256;

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

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }

  private final long[] tmp = new long[BLOCK_SIZE / 2];

  private static void encode1(long[] input, int bitsPerValue, long[] out) {
    LongVector outputVector = LANESIZE_2.zero().reinterpretAsLongs();
    long mask = (1L << 1) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_2, input, 0).and(mask);
    outputVector = inputVector.lanewise(VectorOperators.LSHL, 63);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 2).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 62).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 4).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 61).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 6).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 60).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 8).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 59).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 10).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 58).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 12).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 57).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 14).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 56).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 16).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 55).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 18).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 54).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 20).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 53).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 22).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 52).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 24).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 51).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 26).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 50).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 28).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 49).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 30).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 48).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 32).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 47).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 34).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 46).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 36).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 45).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 38).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 44).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 40).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 43).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 42).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 42).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 44).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 41).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 46).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 40).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 48).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 39).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 50).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 38).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 52).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 37).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 54).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 36).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 56).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 35).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 58).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 34).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 60).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 33).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 62).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 32).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 64).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 31).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 66).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 30).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 68).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 29).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 70).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 28).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 72).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 27).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 74).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 26).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 76).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 25).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 78).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 24).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 80).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 23).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 82).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 22).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 84).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 21).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 86).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 20).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 88).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 19).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 90).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 18).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 92).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 17).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 94).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 16).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 96).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 15).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 98).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 14).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 100).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 13).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 102).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 12).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 104).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 11).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 106).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 10).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 108).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 9).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 110).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 8).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 112).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 7).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 114).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 6).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 116).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 5).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 118).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 4).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 120).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 3).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 122).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 2).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 124).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 1).or(outputVector);
    inputVector = LongVector.fromArray(LANESIZE_2, input, 126).and(mask);

    outputVector = inputVector.lanewise(VectorOperators.LSHL, 0).or(outputVector);
    outputVector.intoArray(out, 0);
  }

   @Override
   public void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    switch (bitsPerValue) {
      case 1 -> encode1(longs, bitsPerValue, tmp);
      case 2 -> encode2(longs, bitsPerValue, tmp);
      case 3 -> encode3(longs, bitsPerValue, tmp);
      case 4 -> encode4(longs, bitsPerValue, tmp);
      case 5 -> encode5(longs, bitsPerValue, tmp);
      case 6 -> encode6(longs, bitsPerValue, tmp);
      case 7 -> encode7(longs, bitsPerValue, tmp);
      case 8 -> encode8(longs, bitsPerValue, tmp);
      case 9 -> encode9(longs, bitsPerValue, tmp);
      case 10 -> encode10(longs, bitsPerValue, tmp);
      case 11 -> encode11(longs, bitsPerValue, tmp);
      case 12 -> encode12(longs, bitsPerValue, tmp);
      case 13 -> encode13(longs, bitsPerValue, tmp);
      case 14 -> encode14(longs, bitsPerValue, tmp);
      case 15 -> encode15(longs, bitsPerValue, tmp);
      case 16 -> encode16(longs, bitsPerValue, tmp);
      case 17 -> encode17(longs, bitsPerValue, tmp);
      case 18 -> encode18(longs, bitsPerValue, tmp);
      case 19 -> encode19(longs, bitsPerValue, tmp);
      case 20 -> encode20(longs, bitsPerValue, tmp);
      case 21 -> encode21(longs, bitsPerValue, tmp);
      case 22 -> encode22(longs, bitsPerValue, tmp);
      case 23 -> encode23(longs, bitsPerValue, tmp);
      case 24 -> encode24(longs, bitsPerValue, tmp);
      case 25 -> encode25(longs, bitsPerValue, tmp);
      case 26 -> encode26(longs, bitsPerValue, tmp);
      case 27 -> encode27(longs, bitsPerValue, tmp);
      case 28 -> encode28(longs, bitsPerValue, tmp);
      case 29 -> encode29(longs, bitsPerValue, tmp);
      case 30 -> encode30(longs, bitsPerValue, tmp);
      case 31 -> encode31(longs, bitsPerValue, tmp);
      case 32 -> encode32(longs, bitsPerValue, tmp);
    }

    for (int i = 0; i < 2 * bitsPerValue; ++i) {
      // Java longs are big endian and we want to read little endian longs, so we need to reverse
      // bytes
      long l = tmp[i];
      out.writeLong(l);
    }
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

    f.write('    shiftLongs(tmp, %d, tmp, 0, 0, MASK%d_%d);\n' %
            (iteration * num_longs, next_primitive, remaining_bits_per_long))
    f.write('    for (int iter = 0, tmpIdx = 0, longsIdx = %d; iter < %d; ++iter, tmpIdx += %d, longsIdx += %d) {\n' % (
        o, iteration, num_longs, num_values))
    tmp_idx = 0
    b = bpv
    b -= remaining_bits_per_long
    f.write('      long l0 = tmp[tmpIdx + %d] << %d;\n' % (tmp_idx, b))
    tmp_idx += 1
    while b >= remaining_bits_per_long:
        b -= remaining_bits_per_long
        f.write('      l0 |= tmp[tmpIdx + %d] << %d;\n' % (tmp_idx, b))
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
    f.write('    for (int iter = 0, tmpIdx = 0, longsIdx = %d; iter < %d; ++iter, tmpIdx += %d, longsIdx += %d) {\n' % (
        o, iteration, num_longs, num_values))
    i = 0
    remaining_bits = 0
    tmp_idx = 0
    for i in range(int(num_values)):
        b = bpv
        if remaining_bits == 0:
            b -= remaining_bits_per_long
            f.write('      long l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' % (
                i, tmp_idx, next_primitive, remaining_bits_per_long, b))
        else:
            b -= remaining_bits
            f.write('      long l%d = (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' % (
                i, tmp_idx, next_primitive, remaining_bits, b))
        tmp_idx += 1
        while b >= remaining_bits_per_long:
            b -= remaining_bits_per_long
            f.write('      l%d |= (tmp[tmpIdx + %d] & MASK%d_%d) << %d;\n' %
                    (i, tmp_idx, next_primitive, remaining_bits_per_long, b))
            tmp_idx += 1
        if b > 0:
            f.write('      l%d |= (tmp[tmpIdx + %d] >>> %d) & MASK%d_%d;\n' %
                    (i, tmp_idx, remaining_bits_per_long-b, next_primitive, b))
            remaining_bits = remaining_bits_per_long-b
        f.write('      longs[longsIdx + %d] = l%d;\n' % (i, i))
    f.write('    }\n')


def writeDecode(bpv, f):
    next_primitive = 32
    if bpv <= 8:
        next_primitive = 8
    elif bpv <= 16:
        next_primitive = 16
    f.write(
        '  private static void decode%d(DataInput in, long[] tmp, long[] longs) throws IOException {\n' % bpv)
    num_values_per_long = 64 / next_primitive
    if bpv == next_primitive:
        f.write('    in.readLongs(longs, 0, %d);\n' % (bpv*2))
    else:
        f.write('    in.readLongs(tmp, 0, %d);\n' % (bpv*2))
        shift = next_primitive - bpv
        o = 0
        while shift >= 0:
            f.write('    shiftLongs(tmp, %d, longs, %d, %d, MASK%d_%d);\n' %
                    (bpv*2, o, shift, next_primitive, bpv))
            o += bpv*2
            shift -= bpv
        if shift + bpv > 0:
            if bpv % (next_primitive % bpv) == 0:
                writeRemainderWithSIMDOptimize(
                    bpv, next_primitive, shift + bpv, o, 128/num_values_per_long - o, f)
            else:
                writeRemainder(bpv, next_primitive, shift + bpv,
                               o, 128/num_values_per_long - o, f)
    f.write('  }\n')


def calculateCompressedFormat(bpv, next_primitive):
    """
    Calculate the compress format.
    Currently it's a rough compress, we will compress it more later.
    For example, if we have 128 values and bpv = 3, we could roughly compress it like:
    0  16 ... ... 112
    1  17 ... ...  .
    2  18 ... ...  .
    .  .   .   .   .
    .  .   .   .   .
    15 31 ... ... 127
    """
    # Initialize a list to store the compressed format
    compressed_format = [[] for _ in range(2 * next_primitive)]
    remaining = next_primitive

    for num in range(128):
        compressed_format[num % (2 * next_primitive)].append(num)

    remaining -= bpv
    return compressed_format, remaining


def compactCompressedFormat(compressed_format, bpv, next_primitive, remaining):
    """
    Suppose we have bpv = 3 and 128 values.
    We will use 8 bits to compress it, and 2 values could fit in 8 bits.
    We have already used 3 bits before, so we still can use 5 bits to compress it.
    So we will try to compress the next_line data into a line which is lower than compress_bound
    until remaining < bpv or we have compressed all data.
    """

    # Initialize variables to compress the next line into compress_bound
    next_line = 2 * bpv
    line_num = 0

    while next_line < 2 * next_primitive and remaining >= bpv:
        pos = int((next_primitive - remaining) / bpv)
        adjcent = pos + 1
        for val in compressed_format[next_line]:
            compressed_format[line_num % (2 * bpv)].insert(pos, val)
            pos += adjcent
        line_num += 1
        next_line += 1
        if line_num % (2 * bpv) == 0:
            remaining -= bpv
    return compressed_format, remaining, next_line


def compressData(compressed_format, bpv, next_primitive, f):
    # Define templates for initializing, resetting, and writing output vectors
    init_template = """
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << %d) - 1;
                                
    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, %d).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, %d));\n"""

    reset_template = """
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();
                                
    inputVector = LongVector.fromArray(LANESIZE_4, input, %d).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, %d));\n"""

    program_template = """
    inputVector = LongVector.fromArray(LANESIZE_4, input, %d).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, %d));\n"""

    output_template = """
    outputVector.intoArray(out, %d);\n"""

    # Compress the data using the compressed format
    first = True
    for iter in range(0, 2 * bpv - 3, 4):
        compress_line = compressed_format[iter]
        block_remaining = next_primitive - bpv
        times = 0

        # Write the initialization or reset template to the file depending on whether this is the first iteration
        if first:
            f.write(init_template % (bpv, compress_line[0], 64 - bpv))
            first = False
        else:
            f.write(reset_template % (compress_line[0], 64 - bpv))

        # Write the program template to the file for each value in the compressed line
        for i in range(1, len(compress_line)):
            if block_remaining - bpv >= 0:
                f.write(program_template % (
                    compress_line[i], 64 - bpv - times * next_primitive - (next_primitive - block_remaining)))
                block_remaining -= bpv
            else:
                times += 1
                block_remaining = next_primitive - bpv
                f.write(program_template %
                        (compress_line[i], 64 - bpv - times * next_primitive))
        f.write(output_template % (iter))


def compressRemainingData(compressed_format, bpv, next_primitive, next_line, f):
  # Define templates for initializing and writing output vectors
    init_template = """
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();
              
    inputVector = LongVector.fromArray(LANESIZE_4, input, %d).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, %d));\n
  """

    program_template = """
    inputVector = LongVector.fromArray(LANESIZE_4, input, %d).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, %d));
  """

    output_template = """
    outputVector.intoArray(out, %d);"""

    # Compress any remaining data that is not in the compress_bound
    start_index = next_line
    while start_index + 4 <= 2 * next_primitive:
        compress_line = compressed_format[start_index]
        f.write(init_template % (compress_line[0], 64 - next_primitive))
        for i in range(1, len(compress_line)):
            f.write(program_template %
                    (compress_line[i], 64 - next_primitive * (i + 1)))
        f.write(output_template % (start_index))
        start_index += 4
    return start_index


def writeEncode(bpv, f):
    # Write the method signature to the file
    f.write(
        '  private static void encode%d(long[] input, int bitsPerValue, long[] out) throws IOException {' % bpv)

    # Determine the next power of 2 which is larger than or equal to bpv to compress the data
    next_primitive = 32 if bpv > 16 else 16 if bpv > 8 else 8

    compressed_format, remaining = calculateCompressedFormat(
        bpv, next_primitive)

    # Calculate the number of longs we need to compress 128 values
    # compress_bound = 128 * bpv / 64 = 2 * bpv
    compress_bound = 2 * bpv

    compressed_format, remaining, next_line = compactCompressedFormat(
        compressed_format, bpv, next_primitive, remaining)

    # Compress any remaining data that is in the compress_bound
    compressData(compressed_format, bpv, next_primitive, f)
    compress_partial_data(bpv, compressed_format,
                          next_primitive, f, bpv // 2 * 4, 2 * bpv)

    # Compress all remaining data that is not in the compress_bound
    start_index = compressRemainingData(
        compressed_format, bpv, next_primitive, next_line, f)
    compress_partial_data(next_primitive, compressed_format,
                          next_primitive, f, start_index, 2 * next_primitive)

    # Write the remaining code to the file
    end = """
    final int remainingBitsPerLong = %d;
    final long maskRemainingBitsPerLong = MASKS%d[remainingBitsPerLong];
            
    int tmpIdx = 0;
    int idx = %d;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < %d) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS%d[remainingBitsPerValue];
        mask2 = MASKS%d[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
"""

    # If we have data which is out of compress_bound, we need to compress them into compress_bound
    # For example, if we have bpv = 3 and 128 values, we have already compressed 2 values into 1 byte,
    # we still have 2 bits remaining, so we could use 2 lines to compress 1 value.
    if next_line < 2 * next_primitive:
        f.write(end % (next_primitive % bpv, next_primitive, next_line,
                2 * next_primitive, next_primitive, next_primitive))

    # Write the end of the method to the file
    f.write("""  }\n\n""")


def compress_partial_data(bpv, compressed_format, next_primitive, f, start, end, step=1):
    """
      Suppose we have bpv = 3 and 128 values 
      we should compress it into 6 long, and we compressed it use simd
      which means we will output 4 long each time
      if the remaining line is lower than 4, we will use scalar code to compress it
    """
    init_template = """
    out[%d] =
        """

    program_template = """(input[%d] << %d)"""

    output_template = ";\n"

    for i in range(start, end, step):
        compress_line = compressed_format[i]
        block_remaining = next_primitive
        times = 0
        f.write(init_template % (i))
        for iter in range(0, len(compress_line)):
            if block_remaining - bpv >= 0:
                f.write(program_template % (
                    compress_line[iter], 64 - bpv - times * next_primitive - (next_primitive - block_remaining)))
                block_remaining -= bpv
            else:
                times += 1
                block_remaining = next_primitive - bpv
                f.write(program_template %
                        (compress_line[iter], 64 - bpv - times * next_primitive))

            if iter != len(compress_line) - 1:
                f.write("""
            | """)

        f.write(output_template)


if __name__ == '__main__':
    f = open(OUTPUT_FILE, 'w')
    f.write(HEADER)
    for i in range(2, 33):
        writeEncode(i, f)
    for primitive_size in PRIMITIVE_SIZE:
        f.write('  private static final long[] MASKS%d = new long[%d];\n' %
                (primitive_size, primitive_size))
    f.write('\n')
    f.write('  static {\n')
    for primitive_size in PRIMITIVE_SIZE:
        f.write('    for (int i = 0; i < %d; ++i) {\n' % primitive_size)
        f.write('      MASKS%d[i] = mask%d(i);\n' %
                (primitive_size, primitive_size))
        f.write('    }\n')
    f.write('  }')
    f.write("""
  // mark values in array as final longs to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
""")
    for primitive_size in PRIMITIVE_SIZE:
        for bpv in range(1, min(MAX_SPECIALIZED_BITS_PER_VALUE + 1, primitive_size)):
            if bpv * 2 != primitive_size or primitive_size == 8:
                f.write('  private static final long MASK%d_%d = MASKS%d[%d];\n' % (
                    primitive_size, bpv, primitive_size, bpv))

    f.write("""
  /** Decode 128 integers into {@code longs}. */
  @Override
  public void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
    for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
        next_primitive = 32
        if bpv <= 8:
            next_primitive = 8
        elif bpv <= 16:
            next_primitive = 16
        f.write('      case %d:\n' % bpv)
        f.write('        decode%d(in, tmp, longs);\n' % bpv)
        f.write('        expand%d(longs);\n' % next_primitive)
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
  @Override
  public void decodeTo32(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
    for bpv in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
        next_primitive = 32
        if bpv <= 8:
            next_primitive = 8
        elif bpv <= 16:
            next_primitive = 16
        f.write('      case %d:\n' % bpv)
        f.write('        decode%d(in, tmp, longs);\n' % bpv)
        if next_primitive <= 16:
            f.write('        expand%dTo32(longs);\n' % next_primitive)
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