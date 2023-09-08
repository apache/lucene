// This file has been automatically generated, DO NOT EDIT

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

  private static void encode2(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 2) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 62));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 60));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 58));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 30));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 28));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 26));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 0);
  }

  private static void encode3(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 3) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 61));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 6).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 58));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 22).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 38).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 29));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 70).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 26));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 86).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 102).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 0);

    out[4] =
        (input[4] << 61)
            | (input[10] << 58)
            | (input[20] << 53)
            | (input[26] << 50)
            | (input[36] << 45)
            | (input[42] << 42)
            | (input[52] << 37)
            | (input[58] << 34)
            | (input[68] << 29)
            | (input[74] << 26)
            | (input[84] << 21)
            | (input[90] << 18)
            | (input[100] << 13)
            | (input[106] << 10)
            | (input[116] << 5)
            | (input[122] << 2);

    out[5] =
        (input[5] << 61)
            | (input[11] << 58)
            | (input[21] << 53)
            | (input[27] << 50)
            | (input[37] << 45)
            | (input[43] << 42)
            | (input[53] << 37)
            | (input[59] << 34)
            | (input[69] << 29)
            | (input[75] << 26)
            | (input[85] << 21)
            | (input[91] << 18)
            | (input[101] << 13)
            | (input[107] << 10)
            | (input[117] << 5)
            | (input[123] << 2);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 12);
    final int remainingBitsPerLong = 2;
    final long maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 12;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 16) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS8[remainingBitsPerValue];
        mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode4(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 4) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 60));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 28));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 60));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 28));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 4);
  }

  private static void encode5(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 5) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 59));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 27));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 59));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 27));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 4);

    out[8] =
        (input[8] << 59)
            | (input[24] << 51)
            | (input[40] << 43)
            | (input[56] << 35)
            | (input[72] << 27)
            | (input[88] << 19)
            | (input[104] << 11)
            | (input[120] << 3);

    out[9] =
        (input[9] << 59)
            | (input[25] << 51)
            | (input[41] << 43)
            | (input[57] << 35)
            | (input[73] << 27)
            | (input[89] << 19)
            | (input[105] << 11)
            | (input[121] << 3);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 10).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 26).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 42).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 74).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 90).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 106).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 10);
    out[14] =
        (input[14] << 56)
            | (input[30] << 48)
            | (input[46] << 40)
            | (input[62] << 32)
            | (input[78] << 24)
            | (input[94] << 16)
            | (input[110] << 8)
            | (input[126] << 0);

    out[15] =
        (input[15] << 56)
            | (input[31] << 48)
            | (input[47] << 40)
            | (input[63] << 32)
            | (input[79] << 24)
            | (input[95] << 16)
            | (input[111] << 8)
            | (input[127] << 0);

    final int remainingBitsPerLong = 3;
    final long maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 10;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 16) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS8[remainingBitsPerValue];
        mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode6(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 6) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 58));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 26));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 58));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 26));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 58));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 26));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 12);
    final int remainingBitsPerLong = 2;
    final long maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 12;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 16) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS8[remainingBitsPerValue];
        mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode7(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 7) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 57));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 25));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 57));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 25));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 57));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 25));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 8);

    out[12] =
        (input[12] << 57)
            | (input[28] << 49)
            | (input[44] << 41)
            | (input[60] << 33)
            | (input[76] << 25)
            | (input[92] << 17)
            | (input[108] << 9)
            | (input[124] << 1);

    out[13] =
        (input[13] << 57)
            | (input[29] << 49)
            | (input[45] << 41)
            | (input[61] << 33)
            | (input[77] << 25)
            | (input[93] << 17)
            | (input[109] << 9)
            | (input[125] << 1);

    out[14] =
        (input[14] << 56)
            | (input[30] << 48)
            | (input[46] << 40)
            | (input[62] << 32)
            | (input[78] << 24)
            | (input[94] << 16)
            | (input[110] << 8)
            | (input[126] << 0);

    out[15] =
        (input[15] << 56)
            | (input[31] << 48)
            | (input[47] << 40)
            | (input[63] << 32)
            | (input[79] << 24)
            | (input[95] << 16)
            | (input[111] << 8)
            | (input[127] << 0);

    final int remainingBitsPerLong = 1;
    final long maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 14;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 16) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS8[remainingBitsPerValue];
        mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode8(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 8) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 56));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 24));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 12);
  }

  private static void encode9(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 9) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 55));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 23));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 55));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 23));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 55));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 23));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 55));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 23));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 12);

    out[16] = (input[16] << 55) | (input[48] << 39) | (input[80] << 23) | (input[112] << 7);

    out[17] = (input[17] << 55) | (input[49] << 39) | (input[81] << 23) | (input[113] << 7);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 18).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 82).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 18);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 22).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 86).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 22);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 26).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 90).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 26);
    out[30] = (input[30] << 48) | (input[62] << 32) | (input[94] << 16) | (input[126] << 0);

    out[31] = (input[31] << 48) | (input[63] << 32) | (input[95] << 16) | (input[127] << 0);

    final int remainingBitsPerLong = 7;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 18;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode10(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 10) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 54));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 22));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 20);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 24);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 28);
    final int remainingBitsPerLong = 6;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 20;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode11(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 11) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 53));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 21));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 16);

    out[20] = (input[20] << 53) | (input[52] << 37) | (input[84] << 21) | (input[116] << 5);

    out[21] = (input[21] << 53) | (input[53] << 37) | (input[85] << 21) | (input[117] << 5);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 22).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 86).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 22);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 26).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 90).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 26);
    out[30] = (input[30] << 48) | (input[62] << 32) | (input[94] << 16) | (input[126] << 0);

    out[31] = (input[31] << 48) | (input[63] << 32) | (input[95] << 16) | (input[127] << 0);

    final int remainingBitsPerLong = 5;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 22;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode12(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 12) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 52));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 20));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 24);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 28);
    final int remainingBitsPerLong = 4;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 24;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode13(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 13) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 51));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 19));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 20);

    out[24] = (input[24] << 51) | (input[56] << 35) | (input[88] << 19) | (input[120] << 3);

    out[25] = (input[25] << 51) | (input[57] << 35) | (input[89] << 19) | (input[121] << 3);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 26).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 90).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 26);
    out[30] = (input[30] << 48) | (input[62] << 32) | (input[94] << 16) | (input[126] << 0);

    out[31] = (input[31] << 48) | (input[63] << 32) | (input[95] << 16) | (input[127] << 0);

    final int remainingBitsPerLong = 3;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 26;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode14(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 14) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 50));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 18));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 28);
    final int remainingBitsPerLong = 2;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 28;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode15(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 15) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 49));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 17));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 24);

    out[28] = (input[28] << 49) | (input[60] << 33) | (input[92] << 17) | (input[124] << 1);

    out[29] = (input[29] << 49) | (input[61] << 33) | (input[93] << 17) | (input[125] << 1);

    out[30] = (input[30] << 48) | (input[62] << 32) | (input[94] << 16) | (input[126] << 0);

    out[31] = (input[31] << 48) | (input[63] << 32) | (input[95] << 16) | (input[127] << 0);

    final int remainingBitsPerLong = 1;
    final long maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 30;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 32) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS16[remainingBitsPerValue];
        mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode16(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 16) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 48));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 16));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 28);
  }

  private static void encode17(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 17) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 47));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 15));

    outputVector.intoArray(out, 28);

    out[32] = (input[32] << 47) | (input[96] << 15);

    out[33] = (input[33] << 47) | (input[97] << 15);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 34).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 98).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 34);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 38).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 102).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 38);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 42).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 106).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 42);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 46).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 110).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 46);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 50);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 15;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 34;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode18(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 18) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 46));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 14));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 36);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 40);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 44);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 48);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 14;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 36;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode19(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 19) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 45));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 13));

    outputVector.intoArray(out, 32);

    out[36] = (input[36] << 45) | (input[100] << 13);

    out[37] = (input[37] << 45) | (input[101] << 13);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 38).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 102).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 38);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 42).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 106).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 42);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 46).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 110).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 46);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 50);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 13;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 38;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode20(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 20) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 44));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 12));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 40);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 44);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 48);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 12;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 40;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode21(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 21) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 43));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 11));

    outputVector.intoArray(out, 36);

    out[40] = (input[40] << 43) | (input[104] << 11);

    out[41] = (input[41] << 43) | (input[105] << 11);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 42).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 106).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 42);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 46).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 110).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 46);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 50);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 11;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 42;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode22(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 22) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 42));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 10));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 44);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 48);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 10;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 44;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode23(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 23) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 41));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 9));

    outputVector.intoArray(out, 40);

    out[44] = (input[44] << 41) | (input[108] << 9);

    out[45] = (input[45] << 41) | (input[109] << 9);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 46).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 110).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 46);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 50);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 9;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 46;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode24(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 24) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 40));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 8));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 48);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 8;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 48;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode25(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 25) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 39));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 7));

    outputVector.intoArray(out, 44);

    out[48] = (input[48] << 39) | (input[112] << 7);

    out[49] = (input[49] << 39) | (input[113] << 7);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 50).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 114).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 50);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 7;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 50;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode26(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 26) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 38));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 6));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 6;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 52;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode27(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 27) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 37));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 5));

    outputVector.intoArray(out, 48);

    out[52] = (input[52] << 37) | (input[116] << 5);

    out[53] = (input[53] << 37) | (input[117] << 5);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 54).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 118).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 54);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 5;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 54;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode28(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 28) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 36));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 4));

    outputVector.intoArray(out, 52);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);
    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 4;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 56;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode29(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 29) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 35));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 3));

    outputVector.intoArray(out, 52);

    out[56] = (input[56] << 35) | (input[120] << 3);

    out[57] = (input[57] << 35) | (input[121] << 3);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 58).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 122).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 58);
    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 3;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 58;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode30(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 30) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 52);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 34));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 2));

    outputVector.intoArray(out, 56);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
    final int remainingBitsPerLong = 2;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 60;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode31(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 31) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 52);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 33));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 1));

    outputVector.intoArray(out, 56);

    out[60] = (input[60] << 33) | (input[124] << 1);

    out[61] = (input[61] << 33) | (input[125] << 1);

    out[62] = (input[62] << 32) | (input[126] << 0);

    out[63] = (input[63] << 32) | (input[127] << 0);

    final int remainingBitsPerLong = 1;
    final long maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];

    int tmpIdx = 0;
    int idx = 62;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < 64) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;

        mask1 = MASKS32[remainingBitsPerValue];
        mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        out[tmpIdx] |= (out[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        out[tmpIdx++] |= (out[idx] >>> remainingBitsPerValue) & mask2;
      }
    }
  }

  private static void encode32(long[] input, int bitsPerValue, long[] out) throws IOException {
    LongVector outputVector = LANESIZE_4.zero().reinterpretAsLongs();
    long mask = (1L << 32) - 1;

    LongVector inputVector = LongVector.fromArray(LANESIZE_4, input, 0).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 64).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 0);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 4).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 68).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 4);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 8).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 72).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 8);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 12).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 76).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 12);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 16).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 80).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 16);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 20).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 84).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 20);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 24).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 88).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 24);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 28).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 92).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 28);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 32).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 96).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 32);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 36).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 100).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 36);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 40).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 104).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 40);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 44).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 108).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 44);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 48).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 112).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 48);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 52).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 116).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 52);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 56).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 120).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 56);

    outputVector = LANESIZE_4.zero().reinterpretAsLongs();

    inputVector = LongVector.fromArray(LANESIZE_4, input, 60).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 32));

    inputVector = LongVector.fromArray(LANESIZE_4, input, 124).and(mask);
    outputVector = outputVector.or(inputVector.lanewise(VectorOperators.LSHL, 0));

    outputVector.intoArray(out, 60);
  }

  private static final long[] MASKS8 = new long[8];
  private static final long[] MASKS16 = new long[16];
  private static final long[] MASKS32 = new long[32];

  static {
    for (int i = 0; i < 8; ++i) {
      MASKS8[i] = mask8(i);
    }
    for (int i = 0; i < 16; ++i) {
      MASKS16[i] = mask16(i);
    }
    for (int i = 0; i < 32; ++i) {
      MASKS32[i] = mask32(i);
    }
  }
  // mark values in array as final longs to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
  private static final long MASK8_1 = MASKS8[1];
  private static final long MASK8_2 = MASKS8[2];
  private static final long MASK8_3 = MASKS8[3];
  private static final long MASK8_4 = MASKS8[4];
  private static final long MASK8_5 = MASKS8[5];
  private static final long MASK8_6 = MASKS8[6];
  private static final long MASK8_7 = MASKS8[7];
  private static final long MASK16_1 = MASKS16[1];
  private static final long MASK16_2 = MASKS16[2];
  private static final long MASK16_3 = MASKS16[3];
  private static final long MASK16_4 = MASKS16[4];
  private static final long MASK16_5 = MASKS16[5];
  private static final long MASK16_6 = MASKS16[6];
  private static final long MASK16_7 = MASKS16[7];
  private static final long MASK16_9 = MASKS16[9];
  private static final long MASK16_10 = MASKS16[10];
  private static final long MASK16_11 = MASKS16[11];
  private static final long MASK16_12 = MASKS16[12];
  private static final long MASK16_13 = MASKS16[13];
  private static final long MASK16_14 = MASKS16[14];
  private static final long MASK16_15 = MASKS16[15];
  private static final long MASK32_1 = MASKS32[1];
  private static final long MASK32_2 = MASKS32[2];
  private static final long MASK32_3 = MASKS32[3];
  private static final long MASK32_4 = MASKS32[4];
  private static final long MASK32_5 = MASKS32[5];
  private static final long MASK32_6 = MASKS32[6];
  private static final long MASK32_7 = MASKS32[7];
  private static final long MASK32_8 = MASKS32[8];
  private static final long MASK32_9 = MASKS32[9];
  private static final long MASK32_10 = MASKS32[10];
  private static final long MASK32_11 = MASKS32[11];
  private static final long MASK32_12 = MASKS32[12];
  private static final long MASK32_13 = MASKS32[13];
  private static final long MASK32_14 = MASKS32[14];
  private static final long MASK32_15 = MASKS32[15];
  private static final long MASK32_17 = MASKS32[17];
  private static final long MASK32_18 = MASKS32[18];
  private static final long MASK32_19 = MASKS32[19];
  private static final long MASK32_20 = MASKS32[20];
  private static final long MASK32_21 = MASKS32[21];
  private static final long MASK32_22 = MASKS32[22];
  private static final long MASK32_23 = MASKS32[23];
  private static final long MASK32_24 = MASKS32[24];

  /** Decode 128 integers into {@code longs}. */
  @Override
  public void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(in, tmp, longs);
        expand8(longs);
        break;
      case 2:
        decode2(in, tmp, longs);
        expand8(longs);
        break;
      case 3:
        decode3(in, tmp, longs);
        expand8(longs);
        break;
      case 4:
        decode4(in, tmp, longs);
        expand8(longs);
        break;
      case 5:
        decode5(in, tmp, longs);
        expand8(longs);
        break;
      case 6:
        decode6(in, tmp, longs);
        expand8(longs);
        break;
      case 7:
        decode7(in, tmp, longs);
        expand8(longs);
        break;
      case 8:
        decode8(in, tmp, longs);
        expand8(longs);
        break;
      case 9:
        decode9(in, tmp, longs);
        expand16(longs);
        break;
      case 10:
        decode10(in, tmp, longs);
        expand16(longs);
        break;
      case 11:
        decode11(in, tmp, longs);
        expand16(longs);
        break;
      case 12:
        decode12(in, tmp, longs);
        expand16(longs);
        break;
      case 13:
        decode13(in, tmp, longs);
        expand16(longs);
        break;
      case 14:
        decode14(in, tmp, longs);
        expand16(longs);
        break;
      case 15:
        decode15(in, tmp, longs);
        expand16(longs);
        break;
      case 16:
        decode16(in, tmp, longs);
        expand16(longs);
        break;
      case 17:
        decode17(in, tmp, longs);
        expand32(longs);
        break;
      case 18:
        decode18(in, tmp, longs);
        expand32(longs);
        break;
      case 19:
        decode19(in, tmp, longs);
        expand32(longs);
        break;
      case 20:
        decode20(in, tmp, longs);
        expand32(longs);
        break;
      case 21:
        decode21(in, tmp, longs);
        expand32(longs);
        break;
      case 22:
        decode22(in, tmp, longs);
        expand32(longs);
        break;
      case 23:
        decode23(in, tmp, longs);
        expand32(longs);
        break;
      case 24:
        decode24(in, tmp, longs);
        expand32(longs);
        break;
      default:
        decodeSlow(bitsPerValue, in, tmp, longs);
        expand32(longs);
        break;
    }
  }

  /**
   * Decodes 128 integers into 64 {@code longs} such that each long contains two values, each
   * represented with 32 bits. Values [0..63] are encoded in the high-order bits of {@code longs}
   * [0..63], and values [64..127] are encoded in the low-order bits of {@code longs} [0..63]. This
   * representation may allow subsequent operations to be performed on two values at a time.
   */
  @Override
  public void decodeTo32(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(in, tmp, longs);
        expand8To32(longs);
        break;
      case 2:
        decode2(in, tmp, longs);
        expand8To32(longs);
        break;
      case 3:
        decode3(in, tmp, longs);
        expand8To32(longs);
        break;
      case 4:
        decode4(in, tmp, longs);
        expand8To32(longs);
        break;
      case 5:
        decode5(in, tmp, longs);
        expand8To32(longs);
        break;
      case 6:
        decode6(in, tmp, longs);
        expand8To32(longs);
        break;
      case 7:
        decode7(in, tmp, longs);
        expand8To32(longs);
        break;
      case 8:
        decode8(in, tmp, longs);
        expand8To32(longs);
        break;
      case 9:
        decode9(in, tmp, longs);
        expand16To32(longs);
        break;
      case 10:
        decode10(in, tmp, longs);
        expand16To32(longs);
        break;
      case 11:
        decode11(in, tmp, longs);
        expand16To32(longs);
        break;
      case 12:
        decode12(in, tmp, longs);
        expand16To32(longs);
        break;
      case 13:
        decode13(in, tmp, longs);
        expand16To32(longs);
        break;
      case 14:
        decode14(in, tmp, longs);
        expand16To32(longs);
        break;
      case 15:
        decode15(in, tmp, longs);
        expand16To32(longs);
        break;
      case 16:
        decode16(in, tmp, longs);
        expand16To32(longs);
        break;
      case 17:
        decode17(in, tmp, longs);
        break;
      case 18:
        decode18(in, tmp, longs);
        break;
      case 19:
        decode19(in, tmp, longs);
        break;
      case 20:
        decode20(in, tmp, longs);
        break;
      case 21:
        decode21(in, tmp, longs);
        break;
      case 22:
        decode22(in, tmp, longs);
        break;
      case 23:
        decode23(in, tmp, longs);
        break;
      case 24:
        decode24(in, tmp, longs);
        break;
      default:
        decodeSlow(bitsPerValue, in, tmp, longs);
        break;
    }
  }

  private static void decode1(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 2);
    shiftLongs(tmp, 2, longs, 0, 7, MASK8_1);
    shiftLongs(tmp, 2, longs, 2, 6, MASK8_1);
    shiftLongs(tmp, 2, longs, 4, 5, MASK8_1);
    shiftLongs(tmp, 2, longs, 6, 4, MASK8_1);
    shiftLongs(tmp, 2, longs, 8, 3, MASK8_1);
    shiftLongs(tmp, 2, longs, 10, 2, MASK8_1);
    shiftLongs(tmp, 2, longs, 12, 1, MASK8_1);
    shiftLongs(tmp, 2, longs, 14, 0, MASK8_1);
  }

  private static void decode2(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 4);
    shiftLongs(tmp, 4, longs, 0, 6, MASK8_2);
    shiftLongs(tmp, 4, longs, 4, 4, MASK8_2);
    shiftLongs(tmp, 4, longs, 8, 2, MASK8_2);
    shiftLongs(tmp, 4, longs, 12, 0, MASK8_2);
  }

  private static void decode3(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 6);
    shiftLongs(tmp, 6, longs, 0, 5, MASK8_3);
    shiftLongs(tmp, 6, longs, 6, 2, MASK8_3);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 2; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = (tmp[tmpIdx + 0] & MASK8_2) << 1;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK8_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK8_1) << 2;
      l1 |= (tmp[tmpIdx + 2] & MASK8_2) << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode4(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 8);
    shiftLongs(tmp, 8, longs, 0, 4, MASK8_4);
    shiftLongs(tmp, 8, longs, 8, 0, MASK8_4);
  }

  private static void decode5(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 10);
    shiftLongs(tmp, 10, longs, 0, 3, MASK8_5);
    for (int iter = 0, tmpIdx = 0, longsIdx = 10; iter < 2; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx + 0] & MASK8_3) << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK8_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK8_1) << 4;
      l1 |= (tmp[tmpIdx + 2] & MASK8_3) << 1;
      l1 |= (tmp[tmpIdx + 3] >>> 2) & MASK8_1;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 3] & MASK8_2) << 3;
      l2 |= (tmp[tmpIdx + 4] & MASK8_3) << 0;
      longs[longsIdx + 2] = l2;
    }
  }

  private static void decode6(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 12);
    shiftLongs(tmp, 12, longs, 0, 2, MASK8_6);
    shiftLongs(tmp, 12, tmp, 0, 0, MASK8_2);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 4; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 4;
      l0 |= tmp[tmpIdx + 1] << 2;
      l0 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode7(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 14);
    shiftLongs(tmp, 14, longs, 0, 1, MASK8_7);
    shiftLongs(tmp, 14, tmp, 0, 0, MASK8_1);
    for (int iter = 0, tmpIdx = 0, longsIdx = 14; iter < 2; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 6;
      l0 |= tmp[tmpIdx + 1] << 5;
      l0 |= tmp[tmpIdx + 2] << 4;
      l0 |= tmp[tmpIdx + 3] << 3;
      l0 |= tmp[tmpIdx + 4] << 2;
      l0 |= tmp[tmpIdx + 5] << 1;
      l0 |= tmp[tmpIdx + 6] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode8(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(longs, 0, 16);
  }

  private static void decode9(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 18);
    shiftLongs(tmp, 18, longs, 0, 7, MASK16_9);
    for (int iter = 0, tmpIdx = 0, longsIdx = 18; iter < 2; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx + 0] & MASK16_7) << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 5) & MASK16_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK16_5) << 4;
      l1 |= (tmp[tmpIdx + 2] >>> 3) & MASK16_4;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 2] & MASK16_3) << 6;
      l2 |= (tmp[tmpIdx + 3] >>> 1) & MASK16_6;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 3] & MASK16_1) << 8;
      l3 |= (tmp[tmpIdx + 4] & MASK16_7) << 1;
      l3 |= (tmp[tmpIdx + 5] >>> 6) & MASK16_1;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 5] & MASK16_6) << 3;
      l4 |= (tmp[tmpIdx + 6] >>> 4) & MASK16_3;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 6] & MASK16_4) << 5;
      l5 |= (tmp[tmpIdx + 7] >>> 2) & MASK16_5;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 7] & MASK16_2) << 7;
      l6 |= (tmp[tmpIdx + 8] & MASK16_7) << 0;
      longs[longsIdx + 6] = l6;
    }
  }

  private static void decode10(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 20);
    shiftLongs(tmp, 20, longs, 0, 6, MASK16_10);
    for (int iter = 0, tmpIdx = 0, longsIdx = 20; iter < 4; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx + 0] & MASK16_6) << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 2) & MASK16_4;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK16_2) << 8;
      l1 |= (tmp[tmpIdx + 2] & MASK16_6) << 2;
      l1 |= (tmp[tmpIdx + 3] >>> 4) & MASK16_2;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 3] & MASK16_4) << 6;
      l2 |= (tmp[tmpIdx + 4] & MASK16_6) << 0;
      longs[longsIdx + 2] = l2;
    }
  }

  private static void decode11(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 22);
    shiftLongs(tmp, 22, longs, 0, 5, MASK16_11);
    for (int iter = 0, tmpIdx = 0, longsIdx = 22; iter < 2; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx + 0] & MASK16_5) << 6;
      l0 |= (tmp[tmpIdx + 1] & MASK16_5) << 1;
      l0 |= (tmp[tmpIdx + 2] >>> 4) & MASK16_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 2] & MASK16_4) << 7;
      l1 |= (tmp[tmpIdx + 3] & MASK16_5) << 2;
      l1 |= (tmp[tmpIdx + 4] >>> 3) & MASK16_2;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 4] & MASK16_3) << 8;
      l2 |= (tmp[tmpIdx + 5] & MASK16_5) << 3;
      l2 |= (tmp[tmpIdx + 6] >>> 2) & MASK16_3;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 6] & MASK16_2) << 9;
      l3 |= (tmp[tmpIdx + 7] & MASK16_5) << 4;
      l3 |= (tmp[tmpIdx + 8] >>> 1) & MASK16_4;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 8] & MASK16_1) << 10;
      l4 |= (tmp[tmpIdx + 9] & MASK16_5) << 5;
      l4 |= (tmp[tmpIdx + 10] & MASK16_5) << 0;
      longs[longsIdx + 4] = l4;
    }
  }

  private static void decode12(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 24);
    shiftLongs(tmp, 24, longs, 0, 4, MASK16_12);
    shiftLongs(tmp, 24, tmp, 0, 0, MASK16_4);
    for (int iter = 0, tmpIdx = 0, longsIdx = 24; iter < 8; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 8;
      l0 |= tmp[tmpIdx + 1] << 4;
      l0 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode13(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 26);
    shiftLongs(tmp, 26, longs, 0, 3, MASK16_13);
    for (int iter = 0, tmpIdx = 0, longsIdx = 26; iter < 2; ++iter, tmpIdx += 13, longsIdx += 3) {
      long l0 = (tmp[tmpIdx + 0] & MASK16_3) << 10;
      l0 |= (tmp[tmpIdx + 1] & MASK16_3) << 7;
      l0 |= (tmp[tmpIdx + 2] & MASK16_3) << 4;
      l0 |= (tmp[tmpIdx + 3] & MASK16_3) << 1;
      l0 |= (tmp[tmpIdx + 4] >>> 2) & MASK16_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 4] & MASK16_2) << 11;
      l1 |= (tmp[tmpIdx + 5] & MASK16_3) << 8;
      l1 |= (tmp[tmpIdx + 6] & MASK16_3) << 5;
      l1 |= (tmp[tmpIdx + 7] & MASK16_3) << 2;
      l1 |= (tmp[tmpIdx + 8] >>> 1) & MASK16_2;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 8] & MASK16_1) << 12;
      l2 |= (tmp[tmpIdx + 9] & MASK16_3) << 9;
      l2 |= (tmp[tmpIdx + 10] & MASK16_3) << 6;
      l2 |= (tmp[tmpIdx + 11] & MASK16_3) << 3;
      l2 |= (tmp[tmpIdx + 12] & MASK16_3) << 0;
      longs[longsIdx + 2] = l2;
    }
  }

  private static void decode14(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 28);
    shiftLongs(tmp, 28, longs, 0, 2, MASK16_14);
    shiftLongs(tmp, 28, tmp, 0, 0, MASK16_2);
    for (int iter = 0, tmpIdx = 0, longsIdx = 28; iter < 4; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 12;
      l0 |= tmp[tmpIdx + 1] << 10;
      l0 |= tmp[tmpIdx + 2] << 8;
      l0 |= tmp[tmpIdx + 3] << 6;
      l0 |= tmp[tmpIdx + 4] << 4;
      l0 |= tmp[tmpIdx + 5] << 2;
      l0 |= tmp[tmpIdx + 6] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode15(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 30);
    shiftLongs(tmp, 30, longs, 0, 1, MASK16_15);
    shiftLongs(tmp, 30, tmp, 0, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 15, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 14;
      l0 |= tmp[tmpIdx + 1] << 13;
      l0 |= tmp[tmpIdx + 2] << 12;
      l0 |= tmp[tmpIdx + 3] << 11;
      l0 |= tmp[tmpIdx + 4] << 10;
      l0 |= tmp[tmpIdx + 5] << 9;
      l0 |= tmp[tmpIdx + 6] << 8;
      l0 |= tmp[tmpIdx + 7] << 7;
      l0 |= tmp[tmpIdx + 8] << 6;
      l0 |= tmp[tmpIdx + 9] << 5;
      l0 |= tmp[tmpIdx + 10] << 4;
      l0 |= tmp[tmpIdx + 11] << 3;
      l0 |= tmp[tmpIdx + 12] << 2;
      l0 |= tmp[tmpIdx + 13] << 1;
      l0 |= tmp[tmpIdx + 14] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode16(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(longs, 0, 32);
  }

  private static void decode17(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 34);
    shiftLongs(tmp, 34, longs, 0, 15, MASK32_17);
    for (int iter = 0, tmpIdx = 0, longsIdx = 34; iter < 2; ++iter, tmpIdx += 17, longsIdx += 15) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_15) << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 13) & MASK32_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_13) << 4;
      l1 |= (tmp[tmpIdx + 2] >>> 11) & MASK32_4;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 2] & MASK32_11) << 6;
      l2 |= (tmp[tmpIdx + 3] >>> 9) & MASK32_6;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 3] & MASK32_9) << 8;
      l3 |= (tmp[tmpIdx + 4] >>> 7) & MASK32_8;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 4] & MASK32_7) << 10;
      l4 |= (tmp[tmpIdx + 5] >>> 5) & MASK32_10;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 5] & MASK32_5) << 12;
      l5 |= (tmp[tmpIdx + 6] >>> 3) & MASK32_12;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 6] & MASK32_3) << 14;
      l6 |= (tmp[tmpIdx + 7] >>> 1) & MASK32_14;
      longs[longsIdx + 6] = l6;
      long l7 = (tmp[tmpIdx + 7] & MASK32_1) << 16;
      l7 |= (tmp[tmpIdx + 8] & MASK32_15) << 1;
      l7 |= (tmp[tmpIdx + 9] >>> 14) & MASK32_1;
      longs[longsIdx + 7] = l7;
      long l8 = (tmp[tmpIdx + 9] & MASK32_14) << 3;
      l8 |= (tmp[tmpIdx + 10] >>> 12) & MASK32_3;
      longs[longsIdx + 8] = l8;
      long l9 = (tmp[tmpIdx + 10] & MASK32_12) << 5;
      l9 |= (tmp[tmpIdx + 11] >>> 10) & MASK32_5;
      longs[longsIdx + 9] = l9;
      long l10 = (tmp[tmpIdx + 11] & MASK32_10) << 7;
      l10 |= (tmp[tmpIdx + 12] >>> 8) & MASK32_7;
      longs[longsIdx + 10] = l10;
      long l11 = (tmp[tmpIdx + 12] & MASK32_8) << 9;
      l11 |= (tmp[tmpIdx + 13] >>> 6) & MASK32_9;
      longs[longsIdx + 11] = l11;
      long l12 = (tmp[tmpIdx + 13] & MASK32_6) << 11;
      l12 |= (tmp[tmpIdx + 14] >>> 4) & MASK32_11;
      longs[longsIdx + 12] = l12;
      long l13 = (tmp[tmpIdx + 14] & MASK32_4) << 13;
      l13 |= (tmp[tmpIdx + 15] >>> 2) & MASK32_13;
      longs[longsIdx + 13] = l13;
      long l14 = (tmp[tmpIdx + 15] & MASK32_2) << 15;
      l14 |= (tmp[tmpIdx + 16] & MASK32_15) << 0;
      longs[longsIdx + 14] = l14;
    }
  }

  private static void decode18(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 36);
    shiftLongs(tmp, 36, longs, 0, 14, MASK32_18);
    for (int iter = 0, tmpIdx = 0, longsIdx = 36; iter < 4; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_14) << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 10) & MASK32_4;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_10) << 8;
      l1 |= (tmp[tmpIdx + 2] >>> 6) & MASK32_8;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 2] & MASK32_6) << 12;
      l2 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_12;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 3] & MASK32_2) << 16;
      l3 |= (tmp[tmpIdx + 4] & MASK32_14) << 2;
      l3 |= (tmp[tmpIdx + 5] >>> 12) & MASK32_2;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 5] & MASK32_12) << 6;
      l4 |= (tmp[tmpIdx + 6] >>> 8) & MASK32_6;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 6] & MASK32_8) << 10;
      l5 |= (tmp[tmpIdx + 7] >>> 4) & MASK32_10;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 7] & MASK32_4) << 14;
      l6 |= (tmp[tmpIdx + 8] & MASK32_14) << 0;
      longs[longsIdx + 6] = l6;
    }
  }

  private static void decode19(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 38);
    shiftLongs(tmp, 38, longs, 0, 13, MASK32_19);
    for (int iter = 0, tmpIdx = 0, longsIdx = 38; iter < 2; ++iter, tmpIdx += 19, longsIdx += 13) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_13) << 6;
      l0 |= (tmp[tmpIdx + 1] >>> 7) & MASK32_6;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_7) << 12;
      l1 |= (tmp[tmpIdx + 2] >>> 1) & MASK32_12;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 2] & MASK32_1) << 18;
      l2 |= (tmp[tmpIdx + 3] & MASK32_13) << 5;
      l2 |= (tmp[tmpIdx + 4] >>> 8) & MASK32_5;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 4] & MASK32_8) << 11;
      l3 |= (tmp[tmpIdx + 5] >>> 2) & MASK32_11;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 5] & MASK32_2) << 17;
      l4 |= (tmp[tmpIdx + 6] & MASK32_13) << 4;
      l4 |= (tmp[tmpIdx + 7] >>> 9) & MASK32_4;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 7] & MASK32_9) << 10;
      l5 |= (tmp[tmpIdx + 8] >>> 3) & MASK32_10;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 8] & MASK32_3) << 16;
      l6 |= (tmp[tmpIdx + 9] & MASK32_13) << 3;
      l6 |= (tmp[tmpIdx + 10] >>> 10) & MASK32_3;
      longs[longsIdx + 6] = l6;
      long l7 = (tmp[tmpIdx + 10] & MASK32_10) << 9;
      l7 |= (tmp[tmpIdx + 11] >>> 4) & MASK32_9;
      longs[longsIdx + 7] = l7;
      long l8 = (tmp[tmpIdx + 11] & MASK32_4) << 15;
      l8 |= (tmp[tmpIdx + 12] & MASK32_13) << 2;
      l8 |= (tmp[tmpIdx + 13] >>> 11) & MASK32_2;
      longs[longsIdx + 8] = l8;
      long l9 = (tmp[tmpIdx + 13] & MASK32_11) << 8;
      l9 |= (tmp[tmpIdx + 14] >>> 5) & MASK32_8;
      longs[longsIdx + 9] = l9;
      long l10 = (tmp[tmpIdx + 14] & MASK32_5) << 14;
      l10 |= (tmp[tmpIdx + 15] & MASK32_13) << 1;
      l10 |= (tmp[tmpIdx + 16] >>> 12) & MASK32_1;
      longs[longsIdx + 10] = l10;
      long l11 = (tmp[tmpIdx + 16] & MASK32_12) << 7;
      l11 |= (tmp[tmpIdx + 17] >>> 6) & MASK32_7;
      longs[longsIdx + 11] = l11;
      long l12 = (tmp[tmpIdx + 17] & MASK32_6) << 13;
      l12 |= (tmp[tmpIdx + 18] & MASK32_13) << 0;
      longs[longsIdx + 12] = l12;
    }
  }

  private static void decode20(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 40);
    shiftLongs(tmp, 40, longs, 0, 12, MASK32_20);
    for (int iter = 0, tmpIdx = 0, longsIdx = 40; iter < 8; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_12) << 8;
      l0 |= (tmp[tmpIdx + 1] >>> 4) & MASK32_8;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_4) << 16;
      l1 |= (tmp[tmpIdx + 2] & MASK32_12) << 4;
      l1 |= (tmp[tmpIdx + 3] >>> 8) & MASK32_4;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 3] & MASK32_8) << 12;
      l2 |= (tmp[tmpIdx + 4] & MASK32_12) << 0;
      longs[longsIdx + 2] = l2;
    }
  }

  private static void decode21(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 42);
    shiftLongs(tmp, 42, longs, 0, 11, MASK32_21);
    for (int iter = 0, tmpIdx = 0, longsIdx = 42; iter < 2; ++iter, tmpIdx += 21, longsIdx += 11) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_11) << 10;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK32_10;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_1) << 20;
      l1 |= (tmp[tmpIdx + 2] & MASK32_11) << 9;
      l1 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_9;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 3] & MASK32_2) << 19;
      l2 |= (tmp[tmpIdx + 4] & MASK32_11) << 8;
      l2 |= (tmp[tmpIdx + 5] >>> 3) & MASK32_8;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 5] & MASK32_3) << 18;
      l3 |= (tmp[tmpIdx + 6] & MASK32_11) << 7;
      l3 |= (tmp[tmpIdx + 7] >>> 4) & MASK32_7;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 7] & MASK32_4) << 17;
      l4 |= (tmp[tmpIdx + 8] & MASK32_11) << 6;
      l4 |= (tmp[tmpIdx + 9] >>> 5) & MASK32_6;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 9] & MASK32_5) << 16;
      l5 |= (tmp[tmpIdx + 10] & MASK32_11) << 5;
      l5 |= (tmp[tmpIdx + 11] >>> 6) & MASK32_5;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 11] & MASK32_6) << 15;
      l6 |= (tmp[tmpIdx + 12] & MASK32_11) << 4;
      l6 |= (tmp[tmpIdx + 13] >>> 7) & MASK32_4;
      longs[longsIdx + 6] = l6;
      long l7 = (tmp[tmpIdx + 13] & MASK32_7) << 14;
      l7 |= (tmp[tmpIdx + 14] & MASK32_11) << 3;
      l7 |= (tmp[tmpIdx + 15] >>> 8) & MASK32_3;
      longs[longsIdx + 7] = l7;
      long l8 = (tmp[tmpIdx + 15] & MASK32_8) << 13;
      l8 |= (tmp[tmpIdx + 16] & MASK32_11) << 2;
      l8 |= (tmp[tmpIdx + 17] >>> 9) & MASK32_2;
      longs[longsIdx + 8] = l8;
      long l9 = (tmp[tmpIdx + 17] & MASK32_9) << 12;
      l9 |= (tmp[tmpIdx + 18] & MASK32_11) << 1;
      l9 |= (tmp[tmpIdx + 19] >>> 10) & MASK32_1;
      longs[longsIdx + 9] = l9;
      long l10 = (tmp[tmpIdx + 19] & MASK32_10) << 11;
      l10 |= (tmp[tmpIdx + 20] & MASK32_11) << 0;
      longs[longsIdx + 10] = l10;
    }
  }

  private static void decode22(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 44);
    shiftLongs(tmp, 44, longs, 0, 10, MASK32_22);
    for (int iter = 0, tmpIdx = 0, longsIdx = 44; iter < 4; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_10) << 12;
      l0 |= (tmp[tmpIdx + 1] & MASK32_10) << 2;
      l0 |= (tmp[tmpIdx + 2] >>> 8) & MASK32_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 2] & MASK32_8) << 14;
      l1 |= (tmp[tmpIdx + 3] & MASK32_10) << 4;
      l1 |= (tmp[tmpIdx + 4] >>> 6) & MASK32_4;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 4] & MASK32_6) << 16;
      l2 |= (tmp[tmpIdx + 5] & MASK32_10) << 6;
      l2 |= (tmp[tmpIdx + 6] >>> 4) & MASK32_6;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 6] & MASK32_4) << 18;
      l3 |= (tmp[tmpIdx + 7] & MASK32_10) << 8;
      l3 |= (tmp[tmpIdx + 8] >>> 2) & MASK32_8;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 8] & MASK32_2) << 20;
      l4 |= (tmp[tmpIdx + 9] & MASK32_10) << 10;
      l4 |= (tmp[tmpIdx + 10] & MASK32_10) << 0;
      longs[longsIdx + 4] = l4;
    }
  }

  private static void decode23(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 46);
    shiftLongs(tmp, 46, longs, 0, 9, MASK32_23);
    for (int iter = 0, tmpIdx = 0, longsIdx = 46; iter < 2; ++iter, tmpIdx += 23, longsIdx += 9) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_9) << 14;
      l0 |= (tmp[tmpIdx + 1] & MASK32_9) << 5;
      l0 |= (tmp[tmpIdx + 2] >>> 4) & MASK32_5;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 2] & MASK32_4) << 19;
      l1 |= (tmp[tmpIdx + 3] & MASK32_9) << 10;
      l1 |= (tmp[tmpIdx + 4] & MASK32_9) << 1;
      l1 |= (tmp[tmpIdx + 5] >>> 8) & MASK32_1;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 5] & MASK32_8) << 15;
      l2 |= (tmp[tmpIdx + 6] & MASK32_9) << 6;
      l2 |= (tmp[tmpIdx + 7] >>> 3) & MASK32_6;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 7] & MASK32_3) << 20;
      l3 |= (tmp[tmpIdx + 8] & MASK32_9) << 11;
      l3 |= (tmp[tmpIdx + 9] & MASK32_9) << 2;
      l3 |= (tmp[tmpIdx + 10] >>> 7) & MASK32_2;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 10] & MASK32_7) << 16;
      l4 |= (tmp[tmpIdx + 11] & MASK32_9) << 7;
      l4 |= (tmp[tmpIdx + 12] >>> 2) & MASK32_7;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 12] & MASK32_2) << 21;
      l5 |= (tmp[tmpIdx + 13] & MASK32_9) << 12;
      l5 |= (tmp[tmpIdx + 14] & MASK32_9) << 3;
      l5 |= (tmp[tmpIdx + 15] >>> 6) & MASK32_3;
      longs[longsIdx + 5] = l5;
      long l6 = (tmp[tmpIdx + 15] & MASK32_6) << 17;
      l6 |= (tmp[tmpIdx + 16] & MASK32_9) << 8;
      l6 |= (tmp[tmpIdx + 17] >>> 1) & MASK32_8;
      longs[longsIdx + 6] = l6;
      long l7 = (tmp[tmpIdx + 17] & MASK32_1) << 22;
      l7 |= (tmp[tmpIdx + 18] & MASK32_9) << 13;
      l7 |= (tmp[tmpIdx + 19] & MASK32_9) << 4;
      l7 |= (tmp[tmpIdx + 20] >>> 5) & MASK32_4;
      longs[longsIdx + 7] = l7;
      long l8 = (tmp[tmpIdx + 20] & MASK32_5) << 18;
      l8 |= (tmp[tmpIdx + 21] & MASK32_9) << 9;
      l8 |= (tmp[tmpIdx + 22] & MASK32_9) << 0;
      longs[longsIdx + 8] = l8;
    }
  }

  private static void decode24(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 48);
    shiftLongs(tmp, 48, longs, 0, 8, MASK32_24);
    shiftLongs(tmp, 48, tmp, 0, 0, MASK32_8);
    for (int iter = 0, tmpIdx = 0, longsIdx = 48; iter < 16; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 16;
      l0 |= tmp[tmpIdx + 1] << 8;
      l0 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 0] = l0;
    }
  }
}
