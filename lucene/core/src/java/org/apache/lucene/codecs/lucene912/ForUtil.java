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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
public final class ForUtil {

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

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2 compiler, which
   * generates SIMD instructions for it in order to shift multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi + i] = (a[i] >>> shift) & mask;
    }
  }

  private static final int[] FAST_BITS_PER_VALUE =
      new int[] {1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 20, 24, 32};
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
  private static final long MASK16_1 = MASKS16[1];
  private static final long MASK16_2 = MASKS16[2];
  private static final long MASK16_3 = MASKS16[3];
  private static final long MASK16_4 = MASKS16[4];
  private static final long MASK16_5 = MASKS16[5];
  private static final long MASK16_6 = MASKS16[6];
  private static final long MASK16_8 = MASKS16[8];
  private static final long MASK16_10 = MASKS16[10];
  private static final long MASK16_12 = MASKS16[12];
  private static final long MASK16_14 = MASKS16[14];
  private static final long MASK32_1 = MASKS32[1];
  private static final long MASK32_2 = MASKS32[2];
  private static final long MASK32_3 = MASKS32[3];
  private static final long MASK32_4 = MASKS32[4];
  private static final long MASK32_5 = MASKS32[5];
  private static final long MASK32_6 = MASKS32[6];
  private static final long MASK32_8 = MASKS32[8];
  private static final long MASK32_10 = MASKS32[10];
  private static final long MASK32_12 = MASKS32[12];
  private static final long MASK32_14 = MASKS32[14];
  private static final long MASK32_16 = MASKS32[16];
  private static final long MASK32_20 = MASKS32[20];
  private static final long MASK32_24 = MASKS32[24];

  // public for benchmarking
  /** Decode 128 integers into {@code longs}. */
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
      case 8:
        decode8(in, tmp, longs);
        expand8(longs);
        break;
      case 10:
        decode10(in, tmp, longs);
        expand16(longs);
        break;
      case 12:
        decode12(in, tmp, longs);
        expand16(longs);
        break;
      case 14:
        decode14(in, tmp, longs);
        expand16(longs);
        break;
      case 16:
        decode16(in, tmp, longs);
        expand16(longs);
        break;
      case 20:
        decode20(in, tmp, longs);
        expand32(longs);
        break;
      case 24:
        decode24(in, tmp, longs);
        expand32(longs);
        break;
      case 32:
        decode32(in, tmp, longs);
        expand32(longs);
        break;
      default:
        throw new UnsupportedOperationException("bpv=" + bitsPerValue);
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

  private static void decode8(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(longs, 0, 16);
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

  private static void decode16(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(longs, 0, 32);
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

  private static void decode32(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(longs, 0, 64);
  }
}
