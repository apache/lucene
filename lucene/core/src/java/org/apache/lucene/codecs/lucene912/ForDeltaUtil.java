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
    final long l = arr[BLOCK_SIZE / 2 - 1];
    for (int i = BLOCK_SIZE / 2; i < BLOCK_SIZE; ++i) {
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

  private static final int[] FAST_BITS_PER_VALUE =
      new int[] {1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 16, 20, 24, 32};
  private static final long[] MASKS16 = new long[16];
  private static final long[] MASKS32 = new long[32];

  static {
    for (int i = 0; i < 16; ++i) {
      MASKS16[i] = mask16(i);
    }
    for (int i = 0; i < 32; ++i) {
      MASKS32[i] = mask32(i);
    }
  }

  // mark values in array as final longs to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
  private static final long MASK16_0 = MASKS16[0];
  private static final long MASK16_1 = MASKS16[1];
  private static final long MASK16_2 = MASKS16[2];
  private static final long MASK16_3 = MASKS16[3];
  private static final long MASK16_4 = MASKS16[4];
  private static final long MASK16_5 = MASKS16[5];
  private static final long MASK16_6 = MASKS16[6];
  private static final long MASK16_7 = MASKS16[7];
  private static final long MASK16_8 = MASKS16[8];
  private static final long MASK16_9 = MASKS16[9];
  private static final long MASK16_10 = MASKS16[10];
  private static final long MASK16_11 = MASKS16[11];
  private static final long MASK16_12 = MASKS16[12];
  private static final long MASK16_13 = MASKS16[13];
  private static final long MASK16_14 = MASKS16[14];
  private static final long MASK16_15 = MASKS16[15];
  private static final long MASK32_0 = MASKS32[0];
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
  private static final long MASK32_16 = MASKS32[16];
  private static final long MASK32_17 = MASKS32[17];
  private static final long MASK32_18 = MASKS32[18];
  private static final long MASK32_19 = MASKS32[19];
  private static final long MASK32_20 = MASKS32[20];
  private static final long MASK32_21 = MASKS32[21];
  private static final long MASK32_22 = MASKS32[22];
  private static final long MASK32_23 = MASKS32[23];
  private static final long MASK32_24 = MASKS32[24];
  private static final long MASK32_25 = MASKS32[25];
  private static final long MASK32_26 = MASKS32[26];
  private static final long MASK32_27 = MASKS32[27];
  private static final long MASK32_28 = MASKS32[28];
  private static final long MASK32_29 = MASKS32[29];
  private static final long MASK32_30 = MASKS32[30];
  private static final long MASK32_31 = MASKS32[31];

  // public for benchmarking
  /** Delta-decode 128 integers into {@code longs}. */
  public void decodeAndPrefixSum(int bitsPerValue, DataInput in, long base, long[] longs)
      throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 2:
        decode2(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 3:
        decode3(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 4:
        decode4(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 5:
        decode5(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 6:
        decode6(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 8:
        decode8(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 9:
        decode9(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 10:
        decode10(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 12:
        decode12(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 14:
        decode14(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 16:
        decode16(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 20:
        decode20(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 24:
        decode24(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 32:
        decode32(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      default:
        throw new UnsupportedOperationException("bpv=" + bitsPerValue);
    }
  }

  private static void decode1(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 2);
    shiftLongs(tmp, 2, longs, 0, 15, MASK16_1);
    shiftLongs(tmp, 2, longs, 2, 14, MASK16_1);
    shiftLongs(tmp, 2, longs, 4, 13, MASK16_1);
    shiftLongs(tmp, 2, longs, 6, 12, MASK16_1);
    shiftLongs(tmp, 2, longs, 8, 11, MASK16_1);
    shiftLongs(tmp, 2, longs, 10, 10, MASK16_1);
    shiftLongs(tmp, 2, longs, 12, 9, MASK16_1);
    shiftLongs(tmp, 2, longs, 14, 8, MASK16_1);
    shiftLongs(tmp, 2, longs, 16, 7, MASK16_1);
    shiftLongs(tmp, 2, longs, 18, 6, MASK16_1);
    shiftLongs(tmp, 2, longs, 20, 5, MASK16_1);
    shiftLongs(tmp, 2, longs, 22, 4, MASK16_1);
    shiftLongs(tmp, 2, longs, 24, 3, MASK16_1);
    shiftLongs(tmp, 2, longs, 26, 2, MASK16_1);
    shiftLongs(tmp, 2, longs, 28, 1, MASK16_1);
    shiftLongs(tmp, 2, longs, 30, 0, MASK16_1);
  }

  private static void decode2(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 4);
    shiftLongs(tmp, 4, longs, 0, 14, MASK16_2);
    shiftLongs(tmp, 4, longs, 4, 12, MASK16_2);
    shiftLongs(tmp, 4, longs, 8, 10, MASK16_2);
    shiftLongs(tmp, 4, longs, 12, 8, MASK16_2);
    shiftLongs(tmp, 4, longs, 16, 6, MASK16_2);
    shiftLongs(tmp, 4, longs, 20, 4, MASK16_2);
    shiftLongs(tmp, 4, longs, 24, 2, MASK16_2);
    shiftLongs(tmp, 4, longs, 28, 0, MASK16_2);
  }

  private static void decode3(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 6);
    shiftLongs(tmp, 6, longs, 0, 13, MASK16_3);
    shiftLongs(tmp, 6, longs, 6, 10, MASK16_3);
    shiftLongs(tmp, 6, longs, 12, 7, MASK16_3);
    shiftLongs(tmp, 6, longs, 18, 4, MASK16_3);
    shiftLongs(tmp, 6, longs, 24, 1, MASK16_3);
    shiftLongs(tmp, 6, tmp, 0, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 2;
      l0 |= tmp[tmpIdx + 1] << 1;
      l0 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode4(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 8);
    shiftLongs(tmp, 8, longs, 0, 12, MASK16_4);
    shiftLongs(tmp, 8, longs, 8, 8, MASK16_4);
    shiftLongs(tmp, 8, longs, 16, 4, MASK16_4);
    shiftLongs(tmp, 8, longs, 24, 0, MASK16_4);
  }

  private static void decode5(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 10);
    shiftLongs(tmp, 10, longs, 0, 11, MASK16_5);
    shiftLongs(tmp, 10, longs, 10, 6, MASK16_5);
    shiftLongs(tmp, 10, longs, 20, 1, MASK16_5);
    shiftLongs(tmp, 10, tmp, 0, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 5, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 4;
      l0 |= tmp[tmpIdx + 1] << 3;
      l0 |= tmp[tmpIdx + 2] << 2;
      l0 |= tmp[tmpIdx + 3] << 1;
      l0 |= tmp[tmpIdx + 4] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode6(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 12);
    shiftLongs(tmp, 12, longs, 0, 10, MASK16_6);
    shiftLongs(tmp, 12, longs, 12, 4, MASK16_6);
    for (int iter = 0, tmpIdx = 0, longsIdx = 24; iter < 4; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = (tmp[tmpIdx + 0] & MASK16_4) << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 2) & MASK16_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK16_2) << 4;
      l1 |= (tmp[tmpIdx + 2] & MASK16_4) << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode8(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 16);
    shiftLongs(tmp, 16, longs, 0, 8, MASK16_8);
    shiftLongs(tmp, 16, longs, 16, 0, MASK16_8);
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
    shiftLongs(tmp, 20, longs, 0, 22, MASK32_10);
    shiftLongs(tmp, 20, longs, 20, 12, MASK32_10);
    shiftLongs(tmp, 20, longs, 40, 2, MASK32_10);
    shiftLongs(tmp, 20, tmp, 0, 0, MASK32_2);
    for (int iter = 0, tmpIdx = 0, longsIdx = 60; iter < 4; ++iter, tmpIdx += 5, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 8;
      l0 |= tmp[tmpIdx + 1] << 6;
      l0 |= tmp[tmpIdx + 2] << 4;
      l0 |= tmp[tmpIdx + 3] << 2;
      l0 |= tmp[tmpIdx + 4] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode12(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 24);
    shiftLongs(tmp, 24, longs, 0, 20, MASK32_12);
    shiftLongs(tmp, 24, longs, 24, 8, MASK32_12);
    for (int iter = 0, tmpIdx = 0, longsIdx = 48; iter < 8; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_8) << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 4) & MASK32_4;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_4) << 8;
      l1 |= (tmp[tmpIdx + 2] & MASK32_8) << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode14(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 28);
    shiftLongs(tmp, 28, longs, 0, 18, MASK32_14);
    shiftLongs(tmp, 28, longs, 28, 4, MASK32_14);
    for (int iter = 0, tmpIdx = 0, longsIdx = 56; iter < 4; ++iter, tmpIdx += 7, longsIdx += 2) {
      long l0 = (tmp[tmpIdx + 0] & MASK32_4) << 10;
      l0 |= (tmp[tmpIdx + 1] & MASK32_4) << 6;
      l0 |= (tmp[tmpIdx + 2] & MASK32_4) << 2;
      l0 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 3] & MASK32_2) << 12;
      l1 |= (tmp[tmpIdx + 4] & MASK32_4) << 8;
      l1 |= (tmp[tmpIdx + 5] & MASK32_4) << 4;
      l1 |= (tmp[tmpIdx + 6] & MASK32_4) << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode16(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLongs(tmp, 0, 32);
    shiftLongs(tmp, 32, longs, 0, 16, MASK32_16);
    shiftLongs(tmp, 32, longs, 32, 0, MASK32_16);
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
    in.readLongs(tmp, 0, 64);
  }
}
