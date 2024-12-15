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
package org.apache.lucene.backward_codecs.lucene912;

import static org.apache.lucene.backward_codecs.lucene912.ForUtil.*;

import java.io.IOException;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/ Encodes multiple integers in a long to get
 * SIMD-like speedups. If bitsPerValue &lt;= 4 then we pack 8 ints per long else if bitsPerValue
 * &lt;= 11 we pack 4 ints per long else we pack 2 ints per long
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
    final long l = arr[BLOCK_SIZE / 2 - 1];
    for (int i = BLOCK_SIZE / 2; i < BLOCK_SIZE; ++i) {
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

  /** Delta-decode 128 integers into {@code longs}. */
  void decodeAndPrefixSum(int bitsPerValue, IndexInput in, long base, long[] longs)
      throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(in, longs);
        prefixSum8(longs, base);
        break;
      case 2:
        decode2(in, longs);
        prefixSum8(longs, base);
        break;
      case 3:
        decode3(in, tmp, longs);
        prefixSum8(longs, base);
        break;
      case 4:
        decode4(in, longs);
        prefixSum8(longs, base);
        break;
      case 5:
        decode5To16(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 6:
        decode6To16(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 7:
        decode7To16(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 8:
        decode8To16(in, longs);
        prefixSum16(longs, base);
        break;
      case 9:
        decode9(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 10:
        decode10(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 11:
        decode11(in, tmp, longs);
        prefixSum16(longs, base);
        break;
      case 12:
        decode12To32(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 13:
        decode13To32(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 14:
        decode14To32(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 15:
        decode15To32(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 16:
        decode16To32(in, longs);
        prefixSum32(longs, base);
        break;
      case 17:
        decode17(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 18:
        decode18(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 19:
        decode19(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 20:
        decode20(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 21:
        decode21(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 22:
        decode22(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 23:
        decode23(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      case 24:
        decode24(in, tmp, longs);
        prefixSum32(longs, base);
        break;
      default:
        decodeSlow(bitsPerValue, in, tmp, longs);
        prefixSum32(longs, base);
        break;
    }
  }

  private static void decode5To16(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 10, longs, 11, 5, MASK16_5, tmp, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 5, longsIdx += 1) {
      long l0 = tmp[tmpIdx + 0] << 4;
      l0 |= tmp[tmpIdx + 1] << 3;
      l0 |= tmp[tmpIdx + 2] << 2;
      l0 |= tmp[tmpIdx + 3] << 1;
      l0 |= tmp[tmpIdx + 4] << 0;
      longs[longsIdx + 0] = l0;
    }
  }

  private static void decode6To16(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 12, longs, 10, 6, MASK16_6, tmp, 0, MASK16_4);
    for (int iter = 0, tmpIdx = 0, longsIdx = 24; iter < 4; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = tmp[tmpIdx + 0] << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 2) & MASK16_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK16_2) << 4;
      l1 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode7To16(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 14, longs, 9, 7, MASK16_7, tmp, 0, MASK16_2);
    for (int iter = 0, tmpIdx = 0, longsIdx = 28; iter < 2; ++iter, tmpIdx += 7, longsIdx += 2) {
      long l0 = tmp[tmpIdx + 0] << 5;
      l0 |= tmp[tmpIdx + 1] << 3;
      l0 |= tmp[tmpIdx + 2] << 1;
      l0 |= (tmp[tmpIdx + 3] >>> 1) & MASK16_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 3] & MASK16_1) << 6;
      l1 |= tmp[tmpIdx + 4] << 4;
      l1 |= tmp[tmpIdx + 5] << 2;
      l1 |= tmp[tmpIdx + 6] << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode8To16(IndexInput in, long[] longs) throws IOException {
    splitLongs(in, 16, longs, 8, 8, MASK16_8, longs, 16, MASK16_8);
  }

  private static void decode12To32(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 24, longs, 20, 12, MASK32_12, tmp, 0, MASK32_8);
    for (int iter = 0, tmpIdx = 0, longsIdx = 48; iter < 8; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = tmp[tmpIdx + 0] << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 4) & MASK32_4;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 1] & MASK32_4) << 8;
      l1 |= tmp[tmpIdx + 2] << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode13To32(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 26, longs, 19, 13, MASK32_13, tmp, 0, MASK32_6);
    for (int iter = 0, tmpIdx = 0, longsIdx = 52; iter < 2; ++iter, tmpIdx += 13, longsIdx += 6) {
      long l0 = tmp[tmpIdx + 0] << 7;
      l0 |= tmp[tmpIdx + 1] << 1;
      l0 |= (tmp[tmpIdx + 2] >>> 5) & MASK32_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 2] & MASK32_5) << 8;
      l1 |= tmp[tmpIdx + 3] << 2;
      l1 |= (tmp[tmpIdx + 4] >>> 4) & MASK32_2;
      longs[longsIdx + 1] = l1;
      long l2 = (tmp[tmpIdx + 4] & MASK32_4) << 9;
      l2 |= tmp[tmpIdx + 5] << 3;
      l2 |= (tmp[tmpIdx + 6] >>> 3) & MASK32_3;
      longs[longsIdx + 2] = l2;
      long l3 = (tmp[tmpIdx + 6] & MASK32_3) << 10;
      l3 |= tmp[tmpIdx + 7] << 4;
      l3 |= (tmp[tmpIdx + 8] >>> 2) & MASK32_4;
      longs[longsIdx + 3] = l3;
      long l4 = (tmp[tmpIdx + 8] & MASK32_2) << 11;
      l4 |= tmp[tmpIdx + 9] << 5;
      l4 |= (tmp[tmpIdx + 10] >>> 1) & MASK32_5;
      longs[longsIdx + 4] = l4;
      long l5 = (tmp[tmpIdx + 10] & MASK32_1) << 12;
      l5 |= tmp[tmpIdx + 11] << 6;
      l5 |= tmp[tmpIdx + 12] << 0;
      longs[longsIdx + 5] = l5;
    }
  }

  private static void decode14To32(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 28, longs, 18, 14, MASK32_14, tmp, 0, MASK32_4);
    for (int iter = 0, tmpIdx = 0, longsIdx = 56; iter < 4; ++iter, tmpIdx += 7, longsIdx += 2) {
      long l0 = tmp[tmpIdx + 0] << 10;
      l0 |= tmp[tmpIdx + 1] << 6;
      l0 |= tmp[tmpIdx + 2] << 2;
      l0 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_2;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 3] & MASK32_2) << 12;
      l1 |= tmp[tmpIdx + 4] << 8;
      l1 |= tmp[tmpIdx + 5] << 4;
      l1 |= tmp[tmpIdx + 6] << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode15To32(IndexInput in, long[] tmp, long[] longs) throws IOException {
    splitLongs(in, 30, longs, 17, 15, MASK32_15, tmp, 0, MASK32_2);
    for (int iter = 0, tmpIdx = 0, longsIdx = 60; iter < 2; ++iter, tmpIdx += 15, longsIdx += 2) {
      long l0 = tmp[tmpIdx + 0] << 13;
      l0 |= tmp[tmpIdx + 1] << 11;
      l0 |= tmp[tmpIdx + 2] << 9;
      l0 |= tmp[tmpIdx + 3] << 7;
      l0 |= tmp[tmpIdx + 4] << 5;
      l0 |= tmp[tmpIdx + 5] << 3;
      l0 |= tmp[tmpIdx + 6] << 1;
      l0 |= (tmp[tmpIdx + 7] >>> 1) & MASK32_1;
      longs[longsIdx + 0] = l0;
      long l1 = (tmp[tmpIdx + 7] & MASK32_1) << 14;
      l1 |= tmp[tmpIdx + 8] << 12;
      l1 |= tmp[tmpIdx + 9] << 10;
      l1 |= tmp[tmpIdx + 10] << 8;
      l1 |= tmp[tmpIdx + 11] << 6;
      l1 |= tmp[tmpIdx + 12] << 4;
      l1 |= tmp[tmpIdx + 13] << 2;
      l1 |= tmp[tmpIdx + 14] << 0;
      longs[longsIdx + 1] = l1;
    }
  }

  private static void decode16To32(IndexInput in, long[] longs) throws IOException {
    splitLongs(in, 32, longs, 16, 16, MASK32_16, longs, 32, MASK32_16);
  }
}
