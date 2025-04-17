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
package org.apache.lucene.codecs.lucene103;

import static org.apache.lucene.codecs.lucene103.ForUtil.*;

import java.io.IOException;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/ Encodes multiple integers in a Java int to
 * get SIMD-like speedups. If bitsPerValue &lt;= 4 then we pack 4 ints per Java int else if
 * bitsPerValue &lt;= 11 we pack 2 ints per Java int else we use scalar operations.
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
      arr[i] += arr[i - 1];
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

  /**
   * Return the number of bits per value required to store the given array containing strictly
   * positive numbers.
   */
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

  /** Delta-decode 128 integers into {@code ints}. */
  void decodeAndPrefixSum(int bitsPerValue, PostingDecodingUtil pdu, int base, int[] ints)
      throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(pdu, ints);
        prefixSum8(ints, base);
        break;
      case 2:
        decode2(pdu, ints);
        prefixSum8(ints, base);
        break;
      case 3:
        decode3(pdu, tmp, ints);
        prefixSum8(ints, base);
        break;
      case 4:
        decode4To16(pdu, ints);
        prefixSum16(ints, base);
        break;
      case 5:
        decode5To16(pdu, tmp, ints);
        prefixSum16(ints, base);
        break;
      case 6:
        decode6To16(pdu, tmp, ints);
        prefixSum16(ints, base);
        break;
      case 7:
        decode7To16(pdu, tmp, ints);
        prefixSum16(ints, base);
        break;
      case 8:
        decode8To16(pdu, ints);
        prefixSum16(ints, base);
        break;
      case 9:
        decode9(pdu, tmp, ints);
        prefixSum16(ints, base);
        break;
      case 10:
        decode10(pdu, tmp, ints);
        prefixSum16(ints, base);
        break;
      case 11:
        decode11To32(pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
      case 12:
        decode12To32(pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
      case 13:
        decode13To32(pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
      case 14:
        decode14To32(pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
      case 15:
        decode15To32(pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
      case 16:
        decode16To32(pdu, ints);
        prefixSum32(ints, base);
        break;
      default:
        if (bitsPerValue < 1 || bitsPerValue > Integer.SIZE) {
          throw new IllegalStateException("Illegal number of bits per value: " + bitsPerValue);
        }
        decodeSlow(bitsPerValue, pdu, tmp, ints);
        prefixSum32(ints, base);
        break;
    }
  }

  private static void decode4To16(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(16, ints, 12, 4, MASK16_4, ints, 48, MASK16_4);
  }

  private static void decode5To16(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(20, ints, 11, 5, MASK16_5, tmp, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, intsIdx = 60; iter < 4; ++iter, tmpIdx += 5, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 4;
      l0 |= tmp[tmpIdx + 1] << 3;
      l0 |= tmp[tmpIdx + 2] << 2;
      l0 |= tmp[tmpIdx + 3] << 1;
      l0 |= tmp[tmpIdx + 4] << 0;
      ints[intsIdx + 0] = l0;
    }
  }

  private static void decode6To16(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(24, ints, 10, 6, MASK16_6, tmp, 0, MASK16_4);
    for (int iter = 0, tmpIdx = 0, intsIdx = 48; iter < 8; ++iter, tmpIdx += 3, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 2) & MASK16_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK16_2) << 4;
      l1 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  private static void decode7To16(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(28, ints, 9, 7, MASK16_7, tmp, 0, MASK16_2);
    for (int iter = 0, tmpIdx = 0, intsIdx = 56; iter < 4; ++iter, tmpIdx += 7, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 5;
      l0 |= tmp[tmpIdx + 1] << 3;
      l0 |= tmp[tmpIdx + 2] << 1;
      l0 |= (tmp[tmpIdx + 3] >>> 1) & MASK16_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 3] & MASK16_1) << 6;
      l1 |= tmp[tmpIdx + 4] << 4;
      l1 |= tmp[tmpIdx + 5] << 2;
      l1 |= tmp[tmpIdx + 6] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  private static void decode8To16(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(32, ints, 8, 8, MASK16_8, ints, 32, MASK16_8);
  }

  private static void decode11To32(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(44, ints, 21, 11, MASK32_11, tmp, 0, MASK32_10);
    for (int iter = 0, tmpIdx = 0, intsIdx = 88; iter < 4; ++iter, tmpIdx += 11, intsIdx += 10) {
      int l0 = tmp[tmpIdx + 0] << 1;
      l0 |= (tmp[tmpIdx + 1] >>> 9) & MASK32_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_9) << 2;
      l1 |= (tmp[tmpIdx + 2] >>> 8) & MASK32_2;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 2] & MASK32_8) << 3;
      l2 |= (tmp[tmpIdx + 3] >>> 7) & MASK32_3;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 3] & MASK32_7) << 4;
      l3 |= (tmp[tmpIdx + 4] >>> 6) & MASK32_4;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 4] & MASK32_6) << 5;
      l4 |= (tmp[tmpIdx + 5] >>> 5) & MASK32_5;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 5] & MASK32_5) << 6;
      l5 |= (tmp[tmpIdx + 6] >>> 4) & MASK32_6;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 6] & MASK32_4) << 7;
      l6 |= (tmp[tmpIdx + 7] >>> 3) & MASK32_7;
      ints[intsIdx + 6] = l6;
      int l7 = (tmp[tmpIdx + 7] & MASK32_3) << 8;
      l7 |= (tmp[tmpIdx + 8] >>> 2) & MASK32_8;
      ints[intsIdx + 7] = l7;
      int l8 = (tmp[tmpIdx + 8] & MASK32_2) << 9;
      l8 |= (tmp[tmpIdx + 9] >>> 1) & MASK32_9;
      ints[intsIdx + 8] = l8;
      int l9 = (tmp[tmpIdx + 9] & MASK32_1) << 10;
      l9 |= tmp[tmpIdx + 10] << 0;
      ints[intsIdx + 9] = l9;
    }
  }

  private static void decode12To32(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(48, ints, 20, 12, MASK32_12, tmp, 0, MASK32_8);
    for (int iter = 0, tmpIdx = 0, intsIdx = 96; iter < 16; ++iter, tmpIdx += 3, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 4) & MASK32_4;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_4) << 8;
      l1 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  private static void decode13To32(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(52, ints, 19, 13, MASK32_13, tmp, 0, MASK32_6);
    for (int iter = 0, tmpIdx = 0, intsIdx = 104; iter < 4; ++iter, tmpIdx += 13, intsIdx += 6) {
      int l0 = tmp[tmpIdx + 0] << 7;
      l0 |= tmp[tmpIdx + 1] << 1;
      l0 |= (tmp[tmpIdx + 2] >>> 5) & MASK32_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 2] & MASK32_5) << 8;
      l1 |= tmp[tmpIdx + 3] << 2;
      l1 |= (tmp[tmpIdx + 4] >>> 4) & MASK32_2;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 4] & MASK32_4) << 9;
      l2 |= tmp[tmpIdx + 5] << 3;
      l2 |= (tmp[tmpIdx + 6] >>> 3) & MASK32_3;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 6] & MASK32_3) << 10;
      l3 |= tmp[tmpIdx + 7] << 4;
      l3 |= (tmp[tmpIdx + 8] >>> 2) & MASK32_4;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 8] & MASK32_2) << 11;
      l4 |= tmp[tmpIdx + 9] << 5;
      l4 |= (tmp[tmpIdx + 10] >>> 1) & MASK32_5;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 10] & MASK32_1) << 12;
      l5 |= tmp[tmpIdx + 11] << 6;
      l5 |= tmp[tmpIdx + 12] << 0;
      ints[intsIdx + 5] = l5;
    }
  }

  private static void decode14To32(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(56, ints, 18, 14, MASK32_14, tmp, 0, MASK32_4);
    for (int iter = 0, tmpIdx = 0, intsIdx = 112; iter < 8; ++iter, tmpIdx += 7, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 10;
      l0 |= tmp[tmpIdx + 1] << 6;
      l0 |= tmp[tmpIdx + 2] << 2;
      l0 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 3] & MASK32_2) << 12;
      l1 |= tmp[tmpIdx + 4] << 8;
      l1 |= tmp[tmpIdx + 5] << 4;
      l1 |= tmp[tmpIdx + 6] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  private static void decode15To32(PostingDecodingUtil pdu, int[] tmp, int[] ints)
      throws IOException {
    pdu.splitInts(60, ints, 17, 15, MASK32_15, tmp, 0, MASK32_2);
    for (int iter = 0, tmpIdx = 0, intsIdx = 120; iter < 4; ++iter, tmpIdx += 15, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 13;
      l0 |= tmp[tmpIdx + 1] << 11;
      l0 |= tmp[tmpIdx + 2] << 9;
      l0 |= tmp[tmpIdx + 3] << 7;
      l0 |= tmp[tmpIdx + 4] << 5;
      l0 |= tmp[tmpIdx + 5] << 3;
      l0 |= tmp[tmpIdx + 6] << 1;
      l0 |= (tmp[tmpIdx + 7] >>> 1) & MASK32_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 7] & MASK32_1) << 14;
      l1 |= tmp[tmpIdx + 8] << 12;
      l1 |= tmp[tmpIdx + 9] << 10;
      l1 |= tmp[tmpIdx + 10] << 8;
      l1 |= tmp[tmpIdx + 11] << 6;
      l1 |= tmp[tmpIdx + 12] << 4;
      l1 |= tmp[tmpIdx + 13] << 2;
      l1 |= tmp[tmpIdx + 14] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  private static void decode16To32(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(64, ints, 16, 16, MASK32_16, ints, 64, MASK32_16);
  }
}
