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
package org.apache.lucene.codecs.lucene101;

import java.io.IOException;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;

/**
 * Inspired from https://fulmicoton.com/posts/bitpacking/ Encodes multiple integers in one to get
 * SIMD-like speedups. If bitsPerValue &lt;= 8 then we pack 4 ints per Java int else if bitsPerValue
 * &lt;= 16 we pack 2 ints per Java int else we do scalar operations.
 */
public final class ForUtil {

  public static final int BLOCK_SIZE = 128;
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
      arr[i] = (arr[i] << 24) | (arr[32 + i] << 16) | (arr[64 + i] << 8) | arr[96 + i];
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

  static void encode(int[] ints, int bitsPerValue, int primitiveSize, DataOutput out, int[] tmp)
      throws IOException {
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

  static final int[] MASKS8 = new int[8];
  static final int[] MASKS16 = new int[16];
  static final int[] MASKS32 = new int[32];

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

  // mark values in array as final ints to avoid the cost of reading array, arrays should only be
  // used when the idx is a variable
  static final int MASK8_1 = MASKS8[1];
  static final int MASK8_2 = MASKS8[2];
  static final int MASK8_3 = MASKS8[3];
  static final int MASK8_4 = MASKS8[4];
  static final int MASK8_5 = MASKS8[5];
  static final int MASK8_6 = MASKS8[6];
  static final int MASK8_7 = MASKS8[7];
  static final int MASK16_1 = MASKS16[1];
  static final int MASK16_2 = MASKS16[2];
  static final int MASK16_3 = MASKS16[3];
  static final int MASK16_4 = MASKS16[4];
  static final int MASK16_5 = MASKS16[5];
  static final int MASK16_6 = MASKS16[6];
  static final int MASK16_7 = MASKS16[7];
  static final int MASK16_8 = MASKS16[8];
  static final int MASK16_9 = MASKS16[9];
  static final int MASK16_10 = MASKS16[10];
  static final int MASK16_11 = MASKS16[11];
  static final int MASK16_12 = MASKS16[12];
  static final int MASK16_13 = MASKS16[13];
  static final int MASK16_14 = MASKS16[14];
  static final int MASK16_15 = MASKS16[15];
  static final int MASK32_1 = MASKS32[1];
  static final int MASK32_2 = MASKS32[2];
  static final int MASK32_3 = MASKS32[3];
  static final int MASK32_4 = MASKS32[4];
  static final int MASK32_5 = MASKS32[5];
  static final int MASK32_6 = MASKS32[6];
  static final int MASK32_7 = MASKS32[7];
  static final int MASK32_8 = MASKS32[8];
  static final int MASK32_9 = MASKS32[9];
  static final int MASK32_10 = MASKS32[10];
  static final int MASK32_11 = MASKS32[11];
  static final int MASK32_12 = MASKS32[12];
  static final int MASK32_13 = MASKS32[13];
  static final int MASK32_14 = MASKS32[14];
  static final int MASK32_15 = MASKS32[15];
  static final int MASK32_16 = MASKS32[16];
  static final int MASK32_17 = MASKS32[17];
  static final int MASK32_18 = MASKS32[18];
  static final int MASK32_19 = MASKS32[19];
  static final int MASK32_20 = MASKS32[20];
  static final int MASK32_21 = MASKS32[21];
  static final int MASK32_22 = MASKS32[22];
  static final int MASK32_23 = MASKS32[23];
  static final int MASK32_24 = MASKS32[24];

  /** Decode 128 integers into {@code ints}. */
  void decode(int bitsPerValue, PostingDecodingUtil pdu, int[] ints) throws IOException {
    switch (bitsPerValue) {
      case 1:
        decode1(pdu, ints);
        expand8(ints);
        break;
      case 2:
        decode2(pdu, ints);
        expand8(ints);
        break;
      case 3:
        decode3(pdu, tmp, ints);
        expand8(ints);
        break;
      case 4:
        decode4(pdu, ints);
        expand8(ints);
        break;
      case 5:
        decode5(pdu, tmp, ints);
        expand8(ints);
        break;
      case 6:
        decode6(pdu, tmp, ints);
        expand8(ints);
        break;
      case 7:
        decode7(pdu, tmp, ints);
        expand8(ints);
        break;
      case 8:
        decode8(pdu, ints);
        expand8(ints);
        break;
      case 9:
        decode9(pdu, tmp, ints);
        expand16(ints);
        break;
      case 10:
        decode10(pdu, tmp, ints);
        expand16(ints);
        break;
      case 11:
        decode11(pdu, tmp, ints);
        expand16(ints);
        break;
      case 12:
        decode12(pdu, tmp, ints);
        expand16(ints);
        break;
      case 13:
        decode13(pdu, tmp, ints);
        expand16(ints);
        break;
      case 14:
        decode14(pdu, tmp, ints);
        expand16(ints);
        break;
      case 15:
        decode15(pdu, tmp, ints);
        expand16(ints);
        break;
      case 16:
        decode16(pdu, ints);
        expand16(ints);
        break;
      case 17:
        decode17(pdu, tmp, ints);
        break;
      case 18:
        decode18(pdu, tmp, ints);
        break;
      case 19:
        decode19(pdu, tmp, ints);
        break;
      case 20:
        decode20(pdu, tmp, ints);
        break;
      case 21:
        decode21(pdu, tmp, ints);
        break;
      case 22:
        decode22(pdu, tmp, ints);
        break;
      case 23:
        decode23(pdu, tmp, ints);
        break;
      case 24:
        decode24(pdu, tmp, ints);
        break;
      default:
        decodeSlow(bitsPerValue, pdu, tmp, ints);
        break;
    }
  }

  static void decode1(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(4, ints, 7, 1, MASK8_1, ints, 28, MASK8_1);
  }

  static void decode2(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(8, ints, 6, 2, MASK8_2, ints, 24, MASK8_2);
  }

  static void decode3(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(12, ints, 5, 3, MASK8_3, tmp, 0, MASK8_2);
    for (int iter = 0, tmpIdx = 0, intsIdx = 24; iter < 4; ++iter, tmpIdx += 3, intsIdx += 2) {
      int l0 = tmp[tmpIdx + 0] << 1;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK8_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK8_1) << 2;
      l1 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 1] = l1;
    }
  }

  static void decode4(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.splitInts(16, ints, 4, 4, MASK8_4, ints, 16, MASK8_4);
  }

  static void decode5(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(20, ints, 3, 5, MASK8_5, tmp, 0, MASK8_3);
    for (int iter = 0, tmpIdx = 0, intsIdx = 20; iter < 4; ++iter, tmpIdx += 5, intsIdx += 3) {
      int l0 = tmp[tmpIdx + 0] << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK8_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK8_1) << 4;
      l1 |= tmp[tmpIdx + 2] << 1;
      l1 |= (tmp[tmpIdx + 3] >>> 2) & MASK8_1;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 3] & MASK8_2) << 3;
      l2 |= tmp[tmpIdx + 4] << 0;
      ints[intsIdx + 2] = l2;
    }
  }

  static void decode6(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(24, ints, 2, 6, MASK8_6, tmp, 0, MASK8_2);
    for (int iter = 0, tmpIdx = 0, intsIdx = 24; iter < 8; ++iter, tmpIdx += 3, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 4;
      l0 |= tmp[tmpIdx + 1] << 2;
      l0 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 0] = l0;
    }
  }

  static void decode7(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(28, ints, 1, 7, MASK8_7, tmp, 0, MASK8_1);
    for (int iter = 0, tmpIdx = 0, intsIdx = 28; iter < 4; ++iter, tmpIdx += 7, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 6;
      l0 |= tmp[tmpIdx + 1] << 5;
      l0 |= tmp[tmpIdx + 2] << 4;
      l0 |= tmp[tmpIdx + 3] << 3;
      l0 |= tmp[tmpIdx + 4] << 2;
      l0 |= tmp[tmpIdx + 5] << 1;
      l0 |= tmp[tmpIdx + 6] << 0;
      ints[intsIdx + 0] = l0;
    }
  }

  static void decode8(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.in.readInts(ints, 0, 32);
  }

  static void decode9(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(36, ints, 7, 9, MASK16_9, tmp, 0, MASK16_7);
    for (int iter = 0, tmpIdx = 0, intsIdx = 36; iter < 4; ++iter, tmpIdx += 9, intsIdx += 7) {
      int l0 = tmp[tmpIdx + 0] << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 5) & MASK16_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK16_5) << 4;
      l1 |= (tmp[tmpIdx + 2] >>> 3) & MASK16_4;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 2] & MASK16_3) << 6;
      l2 |= (tmp[tmpIdx + 3] >>> 1) & MASK16_6;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 3] & MASK16_1) << 8;
      l3 |= tmp[tmpIdx + 4] << 1;
      l3 |= (tmp[tmpIdx + 5] >>> 6) & MASK16_1;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 5] & MASK16_6) << 3;
      l4 |= (tmp[tmpIdx + 6] >>> 4) & MASK16_3;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 6] & MASK16_4) << 5;
      l5 |= (tmp[tmpIdx + 7] >>> 2) & MASK16_5;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 7] & MASK16_2) << 7;
      l6 |= tmp[tmpIdx + 8] << 0;
      ints[intsIdx + 6] = l6;
    }
  }

  static void decode10(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(40, ints, 6, 10, MASK16_10, tmp, 0, MASK16_6);
    for (int iter = 0, tmpIdx = 0, intsIdx = 40; iter < 8; ++iter, tmpIdx += 5, intsIdx += 3) {
      int l0 = tmp[tmpIdx + 0] << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 2) & MASK16_4;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK16_2) << 8;
      l1 |= tmp[tmpIdx + 2] << 2;
      l1 |= (tmp[tmpIdx + 3] >>> 4) & MASK16_2;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 3] & MASK16_4) << 6;
      l2 |= tmp[tmpIdx + 4] << 0;
      ints[intsIdx + 2] = l2;
    }
  }

  static void decode11(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(44, ints, 5, 11, MASK16_11, tmp, 0, MASK16_5);
    for (int iter = 0, tmpIdx = 0, intsIdx = 44; iter < 4; ++iter, tmpIdx += 11, intsIdx += 5) {
      int l0 = tmp[tmpIdx + 0] << 6;
      l0 |= tmp[tmpIdx + 1] << 1;
      l0 |= (tmp[tmpIdx + 2] >>> 4) & MASK16_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 2] & MASK16_4) << 7;
      l1 |= tmp[tmpIdx + 3] << 2;
      l1 |= (tmp[tmpIdx + 4] >>> 3) & MASK16_2;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 4] & MASK16_3) << 8;
      l2 |= tmp[tmpIdx + 5] << 3;
      l2 |= (tmp[tmpIdx + 6] >>> 2) & MASK16_3;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 6] & MASK16_2) << 9;
      l3 |= tmp[tmpIdx + 7] << 4;
      l3 |= (tmp[tmpIdx + 8] >>> 1) & MASK16_4;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 8] & MASK16_1) << 10;
      l4 |= tmp[tmpIdx + 9] << 5;
      l4 |= tmp[tmpIdx + 10] << 0;
      ints[intsIdx + 4] = l4;
    }
  }

  static void decode12(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(48, ints, 4, 12, MASK16_12, tmp, 0, MASK16_4);
    for (int iter = 0, tmpIdx = 0, intsIdx = 48; iter < 16; ++iter, tmpIdx += 3, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 8;
      l0 |= tmp[tmpIdx + 1] << 4;
      l0 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 0] = l0;
    }
  }

  static void decode13(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(52, ints, 3, 13, MASK16_13, tmp, 0, MASK16_3);
    for (int iter = 0, tmpIdx = 0, intsIdx = 52; iter < 4; ++iter, tmpIdx += 13, intsIdx += 3) {
      int l0 = tmp[tmpIdx + 0] << 10;
      l0 |= tmp[tmpIdx + 1] << 7;
      l0 |= tmp[tmpIdx + 2] << 4;
      l0 |= tmp[tmpIdx + 3] << 1;
      l0 |= (tmp[tmpIdx + 4] >>> 2) & MASK16_1;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 4] & MASK16_2) << 11;
      l1 |= tmp[tmpIdx + 5] << 8;
      l1 |= tmp[tmpIdx + 6] << 5;
      l1 |= tmp[tmpIdx + 7] << 2;
      l1 |= (tmp[tmpIdx + 8] >>> 1) & MASK16_2;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 8] & MASK16_1) << 12;
      l2 |= tmp[tmpIdx + 9] << 9;
      l2 |= tmp[tmpIdx + 10] << 6;
      l2 |= tmp[tmpIdx + 11] << 3;
      l2 |= tmp[tmpIdx + 12] << 0;
      ints[intsIdx + 2] = l2;
    }
  }

  static void decode14(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(56, ints, 2, 14, MASK16_14, tmp, 0, MASK16_2);
    for (int iter = 0, tmpIdx = 0, intsIdx = 56; iter < 8; ++iter, tmpIdx += 7, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 12;
      l0 |= tmp[tmpIdx + 1] << 10;
      l0 |= tmp[tmpIdx + 2] << 8;
      l0 |= tmp[tmpIdx + 3] << 6;
      l0 |= tmp[tmpIdx + 4] << 4;
      l0 |= tmp[tmpIdx + 5] << 2;
      l0 |= tmp[tmpIdx + 6] << 0;
      ints[intsIdx + 0] = l0;
    }
  }

  static void decode15(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(60, ints, 1, 15, MASK16_15, tmp, 0, MASK16_1);
    for (int iter = 0, tmpIdx = 0, intsIdx = 60; iter < 4; ++iter, tmpIdx += 15, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 14;
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
      ints[intsIdx + 0] = l0;
    }
  }

  static void decode16(PostingDecodingUtil pdu, int[] ints) throws IOException {
    pdu.in.readInts(ints, 0, 64);
  }

  static void decode17(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(68, ints, 15, 17, MASK32_17, tmp, 0, MASK32_15);
    for (int iter = 0, tmpIdx = 0, intsIdx = 68; iter < 4; ++iter, tmpIdx += 17, intsIdx += 15) {
      int l0 = tmp[tmpIdx + 0] << 2;
      l0 |= (tmp[tmpIdx + 1] >>> 13) & MASK32_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_13) << 4;
      l1 |= (tmp[tmpIdx + 2] >>> 11) & MASK32_4;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 2] & MASK32_11) << 6;
      l2 |= (tmp[tmpIdx + 3] >>> 9) & MASK32_6;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 3] & MASK32_9) << 8;
      l3 |= (tmp[tmpIdx + 4] >>> 7) & MASK32_8;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 4] & MASK32_7) << 10;
      l4 |= (tmp[tmpIdx + 5] >>> 5) & MASK32_10;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 5] & MASK32_5) << 12;
      l5 |= (tmp[tmpIdx + 6] >>> 3) & MASK32_12;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 6] & MASK32_3) << 14;
      l6 |= (tmp[tmpIdx + 7] >>> 1) & MASK32_14;
      ints[intsIdx + 6] = l6;
      int l7 = (tmp[tmpIdx + 7] & MASK32_1) << 16;
      l7 |= tmp[tmpIdx + 8] << 1;
      l7 |= (tmp[tmpIdx + 9] >>> 14) & MASK32_1;
      ints[intsIdx + 7] = l7;
      int l8 = (tmp[tmpIdx + 9] & MASK32_14) << 3;
      l8 |= (tmp[tmpIdx + 10] >>> 12) & MASK32_3;
      ints[intsIdx + 8] = l8;
      int l9 = (tmp[tmpIdx + 10] & MASK32_12) << 5;
      l9 |= (tmp[tmpIdx + 11] >>> 10) & MASK32_5;
      ints[intsIdx + 9] = l9;
      int l10 = (tmp[tmpIdx + 11] & MASK32_10) << 7;
      l10 |= (tmp[tmpIdx + 12] >>> 8) & MASK32_7;
      ints[intsIdx + 10] = l10;
      int l11 = (tmp[tmpIdx + 12] & MASK32_8) << 9;
      l11 |= (tmp[tmpIdx + 13] >>> 6) & MASK32_9;
      ints[intsIdx + 11] = l11;
      int l12 = (tmp[tmpIdx + 13] & MASK32_6) << 11;
      l12 |= (tmp[tmpIdx + 14] >>> 4) & MASK32_11;
      ints[intsIdx + 12] = l12;
      int l13 = (tmp[tmpIdx + 14] & MASK32_4) << 13;
      l13 |= (tmp[tmpIdx + 15] >>> 2) & MASK32_13;
      ints[intsIdx + 13] = l13;
      int l14 = (tmp[tmpIdx + 15] & MASK32_2) << 15;
      l14 |= tmp[tmpIdx + 16] << 0;
      ints[intsIdx + 14] = l14;
    }
  }

  static void decode18(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(72, ints, 14, 18, MASK32_18, tmp, 0, MASK32_14);
    for (int iter = 0, tmpIdx = 0, intsIdx = 72; iter < 8; ++iter, tmpIdx += 9, intsIdx += 7) {
      int l0 = tmp[tmpIdx + 0] << 4;
      l0 |= (tmp[tmpIdx + 1] >>> 10) & MASK32_4;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_10) << 8;
      l1 |= (tmp[tmpIdx + 2] >>> 6) & MASK32_8;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 2] & MASK32_6) << 12;
      l2 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_12;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 3] & MASK32_2) << 16;
      l3 |= tmp[tmpIdx + 4] << 2;
      l3 |= (tmp[tmpIdx + 5] >>> 12) & MASK32_2;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 5] & MASK32_12) << 6;
      l4 |= (tmp[tmpIdx + 6] >>> 8) & MASK32_6;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 6] & MASK32_8) << 10;
      l5 |= (tmp[tmpIdx + 7] >>> 4) & MASK32_10;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 7] & MASK32_4) << 14;
      l6 |= tmp[tmpIdx + 8] << 0;
      ints[intsIdx + 6] = l6;
    }
  }

  static void decode19(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(76, ints, 13, 19, MASK32_19, tmp, 0, MASK32_13);
    for (int iter = 0, tmpIdx = 0, intsIdx = 76; iter < 4; ++iter, tmpIdx += 19, intsIdx += 13) {
      int l0 = tmp[tmpIdx + 0] << 6;
      l0 |= (tmp[tmpIdx + 1] >>> 7) & MASK32_6;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_7) << 12;
      l1 |= (tmp[tmpIdx + 2] >>> 1) & MASK32_12;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 2] & MASK32_1) << 18;
      l2 |= tmp[tmpIdx + 3] << 5;
      l2 |= (tmp[tmpIdx + 4] >>> 8) & MASK32_5;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 4] & MASK32_8) << 11;
      l3 |= (tmp[tmpIdx + 5] >>> 2) & MASK32_11;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 5] & MASK32_2) << 17;
      l4 |= tmp[tmpIdx + 6] << 4;
      l4 |= (tmp[tmpIdx + 7] >>> 9) & MASK32_4;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 7] & MASK32_9) << 10;
      l5 |= (tmp[tmpIdx + 8] >>> 3) & MASK32_10;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 8] & MASK32_3) << 16;
      l6 |= tmp[tmpIdx + 9] << 3;
      l6 |= (tmp[tmpIdx + 10] >>> 10) & MASK32_3;
      ints[intsIdx + 6] = l6;
      int l7 = (tmp[tmpIdx + 10] & MASK32_10) << 9;
      l7 |= (tmp[tmpIdx + 11] >>> 4) & MASK32_9;
      ints[intsIdx + 7] = l7;
      int l8 = (tmp[tmpIdx + 11] & MASK32_4) << 15;
      l8 |= tmp[tmpIdx + 12] << 2;
      l8 |= (tmp[tmpIdx + 13] >>> 11) & MASK32_2;
      ints[intsIdx + 8] = l8;
      int l9 = (tmp[tmpIdx + 13] & MASK32_11) << 8;
      l9 |= (tmp[tmpIdx + 14] >>> 5) & MASK32_8;
      ints[intsIdx + 9] = l9;
      int l10 = (tmp[tmpIdx + 14] & MASK32_5) << 14;
      l10 |= tmp[tmpIdx + 15] << 1;
      l10 |= (tmp[tmpIdx + 16] >>> 12) & MASK32_1;
      ints[intsIdx + 10] = l10;
      int l11 = (tmp[tmpIdx + 16] & MASK32_12) << 7;
      l11 |= (tmp[tmpIdx + 17] >>> 6) & MASK32_7;
      ints[intsIdx + 11] = l11;
      int l12 = (tmp[tmpIdx + 17] & MASK32_6) << 13;
      l12 |= tmp[tmpIdx + 18] << 0;
      ints[intsIdx + 12] = l12;
    }
  }

  static void decode20(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(80, ints, 12, 20, MASK32_20, tmp, 0, MASK32_12);
    for (int iter = 0, tmpIdx = 0, intsIdx = 80; iter < 16; ++iter, tmpIdx += 5, intsIdx += 3) {
      int l0 = tmp[tmpIdx + 0] << 8;
      l0 |= (tmp[tmpIdx + 1] >>> 4) & MASK32_8;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_4) << 16;
      l1 |= tmp[tmpIdx + 2] << 4;
      l1 |= (tmp[tmpIdx + 3] >>> 8) & MASK32_4;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 3] & MASK32_8) << 12;
      l2 |= tmp[tmpIdx + 4] << 0;
      ints[intsIdx + 2] = l2;
    }
  }

  static void decode21(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(84, ints, 11, 21, MASK32_21, tmp, 0, MASK32_11);
    for (int iter = 0, tmpIdx = 0, intsIdx = 84; iter < 4; ++iter, tmpIdx += 21, intsIdx += 11) {
      int l0 = tmp[tmpIdx + 0] << 10;
      l0 |= (tmp[tmpIdx + 1] >>> 1) & MASK32_10;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 1] & MASK32_1) << 20;
      l1 |= tmp[tmpIdx + 2] << 9;
      l1 |= (tmp[tmpIdx + 3] >>> 2) & MASK32_9;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 3] & MASK32_2) << 19;
      l2 |= tmp[tmpIdx + 4] << 8;
      l2 |= (tmp[tmpIdx + 5] >>> 3) & MASK32_8;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 5] & MASK32_3) << 18;
      l3 |= tmp[tmpIdx + 6] << 7;
      l3 |= (tmp[tmpIdx + 7] >>> 4) & MASK32_7;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 7] & MASK32_4) << 17;
      l4 |= tmp[tmpIdx + 8] << 6;
      l4 |= (tmp[tmpIdx + 9] >>> 5) & MASK32_6;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 9] & MASK32_5) << 16;
      l5 |= tmp[tmpIdx + 10] << 5;
      l5 |= (tmp[tmpIdx + 11] >>> 6) & MASK32_5;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 11] & MASK32_6) << 15;
      l6 |= tmp[tmpIdx + 12] << 4;
      l6 |= (tmp[tmpIdx + 13] >>> 7) & MASK32_4;
      ints[intsIdx + 6] = l6;
      int l7 = (tmp[tmpIdx + 13] & MASK32_7) << 14;
      l7 |= tmp[tmpIdx + 14] << 3;
      l7 |= (tmp[tmpIdx + 15] >>> 8) & MASK32_3;
      ints[intsIdx + 7] = l7;
      int l8 = (tmp[tmpIdx + 15] & MASK32_8) << 13;
      l8 |= tmp[tmpIdx + 16] << 2;
      l8 |= (tmp[tmpIdx + 17] >>> 9) & MASK32_2;
      ints[intsIdx + 8] = l8;
      int l9 = (tmp[tmpIdx + 17] & MASK32_9) << 12;
      l9 |= tmp[tmpIdx + 18] << 1;
      l9 |= (tmp[tmpIdx + 19] >>> 10) & MASK32_1;
      ints[intsIdx + 9] = l9;
      int l10 = (tmp[tmpIdx + 19] & MASK32_10) << 11;
      l10 |= tmp[tmpIdx + 20] << 0;
      ints[intsIdx + 10] = l10;
    }
  }

  static void decode22(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(88, ints, 10, 22, MASK32_22, tmp, 0, MASK32_10);
    for (int iter = 0, tmpIdx = 0, intsIdx = 88; iter < 8; ++iter, tmpIdx += 11, intsIdx += 5) {
      int l0 = tmp[tmpIdx + 0] << 12;
      l0 |= tmp[tmpIdx + 1] << 2;
      l0 |= (tmp[tmpIdx + 2] >>> 8) & MASK32_2;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 2] & MASK32_8) << 14;
      l1 |= tmp[tmpIdx + 3] << 4;
      l1 |= (tmp[tmpIdx + 4] >>> 6) & MASK32_4;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 4] & MASK32_6) << 16;
      l2 |= tmp[tmpIdx + 5] << 6;
      l2 |= (tmp[tmpIdx + 6] >>> 4) & MASK32_6;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 6] & MASK32_4) << 18;
      l3 |= tmp[tmpIdx + 7] << 8;
      l3 |= (tmp[tmpIdx + 8] >>> 2) & MASK32_8;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 8] & MASK32_2) << 20;
      l4 |= tmp[tmpIdx + 9] << 10;
      l4 |= tmp[tmpIdx + 10] << 0;
      ints[intsIdx + 4] = l4;
    }
  }

  static void decode23(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(92, ints, 9, 23, MASK32_23, tmp, 0, MASK32_9);
    for (int iter = 0, tmpIdx = 0, intsIdx = 92; iter < 4; ++iter, tmpIdx += 23, intsIdx += 9) {
      int l0 = tmp[tmpIdx + 0] << 14;
      l0 |= tmp[tmpIdx + 1] << 5;
      l0 |= (tmp[tmpIdx + 2] >>> 4) & MASK32_5;
      ints[intsIdx + 0] = l0;
      int l1 = (tmp[tmpIdx + 2] & MASK32_4) << 19;
      l1 |= tmp[tmpIdx + 3] << 10;
      l1 |= tmp[tmpIdx + 4] << 1;
      l1 |= (tmp[tmpIdx + 5] >>> 8) & MASK32_1;
      ints[intsIdx + 1] = l1;
      int l2 = (tmp[tmpIdx + 5] & MASK32_8) << 15;
      l2 |= tmp[tmpIdx + 6] << 6;
      l2 |= (tmp[tmpIdx + 7] >>> 3) & MASK32_6;
      ints[intsIdx + 2] = l2;
      int l3 = (tmp[tmpIdx + 7] & MASK32_3) << 20;
      l3 |= tmp[tmpIdx + 8] << 11;
      l3 |= tmp[tmpIdx + 9] << 2;
      l3 |= (tmp[tmpIdx + 10] >>> 7) & MASK32_2;
      ints[intsIdx + 3] = l3;
      int l4 = (tmp[tmpIdx + 10] & MASK32_7) << 16;
      l4 |= tmp[tmpIdx + 11] << 7;
      l4 |= (tmp[tmpIdx + 12] >>> 2) & MASK32_7;
      ints[intsIdx + 4] = l4;
      int l5 = (tmp[tmpIdx + 12] & MASK32_2) << 21;
      l5 |= tmp[tmpIdx + 13] << 12;
      l5 |= tmp[tmpIdx + 14] << 3;
      l5 |= (tmp[tmpIdx + 15] >>> 6) & MASK32_3;
      ints[intsIdx + 5] = l5;
      int l6 = (tmp[tmpIdx + 15] & MASK32_6) << 17;
      l6 |= tmp[tmpIdx + 16] << 8;
      l6 |= (tmp[tmpIdx + 17] >>> 1) & MASK32_8;
      ints[intsIdx + 6] = l6;
      int l7 = (tmp[tmpIdx + 17] & MASK32_1) << 22;
      l7 |= tmp[tmpIdx + 18] << 13;
      l7 |= tmp[tmpIdx + 19] << 4;
      l7 |= (tmp[tmpIdx + 20] >>> 5) & MASK32_4;
      ints[intsIdx + 7] = l7;
      int l8 = (tmp[tmpIdx + 20] & MASK32_5) << 18;
      l8 |= tmp[tmpIdx + 21] << 9;
      l8 |= tmp[tmpIdx + 22] << 0;
      ints[intsIdx + 8] = l8;
    }
  }

  static void decode24(PostingDecodingUtil pdu, int[] tmp, int[] ints) throws IOException {
    pdu.splitInts(96, ints, 8, 24, MASK32_24, tmp, 0, MASK32_8);
    for (int iter = 0, tmpIdx = 0, intsIdx = 96; iter < 32; ++iter, tmpIdx += 3, intsIdx += 1) {
      int l0 = tmp[tmpIdx + 0] << 16;
      l0 |= tmp[tmpIdx + 1] << 8;
      l0 |= tmp[tmpIdx + 2] << 0;
      ints[intsIdx + 0] = l0;
    }
  }
}
