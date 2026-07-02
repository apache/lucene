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

package org.apache.lucene.util;

/**
 * Simple64: pack multiple small non-negative integers into a single long.
 *
 * <p>The high 4 bits of each long are a selector that encodes the bit-width and count of the packed
 * integers. The remaining 60 bits hold the actual values, packed from LSB to MSB.
 *
 * <p>Designed for {@code int} values (non-negative, max {@link Integer#MAX_VALUE} = 2^31 - 1).
 *
 * <p>Selector table (14 schemes, selectors 14-15 reserved):
 *
 * <pre>
 * selector | integers | bits each | max value
 * ---------+----------+-----------+--------------------
 *    0     |    60    |     1     |                   1
 *    1     |    30    |     2     |                   3
 *    2     |    20    |     3     |                   7
 *    3     |    15    |     4     |                  15
 *    4     |    12    |     5     |                  31
 *    5     |    10    |     6     |                  63
 *    6     |     8    |     7     |                 127
 *    7     |     7    |     8     |                 255
 *    8     |     6    |    10     |                1023
 *    9     |     5    |    12     |                4095
 *   10     |     4    |    15     |               32767
 *   11     |     3    |    20     |             1048575
 *   12     |     2    |    30     |          1073741823
 *   13     |     1    |    31     |  2147483647 (Integer.MAX_VALUE)
 * </pre>
 */
public final class Simple64 {
  /** Number of integers each selector can pack. */
  public static final int[] COUNTS = {60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  /** Bit-width per integer for each selector. */
  public static final int[] BITS = {1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 31};

  /** Bit-mask for extracting one value under each selector. */
  public static final long[] MASKS = new long[14];

  static {
    for (int s = 0; s < 14; s++) {
      MASKS[s] = (1L << BITS[s]) - 1L;
    }
  }

  private Simple64() {}

  /**
   * Encode as many integers from {@code ints[offset..]} as fit into a single long, using the
   * most-compact selector whose bit-width can represent every value.
   *
   * <p>Call {@link #count(long)} on the returned word to find out how many integers were consumed.
   *
   * <p>NOTE: a selector is only accepted when <em>all</em> {@code Math.min(COUNTS[selector],
   * length)} values fit its bit-width, so that {@link #count(long)} always returns the correct
   * number of consumed integers. The exception is the last encoded long, where {@code length <
   * COUNTS[selector]} leaves it partially filled and {@link #count(long)} over-reports; this is
   * harmless because {@link #encodeAll} terminates immediately after.
   *
   * @param ints source array of non-negative integers
   * @param offset start index
   * @param length number of integers available from {@code offset}
   * @return encoded long
   */
  public static long encode(int[] ints, int offset, int length) {
    if (length <= 0) {
      throw new IllegalArgumentException("length must be > 0; got " + length);
    }

    final int limit = Math.min(COUNTS[0], length);
    long prefixOr = 0L;
    int fitMask = 0;

    for (int i = 1; i <= limit; i++) {
      final int v = ints[offset + i - 1];
      if (v < 0) {
        throw new IllegalArgumentException("Simple64 does not support negative values, got: " + v);
      }
      prefixOr |= v;
      final int bits = bitsRequired(prefixOr);

      switch (i) {
        case 1 -> fitMask |= 1 << 13;
        case 2 -> {
          if (bits <= 30) fitMask |= 1 << 12;
        }
        case 3 -> {
          if (bits <= 20) fitMask |= 1 << 11;
        }
        case 4 -> {
          if (bits <= 15) fitMask |= 1 << 10;
        }
        case 5 -> {
          if (bits <= 12) fitMask |= 1 << 9;
        }
        case 6 -> {
          if (bits <= 10) fitMask |= 1 << 8;
        }
        case 7 -> {
          if (bits <= 8) fitMask |= 1 << 7;
        }
        case 8 -> {
          if (bits <= 7) fitMask |= 1 << 6;
        }
        case 10 -> {
          if (bits <= 6) fitMask |= 1 << 5;
        }
        case 12 -> {
          if (bits <= 5) fitMask |= 1 << 4;
        }
        case 15 -> {
          if (length >= 15 && bits == 4) {
            return pack15x4(ints, offset);
          }
          if (bits <= 4) fitMask |= 1 << 3;
        }
        case 20 -> {
          if (length >= 20 && bits == 3) {
            return pack20x3(ints, offset);
          }
          if (bits <= 3) fitMask |= 1 << 2;
        }
        case 30 -> {
          if (length >= 30 && bits == 2) {
            return pack30x2(ints, offset);
          }
          if (bits <= 2) fitMask |= 1 << 1;
        }
        case 60 -> {
          if (bits == 1) {
            return pack60x1(ints, offset);
          }
          if (bits <= 1) fitMask |= 1;
        }
        default -> {}
      }
    }

    final int bits = bitsRequired(prefixOr);
    for (int s = 0; s < 14; s++) {
      final int count = COUNTS[s];
      if (count >= length) {
        if (bits <= BITS[s]) {
          return pack(s, ints, offset, length);
        }
      } else if ((fitMask & (1 << s)) != 0) {
        return pack(s, ints, offset, count);
      }
    }

    // Unreachable: selector 13 covers all non-negative int values (up to 2^31-1).
    throw new AssertionError("unreachable");
  }

  /** Pack {@code count} integers using the given selector (low-to-high bit order). */
  public static long pack(int selector, int[] ints, int offset, int count) {
    if (count == COUNTS[selector]) {
      switch (selector) {
        case 0:
          return pack60x1(ints, offset);
        case 1:
          return pack30x2(ints, offset);
        case 2:
          return pack20x3(ints, offset);
        case 3:
          return pack15x4(ints, offset);
        default:
          break;
      }
    }

    final int bits = BITS[selector];
    long word = (long) selector << 60;
    for (int i = 0; i < count; i++) {
      word |= ((long) ints[offset + i]) << (i * bits);
    }
    return word;
  }

  /**
   * Decode all integers packed in {@code word} into {@code out[outOffset..]}.
   *
   * @return number of integers written
   */
  public static int decode(long word, int[] out, int outOffset) {
    final int selector = selector(word);
    final int count = COUNTS[selector];
    decodeValues(word, selector, out, outOffset, count);
    return count;
  }

  /** Return the number of integers packed in {@code word} without fully decoding it. */
  public static int count(long word) {
    return COUNTS[selector(word)];
  }

  /**
   * Encode all {@code length} integers from {@code ints[offset..offset+length)} into consecutive
   * longs in {@code out}, starting at {@code out[outOffset]}.
   *
   * @return number of longs written
   */
  public static int encodeAll(int[] ints, int offset, int length, long[] out, int outOffset) {
    int inPos = offset;
    int outPos = outOffset;
    final int end = offset + length;
    while (inPos < end) {
      long word = encode(ints, inPos, end - inPos);
      out[outPos++] = word;
      inPos += count(word);
    }
    return outPos - outOffset;
  }

  /**
   * Decode exactly {@code count} integers from {@code longs[offset..)} into {@code out}.
   *
   * <p>The caller must supply the original integer count (e.g. stored as a preceding VInt), because
   * the last packed long may contain more slots than remaining integers.
   *
   * @return number of longs consumed
   */
  public static int decodeAll(long[] longs, int offset, int[] out, int outOffset, int count) {
    int inPos = offset;
    int remaining = count;
    while (remaining > 0) {
      long word = longs[inPos++];
      final int selector = selector(word);
      final int toRead = Math.min(COUNTS[selector], remaining);
      decodeValues(word, selector, out, outOffset, toRead);
      outOffset += toRead;
      remaining -= toRead;
    }
    return inPos - offset;
  }

  private static int selector(long word) {
    final int selector = (int) (word >>> 60);
    if (selector >= 14) {
      throw new IllegalArgumentException("Invalid Simple64 selector: " + selector);
    }
    return selector;
  }

  private static int bitsRequired(long value) {
    return value == 0 ? 1 : Long.SIZE - Long.numberOfLeadingZeros(value);
  }

  private static void decodeValues(long word, int selector, int[] out, int outOffset, int count) {
    if (count == 0) {
      return;
    }

    if (count == COUNTS[selector]) {
      switch (selector) {
        case 0 -> {
          decode60x1(word, out, outOffset);
          return;
        }
        case 1 -> {
          decode30x2(word, out, outOffset);
          return;
        }
        case 2 -> {
          decode20x3(word, out, outOffset);
          return;
        }
        case 3 -> {
          decode15x4(word, out, outOffset);
          return;
        }
        case 4 -> {
          decode12x5(word, out, outOffset);
          return;
        }
        case 5 -> {
          decode10x6(word, out, outOffset);
          return;
        }
        case 6 -> {
          decode8x7(word, out, outOffset);
          return;
        }
        case 7 -> {
          decode7x8(word, out, outOffset);
          return;
        }
        default -> {}
      }
    }

    unpack(word, BITS[selector], MASKS[selector], out, outOffset, count);
  }

  private static void unpack(long word, int bits, long mask, int[] out, int outOffset, int count) {
    for (int i = 0; i < count; i++) {
      out[outOffset + i] = (int) (word & mask);
      word >>>= bits;
    }
  }

  private static void decode60x1(long word, int[] out, int outOffset) {
    unpack(word, 1, MASKS[0], out, outOffset, 60);
  }

  private static void decode30x2(long word, int[] out, int outOffset) {
    out[outOffset] = (int) (word & 0x3L);
    out[outOffset + 1] = (int) ((word >>> 2) & 0x3L);
    out[outOffset + 2] = (int) ((word >>> 4) & 0x3L);
    out[outOffset + 3] = (int) ((word >>> 6) & 0x3L);
    out[outOffset + 4] = (int) ((word >>> 8) & 0x3L);
    out[outOffset + 5] = (int) ((word >>> 10) & 0x3L);
    out[outOffset + 6] = (int) ((word >>> 12) & 0x3L);
    out[outOffset + 7] = (int) ((word >>> 14) & 0x3L);
    out[outOffset + 8] = (int) ((word >>> 16) & 0x3L);
    out[outOffset + 9] = (int) ((word >>> 18) & 0x3L);
    out[outOffset + 10] = (int) ((word >>> 20) & 0x3L);
    out[outOffset + 11] = (int) ((word >>> 22) & 0x3L);
    out[outOffset + 12] = (int) ((word >>> 24) & 0x3L);
    out[outOffset + 13] = (int) ((word >>> 26) & 0x3L);
    out[outOffset + 14] = (int) ((word >>> 28) & 0x3L);
    out[outOffset + 15] = (int) ((word >>> 30) & 0x3L);
    out[outOffset + 16] = (int) ((word >>> 32) & 0x3L);
    out[outOffset + 17] = (int) ((word >>> 34) & 0x3L);
    out[outOffset + 18] = (int) ((word >>> 36) & 0x3L);
    out[outOffset + 19] = (int) ((word >>> 38) & 0x3L);
    out[outOffset + 20] = (int) ((word >>> 40) & 0x3L);
    out[outOffset + 21] = (int) ((word >>> 42) & 0x3L);
    out[outOffset + 22] = (int) ((word >>> 44) & 0x3L);
    out[outOffset + 23] = (int) ((word >>> 46) & 0x3L);
    out[outOffset + 24] = (int) ((word >>> 48) & 0x3L);
    out[outOffset + 25] = (int) ((word >>> 50) & 0x3L);
    out[outOffset + 26] = (int) ((word >>> 52) & 0x3L);
    out[outOffset + 27] = (int) ((word >>> 54) & 0x3L);
    out[outOffset + 28] = (int) ((word >>> 56) & 0x3L);
    out[outOffset + 29] = (int) ((word >>> 58) & 0x3L);
  }

  private static void decode20x3(long word, int[] out, int outOffset) {
    out[outOffset] = (int) (word & 0x7L);
    out[outOffset + 1] = (int) ((word >>> 3) & 0x7L);
    out[outOffset + 2] = (int) ((word >>> 6) & 0x7L);
    out[outOffset + 3] = (int) ((word >>> 9) & 0x7L);
    out[outOffset + 4] = (int) ((word >>> 12) & 0x7L);
    out[outOffset + 5] = (int) ((word >>> 15) & 0x7L);
    out[outOffset + 6] = (int) ((word >>> 18) & 0x7L);
    out[outOffset + 7] = (int) ((word >>> 21) & 0x7L);
    out[outOffset + 8] = (int) ((word >>> 24) & 0x7L);
    out[outOffset + 9] = (int) ((word >>> 27) & 0x7L);
    out[outOffset + 10] = (int) ((word >>> 30) & 0x7L);
    out[outOffset + 11] = (int) ((word >>> 33) & 0x7L);
    out[outOffset + 12] = (int) ((word >>> 36) & 0x7L);
    out[outOffset + 13] = (int) ((word >>> 39) & 0x7L);
    out[outOffset + 14] = (int) ((word >>> 42) & 0x7L);
    out[outOffset + 15] = (int) ((word >>> 45) & 0x7L);
    out[outOffset + 16] = (int) ((word >>> 48) & 0x7L);
    out[outOffset + 17] = (int) ((word >>> 51) & 0x7L);
    out[outOffset + 18] = (int) ((word >>> 54) & 0x7L);
    out[outOffset + 19] = (int) ((word >>> 57) & 0x7L);
  }

  private static void decode15x4(long word, int[] out, int outOffset) {
    out[outOffset] = (int) (word & 0xFL);
    out[outOffset + 1] = (int) ((word >>> 4) & 0xFL);
    out[outOffset + 2] = (int) ((word >>> 8) & 0xFL);
    out[outOffset + 3] = (int) ((word >>> 12) & 0xFL);
    out[outOffset + 4] = (int) ((word >>> 16) & 0xFL);
    out[outOffset + 5] = (int) ((word >>> 20) & 0xFL);
    out[outOffset + 6] = (int) ((word >>> 24) & 0xFL);
    out[outOffset + 7] = (int) ((word >>> 28) & 0xFL);
    out[outOffset + 8] = (int) ((word >>> 32) & 0xFL);
    out[outOffset + 9] = (int) ((word >>> 36) & 0xFL);
    out[outOffset + 10] = (int) ((word >>> 40) & 0xFL);
    out[outOffset + 11] = (int) ((word >>> 44) & 0xFL);
    out[outOffset + 12] = (int) ((word >>> 48) & 0xFL);
    out[outOffset + 13] = (int) ((word >>> 52) & 0xFL);
    out[outOffset + 14] = (int) ((word >>> 56) & 0xFL);
  }

  private static void decode12x5(long word, int[] out, int outOffset) {
    unpack(word, 5, MASKS[4], out, outOffset, 12);
  }

  private static void decode10x6(long word, int[] out, int outOffset) {
    unpack(word, 6, MASKS[5], out, outOffset, 10);
  }

  private static void decode8x7(long word, int[] out, int outOffset) {
    unpack(word, 7, MASKS[6], out, outOffset, 8);
  }

  private static void decode7x8(long word, int[] out, int outOffset) {
    unpack(word, 8, MASKS[7], out, outOffset, 7);
  }

  private static long pack60x1(int[] ints, int offset) {
    return ((long) ints[offset])
        | ((long) ints[offset + 1] << 1)
        | ((long) ints[offset + 2] << 2)
        | ((long) ints[offset + 3] << 3)
        | ((long) ints[offset + 4] << 4)
        | ((long) ints[offset + 5] << 5)
        | ((long) ints[offset + 6] << 6)
        | ((long) ints[offset + 7] << 7)
        | ((long) ints[offset + 8] << 8)
        | ((long) ints[offset + 9] << 9)
        | ((long) ints[offset + 10] << 10)
        | ((long) ints[offset + 11] << 11)
        | ((long) ints[offset + 12] << 12)
        | ((long) ints[offset + 13] << 13)
        | ((long) ints[offset + 14] << 14)
        | ((long) ints[offset + 15] << 15)
        | ((long) ints[offset + 16] << 16)
        | ((long) ints[offset + 17] << 17)
        | ((long) ints[offset + 18] << 18)
        | ((long) ints[offset + 19] << 19)
        | ((long) ints[offset + 20] << 20)
        | ((long) ints[offset + 21] << 21)
        | ((long) ints[offset + 22] << 22)
        | ((long) ints[offset + 23] << 23)
        | ((long) ints[offset + 24] << 24)
        | ((long) ints[offset + 25] << 25)
        | ((long) ints[offset + 26] << 26)
        | ((long) ints[offset + 27] << 27)
        | ((long) ints[offset + 28] << 28)
        | ((long) ints[offset + 29] << 29)
        | ((long) ints[offset + 30] << 30)
        | ((long) ints[offset + 31] << 31)
        | ((long) ints[offset + 32] << 32)
        | ((long) ints[offset + 33] << 33)
        | ((long) ints[offset + 34] << 34)
        | ((long) ints[offset + 35] << 35)
        | ((long) ints[offset + 36] << 36)
        | ((long) ints[offset + 37] << 37)
        | ((long) ints[offset + 38] << 38)
        | ((long) ints[offset + 39] << 39)
        | ((long) ints[offset + 40] << 40)
        | ((long) ints[offset + 41] << 41)
        | ((long) ints[offset + 42] << 42)
        | ((long) ints[offset + 43] << 43)
        | ((long) ints[offset + 44] << 44)
        | ((long) ints[offset + 45] << 45)
        | ((long) ints[offset + 46] << 46)
        | ((long) ints[offset + 47] << 47)
        | ((long) ints[offset + 48] << 48)
        | ((long) ints[offset + 49] << 49)
        | ((long) ints[offset + 50] << 50)
        | ((long) ints[offset + 51] << 51)
        | ((long) ints[offset + 52] << 52)
        | ((long) ints[offset + 53] << 53)
        | ((long) ints[offset + 54] << 54)
        | ((long) ints[offset + 55] << 55)
        | ((long) ints[offset + 56] << 56)
        | ((long) ints[offset + 57] << 57)
        | ((long) ints[offset + 58] << 58)
        | ((long) ints[offset + 59] << 59);
  }

  private static long pack30x2(int[] ints, int offset) {
    return (1L << 60)
        | ((long) ints[offset])
        | ((long) ints[offset + 1] << 2)
        | ((long) ints[offset + 2] << 4)
        | ((long) ints[offset + 3] << 6)
        | ((long) ints[offset + 4] << 8)
        | ((long) ints[offset + 5] << 10)
        | ((long) ints[offset + 6] << 12)
        | ((long) ints[offset + 7] << 14)
        | ((long) ints[offset + 8] << 16)
        | ((long) ints[offset + 9] << 18)
        | ((long) ints[offset + 10] << 20)
        | ((long) ints[offset + 11] << 22)
        | ((long) ints[offset + 12] << 24)
        | ((long) ints[offset + 13] << 26)
        | ((long) ints[offset + 14] << 28)
        | ((long) ints[offset + 15] << 30)
        | ((long) ints[offset + 16] << 32)
        | ((long) ints[offset + 17] << 34)
        | ((long) ints[offset + 18] << 36)
        | ((long) ints[offset + 19] << 38)
        | ((long) ints[offset + 20] << 40)
        | ((long) ints[offset + 21] << 42)
        | ((long) ints[offset + 22] << 44)
        | ((long) ints[offset + 23] << 46)
        | ((long) ints[offset + 24] << 48)
        | ((long) ints[offset + 25] << 50)
        | ((long) ints[offset + 26] << 52)
        | ((long) ints[offset + 27] << 54)
        | ((long) ints[offset + 28] << 56)
        | ((long) ints[offset + 29] << 58);
  }

  private static long pack20x3(int[] ints, int offset) {
    return (2L << 60)
        | ((long) ints[offset])
        | ((long) ints[offset + 1] << 3)
        | ((long) ints[offset + 2] << 6)
        | ((long) ints[offset + 3] << 9)
        | ((long) ints[offset + 4] << 12)
        | ((long) ints[offset + 5] << 15)
        | ((long) ints[offset + 6] << 18)
        | ((long) ints[offset + 7] << 21)
        | ((long) ints[offset + 8] << 24)
        | ((long) ints[offset + 9] << 27)
        | ((long) ints[offset + 10] << 30)
        | ((long) ints[offset + 11] << 33)
        | ((long) ints[offset + 12] << 36)
        | ((long) ints[offset + 13] << 39)
        | ((long) ints[offset + 14] << 42)
        | ((long) ints[offset + 15] << 45)
        | ((long) ints[offset + 16] << 48)
        | ((long) ints[offset + 17] << 51)
        | ((long) ints[offset + 18] << 54)
        | ((long) ints[offset + 19] << 57);
  }

  private static long pack15x4(int[] ints, int offset) {
    return (3L << 60)
        | ((long) ints[offset])
        | ((long) ints[offset + 1] << 4)
        | ((long) ints[offset + 2] << 8)
        | ((long) ints[offset + 3] << 12)
        | ((long) ints[offset + 4] << 16)
        | ((long) ints[offset + 5] << 20)
        | ((long) ints[offset + 6] << 24)
        | ((long) ints[offset + 7] << 28)
        | ((long) ints[offset + 8] << 32)
        | ((long) ints[offset + 9] << 36)
        | ((long) ints[offset + 10] << 40)
        | ((long) ints[offset + 11] << 44)
        | ((long) ints[offset + 12] << 48)
        | ((long) ints[offset + 13] << 52)
        | ((long) ints[offset + 14] << 56);
  }
}
