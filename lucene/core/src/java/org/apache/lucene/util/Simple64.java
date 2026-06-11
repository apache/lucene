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
public class Simple64 {
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
   * @param ints source array of non-negative integers
   * @param offset start index
   * @param length number of integers available from {@code offset}
   * @return encoded long
   * @throws IllegalArgumentException if any value is negative or exceeds {@link Integer#MAX_VALUE}
   */
  public static long encode(int[] ints, int offset, int length) {
    for (int s = 0; s < 14; s++) {
      final int count = Math.min(COUNTS[s], length);
      final long mask = MASKS[s];

      boolean fits = true;
      for (int i = 0; i < count; i++) {
        int v = ints[offset + i];
        if (v < 0) {
          throw new IllegalArgumentException(
              "Simple64 does not support negative values, got: " + v);
        }
        if (v > mask) {
          fits = false;
          break;
        }
      }

      if (fits) {
        return pack(s, ints, offset, count);
      }
    }
    // Unreachable: selector 13 covers all non-negative int values (up to 2^31-1).
    throw new AssertionError("unreachable");
  }

  /** Pack {@code count} integers using the given selector (low-to-high bit order). */
  public static long pack(int selector, int[] ints, int offset, int count) {
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
    final int selector = (int) (word >>> 60);
    if (selector >= 14) {
      throw new IllegalArgumentException("Invalid Simple64 selector: " + selector);
    }
    final int count = COUNTS[selector];
    final int bits = BITS[selector];
    final long mask = MASKS[selector];
    for (int i = 0; i < count; i++) {
      out[outOffset + i] = (int) (word & mask);
      word >>>= bits;
    }
    return count;
  }

  /**
   * Return the number of integers packed in {@code word} without fully decoding it.
   *
   * @throws IllegalArgumentException if the selector field is invalid (14 or 15)
   */
  public static int count(long word) {
    final int selector = (int) (word >>> 60);
    if (selector >= 14) {
      throw new IllegalArgumentException("Invalid Simple64 selector: " + selector);
    }
    return COUNTS[selector];
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
      final int selector = (int) (word >>> 60);
      if (selector >= 14) {
        throw new IllegalArgumentException("Invalid Simple64 selector: " + selector);
      }
      final int toRead = Math.min(COUNTS[selector], remaining);
      final int bits = BITS[selector];
      final long mask = MASKS[selector];
      for (int i = 0; i < toRead; i++) {
        out[outOffset++] = (int) (word & mask);
        word >>>= bits;
      }
      remaining -= toRead;
    }
    return inPos - offset;
  }
}
