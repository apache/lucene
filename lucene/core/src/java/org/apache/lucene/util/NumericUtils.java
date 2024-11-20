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

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Helper APIs to encode numeric values as sortable bytes and vice-versa.
 *
 * <p>To also index floating point numbers, this class supplies two methods to convert them to
 * integer values by changing their bit layout: {@link #doubleToSortableLong}, {@link
 * #floatToSortableInt}. You will have no precision loss by converting floating point numbers to
 * integers and back (only that the integer form is not usable). Other data types like dates can
 * easily converted to longs or ints (e.g. date to long: {@link java.util.Date#getTime}).
 *
 * @lucene.internal
 */
public final class NumericUtils {

  private NumericUtils() {} // no instance!

  /**
   * Converts a <code>double</code> value to a sortable signed <code>long</code>. The value is
   * converted by getting their IEEE 754 floating-point &quot;double format&quot; bit layout and
   * then some bits are swapped, to be able to compare the result as long. By this the precision is
   * not reduced, but the value can easily used as a long. The sort order (including {@link
   * Double#NaN}) is defined by {@link Double#compareTo}; {@code NaN} is greater than positive
   * infinity.
   *
   * @see #sortableLongToDouble
   */
  public static long doubleToSortableLong(double value) {
    return sortableDoubleBits(Double.doubleToLongBits(value));
  }

  /**
   * Converts a sortable <code>long</code> back to a <code>double</code>.
   *
   * @see #doubleToSortableLong
   */
  public static double sortableLongToDouble(long encoded) {
    return Double.longBitsToDouble(sortableDoubleBits(encoded));
  }

  /**
   * Converts a <code>float</code> value to a sortable signed <code>int</code>. The value is
   * converted by getting their IEEE 754 floating-point &quot;float format&quot; bit layout and then
   * some bits are swapped, to be able to compare the result as int. By this the precision is not
   * reduced, but the value can easily used as an int. The sort order (including {@link Float#NaN})
   * is defined by {@link Float#compareTo}; {@code NaN} is greater than positive infinity.
   *
   * @see #sortableIntToFloat
   */
  public static int floatToSortableInt(float value) {
    return sortableFloatBits(Float.floatToIntBits(value));
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   *
   * @see #floatToSortableInt
   */
  public static float sortableIntToFloat(int encoded) {
    return Float.intBitsToFloat(sortableFloatBits(encoded));
  }

  /** Converts IEEE 754 representation of a double to sortable order (or back to the original) */
  public static long sortableDoubleBits(long bits) {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }

  /** Converts IEEE 754 representation of a float to sortable order (or back to the original) */
  public static int sortableFloatBits(int bits) {
    return bits ^ (bits >> 31) & 0x7fffffff;
  }

  /** Result = a - b, where a &gt;= b, else {@code IllegalArgumentException} is thrown. */
  public static void subtract(int bytesPerDim, int dim, byte[] a, byte[] b, byte[] result) {
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
    int borrow = 0;
    for (int i = end - 1; i >= start; i--) {
      int diff = (a[i] & 0xff) - (b[i] & 0xff) - borrow;
      if (diff < 0) {
        diff += 256;
        borrow = 1;
      } else {
        borrow = 0;
      }
      result[i - start] = (byte) diff;
    }
    if (borrow != 0) {
      throw new IllegalArgumentException("a < b");
    }
  }

  /**
   * Result = a + b, where a and b are unsigned. If there is an overflow, {@code
   * IllegalArgumentException} is thrown.
   */
  public static void add(int bytesPerDim, int dim, byte[] a, byte[] b, byte[] result) {
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
    int carry = 0;
    for (int i = end - 1; i >= start; i--) {
      int digitSum = (a[i] & 0xff) + (b[i] & 0xff) + carry;
      if (digitSum > 255) {
        digitSum -= 256;
        carry = 1;
      } else {
        carry = 0;
      }
      result[i - start] = (byte) digitSum;
    }
    if (carry != 0) {
      throw new IllegalArgumentException("a + b overflows bytesPerDim=" + bytesPerDim);
    }
  }

  /**
   * Encodes an integer {@code value} such that unsigned byte order comparison is consistent with
   * {@link Integer#compare(int, int)}
   *
   * @see #sortableBytesToInt(byte[], int)
   */
  public static void intToSortableBytes(int value, byte[] result, int offset) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    value ^= 0x80000000;
    BitUtil.VH_BE_INT.set(result, offset, value);
  }

  /**
   * Decodes an integer value previously written with {@link #intToSortableBytes}
   *
   * @see #intToSortableBytes(int, byte[], int)
   */
  public static int sortableBytesToInt(byte[] encoded, int offset) {
    int x = (int) BitUtil.VH_BE_INT.get(encoded, offset);
    // Re-flip the sign bit to restore the original value:
    return x ^ 0x80000000;
  }

  /**
   * Encodes an long {@code value} such that unsigned byte order comparison is consistent with
   * {@link Long#compare(long, long)}
   *
   * @see #sortableBytesToLong(byte[], int)
   */
  public static void longToSortableBytes(long value, byte[] result, int offset) {
    // Flip the sign bit so negative longs sort before positive longs:
    value ^= 0x8000000000000000L;
    BitUtil.VH_BE_LONG.set(result, offset, value);
  }

  /**
   * Decodes a long value previously written with {@link #longToSortableBytes}
   *
   * @see #longToSortableBytes(long, byte[], int)
   */
  public static long sortableBytesToLong(byte[] encoded, int offset) {
    long v = (long) BitUtil.VH_BE_LONG.get(encoded, offset);
    // Flip the sign bit back
    v ^= 0x8000000000000000L;
    return v;
  }

  /**
   * Encodes a BigInteger {@code value} such that unsigned byte order comparison is consistent with
   * {@link BigInteger#compareTo(BigInteger)}. This also sign-extends the value to {@code
   * bigIntSize} bytes if necessary: useful to create a fixed-width size.
   *
   * @see #sortableBytesToBigInt(byte[], int, int)
   */
  public static void bigIntToSortableBytes(
      BigInteger bigInt, int bigIntSize, byte[] result, int offset) {
    byte[] bigIntBytes = bigInt.toByteArray();
    byte[] fullBigIntBytes;

    if (bigIntBytes.length < bigIntSize) {
      fullBigIntBytes = new byte[bigIntSize];
      System.arraycopy(
          bigIntBytes, 0, fullBigIntBytes, bigIntSize - bigIntBytes.length, bigIntBytes.length);
      if ((bigIntBytes[0] & 0x80) != 0) {
        // sign extend
        Arrays.fill(fullBigIntBytes, 0, bigIntSize - bigIntBytes.length, (byte) 0xff);
      }
    } else if (bigIntBytes.length == bigIntSize) {
      fullBigIntBytes = bigIntBytes;
    } else {
      throw new IllegalArgumentException(
          "BigInteger: " + bigInt + " requires more than " + bigIntSize + " bytes storage");
    }
    // Flip the sign bit so negative bigints sort before positive bigints:
    fullBigIntBytes[0] ^= 0x80;

    System.arraycopy(fullBigIntBytes, 0, result, offset, bigIntSize);

    assert sortableBytesToBigInt(result, offset, bigIntSize).equals(bigInt)
        : "bigInt=" + bigInt + " converted=" + sortableBytesToBigInt(result, offset, bigIntSize);
  }

  /**
   * Decodes a BigInteger value previously written with {@link #bigIntToSortableBytes}
   *
   * @see #bigIntToSortableBytes(BigInteger, int, byte[], int)
   */
  public static BigInteger sortableBytesToBigInt(byte[] encoded, int offset, int length) {
    byte[] bigIntBytes = new byte[length];
    System.arraycopy(encoded, offset, bigIntBytes, 0, length);
    // Flip the sign bit back to the original
    bigIntBytes[0] ^= 0x80;
    return new BigInteger(bigIntBytes);
  }
}
