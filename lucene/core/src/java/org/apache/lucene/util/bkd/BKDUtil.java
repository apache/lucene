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
package org.apache.lucene.util.bkd;

import java.util.Arrays;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.BitUtil;

/** Utility functions to build BKD trees. */
final class BKDUtil {

  private BKDUtil() {}

  /**
   * Return a comparator that computes the common prefix length across the next {@code numBytes} of
   * the provided arrays.
   */
  public static ByteArrayComparator getPrefixLengthComparator(int numBytes) {
    if (numBytes == Long.BYTES) {
      // Used by LongPoint, DoublePoint
      return BKDUtil::commonPrefixLength8;
    } else if (numBytes == Integer.BYTES) {
      // Used by IntPoint, FloatPoint, LatLonPoint, LatLonShape
      return BKDUtil::commonPrefixLength4;
    } else {
      return (a, aOffset, b, bOffset) -> commonPrefixLengthN(a, aOffset, b, bOffset, numBytes);
    }
  }

  /** Return the length of the common prefix across the next 8 bytes of both provided arrays. */
  public static int commonPrefixLength8(byte[] a, int aOffset, byte[] b, int bOffset) {
    long aLong = (long) BitUtil.VH_LE_LONG.get(a, aOffset);
    long bLong = (long) BitUtil.VH_LE_LONG.get(b, bOffset);
    final int commonPrefixInBits = Long.numberOfLeadingZeros(Long.reverseBytes(aLong ^ bLong));
    return commonPrefixInBits >>> 3;
  }

  /** Return the length of the common prefix across the next 4 bytes of both provided arrays. */
  public static int commonPrefixLength4(byte[] a, int aOffset, byte[] b, int bOffset) {
    int aInt = (int) BitUtil.VH_LE_INT.get(a, aOffset);
    int bInt = (int) BitUtil.VH_LE_INT.get(b, bOffset);
    final int commonPrefixInBits = Integer.numberOfLeadingZeros(Integer.reverseBytes(aInt ^ bInt));
    return commonPrefixInBits >>> 3;
  }

  static int commonPrefixLengthN(byte[] a, int aOffset, byte[] b, int bOffset, int numBytes) {
    int cmp = Arrays.mismatch(a, aOffset, aOffset + numBytes, b, bOffset, bOffset + numBytes);
    if (cmp == -1) {
      return numBytes;
    } else {
      return cmp;
    }
  }

  /** Predicate for a fixed number of bytes. */
  @FunctionalInterface
  public static interface ByteArrayPredicate {

    /** Test bytes starting from the given offsets. */
    boolean test(byte[] a, int aOffset, byte[] b, int bOffset);
  }

  /** Return a predicate that tells whether the next {@code numBytes} bytes are equal. */
  public static ByteArrayPredicate getEqualsPredicate(int numBytes) {
    if (numBytes == Long.BYTES) {
      // Used by LongPoint, DoublePoint
      return BKDUtil::equals8;
    } else if (numBytes == Integer.BYTES) {
      // Used by IntPoint, FloatPoint, LatLonPoint, LatLonShape
      return BKDUtil::equals4;
    } else {
      return (a, aOffset, b, bOffset) ->
          Arrays.equals(a, aOffset, aOffset + numBytes, b, bOffset, bOffset + numBytes);
    }
  }

  /** Check whether the next 8 bytes are exactly the same in the provided arrays. */
  public static boolean equals8(byte[] a, int aOffset, byte[] b, int bOffset) {
    long aLong = (long) BitUtil.VH_LE_LONG.get(a, aOffset);
    long bLong = (long) BitUtil.VH_LE_LONG.get(b, bOffset);
    return aLong == bLong;
  }

  /** Check whether the next 4 bytes are exactly the same in the provided arrays. */
  public static boolean equals4(byte[] a, int aOffset, byte[] b, int bOffset) {
    int aInt = (int) BitUtil.VH_LE_INT.get(a, aOffset);
    int bInt = (int) BitUtil.VH_LE_INT.get(b, bOffset);
    return aInt == bInt;
  }
}
