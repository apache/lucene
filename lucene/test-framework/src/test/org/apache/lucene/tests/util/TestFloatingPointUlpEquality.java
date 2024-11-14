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
package org.apache.lucene.tests.util;

import static org.apache.lucene.tests.util.TestUtil.doubleUlpEquals;
import static org.apache.lucene.tests.util.TestUtil.floatUlpEquals;

/**
 * Tests for floating point equality utility methods.
 *
 * <p>Adapted from org.apache.commons.numbers.core.PrecisionTest
 *
 * <p>github: https://github.com/apache/commons-numbers release 1.2
 */
public class TestFloatingPointUlpEquality extends LuceneTestCase {
  public static void testDoubleEqualsWithAllowedUlps() {
    assertTrue(doubleUlpEquals(0.0, -0.0, 1));
    assertTrue(doubleUlpEquals(Double.MIN_VALUE, -0.0, 1));
    assertFalse(doubleUlpEquals(Double.MIN_VALUE, -Double.MIN_VALUE, 1));

    assertTrue(doubleUlpEquals(1.0, 1 + Math.ulp(1d), 1));
    assertFalse(doubleUlpEquals(1.0, 1 + 2 * Math.ulp(1d), 1));

    for (double value : new double[] {153.0, -128.0, 0.0, 1.0}) {
      assertTrue(doubleUlpEquals(value, value, 1));
      assertTrue(doubleUlpEquals(value, Math.nextUp(value), 1));
      assertFalse(doubleUlpEquals(value, Math.nextUp(Math.nextUp(value)), 1));
      assertTrue(doubleUlpEquals(value, Math.nextDown(value), 1));
      assertFalse(doubleUlpEquals(value, Math.nextDown(Math.nextDown(value)), 1));
      assertFalse(doubleUlpEquals(value, value, -1));
      assertFalse(doubleUlpEquals(value, Math.nextUp(value), 0));
      assertTrue(doubleUlpEquals(value, Math.nextUp(Math.nextUp(value)), 2));
      assertTrue(doubleUlpEquals(value, Math.nextDown(Math.nextDown(value)), 2));
    }

    assertTrue(doubleUlpEquals(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 1));
    assertTrue(doubleUlpEquals(Double.MAX_VALUE, Double.POSITIVE_INFINITY, 1));

    assertTrue(doubleUlpEquals(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 1));
    assertTrue(doubleUlpEquals(-Double.MAX_VALUE, Double.NEGATIVE_INFINITY, 1));

    assertFalse(doubleUlpEquals(Double.NaN, Double.NaN, 1));
    assertFalse(doubleUlpEquals(Double.NaN, Double.NaN, 0));
    assertFalse(doubleUlpEquals(Double.NaN, 0, 0));
    assertFalse(doubleUlpEquals(0, Double.NaN, 0));
    assertFalse(doubleUlpEquals(Double.NaN, Double.POSITIVE_INFINITY, 0));
    assertFalse(doubleUlpEquals(Double.NaN, Double.NEGATIVE_INFINITY, 0));

    // Create a NaN representation 1 ulp above infinity.
    // This hits not equal coverage for binary representations within the ulp but using NaN.
    final double nan =
        Double.longBitsToDouble(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY) + 1);
    assertFalse(doubleUlpEquals(nan, Double.POSITIVE_INFINITY, 1));
    assertFalse(doubleUlpEquals(Double.POSITIVE_INFINITY, nan, 1));

    assertFalse(
        doubleUlpEquals(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Integer.MAX_VALUE));
    assertFalse(doubleUlpEquals(0, Double.MAX_VALUE, Integer.MAX_VALUE));
    // Here: f == 5.304989477E-315;
    // it is used to test the maximum ULP distance between two opposite sign numbers.
    final double f = Double.longBitsToDouble(1L << 30);
    assertFalse(doubleUlpEquals(-f, f, Integer.MAX_VALUE));
    assertTrue(doubleUlpEquals(-f, Math.nextDown(f), Integer.MAX_VALUE));
    assertTrue(doubleUlpEquals(Math.nextUp(-f), f, Integer.MAX_VALUE));
    // Maximum distance between same sign numbers.
    final double f2 = Double.longBitsToDouble((1L << 30) + Integer.MAX_VALUE);
    assertTrue(doubleUlpEquals(f, f2, Integer.MAX_VALUE));
    assertFalse(doubleUlpEquals(f, Math.nextUp(f2), Integer.MAX_VALUE));
    assertFalse(doubleUlpEquals(Math.nextDown(f), f2, Integer.MAX_VALUE));
  }

  public static void testFloatEqualsWithAllowedUlps() {
    assertTrue(floatUlpEquals(0.0f, -0.0f, (short) 1));
    assertTrue(floatUlpEquals(Float.MIN_VALUE, -0.0f, (short) 1));
    assertFalse(floatUlpEquals(Float.MIN_VALUE, -Float.MIN_VALUE, (short) 1));

    assertTrue(floatUlpEquals(1.0f, 1f + Math.ulp(1f), (short) 1));
    assertFalse(floatUlpEquals(1.0f, 1f + 2 * Math.ulp(1f), (short) 1));

    for (float value : new float[] {153.0f, -128.0f, 0.0f, 1.0f}) {
      assertTrue(floatUlpEquals(value, value, (short) 1));
      assertTrue(floatUlpEquals(value, Math.nextUp(value), (short) 1));
      assertFalse(floatUlpEquals(value, Math.nextUp(Math.nextUp(value)), (short) 1));
      assertTrue(floatUlpEquals(value, Math.nextDown(value), (short) 1));
      assertFalse(floatUlpEquals(value, Math.nextDown(Math.nextDown(value)), (short) 1));
      assertFalse(floatUlpEquals(value, value, (short) -1));
      assertFalse(floatUlpEquals(value, Math.nextUp(value), (short) 0));
      assertTrue(floatUlpEquals(value, Math.nextUp(Math.nextUp(value)), (short) 2));
      assertTrue(floatUlpEquals(value, Math.nextDown(Math.nextDown(value)), (short) 2));
    }

    assertTrue(floatUlpEquals(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, (short) 1));
    assertTrue(floatUlpEquals(Float.MAX_VALUE, Float.POSITIVE_INFINITY, (short) 1));

    assertTrue(floatUlpEquals(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, (short) 1));
    assertTrue(floatUlpEquals(-Float.MAX_VALUE, Float.NEGATIVE_INFINITY, (short) 1));

    assertFalse(floatUlpEquals(Float.NaN, Float.NaN, (short) 1));
    assertFalse(floatUlpEquals(Float.NaN, Float.NaN, (short) 0));
    assertFalse(floatUlpEquals(Float.NaN, 0, (short) 0));
    assertFalse(floatUlpEquals(0, Float.NaN, (short) 0));
    assertFalse(floatUlpEquals(Float.NaN, Float.POSITIVE_INFINITY, (short) 0));
    assertFalse(floatUlpEquals(Float.NaN, Float.NEGATIVE_INFINITY, (short) 0));

    assertFalse(floatUlpEquals(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, (short) 32767));
    // The 31-bit integer specification of the max positive ULP allows an extremely
    // large range of a 23-bit mantissa and 8-bit exponent
    assertTrue(floatUlpEquals(0, Float.MAX_VALUE, (short) 32767));
    // Here: f == 2;
    // it is used to test the maximum ULP distance between two opposite sign numbers.
    final float f = Float.intBitsToFloat(1 << 30);
    assertFalse(floatUlpEquals(-f, f, (short) 32767));
    assertTrue(floatUlpEquals(-f, Math.nextDown(f), (short) 32767));
    assertTrue(floatUlpEquals(Math.nextUp(-f), f, (short) 32767));
    // Maximum distance between same sign finite numbers is not possible as the upper
    // limit is NaN. Check that it is not equal to anything.
    final float f2 = Float.intBitsToFloat(Integer.MAX_VALUE);
    assertEquals(Double.NaN, f2, 0);
    assertFalse(floatUlpEquals(f2, Float.MAX_VALUE, (short) 32767));
    assertFalse(floatUlpEquals(f2, 0, (short) 32767));
  }
}
