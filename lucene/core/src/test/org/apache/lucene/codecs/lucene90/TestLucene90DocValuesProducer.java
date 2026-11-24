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
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestLucene90DocValuesProducer extends LuceneTestCase {

  public void testCanTransformGcdBounds() {
    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, 9, 10, 64));
    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(11, Long.MAX_VALUE, 10, 64));

    long delta = -10;
    long highestSafeBound = Long.MAX_VALUE + delta;
    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(delta, highestSafeBound, delta, 64));
    assertFalse(
        Lucene90DocValuesProducer.canTransformGcdBounds(delta, highestSafeBound + 1, delta, 64));
    assertFalse(
        Lucene90DocValuesProducer.canTransformGcdBounds(
            highestSafeBound + 1, highestSafeBound + 1, delta, 64));
    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(delta, Long.MAX_VALUE, delta, 56));
    assertFalse(Lucene90DocValuesProducer.canTransformGcdBounds(delta, Long.MAX_VALUE, delta, 64));

    assertTrue(
        Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, -1, Long.MIN_VALUE, 64));
    assertFalse(Lucene90DocValuesProducer.canTransformGcdBounds(0, 0, Long.MIN_VALUE, 64));
  }

  public void testTransformGcdBounds() {
    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(-3, 0, -4, 4));
    assertArrayEquals(
        new long[] {1, 2}, Lucene90DocValuesProducer.transformGcdBounds(-3, 0, 2, -4, 4));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, -2, -4, 4));
    assertArrayEquals(
        new long[] {0, 1},
        Lucene90DocValuesProducer.transformGcdBounds(Long.MIN_VALUE, -2, 2, -4, 4));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(1, Long.MAX_VALUE, -4, 4));
    assertArrayEquals(
        new long[] {3, Long.MAX_VALUE},
        Lucene90DocValuesProducer.transformGcdBounds(1, Long.MAX_VALUE, 2, -4, 4));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, -5, -4, 4));
    assertNull(Lucene90DocValuesProducer.transformGcdBounds(Long.MIN_VALUE, -5, 2, -4, 4));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(11, 14, 10, 64));
    assertNull(Lucene90DocValuesProducer.transformGcdBounds(11, 14, 5, 10, 64));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, 10, 10, 64));
    assertArrayEquals(
        new long[] {0, 0},
        Lucene90DocValuesProducer.transformGcdBounds(Long.MIN_VALUE, 10, 5, 10, 64));

    assertTrue(
        Lucene90DocValuesProducer.canTransformGcdBounds(Long.MIN_VALUE, -1, Long.MIN_VALUE, 64));
    assertArrayEquals(
        new long[] {0, Long.MAX_VALUE},
        Lucene90DocValuesProducer.transformGcdBounds(Long.MIN_VALUE, -1, 1, Long.MIN_VALUE, 64));

    assertTrue(Lucene90DocValuesProducer.canTransformGcdBounds(-10, Long.MAX_VALUE - 10, -10, 64));
    assertArrayEquals(
        new long[] {0, Long.MAX_VALUE},
        Lucene90DocValuesProducer.transformGcdBounds(-10, Long.MAX_VALUE - 10, 1, -10, 64));
  }
}
