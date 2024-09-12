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

public class TestFloatingPointComparisons extends LuceneTestCase {
  public void test() {
    // Test adjacent numbers
    assertFalse(doubleEquals(Double.longBitsToDouble(1L), Double.longBitsToDouble(2L), 0));
    assertTrue(doubleEquals(Double.longBitsToDouble(1L), Double.longBitsToDouble(2L), 1));
    assertFalse(floatEquals(Float.intBitsToFloat(1), Float.intBitsToFloat(2), (short) 0));
    assertTrue(floatEquals(Float.intBitsToFloat(1), Float.intBitsToFloat(2), (short) 1));

    // Test signed zeros
    assertTrue(doubleEquals(0.0d, -0.0d, 0));
    assertTrue(floatEquals(0.0f, -0.0f, (short) 0));

    // Test NaNs
    assertFalse(doubleEquals(Double.NaN, Double.NaN, Integer.MAX_VALUE));
    assertFalse(floatEquals(Float.NaN, Float.NaN, (short) 32767));
  }
}
