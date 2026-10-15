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
package org.apache.lucene.internal.vectorization;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestDocValuesBulkDecodeSupport extends LuceneTestCase {

  public void testDuelDecode8() throws Exception {
    runDuel(Byte.SIZE, 1);
  }

  public void testDuelDecode16() throws Exception {
    runDuel(Short.SIZE, Short.BYTES);
  }

  public void testDuelDecode32() throws Exception {
    runDuel(Integer.SIZE, Integer.BYTES);
  }

  public void testDuelDecode64() throws Exception {
    runDuel(Long.SIZE, Long.BYTES);
  }

  private void runDuel(int bitsPerValue, int bytesPerValue) throws Exception {
    VectorizationProvider provider = VectorizationProvider.lookup(true);
    DocValuesBulkDecodeSupport optimizedSupport = provider.getDocValuesBulkDecodeSupport();
    DocValuesBulkDecodeSupport defaultSupport = DefaultDocValuesBulkDecodeSupport.INSTANCE;

    if (optimizedSupport == defaultSupport) {
      assumeTrue("Hardware vectorization not loaded", false);
    }

    final int iterations = atLeast(100);
    for (int iter = 0; iter < iterations; ++iter) {
      int count = TestUtil.nextInt(random(), 1, 1000);
      int bytesOffset = random().nextInt(16);
      int valuesOffset = random().nextInt(16);

      byte[] bytes = new byte[bytesOffset + (count * bytesPerValue) + random().nextInt(16)];
      random().nextBytes(bytes);

      long[] expectedValues = new long[valuesOffset + count + random().nextInt(16)];
      long[] actualValues = new long[expectedValues.length];

      /* Scalar default */
      defaultSupport.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, expectedValues, valuesOffset, count);
      /* Panama Vector API SIMD */
      optimizedSupport.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, actualValues, valuesOffset, count);

      assertArrayEquals(
          "bits=" + bitsPerValue + " count=" + count + " iter=" + iter,
          expectedValues,
          actualValues);
    }
  }
}
