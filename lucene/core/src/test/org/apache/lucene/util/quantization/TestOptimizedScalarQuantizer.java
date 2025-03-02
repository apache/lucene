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
package org.apache.lucene.util.quantization;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.MINIMUM_MSE_GRID;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

public class TestOptimizedScalarQuantizer extends LuceneTestCase {
  static final byte[] ALL_BITS = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

  public void testAbusiveEdgeCases() {
    // large zero array
    for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
      if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
        continue;
      }
      float[] vector = new float[4096];
      float[] centroid = new float[4096];
      OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
      byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][4096];
      OptimizedScalarQuantizer.QuantizationResult[] results =
          osq.multiScalarQuantize(vector, destinations, ALL_BITS, centroid);
      assertEquals(MINIMUM_MSE_GRID.length, results.length);
      assertValidResults(results);
      for (byte[] destination : destinations) {
        assertArrayEquals(new byte[4096], destination);
      }
      byte[] destination = new byte[4096];
      for (byte bit : ALL_BITS) {
        OptimizedScalarQuantizer.QuantizationResult result =
            osq.scalarQuantize(vector, destination, bit, centroid);
        assertValidResults(result);
        assertArrayEquals(new byte[4096], destination);
      }
    }

    // single value array
    for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
      float[] vector = new float[] {randomFloat()};
      float[] centroid = new float[] {randomFloat()};
      if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
        VectorUtil.l2normalize(vector);
        VectorUtil.l2normalize(centroid);
      }
      OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
      byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][1];
      OptimizedScalarQuantizer.QuantizationResult[] results =
          osq.multiScalarQuantize(vector, destinations, ALL_BITS, centroid);
      assertEquals(MINIMUM_MSE_GRID.length, results.length);
      assertValidResults(results);
      for (int i = 0; i < ALL_BITS.length; i++) {
        assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
      }
      for (byte bit : ALL_BITS) {
        vector = new float[] {randomFloat()};
        centroid = new float[] {randomFloat()};
        if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
          VectorUtil.l2normalize(vector);
          VectorUtil.l2normalize(centroid);
        }
        byte[] destination = new byte[1];
        OptimizedScalarQuantizer.QuantizationResult result =
            osq.scalarQuantize(vector, destination, bit, centroid);
        assertValidResults(result);
        assertValidQuantizedRange(destination, bit);
      }
    }
  }

  public void testMathematicalConsistency() {
    int dims = randomIntBetween(1, 4096);
    float[] vector = new float[dims];
    for (int i = 0; i < dims; ++i) {
      vector[i] = randomFloat();
    }
    float[] centroid = new float[dims];
    for (int i = 0; i < dims; ++i) {
      centroid[i] = randomFloat();
    }
    float[] copy = new float[dims];
    for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
      // copy the vector to avoid modifying it
      System.arraycopy(vector, 0, copy, 0, dims);
      if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
        VectorUtil.l2normalize(copy);
        VectorUtil.l2normalize(centroid);
      }
      OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
      byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][dims];
      OptimizedScalarQuantizer.QuantizationResult[] results =
          osq.multiScalarQuantize(copy, destinations, ALL_BITS, centroid);
      assertEquals(MINIMUM_MSE_GRID.length, results.length);
      assertValidResults(results);
      for (int i = 0; i < ALL_BITS.length; i++) {
        assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
      }
      for (byte bit : ALL_BITS) {
        byte[] destination = new byte[dims];
        System.arraycopy(vector, 0, copy, 0, dims);
        if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
          VectorUtil.l2normalize(copy);
          VectorUtil.l2normalize(centroid);
        }
        OptimizedScalarQuantizer.QuantizationResult result =
            osq.scalarQuantize(copy, destination, bit, centroid);
        assertValidResults(result);
        assertValidQuantizedRange(destination, bit);
      }
    }
  }

  static void assertValidQuantizedRange(byte[] quantized, byte bits) {
    for (byte b : quantized) {
      if (bits < 8) {
        assertTrue(b >= 0);
      }
      assertTrue(b < 1 << bits);
    }
  }

  static void assertValidResults(OptimizedScalarQuantizer.QuantizationResult... results) {
    for (OptimizedScalarQuantizer.QuantizationResult result : results) {
      assertTrue(Float.isFinite(result.lowerInterval()));
      assertTrue(Float.isFinite(result.upperInterval()));
      assertTrue(result.lowerInterval() <= result.upperInterval());
      assertTrue(Float.isFinite(result.additionalCorrection()));
      assertTrue(result.quantizedComponentSum() >= 0);
    }
  }
}
