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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.MINIMUM_MSE_GRID;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.deQuantize;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.packAsBinary;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.unpackBinary;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

public class TestOptimizedScalarQuantizer extends LuceneTestCase {
  static final byte[] ALL_BITS = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

  public void testQuantizationQuality() {
    int dims = 16;
    int numVectors = 32;
    float[][] vectors = new float[numVectors][];
    float[] centroid = new float[dims];
    for (int i = 0; i < numVectors; ++i) {
      vectors[i] = new float[dims];
      for (int j = 0; j < dims; ++j) {
        vectors[i][j] = randomFloat();
        centroid[j] += vectors[i][j];
      }
    }
    for (int j = 0; j < dims; ++j) {
      centroid[j] /= numVectors;
    }
    // similarity doesn't matter for this test
    OptimizedScalarQuantizer osq =
        new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);
    float[] scratch = new float[dims];
    for (byte bit : ALL_BITS) {
      float eps = (1f / (float) (1 << (bit)));
      byte[] destination = new byte[dims];
      for (int i = 0; i < numVectors; ++i) {
        System.arraycopy(vectors[i], 0, scratch, 0, dims);
        OptimizedScalarQuantizer.QuantizationResult result =
            osq.scalarQuantize(scratch, destination, bit, centroid);
        assertValidResults(result);
        assertValidQuantizedRange(destination, bit);

        float[] dequantized = new float[dims];
        deQuantize(
            destination,
            dequantized,
            bit,
            result.lowerInterval(),
            result.upperInterval(),
            centroid);
        float mae = 0;
        for (int k = 0; k < dims; ++k) {
          mae += Math.abs(dequantized[k] - vectors[i][k]);
        }
        mae /= dims;
        assertTrue("bits: " + bit + " mae: " + mae + " > eps: " + eps, mae <= eps);
      }
    }
  }

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

  /**
   * Verifies that {@code multiScalarQuantize} matches independent {@code scalarQuantize} calls at
   * both bit widths. The merge path depends on byte-identical index-side and query-side records.
   */
  public void testMultiScalarQuantizeMatchesScalarQuantize() {
    byte[][] bitPairs = new byte[][] {{1, 4}, {2, 4}, {4, 8}};
    for (int trial = 0; trial < 25; trial++) {
      int dims = randomIntBetween(1, 1024);
      float[] vector = new float[dims];
      float[] centroid = new float[dims];
      for (int i = 0; i < dims; ++i) {
        vector[i] = randomFloat();
        centroid[i] = randomFloat();
      }
      for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
        float[] scratch = new float[dims];
        System.arraycopy(vector, 0, scratch, 0, dims);
        float[] centroidCopy = new float[dims];
        System.arraycopy(centroid, 0, centroidCopy, 0, dims);
        if (similarityFunction == VectorSimilarityFunction.COSINE) {
          VectorUtil.l2normalize(scratch);
          VectorUtil.l2normalize(centroidCopy);
        }
        OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(similarityFunction);
        for (byte[] bits : bitPairs) {
          byte[] multiLow = new byte[dims];
          byte[] multiHigh = new byte[dims];
          OptimizedScalarQuantizer.QuantizationResult[] multi =
              osq.multiScalarQuantize(
                  scratch.clone(), new byte[][] {multiLow, multiHigh}, bits, centroidCopy);
          byte[] singleLow = new byte[dims];
          byte[] singleHigh = new byte[dims];
          // each single-width call gets its own copy: both center the vector in place
          OptimizedScalarQuantizer.QuantizationResult low =
              osq.scalarQuantize(scratch.clone(), singleLow, bits[0], centroidCopy);
          OptimizedScalarQuantizer.QuantizationResult high =
              osq.scalarQuantize(scratch.clone(), singleHigh, bits[1], centroidCopy);
          String where = similarityFunction + " dims=" + dims + " bits=" + bits[0] + "/" + bits[1];
          assertArrayEquals(where, singleLow, multiLow);
          assertArrayEquals(where, singleHigh, multiHigh);
          assertIdenticalResults(where, low, multi[0]);
          assertIdenticalResults(where, high, multi[1]);
        }
      }
    }
  }

  private static void assertIdenticalResults(
      String where,
      OptimizedScalarQuantizer.QuantizationResult expected,
      OptimizedScalarQuantizer.QuantizationResult actual) {
    assertEquals(
        where + " lowerInterval",
        Float.floatToRawIntBits(expected.lowerInterval()),
        Float.floatToRawIntBits(actual.lowerInterval()));
    assertEquals(
        where + " upperInterval",
        Float.floatToRawIntBits(expected.upperInterval()),
        Float.floatToRawIntBits(actual.upperInterval()));
    assertEquals(
        where + " additionalCorrection",
        Float.floatToRawIntBits(expected.additionalCorrection()),
        Float.floatToRawIntBits(actual.additionalCorrection()));
    assertEquals(
        where + " quantizedComponentSum",
        expected.quantizedComponentSum(),
        actual.quantizedComponentSum());
  }

  public void testUnpackBinary() {
    int dim = randomIntBetween(1, 4096);
    QuantizedByteVectorValues.ScalarEncoding encoding =
        QuantizedByteVectorValues.ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE;
    byte[] scratch = new byte[encoding.getDiscreteDimensions(dim)];
    for (int i = 0; i < scratch.length; i++) {
      scratch[i] = randomBoolean() ? (byte) 1 : (byte) 0;
    }
    byte[] packed = new byte[encoding.getDocPackedLength(scratch.length)];
    byte[] unpacked = new byte[scratch.length];
    packAsBinary(scratch, packed);
    unpackBinary(packed, unpacked);
    assertArrayEquals(scratch, unpacked);
  }

  public void testPackTransposeDibit() {
    int dim = randomIntBetween(1, 4096);
    QuantizedByteVectorValues.ScalarEncoding encoding =
        QuantizedByteVectorValues.ScalarEncoding.DIBIT_QUERY_NIBBLE;
    byte[] scratch = new byte[encoding.getDiscreteDimensions(dim)];
    for (int i = 0; i < scratch.length; i++) {
      scratch[i] = (byte) randomIntBetween(0, 3);
    }
    byte[] packed = new byte[encoding.getDocPackedLength(scratch.length)];
    byte[] unpacked = new byte[scratch.length];
    OptimizedScalarQuantizer.transposeDibit(scratch, packed);
    OptimizedScalarQuantizer.untransposeDibit(packed, unpacked);
    assertArrayEquals(scratch, unpacked);
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
