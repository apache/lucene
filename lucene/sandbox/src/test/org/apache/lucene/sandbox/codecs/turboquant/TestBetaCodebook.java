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
package org.apache.lucene.sandbox.codecs.turboquant;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBetaCodebook extends LuceneTestCase {

  public void testCentroidsSymmetric() {
    for (int b : new int[] {2, 3, 4, 8}) {
      float[] c = BetaCodebook.centroids(4096, b);
      assertEquals(1 << b, c.length);
      for (int i = 0; i < c.length; i++) {
        assertEquals(
            "b=" + b + " centroid[" + i + "] not symmetric",
            -c[c.length - 1 - i],
            c[i],
            1e-6f);
      }
    }
  }

  public void testCentroidsCount() {
    assertEquals(4, BetaCodebook.centroids(4096, 2).length);
    assertEquals(8, BetaCodebook.centroids(4096, 3).length);
    assertEquals(16, BetaCodebook.centroids(4096, 4).length);
    assertEquals(256, BetaCodebook.centroids(4096, 8).length);
  }

  public void testCentroidsScaling() {
    // Centroids at d=1 should be the canonical values (scale = 1/√1 = 1)
    float[] c1 = BetaCodebook.centroids(1, 2);
    // Centroids at d=4 should be half (scale = 1/√4 = 0.5)
    float[] c4 = BetaCodebook.centroids(4, 2);
    for (int i = 0; i < c1.length; i++) {
      assertEquals(c1[i] * 0.5f, c4[i], 1e-6f);
    }
  }

  public void testCentroidsReferenceValues() {
    // Verify b=2 canonical centroids match reference implementation within 1e-4
    float[] c = BetaCodebook.centroids(1, 2); // d=1 → no scaling
    assertEquals(-1.5104f, c[0], 1e-3f);
    assertEquals(-0.4528f, c[1], 1e-3f);
    assertEquals(0.4528f, c[2], 1e-3f);
    assertEquals(1.5104f, c[3], 1e-3f);
  }

  public void testBoundariesCount() {
    for (int b : new int[] {2, 3, 4, 8}) {
      float[] bd = BetaCodebook.boundaries(4096, b);
      assertEquals((1 << b) + 1, bd.length);
      assertEquals(-Float.MAX_VALUE, bd[0], 0f);
      assertEquals(Float.MAX_VALUE, bd[bd.length - 1], 0f);
    }
  }

  public void testBoundariesAreMidpoints() {
    float[] c = BetaCodebook.centroids(4096, 4);
    float[] bd = BetaCodebook.boundaries(4096, 4);
    for (int i = 0; i < c.length - 1; i++) {
      float expected = (c[i] + c[i + 1]) / 2;
      assertEquals(expected, bd[i + 1], 1e-7f);
    }
  }

  public void testQuantize() {
    float[] bd = BetaCodebook.boundaries(4096, 2);
    // Very negative → index 0
    assertEquals(0, BetaCodebook.quantize(-10f, bd));
    // Very positive → last index
    assertEquals(3, BetaCodebook.quantize(10f, bd));
    // Zero → middle (index 1 or 2 depending on boundary)
    int idx = BetaCodebook.quantize(0f, bd);
    assertTrue(idx == 1 || idx == 2);
  }

  public void testMseDistortionBits4() {
    // Empirical MSE distortion test at d=4096, b=4
    // Generate random unit vectors, quantize, measure MSE
    int d = 4096;
    int b = 4;
    int numVectors = 1000;
    float[] centroids = BetaCodebook.centroids(d, b);
    float[] boundaries = BetaCodebook.boundaries(d, b);

    java.util.Random rng = new java.util.Random(42);
    double totalMse = 0;

    for (int v = 0; v < numVectors; v++) {
      // Generate random unit vector
      float[] x = new float[d];
      float norm = 0;
      for (int i = 0; i < d; i++) {
        x[i] = (float) rng.nextGaussian();
        norm += x[i] * x[i];
      }
      norm = (float) Math.sqrt(norm);
      for (int i = 0; i < d; i++) {
        x[i] /= norm;
      }

      // Rotate
      HadamardRotation rot = HadamardRotation.create(d, 12345L);
      float[] rotated = new float[d];
      rot.rotate(x, rotated);

      // Quantize and dequantize
      double mse = 0;
      for (int i = 0; i < d; i++) {
        int idx = BetaCodebook.quantize(rotated[i], boundaries);
        float reconstructed = centroids[idx];
        double err = rotated[i] - reconstructed;
        mse += err * err;
      }
      totalMse += mse;
    }
    // Total MSE over all d coordinates of a unit vector
    double avgMse = totalMse / numVectors;
    // Paper says 0.009 for b=4. Allow range [0.007, 0.011]
    assertTrue(
        "MSE distortion " + avgMse + " outside expected range [0.007, 0.011]",
        avgMse >= 0.007 && avgMse <= 0.011);
  }
}
