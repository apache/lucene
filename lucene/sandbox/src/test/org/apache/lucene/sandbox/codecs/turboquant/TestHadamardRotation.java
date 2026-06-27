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

public class TestHadamardRotation extends LuceneTestCase {

  public void testDecomposeBlocksPowerOf2() {
    assertArrayEquals(new int[] {4096}, HadamardRotation.decomposeBlocks(4096));
    assertArrayEquals(new int[] {1024}, HadamardRotation.decomposeBlocks(1024));
    assertArrayEquals(new int[] {256}, HadamardRotation.decomposeBlocks(256));
    assertArrayEquals(new int[] {1}, HadamardRotation.decomposeBlocks(1));
  }

  public void testDecomposeBlocksNonPowerOf2() {
    assertArrayEquals(new int[] {512, 256}, HadamardRotation.decomposeBlocks(768));
    assertArrayEquals(new int[] {256, 128}, HadamardRotation.decomposeBlocks(384));
    assertArrayEquals(new int[] {1024, 512}, HadamardRotation.decomposeBlocks(1536));
    assertArrayEquals(new int[] {2048, 1024}, HadamardRotation.decomposeBlocks(3072));
  }

  public void testDecomposeBlocksSumsToD() {
    for (int d = 1; d <= 8192; d++) {
      int[] blocks = HadamardRotation.decomposeBlocks(d);
      int sum = 0;
      for (int b : blocks) {
        assertTrue("Block " + b + " is not power of 2", (b & (b - 1)) == 0);
        sum += b;
      }
      assertEquals("Blocks don't sum to d=" + d, d, sum);
    }
  }

  public void testRoundTrip() {
    for (int d : new int[] {4096, 768, 384, 100, 33}) {
      HadamardRotation rot = HadamardRotation.create(d, 42L);
      java.util.Random rng = new java.util.Random(123);
      float[] x = new float[d];
      for (int i = 0; i < d; i++) {
        x[i] = (float) rng.nextGaussian();
      }

      float[] rotated = new float[d];
      rot.rotate(x, rotated);
      float[] recovered = new float[d];
      rot.inverseRotate(rotated, recovered);

      for (int i = 0; i < d; i++) {
        assertEquals("d=" + d + " coord " + i, x[i], recovered[i], 1e-4f);
      }
    }
  }

  public void testNormPreservation() {
    int d = 4096;
    HadamardRotation rot = HadamardRotation.create(d, 42L);
    java.util.Random rng = new java.util.Random(0);

    for (int trial = 0; trial < 100; trial++) {
      float[] x = new float[d];
      double normSqX = 0;
      for (int i = 0; i < d; i++) {
        x[i] = (float) rng.nextGaussian();
        normSqX += (double) x[i] * x[i];
      }

      float[] rotated = new float[d];
      rot.rotate(x, rotated);
      double normSqR = 0;
      for (int i = 0; i < d; i++) {
        normSqR += (double) rotated[i] * rotated[i];
      }

      double relError = Math.abs(normSqR - normSqX) / normSqX;
      assertTrue(
          "Norm not preserved: relError=" + relError + " at trial " + trial, relError < 1e-4);
    }
  }

  public void testInnerProductPreservation() {
    int d = 4096;
    HadamardRotation rot = HadamardRotation.create(d, 42L);
    java.util.Random rng = new java.util.Random(7);

    for (int trial = 0; trial < 100; trial++) {
      float[] a = new float[d], b = new float[d];
      double dotOrig = 0;
      for (int i = 0; i < d; i++) {
        a[i] = (float) rng.nextGaussian();
        b[i] = (float) rng.nextGaussian();
        dotOrig += (double) a[i] * b[i];
      }

      float[] ra = new float[d], rb = new float[d];
      rot.rotate(a, ra);
      rot.rotate(b, rb);
      double dotRot = 0;
      for (int i = 0; i < d; i++) {
        dotRot += (double) ra[i] * rb[i];
      }

      double relError = Math.abs(dotRot - dotOrig) / (Math.abs(dotOrig) + 1e-10);
      assertTrue("Inner product not preserved: relError=" + relError, relError < 1e-4);
    }
  }

  public void testDeterminism() {
    int d = 768;
    HadamardRotation rot1 = HadamardRotation.create(d, 42L);
    HadamardRotation rot2 = HadamardRotation.create(d, 42L);

    float[] x = new float[d];
    for (int i = 0; i < d; i++) x[i] = i * 0.001f;

    float[] out1 = new float[d], out2 = new float[d];
    rot1.rotate(x, out1);
    rot2.rotate(x, out2);

    for (int i = 0; i < d; i++) {
      assertEquals(out1[i], out2[i], 0f);
    }
  }

  public void testDifferentSeeds() {
    int d = 768;
    HadamardRotation rot1 = HadamardRotation.create(d, 1L);
    HadamardRotation rot2 = HadamardRotation.create(d, 2L);

    float[] x = new float[d];
    for (int i = 0; i < d; i++) x[i] = 1.0f / d;

    float[] out1 = new float[d], out2 = new float[d];
    rot1.rotate(x, out1);
    rot2.rotate(x, out2);

    boolean anyDifferent = false;
    for (int i = 0; i < d; i++) {
      if (Math.abs(out1[i] - out2[i]) > 1e-6f) {
        anyDifferent = true;
        break;
      }
    }
    assertTrue("Different seeds should produce different rotations", anyDifferent);
  }

  public void testZeroVector() {
    int d = 128;
    HadamardRotation rot = HadamardRotation.create(d, 42L);
    float[] x = new float[d]; // all zeros
    float[] out = new float[d];
    rot.rotate(x, out);
    for (int i = 0; i < d; i++) {
      assertEquals(0f, out[i], 0f);
    }
  }

  /**
   * Block-diagonal MSE at d=768 should be within 5% of a single-block Hadamard at d=1024 (padded).
   * This validates that the block-diagonal approach doesn't degrade quantization quality.
   */
  public void testBlockDiagonalMseQuality() {
    int d = 768;
    int b = 4;
    int numVectors = 1000;
    java.util.Random rng = new java.util.Random(42);
    float[] centroids768 = BetaCodebook.centroids(d, b);
    float[] boundaries768 = BetaCodebook.boundaries(d, b);
    HadamardRotation rot768 = HadamardRotation.create(d, 12345L);

    // Also test with d=1024 (power of 2, single block) for comparison
    int dRef = 1024;
    float[] centroidsRef = BetaCodebook.centroids(dRef, b);
    float[] boundariesRef = BetaCodebook.boundaries(dRef, b);
    HadamardRotation rotRef = HadamardRotation.create(dRef, 12345L);

    double mse768 = 0, mseRef = 0;
    for (int v = 0; v < numVectors; v++) {
      // d=768 block-diagonal
      float[] x768 = randomUnitVector(d, rng);
      float[] rotated768 = new float[d];
      rot768.rotate(x768, rotated768);
      double err768 = 0;
      for (int i = 0; i < d; i++) {
        int idx = BetaCodebook.quantize(rotated768[i], boundaries768);
        double diff = rotated768[i] - centroids768[idx];
        err768 += diff * diff;
      }
      mse768 += err768;

      // d=1024 single block reference
      float[] xRef = randomUnitVector(dRef, rng);
      float[] rotatedRef = new float[dRef];
      rotRef.rotate(xRef, rotatedRef);
      double errRef = 0;
      for (int i = 0; i < dRef; i++) {
        int idx = BetaCodebook.quantize(rotatedRef[i], boundariesRef);
        double diff = rotatedRef[i] - centroidsRef[idx];
        errRef += diff * diff;
      }
      mseRef += errRef;
    }
    mse768 /= numVectors;
    mseRef /= numVectors;

    // Block-diagonal MSE should be within 5% of single-block MSE
    double ratio = mse768 / mseRef;
    assertTrue(
        "Block-diagonal MSE ratio " + ratio + " exceeds 5% threshold (768 mse="
            + mse768 + ", 1024 mse=" + mseRef + ")",
        ratio < 1.05 && ratio > 0.95);
  }

  private static float[] randomUnitVector(int d, java.util.Random rng) {
    float[] v = new float[d];
    float norm = 0;
    for (int i = 0; i < d; i++) {
      v[i] = (float) rng.nextGaussian();
      norm += v[i] * v[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < d; i++) v[i] /= norm;
    return v;
  }

  public void testOneHotVectors() {
    int d = 128;
    HadamardRotation rot = HadamardRotation.create(d, 42L);
    for (int k = 0; k < d; k++) {
      float[] x = new float[d];
      x[k] = 1.0f;
      float[] out = new float[d];
      rot.rotate(x, out);
      // Norm should be preserved
      double normSq = 0;
      for (int i = 0; i < d; i++) normSq += (double) out[i] * out[i];
      assertEquals("One-hot e_" + k + " norm not preserved", 1.0, normSq, 1e-4);
    }
  }
}
