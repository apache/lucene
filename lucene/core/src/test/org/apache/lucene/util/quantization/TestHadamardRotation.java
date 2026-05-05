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

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Correctness tests for {@link HadamardRotation}. */
public class TestHadamardRotation extends LuceneTestCase {

  public void testDecompositionOfPowerOfTwo() {
    assertArrayEquals(new int[] {1024}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(1024));
    assertArrayEquals(new int[] {1}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(1));
    assertArrayEquals(new int[] {512}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(512));
  }

  public void testDecompositionOfNonPowerOfTwo() {
    // 768 = 512 + 256
    assertArrayEquals(new int[] {512, 256}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(768));
    // 100 = 64 + 32 + 4
    assertArrayEquals(new int[] {64, 32, 4}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(100));
    // 3 = 2 + 1
    assertArrayEquals(new int[] {2, 1}, HadamardRotation.decomposeIntoPowerOfTwoBlocks(3));
  }

  public void testDimensionValidation() {
    expectThrows(IllegalArgumentException.class, () -> HadamardRotation.create(0, 42L));
    expectThrows(IllegalArgumentException.class, () -> HadamardRotation.create(-1, 42L));
  }

  public void testLengthMismatchThrows() {
    HadamardRotation r = HadamardRotation.create(16, 42L);
    expectThrows(
        IllegalArgumentException.class, () -> r.rotate(new float[8], new float[16]));
    expectThrows(
        IllegalArgumentException.class, () -> r.rotate(new float[16], new float[8]));
  }

  /** Orthogonality: ||R x|| == ||x|| for many random vectors and dims. */
  public void testPreservesL2Norm() {
    int[] dims = {1, 2, 3, 4, 7, 16, 100, 128, 768, 1024};
    for (int dim : dims) {
      HadamardRotation r = HadamardRotation.create(dim, random().nextLong());
      float[] in = randomVector(dim);
      float[] out = new float[dim];
      r.rotate(in, out);
      double normIn = l2Norm(in);
      double normOut = l2Norm(out);
      assertEquals("L2 norm must be preserved for dim=" + dim, normIn, normOut, 1e-4);
    }
  }

  /** Orthogonality: (R x)^T (R y) == x^T y for many random pairs. */
  public void testPreservesDotProduct() {
    int[] dims = {4, 16, 128, 768, 1024};
    for (int dim : dims) {
      HadamardRotation r = HadamardRotation.create(dim, random().nextLong());
      for (int trial = 0; trial < 10; trial++) {
        float[] x = randomVector(dim);
        float[] y = randomVector(dim);
        float[] rx = new float[dim];
        float[] ry = new float[dim];
        r.rotate(x, rx);
        r.rotate(y, ry);
        double dotOriginal = dot(x, y);
        double dotRotated = dot(rx, ry);
        assertEquals(
            "dot product must be preserved for dim=" + dim,
            dotOriginal,
            dotRotated,
            1e-3 * Math.max(1.0, Math.abs(dotOriginal)));
      }
    }
  }

  /** Orthogonality: ||R x - R y|| == ||x - y|| (Euclidean distance preserved). */
  public void testPreservesEuclideanDistance() {
    int dim = 128;
    HadamardRotation r = HadamardRotation.create(dim, 0xdeadbeefL);
    float[] x = randomVector(dim);
    float[] y = randomVector(dim);
    float[] rx = new float[dim];
    float[] ry = new float[dim];
    r.rotate(x, rx);
    r.rotate(y, ry);
    double distOriginal = l2Dist(x, y);
    double distRotated = l2Dist(rx, ry);
    assertEquals(distOriginal, distRotated, 1e-3);
  }

  /** Determinism: same (dim, seed) yields the same output. */
  public void testDeterminismForSameSeed() {
    int dim = 256;
    HadamardRotation r1 = HadamardRotation.create(dim, 42L);
    HadamardRotation r2 = HadamardRotation.create(dim, 42L);
    float[] x = randomVector(dim);
    float[] out1 = new float[dim];
    float[] out2 = new float[dim];
    r1.rotate(x, out1);
    r2.rotate(x, out2);
    assertArrayEquals(out1, out2, 0f);
  }

  /** Different seeds produce different rotations (with high probability). */
  public void testDifferentSeedsDiffer() {
    int dim = 128;
    HadamardRotation r1 = HadamardRotation.create(dim, 1L);
    HadamardRotation r2 = HadamardRotation.create(dim, 2L);
    float[] x = randomVector(dim);
    float[] out1 = new float[dim];
    float[] out2 = new float[dim];
    r1.rotate(x, out1);
    r2.rotate(x, out2);
    assertFalse("Different seeds should produce different rotations", Arrays.equals(out1, out2));
  }

  /**
   * Functional test: a skewed input (mostly zeros with a couple of huge values) should be
   * "spread" by the rotation so that component variance is more evenly distributed.
   */
  public void testSpreadsConcentratedInput() {
    int dim = 256;
    HadamardRotation r = HadamardRotation.create(dim, 0xabc123L);
    float[] spike = new float[dim];
    spike[0] = 10f; // all energy concentrated in one dimension
    float[] rotated = new float[dim];
    r.rotate(spike, rotated);

    // After rotation the L2 norm must still be 10, but the energy should be spread across many
    // dimensions. Measure the max-component magnitude: it should be much smaller than 10.
    float maxAbs = 0f;
    for (float v : rotated) {
      maxAbs = Math.max(maxAbs, Math.abs(v));
    }
    // For a truly spreading rotation, max component should be roughly 10/sqrt(d) = ~0.625.
    // Accept any value < 5 (= 50% of norm) as clear evidence of spreading.
    assertTrue(
        "Rotation should spread energy; saw maxAbs=" + maxAbs,
        maxAbs < 5f);
    // Also verify norm is still ~10.
    assertEquals(10.0, l2Norm(rotated), 1e-3);
  }

  /** The caller-owned scratch overload should match the auto-allocating overload exactly. */
  public void testScratchOverloadEquivalence() {
    int dim = 128;
    HadamardRotation r = HadamardRotation.create(dim, 7L);
    float[] x = randomVector(dim);
    float[] outAuto = new float[dim];
    float[] outScratch = new float[dim];
    float[] scratch = new float[dim];
    r.rotate(x, outAuto);
    r.rotate(x, outScratch, scratch);
    assertArrayEquals(outAuto, outScratch, 0f);
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) random().nextGaussian();
    }
    return v;
  }

  private static double l2Norm(float[] v) {
    double s = 0;
    for (float x : v) s += x * x;
    return Math.sqrt(s);
  }

  private static double dot(float[] a, float[] b) {
    double s = 0;
    for (int i = 0; i < a.length; i++) s += a[i] * b[i];
    return s;
  }

  private static double l2Dist(float[] a, float[] b) {
    double s = 0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      s += d * d;
    }
    return Math.sqrt(s);
  }
}
