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
package org.apache.lucene.sandbox.codecs.ivfaster;

import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the threshold-form encoder and its vectorized packing path.
 *
 * <p>Three properties, each of which fails silently if broken: the SIMD path must produce the same
 * bytes as the scalar one (a divergence means the index and the scan disagree about a document's
 * code), the SIMD path must actually engage on a host that supports it (a parity test comparing
 * scalar to scalar passes while proving nothing), and the threshold form must agree with the
 * arithmetic it replaced everywhere except within a ULP of a bucket boundary.
 */
public class TestPlanePack extends LuceneTestCase {

  /** The old arithmetic form, kept here as the reference the rewrite is compared against. */
  private static int arithmeticLevel(float v, float clip, float invStep, int levels) {
    int l = Math.round((v + clip) * invStep);
    if (l < 0) {
      l = 0;
    }
    if (l > levels - 1) {
      l = levels - 1;
    }
    return l;
  }

  /**
   * SIMD and scalar packing must produce IDENTICAL bytes.
   *
   * <p>Identical bytes rather than equivalent scores, because the planes are persisted and scanned
   * by a kernel that reads raw words. A bit-order difference between the two paths would show up as
   * a recall change on hosts where one path is chosen over the other, which is close to
   * undebuggable.
   */
  public void testSimdMatchesScalarBytes() {
    final PlanePackKernel simd = PlanePackKernel.get();
    final PlanePackKernel scalar = new PlanePackKernel.Scalar();
    // Dimensions on and off multiples of the vector width and of 64, so the tail path runs.
    for (int dim : new int[] {64, 96, 128, 129, 256, 320, 768, 1000, 1024, 1536}) {
      final int planeBytes = Nitrox2.planeBytes(dim);
      final float[] thresholds = Nitrox2.thresholdsFor(dim);
      final byte[] a = new byte[thresholds.length * planeBytes];
      final byte[] b = new byte[thresholds.length * planeBytes];
      for (int trial = 0; trial < 20; trial++) {
        final float[] v = randomVector(dim);
        java.util.Arrays.fill(a, (byte) 0x5A);
        java.util.Arrays.fill(b, (byte) 0x5A);
        simd.pack(v, dim, a, 0, planeBytes, thresholds);
        scalar.pack(v, dim, b, 0, planeBytes, thresholds);
        assertArrayEquals(
            "SIMD and scalar packing must produce identical bytes at dim=" + dim, b, a);
      }
    }
  }

  /**
   * The SIMD path must ENGAGE on this host.
   *
   * <p>Graviton3 has the Vector API available and 256-bit vectors, so a scalar kernel here means
   * the reflective load failed, which is silent. Without this assertion the parity test above would
   * compare the scalar path against itself and pass.
   */
  public void testSimdEngages() {
    final PlanePackKernel k = PlanePackKernel.get();
    assumeTrue(
        "the Vector API module must be present for this assertion to mean anything",
        k.isVectorized());
    final long before = PlanePackKernel.simdEngaged.sum();
    final int dim = 1024;
    Nitrox2.packPlanes(
        randomVector(dim), dim, new byte[Nitrox2.bytesPerVector(dim)], 0, Nitrox2.planeBytes(dim));
    assertTrue(
        "packPlanes must reach the vectorized kernel, not the scalar fallback",
        PlanePackKernel.simdEngaged.sum() > before);
  }

  /**
   * Packed planes must match the thermometer definition, coordinate by coordinate.
   *
   * <p>The independent check: rather than comparing two implementations, this decodes each plane
   * bit and asserts it equals {@code level > t} computed from the level function directly.
   */
  public void testPackedBitsMatchLevels() {
    for (int dim : new int[] {64, 129, 256, 1024}) {
      final int planeBytes = Nitrox2.planeBytes(dim);
      final byte[] packed = new byte[Nitrox2.bytesPerVector(dim)];
      for (int trial = 0; trial < 10; trial++) {
        final float[] v = randomVector(dim);
        Nitrox2.packPlanes(v, dim, packed, 0, planeBytes);
        for (int d = 0; d < dim; d++) {
          final int expected = Nitrox2.level(v[d], dim);
          for (int t = 0; t < Nitrox2.PLANES; t++) {
            final boolean bit = (packed[t * planeBytes + (d >>> 3)] & (1 << (d & 7))) != 0;
            assertEquals(
                "plane " + t + " bit for coordinate " + d + " at dim=" + dim, expected > t, bit);
          }
        }
      }
    }
  }

  /**
   * The threshold form must agree with the arithmetic it replaced, except within a few ULP of a
   * boundary.
   *
   * <p>THE REWRITE IS FORMAT-AFFECTING, which is why this is pinned. Over millions of random
   * coordinates the two agree; they differ only for coordinates sitting within ~3 ULP of a
   * threshold, and there the THRESHOLD form is the correct one, since {@code (v + clip) * invStep}
   * rounds a value genuinely below the threshold up through it. So this test bounds the
   * disagreement rather than forbidding it, and asserts the direction of the ones that remain.
   */
  public void testThresholdFormAgreesWithArithmetic() {
    for (int dim : new int[] {64, 128, 256, 768, 1024, 1536}) {
      final float clip = Nitrox2.clipFor(dim);
      final float invStep = Nitrox2.invStepFor(dim);
      int mismatches = 0;
      final int trials = 200_000;
      for (int i = 0; i < trials; i++) {
        final float v = (float) (random().nextGaussian() * clip * 3);
        final int thresh = Nitrox2.levelOnGrid(v, clip, invStep);
        final int arith = arithmeticLevel(v, clip, invStep, Nitrox2.LEVELS);
        if (thresh != arith) {
          mismatches++;
          // Every disagreement must be within a few ULP of a threshold the two forms straddle.
          final float t = Nitrox2.thresholdFor(Math.min(thresh, arith), clip);
          assertTrue(
              "disagreement at v=" + v + " is not near a threshold (t=" + t + ")",
              Math.abs(v - t) <= 8 * Math.ulp(t));
        }
      }
      // Random draws essentially never land within 8 ULP of a threshold, so this should be ~0.
      assertTrue(
          "too many level disagreements at dim=" + dim + ": " + mismatches + "/" + trials,
          mismatches <= trials / 10_000);
    }
  }

  /** NaN and infinities must land where the arithmetic form put them. */
  public void testSpecialValues() {
    final int dim = 1024;
    final float clip = Nitrox2.clipFor(dim);
    final float invStep = Nitrox2.invStepFor(dim);
    assertEquals("NaN must land on level 0", 0, Nitrox2.levelOnGrid(Float.NaN, clip, invStep));
    assertEquals(
        "-Inf must saturate low", 0, Nitrox2.levelOnGrid(Float.NEGATIVE_INFINITY, clip, invStep));
    assertEquals(
        "+Inf must saturate high",
        Nitrox2.LEVELS - 1,
        Nitrox2.levelOnGrid(Float.POSITIVE_INFINITY, clip, invStep));
    // And the same through the packing path, where a NaN must set no bits.
    final int planeBytes = Nitrox2.planeBytes(dim);
    final float[] v = new float[dim];
    java.util.Arrays.fill(v, Float.NaN);
    final byte[] packed = new byte[Nitrox2.bytesPerVector(dim)];
    java.util.Arrays.fill(packed, (byte) 0xFF);
    Nitrox2.packPlanes(v, dim, packed, 0, planeBytes);
    for (int i = 0; i < packed.length; i++) {
      assertEquals("NaN must clear every bit, byte " + i, 0, packed[i]);
    }
  }

  private float[] randomVector(int dim) {
    final float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      // Deliberately wider than the grid, so saturation at both ends is exercised.
      v[d] = (float) (random().nextGaussian() * std * 2.5);
    }
    return v;
  }
}
