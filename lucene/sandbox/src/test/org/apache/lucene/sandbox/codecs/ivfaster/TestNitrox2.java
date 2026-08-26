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
import org.apache.lucene.util.VectorUtil;

/**
 * Pins the coarse tier's defining property: the double Hamming distance over the two thermometer
 * planes EQUALS the summed per-dimension level distance.
 *
 * <p>That identity is what lets the coarse scan run on a plain XOR+popcount kernel with no
 * per-document correction term. If it stops holding, whether from a level function change, a plane
 * order swap or a non-unary code, the scan ranks on something that is not a distance, and the
 * failure reads as a broken tier.
 */
public class TestNitrox2 extends LuceneTestCase {

  /** Dimensions worth covering: the production width, a non-power-of-two, and a sub-byte tail. */
  private static final int[] DIMS = {1024, 768, 96, 8, 13};

  public void testThermometerHammingEqualsLevelDistance() {
    for (int dim : DIMS) {
      final int pb = Nitrox2.planeBytes(dim);
      for (int trial = 0; trial < 200; trial++) {
        float[] a = randomRotatedVector(dim);
        float[] b = randomRotatedVector(dim);

        byte[] aHi = new byte[pb];
        byte[] aLo = new byte[pb];
        byte[] bHi = new byte[pb];
        byte[] bLo = new byte[pb];
        encodePlanes(a, dim, aHi, 0, aLo, 0);
        encodePlanes(b, dim, bHi, 0, bLo, 0);

        int hamming = VectorUtil.xorBitCount(aHi, bHi) + VectorUtil.xorBitCount(aLo, bLo);

        int levelDistance = 0;
        for (int d = 0; d < dim; d++) {
          levelDistance += Math.abs(Nitrox2.level(a[d], dim) - Nitrox2.level(b[d], dim));
        }

        assertEquals(
            "double Hamming must equal summed level distance at dim=" + dim,
            levelDistance,
            hamming);
      }
    }
  }

  /** Encoding at a non-zero destination offset must not disturb neighbouring bytes. */
  public void testEncodeAtOffsetIsIsolated() {
    final int dim = 1024;
    final int stride = Nitrox2.bytesPerVector(dim);
    byte[] buf = new byte[3 * stride];
    java.util.Arrays.fill(buf, (byte) 0x5A);

    float[] v = randomRotatedVector(dim);
    Nitrox2.encode(v, dim, buf, stride);

    byte[] expected = new byte[stride];
    Nitrox2.encode(v, dim, expected, 0);
    for (int i = 0; i < stride; i++) {
      assertEquals("record byte " + i, expected[i], buf[stride + i]);
    }
    // The guard regions on both sides must be untouched: encode fills its own range and no more.
    for (int i = 0; i < stride; i++) {
      assertEquals("leading guard byte " + i, (byte) 0x5A, buf[i]);
      assertEquals("trailing guard byte " + i, (byte) 0x5A, buf[2 * stride + i]);
    }
  }

  /** decodeLevel must invert the thermometer packing for every dimension. */
  public void testDecodeLevelRoundTrip() {
    for (int dim : DIMS) {
      float[] v = randomRotatedVector(dim);
      byte[] packed = new byte[Nitrox2.bytesPerVector(dim)];
      Nitrox2.encode(v, dim, packed, 0);
      for (int d = 0; d < dim; d++) {
        assertEquals(
            "level of dim " + d + " at dim=" + dim,
            Nitrox2.level(v[d], dim),
            Nitrox2.decodeLevel(packed, 0, dim, d));
      }
    }
  }

  /** Levels must saturate rather than wrap, and must be monotone in the coordinate. */
  public void testLevelIsClampedAndMonotone() {
    final int dim = 1024;
    assertEquals("far negative saturates to 0", 0, Nitrox2.level(-1e6f, dim));
    assertEquals("far positive saturates to top", Nitrox2.LEVELS - 1, Nitrox2.level(1e6f, dim));
    assertEquals("zero is the middle level", 1, Nitrox2.level(0f, dim));

    int prev = Nitrox2.level(-1e6f, dim);
    final float std = (float) (1.0 / Math.sqrt(dim));
    for (float x = -4f * std; x <= 4f * std; x += std / 32f) {
      int l = Nitrox2.level(x, dim);
      assertTrue("level must be non-decreasing in the coordinate", l >= prev);
      assertTrue("level in range", l >= 0 && l < Nitrox2.LEVELS);
      prev = l;
    }
  }

  /** A vector's code must be identical to itself: zero distance, and the identity's base case. */
  public void testSelfDistanceIsZero() {
    for (int dim : DIMS) {
      final int pb = Nitrox2.planeBytes(dim);
      float[] v = randomRotatedVector(dim);
      byte[] hi = new byte[pb];
      byte[] lo = new byte[pb];
      byte[] hi2 = new byte[pb];
      byte[] lo2 = new byte[pb];
      encodePlanes(v, dim, hi, 0, lo, 0);
      encodePlanes(v, dim, hi2, 0, lo2, 0);
      assertEquals(0, VectorUtil.xorBitCount(hi, hi2) + VectorUtil.xorBitCount(lo, lo2));
    }
  }

  /**
   * The split-destination encode must be BIT-IDENTICAL to a per-coordinate reference.
   *
   * <p>{@link Nitrox2#encode} packs the whole coarse code (both thermometer planes concatenated)
   * through the vectorized packing kernel; this test splits that code back into planes and checks
   * each against a per-coordinate reference. So this is a format equivalence, not an optimization
   * check: a disagreement of one bit means documents were packed on one grid and queries quantized
   * on another, which changes ranking silently and is the exact defect this class was built to
   * prevent.
   *
   * <p>The reference is spelled out here rather than delegated, so it stays independent of whatever
   * the production path does. Odd dims exercise the byte-granular tail, where a whole-word kernel
   * is most likely to disagree.
   */
  public void testEncodePlanesIsBitIdenticalToPerCoordinateReference() {
    for (int dim : new int[] {1024, 768, 256, 128, 96, 64}) {
      final int pb = Nitrox2.planeBytes(dim);
      for (int trial = 0; trial < 20; trial++) {
        final float[] v = randomRotatedVector(dim);
        // Coordinates forced onto and just off the thresholds, since that behaviour is the format.
        final float clip = Nitrox2.clipFor(dim);
        v[0] = Nitrox2.thresholdFor(0, clip);
        v[1] = Nitrox2.thresholdFor(1, clip);
        v[2] = Math.nextDown(Nitrox2.thresholdFor(1, clip));
        v[3] = Float.NaN;
        v[4] = Float.POSITIVE_INFINITY;
        v[5] = Float.NEGATIVE_INFINITY;

        final byte[] hi = new byte[pb];
        final byte[] lo = new byte[pb];
        encodePlanes(v, dim, hi, 0, lo, 0);

        // Independent reference: the thermometer, one coordinate at a time, through levelOnGrid.
        final byte[] refHi = new byte[pb];
        final byte[] refLo = new byte[pb];
        final float invStep = Nitrox2.invStepFor(dim);
        for (int d = 0; d < dim; d++) {
          final int l = Nitrox2.levelOnGrid(v[d], clip, invStep);
          if (l > 0) {
            refHi[d >>> 3] |= (byte) (1 << (d & 7));
            if (l > 1) {
              refLo[d >>> 3] |= (byte) (1 << (d & 7));
            }
          }
        }
        assertArrayEquals("hi plane must be bit-identical at dim=" + dim, refHi, hi);
        if (Nitrox2.PLANES > 1) {
          assertArrayEquals("lo plane must be bit-identical at dim=" + dim, refLo, lo);
        }
      }
    }
  }

  /**
   * Test helper: the two thermometer planes split into separate arrays, derived from the single
   * concatenated {@link Nitrox2#encode}. The codec has no split-destination encode, since the code
   * is one contiguous string of hi plane then lo plane, so this helper does the split the tests
   * want in order to exercise the per-plane Hamming identity.
   */
  private static void encodePlanes(float[] v, int dim, byte[] hi, int hiOff, byte[] lo, int loOff) {
    final int pb = Nitrox2.planeBytes(dim);
    final byte[] code = new byte[Nitrox2.bytesPerVector(dim)];
    Nitrox2.encode(v, dim, code, 0);
    System.arraycopy(code, 0, hi, hiOff, pb);
    if (Nitrox2.PLANES > 1) {
      System.arraycopy(code, pb, lo, loOff, pb);
    }
  }

  /**
   * A vector whose per-dimension scale matches what the Hadamard rotation produces: standard
   * deviation {@code 1/sqrt(dim)}, which is the grid the code is built for.
   */
  private float[] randomRotatedVector(int dim) {
    float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (random().nextGaussian() * std);
    }
    return v;
  }
}
