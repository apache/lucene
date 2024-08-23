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
package org.apache.lucene.util;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static org.apache.lucene.util.VectorUtil.B_QUERY;

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.vectorization.BaseVectorizationTestCase;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestVectorUtil extends LuceneTestCase {

  public static final double DELTA = 1e-4;

  public void testBasicDotProduct() {
    assertEquals(5, VectorUtil.dotProduct(new float[] {1, 2, 3}, new float[] {-10, 0, 5}), 0);
  }

  public void testSelfDotProduct() {
    // the dot product of a vector with itself is equal to the sum of the squares of its components
    float[] v = randomVector();
    assertEquals(l2(v), VectorUtil.dotProduct(v, v), DELTA);
  }

  public void testOrthogonalDotProduct() {
    // the dot product of two perpendicular vectors is 0
    float[] v = new float[2];
    v[0] = random().nextInt(100);
    v[1] = random().nextInt(100);
    float[] u = new float[2];
    u[0] = v[1];
    u[1] = -v[0];
    assertEquals(0, VectorUtil.dotProduct(u, v), DELTA);
  }

  public void testDotProductThrowsForDimensionMismatch() {
    float[] v = {1, 0, 0}, u = {0, 1};
    expectThrows(IllegalArgumentException.class, () -> VectorUtil.dotProduct(u, v));
  }

  public void testSelfSquareDistance() {
    // the l2 distance of a vector with itself is zero
    float[] v = randomVector();
    assertEquals(0, VectorUtil.squareDistance(v, v), DELTA);
  }

  public void testBasicSquareDistance() {
    assertEquals(12, VectorUtil.squareDistance(new float[] {1, 2, 3}, new float[] {-1, 0, 5}), 0);
  }

  public void testSquareDistanceThrowsForDimensionMismatch() {
    float[] v = {1, 0, 0}, u = {0, 1};
    expectThrows(IllegalArgumentException.class, () -> VectorUtil.squareDistance(u, v));
  }

  public void testRandomSquareDistance() {
    // the square distance of a vector with its inverse is equal to four times the sum of squares of
    // its components
    float[] v = randomVector();
    float[] u = negative(v);
    assertEquals(4 * l2(v), VectorUtil.squareDistance(u, v), DELTA);
  }

  public void testBasicCosine() {
    assertEquals(
        0.11952f, VectorUtil.cosine(new float[] {1, 2, 3}, new float[] {-10, 0, 5}), DELTA);
  }

  public void testSelfCosine() {
    // the dot product of a vector with itself is always equal to 1
    float[] v = randomVector();
    // ensure the vector is non-zero so that cosine is defined
    v[0] = random().nextFloat() + 0.01f;
    assertEquals(1.0f, VectorUtil.cosine(v, v), DELTA);
  }

  public void testOrthogonalCosine() {
    // the cosine of two perpendicular vectors is 0
    float[] v = new float[2];
    v[0] = random().nextInt(100);
    // ensure the vector is non-zero so that cosine is defined
    v[1] = random().nextInt(1, 100);
    float[] u = new float[2];
    u[0] = v[1];
    u[1] = -v[0];
    assertEquals(0, VectorUtil.cosine(u, v), DELTA);
  }

  public void testCosineThrowsForDimensionMismatch() {
    float[] v = {1, 0, 0}, u = {0, 1};
    expectThrows(IllegalArgumentException.class, () -> VectorUtil.cosine(u, v));
  }

  public void testNormalize() {
    float[] v = randomVector();
    v[random().nextInt(v.length)] = 1; // ensure vector is not all zeroes
    VectorUtil.l2normalize(v);
    assertEquals(1f, l2(v), DELTA);
  }

  public void testNormalizeZeroThrows() {
    float[] v = {0, 0, 0};
    expectThrows(IllegalArgumentException.class, () -> VectorUtil.l2normalize(v));
  }

  public void testExtremeNumerics() {
    float[] v1 = new float[1536];
    float[] v2 = new float[1536];
    for (int i = 0; i < 1536; i++) {
      v1[i] = 0.888888f;
      v2[i] = -0.777777f;
    }
    for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
      float v = vectorSimilarityFunction.compare(v1, v2);
      assertTrue(vectorSimilarityFunction + " expected >=0 got:" + v, v >= 0);
    }
  }

  private static float l2(float[] v) {
    float l2 = 0;
    for (float x : v) {
      l2 += x * x;
    }
    return l2;
  }

  private static float[] negative(float[] v) {
    float[] u = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      u[i] = -v[i];
    }
    return u;
  }

  private static byte[] negative(byte[] v) {
    byte[] u = new byte[v.length];
    for (int i = 0; i < v.length; i++) {
      // what is (byte) -(-128)? 127?
      u[i] = (byte) -v[i];
    }
    return u;
  }

  private static float l2(byte[] v) {
    float l2 = 0;
    for (int i = 0; i < v.length; i++) {
      l2 += v[i] * v[i];
    }
    return l2;
  }

  private static float[] randomVector() {
    return randomVector(random().nextInt(100) + 1);
  }

  public static float[] randomVector(int dim) {
    float[] v = new float[dim];
    Random random = random();
    for (int i = 0; i < dim; i++) {
      v[i] = random.nextFloat();
    }
    return v;
  }

  private static byte[] randomVectorBytes() {
    BytesRef v = TestUtil.randomBinaryTerm(random(), TestUtil.nextInt(random(), 1, 100));
    // clip at -127 to avoid overflow
    for (int i = v.offset; i < v.offset + v.length; i++) {
      if (v.bytes[i] == -128) {
        v.bytes[i] = -127;
      }
    }
    assert v.offset == 0;
    return v.bytes;
  }

  public static byte[] randomVectorBytes(int dim) {
    BytesRef v = TestUtil.randomBinaryTerm(random(), dim);
    // clip at -127 to avoid overflow
    for (int i = v.offset; i < v.offset + v.length; i++) {
      if (v.bytes[i] == -128) {
        v.bytes[i] = -127;
      }
    }
    return v.bytes;
  }

  public void testBasicDotProductBytes() {
    byte[] a = new byte[] {1, 2, 3};
    byte[] b = new byte[] {-10, 0, 5};
    assertEquals(5, VectorUtil.dotProduct(a, b), 0);
    float denom = a.length * (1 << 15);
    assertEquals(0.5 + 5 / denom, VectorUtil.dotProductScore(a, b), DELTA);

    // dot product 0 maps to dotProductScore 0.5
    byte[] zero = new byte[] {0, 0, 0};
    assertEquals(0.5, VectorUtil.dotProductScore(a, zero), DELTA);

    byte[] min = new byte[] {-128, -128};
    byte[] max = new byte[] {127, 127};
    // minimum dot product score is not quite zero because 127 < 128
    assertEquals(0.0039, VectorUtil.dotProductScore(min, max), DELTA);

    // maximum dot product score
    assertEquals(1, VectorUtil.dotProductScore(min, min), DELTA);
  }

  public void testSelfDotProductBytes() {
    // the dot product of a vector with itself is equal to the sum of the squares of its components
    byte[] v = randomVectorBytes();
    assertEquals(l2(v), VectorUtil.dotProduct(v, v), DELTA);
  }

  public void testOrthogonalDotProductBytes() {
    // the dot product of two perpendicular vectors is 0
    byte[] a = new byte[2];
    a[0] = (byte) random().nextInt(100);
    a[1] = (byte) random().nextInt(100);
    byte[] b = new byte[2];
    b[0] = a[1];
    b[1] = (byte) -a[0];
    assertEquals(0, VectorUtil.dotProduct(a, b), DELTA);
  }

  public void testSelfSquareDistanceBytes() {
    // the l2 distance of a vector with itself is zero
    byte[] v = randomVectorBytes();
    assertEquals(0, VectorUtil.squareDistance(v, v), DELTA);
  }

  public void testBasicSquareDistanceBytes() {
    assertEquals(12, VectorUtil.squareDistance(new byte[] {1, 2, 3}, new byte[] {-1, 0, 5}), 0);
  }

  public void testRandomSquareDistanceBytes() {
    // the square distance of a vector with its inverse is equal to four times the sum of squares of
    // its components
    byte[] v = randomVectorBytes();
    byte[] u = negative(v);
    assertEquals(4 * l2(v), VectorUtil.squareDistance(u, v), DELTA);
  }

  public void testBasicCosineBytes() {
    assertEquals(0.11952f, VectorUtil.cosine(new byte[] {1, 2, 3}, new byte[] {-10, 0, 5}), DELTA);
  }

  public void testSelfCosineBytes() {
    // the dot product of a vector with itself is always equal to 1
    byte[] v = randomVectorBytes();
    // ensure the vector is non-zero so that cosine is defined
    v[0] = (byte) (random().nextInt(126) + 1);
    assertEquals(1.0f, VectorUtil.cosine(v, v), DELTA);
  }

  public void testOrthogonalCosineBytes() {
    // the cosine of two perpendicular vectors is 0
    float[] v = new float[2];
    v[0] = random().nextInt(100);
    // ensure the vector is non-zero so that cosine is defined
    v[1] = random().nextInt(1, 100);
    float[] u = new float[2];
    u[0] = v[1];
    u[1] = -v[0];
    assertEquals(0, VectorUtil.cosine(u, v), DELTA);
  }

  interface ToIntBiFunction {
    int apply(byte[] a, byte[] b);
  }

  public void testBasicXorBitCount() {
    testBasicXorBitCountImpl(VectorUtil::xorBitCount);
    testBasicXorBitCountImpl(VectorUtil::xorBitCountInt);
    testBasicXorBitCountImpl(VectorUtil::xorBitCountLong);
    // test sanity
    testBasicXorBitCountImpl(TestVectorUtil::xorBitCount);
  }

  void testBasicXorBitCountImpl(ToIntBiFunction xorBitCount) {
    assertEquals(0, xorBitCount.apply(new byte[] {1}, new byte[] {1}));
    assertEquals(0, xorBitCount.apply(new byte[] {1, 2, 3}, new byte[] {1, 2, 3}));
    assertEquals(1, xorBitCount.apply(new byte[] {1, 2, 3}, new byte[] {0, 2, 3}));
    assertEquals(2, xorBitCount.apply(new byte[] {1, 2, 3}, new byte[] {0, 6, 3}));
    assertEquals(3, xorBitCount.apply(new byte[] {1, 2, 3}, new byte[] {0, 6, 7}));
    assertEquals(4, xorBitCount.apply(new byte[] {1, 2, 3}, new byte[] {2, 6, 7}));

    // 32-bit / int boundary
    assertEquals(0, xorBitCount.apply(new byte[] {1, 2, 3, 4}, new byte[] {1, 2, 3, 4}));
    assertEquals(1, xorBitCount.apply(new byte[] {1, 2, 3, 4}, new byte[] {0, 2, 3, 4}));
    assertEquals(0, xorBitCount.apply(new byte[] {1, 2, 3, 4, 5}, new byte[] {1, 2, 3, 4, 5}));
    assertEquals(1, xorBitCount.apply(new byte[] {1, 2, 3, 4, 5}, new byte[] {0, 2, 3, 4, 5}));

    // 64-bit / long boundary
    assertEquals(
        0,
        xorBitCount.apply(
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    assertEquals(
        1,
        xorBitCount.apply(
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, new byte[] {0, 2, 3, 4, 5, 6, 7, 8}));

    assertEquals(
        0,
        xorBitCount.apply(
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));
    assertEquals(
        1,
        xorBitCount.apply(
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, new byte[] {0, 2, 3, 4, 5, 6, 7, 8, 9}));
  }

  public void testXorBitCount() {
    int iterations = atLeast(100);
    for (int i = 0; i < iterations; i++) {
      int size = random().nextInt(1024);
      byte[] a = new byte[size];
      byte[] b = new byte[size];
      random().nextBytes(a);
      random().nextBytes(b);

      int expected = xorBitCount(a, b);
      assertEquals(expected, VectorUtil.xorBitCount(a, b));
      assertEquals(expected, VectorUtil.xorBitCountInt(a, b));
      assertEquals(expected, VectorUtil.xorBitCountLong(a, b));
    }
  }

  private static int xorBitCount(byte[] a, byte[] b) {
    int res = 0;
    for (int i = 0; i < a.length; i++) {
      byte x = a[i];
      byte y = b[i];
      for (int j = 0; j < Byte.SIZE; j++) {
        if (x == y) break;
        if ((x & 0x01) != (y & 0x01)) res++;
        x = (byte) ((x & 0xFF) >> 1);
        y = (byte) ((y & 0xFF) >> 1);
      }
    }
    return res;
  }

  public void testIpByteBinInvariants() {
    int iterations = atLeast(10);
    for (int i = 0; i < iterations; i++) {
      int size = randomIntBetween(random(), 1, 10);
      var d = new byte[size];
      var q = new byte[size * B_QUERY - 1];
      expectThrows(IllegalArgumentException.class, () -> VectorUtil.ipByteBinByte(q, d));
    }
  }

  static final VectorizationProvider defaultedProvider =
      BaseVectorizationTestCase.defaultProvider();
  static final VectorizationProvider defOrPanamaProvider =
      BaseVectorizationTestCase.maybePanamaProvider();

  public void testBasicIpByteBin() {
    testBasicIpByteBinImpl(VectorUtil::ipByteBinByte);
    testBasicIpByteBinImpl(defaultedProvider.getVectorUtilSupport()::ipByteBinByte);
    testBasicIpByteBinImpl(defOrPanamaProvider.getVectorUtilSupport()::ipByteBinByte);
  }

  interface IpByteBin {
    long apply(byte[] q, byte[] d);
  }

  void testBasicIpByteBinImpl(IpByteBin ipByteBinFunc) {
    assertEquals(15L, ipByteBinFunc.apply(new byte[] {1, 1, 1, 1}, new byte[] {1}));
    assertEquals(30L, ipByteBinFunc.apply(new byte[] {1, 2, 1, 2, 1, 2, 1, 2}, new byte[] {1, 2}));

    var d = new byte[] {1, 2, 3};
    var q = new byte[] {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    assert scalarIpByteBin(q, d) == 60L; // 4 + 8 + 16 + 32
    assertEquals(60L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4};
    q = new byte[] {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    assert scalarIpByteBin(q, d) == 75L; // 5 + 10 + 20 + 40
    assertEquals(75L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5};
    q = new byte[] {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5};
    assert scalarIpByteBin(q, d) == 105L; // 7 + 14 + 28 + 56
    assertEquals(105L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6};
    q = new byte[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
    assert scalarIpByteBin(q, d) == 135L; // 9 + 18 + 36 + 72
    assertEquals(135L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7
        };
    assert scalarIpByteBin(q, d) == 180L; // 12 + 24 + 48 + 96
    assertEquals(180L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6,
          7, 8
        };
    assert scalarIpByteBin(q, d) == 195L; // 13 + 26 + 52 + 104
    assertEquals(195L, ipByteBinFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3,
          4, 5, 6, 7, 8, 9
        };
    assert scalarIpByteBin(q, d) == 225L; // 15 + 30 + 60 + 120
    assertEquals(225L, ipByteBinFunc.apply(q, d));
  }

  public void testIpByteBin() {
    testIpByteBinImpl(VectorUtil::ipByteBinByte);
    testIpByteBinImpl(defaultedProvider.getVectorUtilSupport()::ipByteBinByte);
    testIpByteBinImpl(defOrPanamaProvider.getVectorUtilSupport()::ipByteBinByte);
  }

  void testIpByteBinImpl(IpByteBin ipByteBinFunc) {
    int iterations = atLeast(50);
    for (int i = 0; i < iterations; i++) {
      int size = random().nextInt(5000);
      var d = new byte[size];
      var q = new byte[size * B_QUERY];
      random().nextBytes(d);
      random().nextBytes(q);
      assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.apply(q, d));

      Arrays.fill(d, Byte.MAX_VALUE);
      Arrays.fill(q, Byte.MAX_VALUE);
      assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.apply(q, d));

      Arrays.fill(d, Byte.MIN_VALUE);
      Arrays.fill(q, Byte.MIN_VALUE);
      assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.apply(q, d));
    }
  }

  static int scalarIpByteBin(byte[] q, byte[] d) {
    int res = 0;
    for (int i = 0; i < B_QUERY; i++) {
      res += (popcount(q, i * d.length, d, d.length) << i);
    }
    return res;
  }

  static int popcount(byte[] a, int aOffset, byte[] b, int length) {
    int res = 0;
    for (int j = 0; j < length; j++) {
      int value = (a[aOffset + j] & b[j]) & 0xFF;
      for (int k = 0; k < Byte.SIZE; k++) {
        if ((value & (1 << k)) != 0) {
          ++res;
        }
      }
    }
    return res;
  }
}
