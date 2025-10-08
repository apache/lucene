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

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.vectorization.BaseVectorizationTestCase;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.search.DocAndScoreAccBuffer;
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

  public void testNormalizeToUnitInterval() {
    for (int i = 0; i < 100; i++) {
      // Generates a float in the range [-1.0, 1.0)
      float f = random().nextFloat() * 2 - 1;
      float v = VectorUtil.normalizeToUnitInterval(f);
      assertTrue(v >= 0);
      assertTrue(v <= 1);
      assertEquals(Math.max((1 + f) / 2, 0), v, 0.0f);
    }
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

  private static float uint8L2(byte[] v) {
    float l2 = 0;
    for (int i = 0; i < v.length; i++) {
      l2 += Byte.toUnsignedInt(v[i]) * Byte.toUnsignedInt(v[i]);
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

  public void testBasicDotProductUint8() {
    byte[] a = new byte[] {1, 2, 3};
    byte[] b = new byte[] {-10, 0, 5};
    assertEquals(261, VectorUtil.uint8DotProduct(a, b), 0);

    byte[] min = new byte[] {-128, -128};
    byte[] max = new byte[] {127, 127};
    assertEquals(32512, VectorUtil.uint8DotProduct(min, max), DELTA);
  }

  public void testSelfDotProductUint8() {
    // the dot product of a vector with itself is equal to the sum of the squares of its components
    byte[] v = randomVectorBytes();
    assertEquals(uint8L2(v), VectorUtil.uint8DotProduct(v, v), DELTA);
  }

  public void testSelfSquareDistanceUint8() {
    // the l2 distance of a vector with itself is zero
    byte[] v = randomVectorBytes();
    assertEquals(0, VectorUtil.uint8SquareDistance(v, v), DELTA);
  }

  public void testBasicSquareDistanceUint8() {
    assertEquals(
        64524, VectorUtil.uint8SquareDistance(new byte[] {1, 2, 3}, new byte[] {-1, 0, 5}), 0);
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

  public void testFindNextGEQ() {
    int padding = TestUtil.nextInt(random(), 0, 5);
    int[] values = new int[128 + padding];
    int v = 0;
    for (int i = 0; i < 128; ++i) {
      v += TestUtil.nextInt(random(), 1, 1000);
      values[i] = v;
    }

    // Now duel with slowFindFirstGreater
    for (int iter = 0; iter < 1_000; ++iter) {
      int from = TestUtil.nextInt(random(), 0, 127);
      int target =
          TestUtil.nextInt(random(), values[from], Math.max(values[from], values[127]))
              + random().nextInt(10)
              - 5;
      assertEquals(
          slowFindNextGEQ(values, 128, target, from),
          VectorUtil.findNextGEQ(values, target, from, 128));
    }
  }

  private static int slowFindNextGEQ(int[] buffer, int length, int target, int from) {
    for (int i = from; i < length; ++i) {
      if (buffer[i] >= target) {
        return i;
      }
    }
    return length;
  }

  public void testFilterByScore() {
    for (int iter = 0; iter < 1_000; ++iter) {
      int padding = TestUtil.nextInt(random(), 0, 5);
      DocAndScoreAccBuffer b1 = new DocAndScoreAccBuffer();
      DocAndScoreAccBuffer b2 = new DocAndScoreAccBuffer();
      b1.growNoCopy(128 + padding);
      b2.growNoCopy(128 + padding);

      int doc = 0;
      for (int i = 0; i < 128 + padding; ++i) {
        doc += TestUtil.nextInt(random(), 1, 1000);
        b1.docs[i] = b2.docs[i] = doc;
        b1.scores[i] = b2.scores[i] = random().nextDouble();
      }

      double minScoreInclusive = random().nextDouble();
      int upTo = TestUtil.nextInt(random(), 0, 127);
      b1.size = slowFilterByScore(b1.docs, b1.scores, minScoreInclusive, upTo);
      b2.size = VectorUtil.filterByScore(b2.docs, b2.scores, minScoreInclusive, upTo);
      assertEquals(b1.size, b2.size);
      assertTrue(Arrays.equals(b1.docs, 0, b1.size, b2.docs, 0, b2.size));
      // two double array should be exactly the same, so just use simple Arrays.equals
      assertTrue(Arrays.equals(b1.scores, 0, b1.size, b2.scores, 0, b2.size));
    }
  }

  private static int slowFilterByScore(
      int[] docBuffer, double[] scoreBuffer, double minScoreInclusive, int upTo) {
    int newSize = 0;
    for (int i = 0; i < upTo; i++) {
      if (scoreBuffer[i] >= minScoreInclusive) {
        docBuffer[newSize] = docBuffer[i];
        scoreBuffer[newSize] = scoreBuffer[i];
        newSize++;
      }
    }
    return newSize;
  }

  public void testInt4BitDotProductInvariants() {
    int iterations = atLeast(10);
    for (int i = 0; i < iterations; i++) {
      int size = randomIntBetween(random(), 1, 10);
      var d = new byte[size];
      var q = new byte[size * 4 - 1];
      expectThrows(IllegalArgumentException.class, () -> VectorUtil.int4BitDotProduct(q, d));
    }
  }

  static final VectorizationProvider defaultedProvider =
      BaseVectorizationTestCase.defaultProvider();
  static final VectorizationProvider defOrPanamaProvider =
      BaseVectorizationTestCase.maybePanamaProvider();

  public void testBasicInt4BitDotProduct() {
    testBasicInt4BitDotProductImpl(VectorUtil::int4BitDotProduct);
    testBasicInt4BitDotProductImpl(defaultedProvider.getVectorUtilSupport()::int4BitDotProduct);
    testBasicInt4BitDotProductImpl(defOrPanamaProvider.getVectorUtilSupport()::int4BitDotProduct);
  }

  interface Int4BitDotProduct {
    long apply(byte[] q, byte[] d);
  }

  void testBasicInt4BitDotProductImpl(Int4BitDotProduct Int4BitDotProductFunc) {
    assertEquals(15L, Int4BitDotProductFunc.apply(new byte[] {1, 1, 1, 1}, new byte[] {1}));
    assertEquals(
        30L, Int4BitDotProductFunc.apply(new byte[] {1, 2, 1, 2, 1, 2, 1, 2}, new byte[] {1, 2}));

    var d = new byte[] {1, 2, 3};
    var q = new byte[] {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    assert scalarInt4BitDotProduct(q, d) == 60L; // 4 + 8 + 16 + 32
    assertEquals(60L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4};
    q = new byte[] {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    assert scalarInt4BitDotProduct(q, d) == 75L; // 5 + 10 + 20 + 40
    assertEquals(75L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5};
    q = new byte[] {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5};
    assert scalarInt4BitDotProduct(q, d) == 105L; // 7 + 14 + 28 + 56
    assertEquals(105L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6};
    q = new byte[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
    assert scalarInt4BitDotProduct(q, d) == 135L; // 9 + 18 + 36 + 72
    assertEquals(135L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7
        };
    assert scalarInt4BitDotProduct(q, d) == 180L; // 12 + 24 + 48 + 96
    assertEquals(180L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6,
          7, 8
        };
    assert scalarInt4BitDotProduct(q, d) == 195L; // 13 + 26 + 52 + 104
    assertEquals(195L, Int4BitDotProductFunc.apply(q, d));

    d = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
    q =
        new byte[] {
          1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3,
          4, 5, 6, 7, 8, 9
        };
    assert scalarInt4BitDotProduct(q, d) == 225L; // 15 + 30 + 60 + 120
    assertEquals(225L, Int4BitDotProductFunc.apply(q, d));
  }

  public void testInt4BitDotProduct() {
    testInt4BitDotProductImpl(VectorUtil::int4BitDotProduct);
    testInt4BitDotProductImpl(defaultedProvider.getVectorUtilSupport()::int4BitDotProduct);
    testInt4BitDotProductImpl(defOrPanamaProvider.getVectorUtilSupport()::int4BitDotProduct);
  }

  void testInt4BitDotProductImpl(Int4BitDotProduct Int4BitDotProductFunc) {
    int iterations = atLeast(50);
    for (int i = 0; i < iterations; i++) {
      int size = random().nextInt(5000);
      var d = new byte[size];
      var q = new byte[size * 4];
      random().nextBytes(d);
      random().nextBytes(q);
      assertEquals(scalarInt4BitDotProduct(q, d), Int4BitDotProductFunc.apply(q, d));

      Arrays.fill(d, Byte.MAX_VALUE);
      Arrays.fill(q, Byte.MAX_VALUE);
      assertEquals(scalarInt4BitDotProduct(q, d), Int4BitDotProductFunc.apply(q, d));

      Arrays.fill(d, Byte.MIN_VALUE);
      Arrays.fill(q, Byte.MIN_VALUE);
      assertEquals(scalarInt4BitDotProduct(q, d), Int4BitDotProductFunc.apply(q, d));
    }
  }

  static int scalarInt4BitDotProduct(byte[] q, byte[] d) {
    int res = 0;
    for (int i = 0; i < 4; i++) {
      res += (popcount(q, i * d.length, d, d.length) << i);
    }
    return res;
  }

  public static int popcount(byte[] a, int aOffset, byte[] b, int length) {
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
