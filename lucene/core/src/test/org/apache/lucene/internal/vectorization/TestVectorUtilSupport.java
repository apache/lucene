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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

public class TestVectorUtilSupport extends BaseVectorizationTestCase {

  private static final int[] VECTOR_SIZES = {
    1, 4, 6, 8, 13, 16, 25, 32, 64, 100, 128, 207, 256, 300, 512, 702, 1024, 1536, 2046, 2048, 4096,
    4098
  };

  private final int size;
  private final double delta;

  public TestVectorUtilSupport(int size) {
    this.size = size;
    // scale the delta with the size
    this.delta = 1e-5 * size;
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    return () -> IntStream.of(VECTOR_SIZES).boxed().map(i -> new Object[] {i}).iterator();
  }

  public void testFloatVectors() {
    var a = new float[size];
    var b = new float[size];
    for (int i = 0; i < size; ++i) {
      a[i] = random().nextFloat();
      b[i] = random().nextFloat();
    }
    assertFloatReturningProviders(p -> p.dotProduct(a, b));
    assertFloatReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  public void testBinaryVectors() {
    var a = new byte[size];
    var b = new byte[size];
    random().nextBytes(a);
    random().nextBytes(b);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  public void testBinaryVectorsBoundaries() {
    var a = new byte[size];
    var b = new byte[size];

    Arrays.fill(a, Byte.MIN_VALUE);
    Arrays.fill(b, Byte.MIN_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MAX_VALUE);
    Arrays.fill(b, Byte.MAX_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MIN_VALUE);
    Arrays.fill(b, Byte.MAX_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MAX_VALUE);
    Arrays.fill(b, Byte.MIN_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  public void testInt4DotProduct() {
    assumeTrue("even sizes only", size % 2 == 0);
    var a = new byte[size];
    var b = new byte[size];
    for (int i = 0; i < size; ++i) {
      a[i] = (byte) random().nextInt(16);
      b[i] = (byte) random().nextInt(16);
    }

    assertIntReturningProviders(p -> p.int4DotProduct(a, b));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(a, pack(b)));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(b, pack(a)));
    assertIntReturningProviders(p -> p.int4DotProductBothPacked(pack(a), pack(b)));

    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProduct(a, b));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(a, pack(b)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(b, pack(a)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductBothPacked(pack(a), pack(b)));
  }

  public void testInt4DotProductBoundaries() {
    assumeTrue("even sizes only", size % 2 == 0);
    byte MAX_VALUE = 15;
    var a = new byte[size];
    var b = new byte[size];

    Arrays.fill(a, MAX_VALUE);
    Arrays.fill(b, MAX_VALUE);

    assertIntReturningProviders(p -> p.int4DotProduct(a, b));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(a, pack(b)));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(b, pack(a)));
    assertIntReturningProviders(p -> p.int4DotProductBothPacked(pack(a), pack(b)));

    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProduct(a, b));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(a, pack(b)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(b, pack(a)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductBothPacked(pack(a), pack(b)));

    byte MIN_VALUE = 0;
    Arrays.fill(a, MIN_VALUE);
    Arrays.fill(b, MIN_VALUE);

    assertIntReturningProviders(p -> p.int4DotProduct(a, b));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(a, pack(b)));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(b, pack(a)));
    assertIntReturningProviders(p -> p.int4DotProductBothPacked(pack(a), pack(b)));

    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProduct(a, b));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(a, pack(b)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(b, pack(a)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductBothPacked(pack(a), pack(b)));
  }

  public void testInt4SquareDistance() {
    assumeTrue("even sizes only", size % 2 == 0);
    var a = new byte[size];
    var b = new byte[size];
    for (int i = 0; i < size; ++i) {
      a[i] = (byte) random().nextInt(16);
      b[i] = (byte) random().nextInt(16);
    }

    assertIntReturningProviders(p -> p.int4SquareDistance(a, b));
    assertIntReturningProviders(p -> p.int4SquareDistanceSinglePacked(a, pack(b)));
    assertIntReturningProviders(p -> p.int4SquareDistanceSinglePacked(b, pack(a)));
    assertIntReturningProviders(p -> p.int4SquareDistanceBothPacked(pack(a), pack(b)));

    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().squareDistance(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4SquareDistance(a, b));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().squareDistance(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4SquareDistanceSinglePacked(a, pack(b)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().squareDistance(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4SquareDistanceSinglePacked(b, pack(a)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().squareDistance(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4SquareDistanceBothPacked(pack(a), pack(b)));
  }

  public void testInt4SquareDistanceBoundaries() {
    assumeTrue("even sizes only", size % 2 == 0);

    // squareDistance is maximized when the points are farther away

    byte MAX_VALUE = 15;
    var a = new byte[size];
    Arrays.fill(a, MAX_VALUE);

    byte MIN_VALUE = 0;
    var b = new byte[size];
    Arrays.fill(b, MIN_VALUE);

    assertIntReturningProviders(p -> p.int4DotProduct(a, b));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(a, pack(b)));
    assertIntReturningProviders(p -> p.int4DotProductSinglePacked(b, pack(a)));
    assertIntReturningProviders(p -> p.int4DotProductBothPacked(pack(a), pack(b)));

    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProduct(a, b));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(a, pack(b)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductSinglePacked(b, pack(a)));
    assertEquals(
        LUCENE_PROVIDER.getVectorUtilSupport().dotProduct(a, b),
        PANAMA_PROVIDER.getVectorUtilSupport().int4DotProductBothPacked(pack(a), pack(b)));
  }

  public void testInt4BitDotProduct() {
    var binaryQuantized = new byte[size];
    var int4Quantized = new byte[size * 4];
    random().nextBytes(binaryQuantized);
    random().nextBytes(int4Quantized);
    assertLongReturningProviders(p -> p.int4BitDotProduct(int4Quantized, binaryQuantized));
  }

  public void testInt4BitDotProductBoundaries() {
    var binaryQuantized = new byte[size];
    var int4Quantized = new byte[size * 4];

    Arrays.fill(binaryQuantized, Byte.MAX_VALUE);
    Arrays.fill(int4Quantized, Byte.MAX_VALUE);
    assertLongReturningProviders(p -> p.int4BitDotProduct(int4Quantized, binaryQuantized));

    Arrays.fill(binaryQuantized, Byte.MIN_VALUE);
    Arrays.fill(int4Quantized, Byte.MIN_VALUE);
    assertLongReturningProviders(p -> p.int4BitDotProduct(int4Quantized, binaryQuantized));
  }

  static byte[] pack(byte[] unpacked) {
    int len = (unpacked.length + 1) / 2;
    var packed = new byte[len];
    for (int i = 0; i < len; i++) {
      packed[i] = (byte) (unpacked[i] << 4 | unpacked[packed.length + i]);
    }
    return packed;
  }

  public void testMinMaxScalarQuantize() {
    Random r = random();
    float min = r.nextFloat(-1, 1);
    float max = r.nextFloat(min, 1);
    float divisor = (float) ((1 << 8) - 1); // 8 bit quantization here

    float scale = divisor / (max - min);
    float alpha = (max - min) / divisor;

    float[] vector = new float[size];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = (r.nextFloat() * (max - min)) + min;
    }

    List<byte[]> outputs = new ArrayList<>();
    assertFloatReturningProviders(
        p -> {
          byte[] output = new byte[size];
          outputs.add(output);
          return p.minMaxScalarQuantize(vector, output, scale, alpha, min, max);
        });

    // check the outputs are identical
    for (int o = 1; o < outputs.size(); o++) {
      assertArrayEquals(outputs.getFirst(), outputs.get(o));
    }

    // check recalculation too
    float newMax = max * 2;
    float newMin = min / 2;
    float newScale = divisor / (newMax - newMin);
    float newAlpha = (newMax - newMin) / divisor;

    assertFloatReturningProviders(
        p ->
            p.recalculateScalarQuantizationOffset(
                outputs.getFirst(), alpha, min, newScale, newAlpha, newMin, newMax));
  }

  private void assertFloatReturningProviders(ToDoubleFunction<VectorUtilSupport> func) {
    double luceneProviderResults = func.applyAsDouble(LUCENE_PROVIDER.getVectorUtilSupport());
    double panamaProviderResults = func.applyAsDouble(PANAMA_PROVIDER.getVectorUtilSupport());
    double delta =
        Math.max(this.delta, this.delta * Math.max(luceneProviderResults, panamaProviderResults));
    assertEquals(luceneProviderResults, panamaProviderResults, delta);
  }

  private void assertIntReturningProviders(ToIntFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsInt(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsInt(PANAMA_PROVIDER.getVectorUtilSupport()));
  }

  private void assertLongReturningProviders(ToLongFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsLong(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsLong(PANAMA_PROVIDER.getVectorUtilSupport()));
  }
}
