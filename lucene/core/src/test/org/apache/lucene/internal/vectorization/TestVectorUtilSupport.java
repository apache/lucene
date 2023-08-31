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
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

public class TestVectorUtilSupport extends BaseVectorizationTestCase {

  private static final double DELTA = 1e-3;

  private static final int[] VECTOR_SIZES = {
    1, 4, 6, 8, 13, 16, 25, 32, 64, 100, 128, 207, 256, 300, 512, 702, 1024
  };

  private final int size;

  public TestVectorUtilSupport(int size) {
    this.size = size;
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

  private void assertFloatReturningProviders(ToDoubleFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsDouble(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsDouble(PANAMA_PROVIDER.getVectorUtilSupport()),
        DELTA);
  }

  private void assertIntReturningProviders(ToIntFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsInt(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsInt(PANAMA_PROVIDER.getVectorUtilSupport()));
  }
}
