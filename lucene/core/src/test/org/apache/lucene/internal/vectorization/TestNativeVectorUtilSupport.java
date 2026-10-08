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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;

/** Exercises native dispatch without loading a native library or creating downcall handles. */
public class TestNativeVectorUtilSupport extends TestVectorUtilSupport {

  public enum Mode {
    ALL,
    NONE,
    PARTIAL
  }

  private final Mode mode;
  private VectorUtilSupport support;

  public TestNativeVectorUtilSupport(int size, Mode mode) {
    super(size);
    this.mode = mode;
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    List<Object[]> parameters = new ArrayList<>();
    for (Object[] parameter : TestVectorUtilSupport.parametersFactory()) {
      for (Mode mode : Mode.values()) {
        parameters.add(new Object[] {parameter[0], mode});
      }
    }
    return parameters;
  }

  @Before
  public void createSupport() {
    Set<String> available = new HashSet<>();
    List<String> calls = new ArrayList<>();
    JavaNativeFunctions javaFunctions = new JavaNativeFunctions(calls);
    var functions =
        new NativeVectorUtilSupport.NativeFunctions(
            (name, descriptor) -> {
              boolean enabled =
                  switch (mode) {
                    case ALL -> true;
                    case NONE -> false;
                    // Always exercise both dispatch and fallback, and vary the other symbols.
                    case PARTIAL ->
                        name.equals("dotProduct")
                            || (!name.equals("squareDistance") && random().nextBoolean());
                  };
              if (enabled) {
                available.add(name);
                return javaFunctions.lookup(name, descriptor);
              }
              return null;
            });
    VectorUtilSupport delegate = PANAMA_OR_NATIVE_PROVIDER.getVectorUtilSupport();
    VectorUtilSupport checkedDelegate =
        (VectorUtilSupport)
            Proxy.newProxyInstance(
                VectorUtilSupport.class.getClassLoader(),
                new Class<?>[] {VectorUtilSupport.class},
                (_, method, args) -> {
                  calls.add("delegate:" + symbol(method));
                  return invoke(method, delegate, args);
                });
    var nativeSupport = new NativeVectorUtilSupport(checkedDelegate, functions);
    // Check the dispatch as well as the result, including accidental calls to static defaults.
    support =
        (VectorUtilSupport)
            Proxy.newProxyInstance(
                VectorUtilSupport.class.getClassLoader(),
                new Class<?>[] {VectorUtilSupport.class},
                (_, method, args) -> {
                  calls.clear();
                  Object result = invoke(method, nativeSupport, args);
                  String symbol = symbol(method);
                  boolean hasHandle =
                      method.getParameterTypes()[0] != short[].class && available.contains(symbol);
                  assertEquals(List.of((hasHandle ? "native:" : "delegate:") + symbol), calls);
                  return result;
                });
  }

  private static String symbol(Method method) {
    String name = method.getName();
    if (method.getParameterTypes()[0] == float[].class
        && (name.equals("dotProduct") || name.equals("cosine") || name.equals("squareDistance"))) {
      return name + "Float";
    }
    return name;
  }

  private static Object invoke(Method method, VectorUtilSupport target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Override
  protected VectorUtilSupport vectorUtilSupport() {
    return support;
  }

  public void testFindNextGEQ() {
    int[] values = {-10, -3, 0, 0, 4, 9, 13, 20};
    VectorUtilSupport reference = LUCENE_PROVIDER.getVectorUtilSupport();
    for (int from = 0; from <= values.length; from++) {
      for (int to = from; to <= values.length; to++) {
        for (int target : new int[] {-11, -10, 0, 1, 13, 21}) {
          assertEquals(
              reference.findNextGEQ(values, target, from, to),
              support.findNextGEQ(values, target, from, to));
        }
      }
    }
  }

  public void testFilterByScore() {
    VectorUtilSupport reference = LUCENE_PROVIDER.getVectorUtilSupport();
    int[] docs = {10, 20, 30, 40, 50, 60};
    double[] scores = {-1, 0, 0.5, 1, 1.5, 2};
    for (int upTo = 0; upTo <= docs.length; upTo++) {
      for (double threshold : new double[] {-2, 0, 0.5, 1.5, 3}) {
        int[] expectedDocs = docs.clone();
        double[] expectedScores = scores.clone();
        int[] actualDocs = docs.clone();
        double[] actualScores = scores.clone();
        int count = reference.filterByScore(expectedDocs, expectedScores, threshold, upTo);
        assertEquals(count, support.filterByScore(actualDocs, actualScores, threshold, upTo));
        for (int i = 0; i < count; i++) {
          assertEquals(expectedDocs[i], actualDocs[i]);
          assertEquals(expectedScores[i], actualScores[i], 0);
        }
        // Entries beyond upTo must not be modified.
        for (int i = upTo; i < docs.length; i++) {
          assertEquals(docs[i], actualDocs[i]);
          assertEquals(scores[i], actualScores[i], 0);
        }
      }
    }
  }

  public void testL2Normalize() {
    for (boolean throwOnZero : new boolean[] {false, true}) {
      float[] actual = {1, -2, 3, -4, 5};
      float[] expected = actual.clone();
      LUCENE_PROVIDER.getVectorUtilSupport().l2normalize(expected, throwOnZero);
      assertSame(actual, support.l2normalize(actual, throwOnZero));
      assertArrayEquals(expected, actual, 1e-6f);
    }
    float[] zeros = new float[5];
    assertSame(zeros, support.l2normalize(zeros, false));
    assertArrayEquals(new float[5], zeros, 0f);
  }

  public void testExpand8() {
    int[] actual = new int[256];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = random().nextInt();
    }
    int[] expected = actual.clone();
    LUCENE_PROVIDER.getVectorUtilSupport().expand8(expected);
    support.expand8(actual);
    assertArrayEquals(expected, actual);
  }

  public void testByteCosineFractional() {
    assertEquals(0.5f, support.cosine(new byte[] {1, 0, 1}, new byte[] {1, 1, 0}), 1e-6f);
  }

  public void testFloat16Fallback() {
    short[] a = new short[37];
    short[] b = new short[a.length];
    for (int i = 0; i < a.length; i++) {
      a[i] = Float.floatToFloat16(random().nextFloat());
      b[i] = Float.floatToFloat16(random().nextFloat());
    }
    VectorUtilSupport reference = LUCENE_PROVIDER.getVectorUtilSupport();
    assertEquals(reference.dotProduct(a, b), support.dotProduct(a, b), 1e-4f);
    assertEquals(reference.cosine(a, b), support.cosine(a, b), 1e-4f);
    assertEquals(reference.squareDistance(a, b), support.squareDistance(a, b), 1e-4f);
  }

  public void testInvocationFailures() {
    RuntimeException failure = new IllegalStateException("test native invocation failure");
    var functions =
        new NativeVectorUtilSupport.NativeFunctions(
            (_, descriptor) -> {
              MethodType type = descriptor.toMethodType();
              return MethodHandles.dropArguments(
                  MethodHandles.throwException(type.returnType(), RuntimeException.class)
                      .bindTo(failure),
                  0,
                  type.parameterList());
            });
    VectorUtilSupport failingSupport =
        new NativeVectorUtilSupport(LUCENE_PROVIDER.getVectorUtilSupport(), functions);
    // Each invocation helper must propagate the original cause, never silently fall back.
    assertSame(
        failure,
        expectThrows(
                AssertionError.class,
                () -> failingSupport.dotProduct(new byte[] {1}, new byte[] {2}))
            .getCause());
    assertSame(
        failure,
        expectThrows(
                AssertionError.class, () -> failingSupport.cosine(new float[] {1}, new float[] {2}))
            .getCause());
    assertSame(
        failure,
        expectThrows(
                AssertionError.class,
                () -> failingSupport.int4BitDotProduct(new byte[4], new byte[1]))
            .getCause());
    assertSame(
        failure,
        expectThrows(AssertionError.class, () -> failingSupport.findNextGEQ(new int[] {1}, 1, 0, 1))
            .getCause());
    assertSame(
        failure,
        expectThrows(AssertionError.class, () -> failingSupport.l2normalize(new float[] {1}, true))
            .getCause());
    assertSame(
        failure,
        expectThrows(AssertionError.class, () -> failingSupport.expand8(new int[256])).getCause());
  }

  /** Java methods with the same signatures and in-place updates as the native symbols. */
  @SuppressWarnings("unused") // Methods are looked up by name through method handles.
  private static class JavaNativeFunctions {
    private final VectorUtilSupport reference = new DefaultVectorUtilSupport();
    private final List<String> calls;

    JavaNativeFunctions(List<String> calls) {
      this.calls = calls;
    }

    private void recordInvocation(String symbol) {
      calls.add("native:" + symbol);
    }

    MethodHandle lookup(String name, FunctionDescriptor descriptor) {
      try {
        var lookup = MethodHandles.lookup();
        MethodHandle target =
            lookup
                .findVirtual(JavaNativeFunctions.class, name, descriptor.toMethodType())
                .bindTo(this);
        MethodHandle record =
            lookup
                .findVirtual(
                    JavaNativeFunctions.class,
                    "recordInvocation",
                    MethodType.methodType(void.class, String.class))
                .bindTo(this)
                .bindTo(name);
        return MethodHandles.foldArguments(target, record);
      } catch (ReflectiveOperationException e) {
        throw new AssertionError("Invalid native signature for " + name, e);
      }
    }

    private static void checkByteSize(MemorySegment a, int byteSize) {
      assertEquals(a.byteSize(), byteSize);
    }

    int dotProduct(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.dotProduct(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int squareDistance(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.squareDistance(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    float cosine(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.cosine(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    float dotProductFloat(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.dotProduct(a.toArray(JAVA_FLOAT), b.toArray(JAVA_FLOAT));
    }

    float squareDistanceFloat(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.squareDistance(a.toArray(JAVA_FLOAT), b.toArray(JAVA_FLOAT));
    }

    float cosineFloat(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.cosine(a.toArray(JAVA_FLOAT), b.toArray(JAVA_FLOAT));
    }

    int int4SquareDistance(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4SquareDistance(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int int4SquareDistanceSinglePacked(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4SquareDistanceSinglePacked(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int int4SquareDistanceBothPacked(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4SquareDistanceBothPacked(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int uint8SquareDistance(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.uint8SquareDistance(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int uint8DotProduct(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.uint8DotProduct(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int int4DotProduct(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4DotProduct(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int int4DotProductSinglePacked(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4DotProductSinglePacked(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    int int4DotProductBothPacked(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4DotProductBothPacked(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    long int4BitDotProduct(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4BitDotProduct(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    long int4DibitDotProduct(MemorySegment a, MemorySegment b, int byteSize) {
      checkByteSize(a, byteSize);
      return reference.int4DibitDotProduct(a.toArray(JAVA_BYTE), b.toArray(JAVA_BYTE));
    }

    float minMaxScalarQuantize(
        MemorySegment vector,
        MemorySegment dest,
        float scale,
        float alpha,
        float minQuantile,
        float maxQuantile,
        int length) {
      float[] values = vector.toArray(JAVA_FLOAT);
      assertEquals(values.length, length);
      byte[] output = dest.toArray(JAVA_BYTE);
      float result =
          reference.minMaxScalarQuantize(values, output, scale, alpha, minQuantile, maxQuantile);
      MemorySegment.copy(MemorySegment.ofArray(output), 0, dest, 0, dest.byteSize());
      return result;
    }

    float recalculateScalarQuantizationOffset(
        MemorySegment vector,
        float oldAlpha,
        float oldMinQuantile,
        float scale,
        float alpha,
        float minQuantile,
        float maxQuantile,
        int length) {
      byte[] values = vector.toArray(JAVA_BYTE);
      assertEquals(values.length, length);
      return reference.recalculateScalarQuantizationOffset(
          values, oldAlpha, oldMinQuantile, scale, alpha, minQuantile, maxQuantile);
    }

    int findNextGEQ(MemorySegment buffer, int target, int from, int to) {
      return reference.findNextGEQ(buffer.toArray(JAVA_INT), target, from, to);
    }

    int filterByScore(MemorySegment docs, MemorySegment scores, double minScore, int upTo) {
      int[] docBuffer = docs.toArray(JAVA_INT);
      double[] scoreBuffer = scores.toArray(JAVA_DOUBLE);
      int count = reference.filterByScore(docBuffer, scoreBuffer, minScore, upTo);
      MemorySegment.copy(MemorySegment.ofArray(docBuffer), 0, docs, 0, docs.byteSize());
      MemorySegment.copy(MemorySegment.ofArray(scoreBuffer), 0, scores, 0, scores.byteSize());
      return count;
    }

    MemorySegment l2normalize(MemorySegment vector, byte throwOnZero, int length) {
      float[] values = vector.toArray(JAVA_FLOAT);
      assertEquals(values.length, length);
      assertTrue(throwOnZero == 0 || throwOnZero == 1);
      reference.l2normalize(values, throwOnZero != 0);
      MemorySegment.copy(MemorySegment.ofArray(values), 0, vector, 0, vector.byteSize());
      return vector;
    }

    void expand8(MemorySegment vector, int length) {
      int[] values = vector.toArray(JAVA_INT);
      assertEquals(values.length, length);
      reference.expand8(values);
      MemorySegment.copy(MemorySegment.ofArray(values), 0, vector, 0, vector.byteSize());
    }
  }
}
