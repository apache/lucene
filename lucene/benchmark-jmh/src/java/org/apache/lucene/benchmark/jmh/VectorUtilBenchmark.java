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
package org.apache.lucene.benchmark.jmh;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class VectorUtilBenchmark {
  static void compressBytes(byte[] raw, byte[] compressed) {
    for (int i = 0; i < compressed.length; ++i) {
      int v = (raw[i] << 4) | raw[compressed.length + i];
      compressed[i] = (byte) v;
    }
  }

  private byte[] bytesA;
  private byte[] bytesB;
  private byte[] halfBytesA;
  private byte[] halfBytesB;
  private byte[] halfBytesBPacked;
  private float[] floatsA;
  private float[] floatsB;
  private int expectedhalfByteDotProduct;

  private Object nativeBytesA;
  private Object nativeBytesB;

  /** private Object nativeBytesA; private Object nativeBytesB; */
  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  int size;

  @Setup(Level.Iteration)
  public void init() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    // random byte arrays for binary methods
    bytesA = new byte[size];
    bytesB = new byte[size];
    random.nextBytes(bytesA);
    random.nextBytes(bytesB);
    // random half byte arrays for binary methods
    // this means that all values must be between 0 and 15
    expectedhalfByteDotProduct = 0;
    halfBytesA = new byte[size];
    halfBytesB = new byte[size];
    for (int i = 0; i < size; ++i) {
      halfBytesA[i] = (byte) random.nextInt(16);
      halfBytesB[i] = (byte) random.nextInt(16);
      expectedhalfByteDotProduct += halfBytesA[i] * halfBytesB[i];
    }
    // pack the half byte arrays
    if (size % 2 == 0) {
      halfBytesBPacked = new byte[(size + 1) >> 1];
      compressBytes(halfBytesB, halfBytesBPacked);
    }

    // random float arrays for float methods
    floatsA = new float[size];
    floatsB = new float[size];
    for (int i = 0; i < size; ++i) {
      floatsA[i] = random.nextFloat();
      floatsB[i] = random.nextFloat();
    }
    // Java 21+ specific initialization
    final int runtimeVersion = Runtime.version().feature();
    if (runtimeVersion >= 21) {
      // Reflection based code to eliminate the use of Preview classes in JMH benchmarks
      try {
        final Class<?> vectorUtilSupportClass = VectorUtil.getVectorUtilSupportClass();
        final var className = "org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport";
        if (vectorUtilSupportClass.getName().equals(className) == false) {
          nativeBytesA = null;
          nativeBytesB = null;
        } else {
          MethodHandles.Lookup lookup = MethodHandles.lookup();
          final var MemorySegment = "java.lang.foreign.MemorySegment";
          final var methodType =
              MethodType.methodType(lookup.findClass(MemorySegment), byte[].class);
          MethodHandle nativeMemorySegment =
              lookup.findStatic(vectorUtilSupportClass, "nativeMemorySegment", methodType);
          byte[] a = new byte[size];
          byte[] b = new byte[size];
          for (int i = 0; i < size; ++i) {
            a[i] = (byte) random.nextInt(128);
            b[i] = (byte) random.nextInt(128);
          }
          nativeBytesA = nativeMemorySegment.invoke(a);
          nativeBytesB = nativeMemorySegment.invoke(b);
        }
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
      /*
      Arena offHeap = Arena.ofAuto();
      nativeBytesA = offHeap.allocate(size, ValueLayout.JAVA_BYTE.byteAlignment());
      nativeBytesB = offHeap.allocate(size, ValueLayout.JAVA_BYTE.byteAlignment());
      for (int i = 0; i < size; ++i) {
        nativeBytesA.set(ValueLayout.JAVA_BYTE, i, (byte) random.nextInt(128));
        nativeBytesB.set(ValueLayout.JAVA_BYTE, i, (byte) random.nextInt(128));
      }*/
    }
  }

  /**
   * High overhead (lower score) from using NATIVE_DOT_PRODUCT.invoke(nativeBytesA, nativeBytesB).
   * Both nativeBytesA and nativeBytesB are offHeap MemorySegments created by invoking the method
   * PanamaVectorUtilSupport.nativeMemorySegment(byte[]) which allocated these segments and copies
   * bytes from the supplied byte[] to offHeap memory. The benchmark output below shows
   * significantly more overhead. <b>NOTE:</b> Return type of dots8s() was set to void for the
   * benchmark run to avoid boxing/unboxing overhead.
   *
   * <pre>
   * Benchmark                  (size)   Mode  Cnt   Score   Error   Units
   * VectorUtilBenchmark.dot8s     768  thrpt   15  36.406 ± 0.496  ops/us
   * </pre>
   *
   * Much lower overhead was observed when preview APIs were used directly in JMH benchmarking code
   * and exact method invocation was made as shown below <b>return (int)
   * VectorUtil.NATIVE_DOT_PRODUCT.invokeExact(nativeBytesA, nativeBytesB);</b>
   *
   * <pre>
   * Benchmark                  (size)   Mode  Cnt   Score   Error   Units
   * VectorUtilBenchmark.dot8s     768   thrpt   15   43.662 ± 0.818  ops/us
   * </pre>
   */
  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public void dot8s() {
    try {
      VectorUtil.NATIVE_DOT_PRODUCT.invoke(nativeBytesA, nativeBytesB);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public float binaryCosineScalar() {
    return VectorUtil.cosine(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float binaryCosineVector() {
    return VectorUtil.cosine(bytesA, bytesB);
  }

  @Benchmark
  public int binaryDotProductScalar() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryDotProductVector() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @Benchmark
  public int binarySquareScalar() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binarySquareVector() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  public int binaryHalfByteScalar() {
    return VectorUtil.int4DotProduct(halfBytesA, halfBytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteVector() {
    return VectorUtil.int4DotProduct(halfBytesA, halfBytesB);
  }

  @Benchmark
  public int binaryHalfByteScalarPacked() {
    if (size % 2 != 0) {
      throw new RuntimeException("Size must be even for this benchmark");
    }
    int v = VectorUtil.int4DotProductPacked(halfBytesA, halfBytesBPacked);
    if (v != expectedhalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedhalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteVectorPacked() {
    if (size % 2 != 0) {
      throw new RuntimeException("Size must be even for this benchmark");
    }
    int v = VectorUtil.int4DotProductPacked(halfBytesA, halfBytesBPacked);
    if (v != expectedhalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedhalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public float floatCosineScalar() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 15,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatCosineVector() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  public float floatDotProductScalar() {
    return VectorUtil.dotProduct(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 15,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatDotProductVector() {
    return VectorUtil.dotProduct(floatsA, floatsB);
  }

  @Benchmark
  public float floatSquareScalar() {
    return VectorUtil.squareDistance(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 15,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatSquareVector() {
    return VectorUtil.squareDistance(floatsA, floatsB);
  }
}
