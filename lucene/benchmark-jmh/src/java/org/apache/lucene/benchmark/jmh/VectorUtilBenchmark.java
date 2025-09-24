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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

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
  private byte[] halfBytesAPacked;
  private byte[] halfBytesB;
  private byte[] halfBytesBPacked;
  private float[] floatsA;
  private float[] floatsB;
  private int expectedHalfByteDotProduct;
  private int expectedHalfByteSquareDistance;

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
    expectedHalfByteDotProduct = 0;
    expectedHalfByteSquareDistance = 0;
    halfBytesA = new byte[size];
    halfBytesB = new byte[size];
    for (int i = 0; i < size; ++i) {
      halfBytesA[i] = (byte) random.nextInt(16);
      halfBytesB[i] = (byte) random.nextInt(16);
      expectedHalfByteDotProduct += halfBytesA[i] * halfBytesB[i];

      int diff = halfBytesA[i] - halfBytesB[i];
      expectedHalfByteSquareDistance += diff * diff;
    }
    // pack the half byte arrays
    if (size % 2 == 0) {
      halfBytesAPacked = new byte[(size + 1) >> 1];
      compressBytes(halfBytesA, halfBytesAPacked);

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
  public int binarySquareScalar() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binarySquareVector() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  public int binaryHalfByteSquareScalar() {
    int v = VectorUtil.int4SquareDistance(halfBytesA, halfBytesB);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteSquareVector() {
    int v = VectorUtil.int4SquareDistance(halfBytesA, halfBytesB);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public int binaryHalfByteSquareSinglePackedScalar() {
    int v = VectorUtil.int4SquareDistanceSinglePacked(halfBytesA, halfBytesBPacked);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteSquareSinglePackedVector() {
    int v = VectorUtil.int4SquareDistanceSinglePacked(halfBytesA, halfBytesBPacked);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public int binaryHalfByteSquareBothPackedScalar() {
    int v = VectorUtil.int4SquareDistanceBothPacked(halfBytesAPacked, halfBytesBPacked);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteSquareBothPackedVector() {
    int v = VectorUtil.int4SquareDistanceBothPacked(halfBytesAPacked, halfBytesBPacked);
    if (v != expectedHalfByteSquareDistance) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
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
  public int binaryDotProductUint8Scalar() {
    return VectorUtil.uint8DotProduct(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryDotProductUint8Vector() {
    return VectorUtil.uint8DotProduct(bytesA, bytesB);
  }

  @Benchmark
  public int binaryHalfByteDotProductScalar() {
    int v = VectorUtil.int4DotProduct(halfBytesA, halfBytesB);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteDotProductVector() {
    int v = VectorUtil.int4DotProduct(halfBytesA, halfBytesB);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public int binarySquareUint8Scalar() {
    return VectorUtil.uint8SquareDistance(bytesA, bytesB);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binarySquareUint8Vector() {
    return VectorUtil.uint8SquareDistance(bytesA, bytesB);
  }

  @Benchmark
  public int binaryHalfByteDotProductSinglePackedScalar() {
    int v = VectorUtil.int4DotProductSinglePacked(halfBytesA, halfBytesBPacked);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteDotProductSinglePackedVector() {
    int v = VectorUtil.int4DotProductSinglePacked(halfBytesA, halfBytesBPacked);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public int binaryHalfByteDotProductBothPackedScalar() {
    int v = VectorUtil.int4DotProductBothPacked(halfBytesAPacked, halfBytesBPacked);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryHalfByteDotProductBothPackedVector() {
    int v = VectorUtil.int4DotProductBothPacked(halfBytesAPacked, halfBytesBPacked);
    if (v != expectedHalfByteDotProduct) {
      throw new RuntimeException("Expected " + expectedHalfByteDotProduct + " but got " + v);
    }
    return v;
  }

  @Benchmark
  public float floatCosineScalar() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  public float[] l2Normalize() {
    return VectorUtil.l2normalize(floatsA, false);
  }

  @Benchmark
  @Fork(
      value = 15,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatCosineVector() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 15,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float[] l2NormalizeVector() {
    return VectorUtil.l2normalize(floatsA, false);
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
