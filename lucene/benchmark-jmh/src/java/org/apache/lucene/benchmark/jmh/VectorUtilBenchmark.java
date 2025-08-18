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
  private byte[] halfBytesB;
  private byte[] halfBytesBPacked;
  private float[] floatsA;
  private float[] floatsB;
  private int expectedhalfByteDotProduct;

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
