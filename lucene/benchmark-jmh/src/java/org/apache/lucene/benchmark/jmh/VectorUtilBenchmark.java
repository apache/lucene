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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class VectorUtilBenchmark {

  private byte[] bytesA;
  private byte[] bytesB;
  private float[] floatsA;
  private float[] floatsB;

  private Directory dir;
  private RandomAccessVectorValues<float[]> floatValuesA;
  private RandomAccessVectorValues<float[]> floatValuesB;

  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  int size;

  @Setup(Level.Trial)
  public void init() throws IOException {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    // random byte arrays for binary methods
    bytesA = new byte[size];
    bytesB = new byte[size];
    random.nextBytes(bytesA);
    random.nextBytes(bytesB);

    // random float arrays for float methods
    floatsA = new float[size];
    floatsB = new float[size];
    for (int i = 0; i < size; ++i) {
      floatsA[i] = random.nextFloat();
      floatsB[i] = random.nextFloat();
    }

    dir = new MMapDirectory(Files.createTempDirectory("benchmark-floats"));
    var aIndex = inputForFloats(floatsA, 0, 1, dir, "vector-a");
    var bIndex = inputForFloats(floatsB, 0, 3, dir, "vector-b");
    floatValuesA =
        newDenseOffHeapFloatVectorValues(floatsA.length, 1, aIndex, floatsA.length * Float.BYTES);
    floatValuesB =
        newDenseOffHeapFloatVectorValues(floatsB.length, 1, bIndex, floatsB.length * Float.BYTES);
  }

  @Benchmark
  @Fork(value = 1)
  public float binaryCosineScalar() {
    return VectorUtil.cosine(bytesA, bytesB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float binaryCosineVector() {
    return VectorUtil.cosine(bytesA, bytesB);
  }

  @Benchmark
  @Fork(value = 1)
  public int binaryDotProductScalar() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binaryDotProductVector() {
    return VectorUtil.dotProduct(bytesA, bytesB);
  }

  @Benchmark
  @Fork(value = 1)
  public int binarySquareScalar() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public int binarySquareVector() {
    return VectorUtil.squareDistance(bytesA, bytesB);
  }

  @Benchmark
  @Fork(value = 1)
  public float floatCosineScalar() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatCosineVector() {
    return VectorUtil.cosine(floatsA, floatsB);
  }

  @Benchmark
  @Fork(value = 1)
  public float floatDotProductScalar() {
    return VectorUtil.dotProduct(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatDotProductVector() throws IOException {
    // REVERT/REMOVE - this change is required to ensure fair comparison with MS1 and MS2
    return VectorUtil.dotProduct(floatValuesA.vectorValue(0), floatValuesB.vectorValue(0));
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatDotProductVectorMS1() throws IOException {
    return VectorUtil.dotProduct(floatValuesA.vectorValue(0), floatValuesB, 0);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatDotProductVectorMS2() throws IOException {
    return VectorUtil.dotProduct(floatValuesA, 0, floatValuesB, 0);
  }

  @Benchmark
  @Fork(value = 1)
  public float floatSquareScalar() {
    return VectorUtil.squareDistance(floatsA, floatsB);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatSquareVector() {
    return VectorUtil.squareDistance(floatsA, floatsB);
  }

  // ---

  public static RandomAccessVectorValues<float[]> newDenseOffHeapFloatVectorValues(
      int dimension, int size, IndexInput slice, int byteSize) {
    return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(dimension, size, slice, byteSize);
  }

  static IndexInput inputForFloats(
      float[] vector, int vectorPosition, int initialBytes, Directory dir, String name)
      throws IOException {
    int vectorLenBytes = vector.length * Float.BYTES;
    try (var out = dir.createOutput(name + ".data", IOContext.DEFAULT)) {
      if (initialBytes != 0) {
        out.writeBytes(new byte[initialBytes], initialBytes);
      }
      if (vectorPosition != 0) {
        out.writeBytes(new byte[vectorPosition * vectorLenBytes], vectorPosition * vectorLenBytes);
      }
      writeFloat32(vector, out);
    }
    var in = dir.openInput(name + ".data", IOContext.DEFAULT);
    return in.slice(name, initialBytes, (long) (vectorPosition + 1) * vectorLenBytes);
  }

  static void writeFloat32(float[] arr, IndexOutput out) throws IOException {
    int lenBytes = arr.length * Float.BYTES;
    final ByteBuffer buffer = ByteBuffer.allocate(lenBytes).order(ByteOrder.LITTLE_ENDIAN);
    buffer.asFloatBuffer().put(arr);
    out.writeBytes(buffer.array(), lenBytes);
  }
}
