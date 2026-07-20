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

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
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
import org.openjdk.jmh.annotations.TearDown;
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
public class VectorScorerBenchmark {
  private static final float EPSILON = 1e-4f;

  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  public int size;

  @Param({"0", "1", "4", "64"})
  public int padBytes; // capture performance impact of byte alignment in the index

  Directory dir;
  IndexInput bytesIn;
  IndexInput floatsIn;
  KnnVectorValues byteVectorValues;
  KnnVectorValues floatVectorValues;
  byte[] vec1, vec2;
  float[] floatsA, floatsB;
  float expectedBytes, expectedFloats;
  UpdateableRandomVectorScorer byteScorer;
  UpdateableRandomVectorScorer floatScorer;

  @Setup(Level.Iteration)
  public void init() throws IOException {
    Random random = ThreadLocalRandom.current();

    vec1 = new byte[size];
    vec2 = new byte[size];
    random.nextBytes(vec1);
    random.nextBytes(vec2);
    expectedBytes = DOT_PRODUCT.compare(vec1, vec2);

    // random float arrays for float methods
    floatsA = new float[size];
    floatsB = new float[size];
    for (int i = 0; i < size; ++i) {
      floatsA[i] = random.nextFloat();
      floatsB[i] = random.nextFloat();
    }
    expectedFloats = DOT_PRODUCT.compare(floatsA, floatsB);

    dir = new MMapDirectory(Files.createTempDirectory("VectorScorerBenchmark"));
    try (IndexOutput out = dir.createOutput("byteVector.data", IOContext.DEFAULT)) {
      out.writeBytes(new byte[padBytes], 0, padBytes);

      out.writeBytes(vec1, 0, vec1.length);
      out.writeBytes(vec2, 0, vec2.length);
    }
    try (IndexOutput out = dir.createOutput("floatVector.data", IOContext.DEFAULT)) {
      out.writeBytes(new byte[padBytes], 0, padBytes);

      byte[] buffer = new byte[size * Float.BYTES];
      ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floatsA);
      out.writeBytes(buffer, 0, buffer.length);
      ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floatsB);
      out.writeBytes(buffer, 0, buffer.length);
    }

    bytesIn = dir.openInput("byteVector.data", IOContext.DEFAULT);
    byteVectorValues = byteVectorValues(DOT_PRODUCT);
    byteScorer =
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
            .getRandomVectorScorerSupplier(DOT_PRODUCT, byteVectorValues)
            .scorer();
    byteScorer.setScoringOrdinal(0);

    floatsIn = dir.openInput("floatVector.data", IOContext.DEFAULT);
    floatVectorValues = floatVectorValues(DOT_PRODUCT);
    floatScorer =
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
            .getRandomVectorScorerSupplier(DOT_PRODUCT, floatVectorValues)
            .scorer();
    floatScorer.setScoringOrdinal(0);
  }

  @TearDown
  public void teardown() throws IOException {
    IOUtils.close(dir, bytesIn);
  }

  @Benchmark
  public float binaryDotProductDefault() throws IOException {
    float result = byteScorer.score(1);
    if (Math.abs(result - expectedBytes) > EPSILON) {
      throw new RuntimeException("Expected " + result + " but got " + expectedBytes);
    }
    return result;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float binaryDotProductMemSeg() throws IOException {
    float result = byteScorer.score(1);
    if (Math.abs(result - expectedBytes) > EPSILON) {
      throw new RuntimeException("Expected " + result + " but got " + expectedBytes);
    }
    return result;
  }

  @Benchmark
  public float floatDotProductDefault() throws IOException {
    float result = floatScorer.score(1);
    if (Math.abs(result - expectedFloats) > EPSILON) {
      throw new RuntimeException("Expected " + result + " but got " + expectedFloats);
    }
    return result;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float floatDotProductMemSeg() throws IOException {
    float result = floatScorer.score(1);
    if (Math.abs(result - expectedFloats) > EPSILON) {
      throw new RuntimeException("Expected " + result + " but got " + expectedFloats);
    }
    return result;
  }

  KnnVectorValues byteVectorValues(VectorSimilarityFunction sim) throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        size,
        2,
        bytesIn.slice("test", padBytes, size * 2L),
        size,
        new ThrowingFlatVectorScorer(),
        sim);
  }

  KnnVectorValues floatVectorValues(VectorSimilarityFunction sim) throws IOException {
    int byteSize = size * Float.BYTES;
    return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
        size,
        2,
        floatsIn.slice("test", padBytes, byteSize * 2L),
        byteSize,
        new ThrowingFlatVectorScorer(),
        sim);
  }

  static final class ThrowingFlatVectorScorer implements FlatVectorsScorer {

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target) {
      throw new UnsupportedOperationException();
    }
  }
}
