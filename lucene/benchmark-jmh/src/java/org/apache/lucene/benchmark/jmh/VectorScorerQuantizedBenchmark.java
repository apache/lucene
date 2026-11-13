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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
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

/**
 * One {@link RandomVectorScorer#bulkScore} call over eight memory mapped {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat} records, with the
 * scorer the format uses, in a JVM without the Vector API ({@link #bulkScoreScalar}) and with it
 * ({@link #bulkScoreVector}). Each call scores the next eight nodes of a random permutation of the
 * vectors, so with many vectors every node is a cache miss and with few they are all cache hits.
 * Run it on two checkouts to compare them.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class VectorScorerQuantizedBenchmark {

  /** Nodes per {@link RandomVectorScorer#bulkScore} call. */
  private static final int BATCH = 8;

  private static final int DIMS = 1024;

  @Param({
    "UNSIGNED_BYTE",
    "SEVEN_BIT",
    "PACKED_NIBBLE",
    "SINGLE_BIT_QUERY_NIBBLE",
    "DIBIT_QUERY_NIBBLE"
  })
  String encodingName;

  /** 1K vectors stay in cache; 1M vectors (1 GB at 8 bits) do not. */
  @Param({"1000", "1000000"})
  int numVectors;

  private Path path;
  private Directory dir;
  private IndexInput in;
  private RandomVectorScorer scorer;

  /** A random permutation of the vectors, cut into batches; each call scores the next one. */
  private int[][] batches;

  private int cursor;
  private final float[] scores = new float[BATCH];

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    ScalarEncoding encoding = ScalarEncoding.valueOf(encodingName);
    path = Files.createTempDirectory("VectorScorerQuantizedBenchmark");
    dir = new MMapDirectory(path);
    byte[] packed = new byte[encoding.getDocPackedLength(DIMS)];
    try (IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT)) {
      for (int i = 0; i < numVectors; i++) {
        // random bytes with plausible corrective terms, like the format lays them out
        random.nextBytes(packed);
        if (encoding == ScalarEncoding.SEVEN_BIT) {
          for (int j = 0; j < packed.length; j++) {
            packed[j] &= 0x7F;
          }
        }
        out.writeBytes(packed, packed.length);
        out.writeInt(Float.floatToIntBits(-random.nextFloat()));
        out.writeInt(Float.floatToIntBits(random.nextFloat()));
        out.writeInt(Float.floatToIntBits(random.nextFloat()));
        out.writeInt(random.nextInt(DIMS * 4));
      }
    }
    in = dir.openInput("vectors", IOContext.DEFAULT);
    QuantizedByteVectorValues values =
        new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
            false,
            DIMS,
            numVectors,
            new float[DIMS],
            0f,
            new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT),
            encoding,
            VectorSimilarityFunction.DOT_PRODUCT,
            DefaultFlatVectorScorer.INSTANCE,
            in.slice("vectors", 0, in.length()));

    int[] permutation = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      permutation[i] = i;
    }
    for (int i = numVectors - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int tmp = permutation[i];
      permutation[i] = permutation[j];
      permutation[j] = tmp;
    }
    batches = new int[numVectors / BATCH][];
    for (int b = 0; b < batches.length; b++) {
      batches[b] = ArrayUtil.copyOfSubArray(permutation, b * BATCH, (b + 1) * BATCH);
    }

    float[] query = new float[DIMS];
    for (int i = 0; i < DIMS; i++) {
      query[i] = random.nextFloat() - 0.5f;
    }
    scorer =
        FlatVectorScorerUtil.getLucene104ScalarQuantizedVectorsScorer()
            .getRandomVectorScorer(VectorSimilarityFunction.DOT_PRODUCT, values, query.clone());
    // the scorer must agree with the plain on-heap one, and must be the memory segment one whenever
    // the Vector API is available (it is not in the fork of bulkScoreScalar)
    RandomVectorScorer reference =
        new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)
            .getRandomVectorScorer(
                VectorSimilarityFunction.DOT_PRODUCT, values.copy(), query.clone());
    for (int node : batches[random.nextInt(batches.length)]) {
      if (reference.score(node) != scorer.score(node)) {
        throw new AssertionError("scorers disagree on node " + node);
      }
    }
    boolean vectorApi = ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    if (scorer.getClass().getName().contains("MemorySegment") != vectorApi) {
      throw new AssertionError("unexpected scorer: " + scorer.getClass());
    }
  }

  @TearDown
  public void teardown() throws IOException {
    IOUtils.close(in, dir);
    IOUtils.rm(path);
  }

  @Benchmark
  public float bulkScoreScalar() throws IOException {
    return scorer.bulkScore(nextBatch(), scores, BATCH);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float bulkScoreVector() throws IOException {
    return scorer.bulkScore(nextBatch(), scores, BATCH);
  }

  private int[] nextBatch() {
    if (cursor == batches.length) {
      cursor = 0;
    }
    return batches[cursor++];
  }
}
