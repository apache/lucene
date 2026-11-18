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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
 * Scores a random subset of memory mapped {@link
 * org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat} records with the scorer
 * the format uses, one {@link RandomVectorScorer#score} call per vector or one {@link
 * RandomVectorScorer#bulkScore} call for all of them, in a JVM without the Vector API and with it.
 * With many vectors every vector is a cache miss and with few they are all cache hits. Run it on
 * two checkouts to compare them.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class VectorScorerQuantizedBenchmark {
  @Param({"1024"})
  int dims;

  @Param ScalarEncoding encoding;

  int numVectors = 1_000_000;
  int numVectorsToScore = 20_000;

  private Path path;
  private Directory dir;
  private IndexInput in;
  private QuantizedByteVectorValues values;
  private float[] query;
  private RandomVectorScorer scorer;
  private int[] indices;
  private float[] scores;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    path = Files.createTempDirectory("VectorScorerQuantizedBenchmark");
    dir = new MMapDirectory(path);
    byte[] packed = new byte[encoding.getDocPackedLength(dims)];
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
        out.writeInt(random.nextInt(dims * 4));
      }
    }
    in = dir.openInput("vectors", IOContext.DEFAULT);
    values =
        new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
            false,
            dims,
            numVectors,
            new float[dims],
            0f,
            new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT),
            encoding,
            VectorSimilarityFunction.DOT_PRODUCT,
            DefaultFlatVectorScorer.INSTANCE,
            in.slice("vectors", 0, in.length()));

    query = new float[dims];
    for (int i = 0; i < dims; i++) {
      query[i] = random.nextFloat() - 0.5f;
    }
    scorer =
        FlatVectorScorerUtil.getLucene104ScalarQuantizedVectorsScorer()
            .getRandomVectorScorer(VectorSimilarityFunction.DOT_PRODUCT, values, query.clone());
    // the scorer must be the memory segment one whenever the Vector API is available (it is not in
    // the forks of the *Scalar benchmarks)
    boolean vectorApi = ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    if (scorer.getClass().getName().contains("MemorySegment") != vectorApi) {
      throw new AssertionError("unexpected scorer: " + scorer.getClass());
    }
  }

  @Setup(Level.Iteration)
  public void perIterationInit() {
    List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
    Collections.shuffle(list, ThreadLocalRandom.current());
    indices = list.stream().limit(numVectorsToScore).mapToInt(i -> i).toArray();
    scores = new float[indices.length];
  }

  /** Checks the scorer against the plain on-heap one on the last subset before releasing them. */
  @TearDown
  public void tearDown() throws IOException {
    RandomVectorScorer reference =
        new Lucene104ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)
            .getRandomVectorScorer(
                VectorSimilarityFunction.DOT_PRODUCT, values.copy(), query.clone());
    scorer.bulkScore(indices, scores, indices.length);
    for (int i = 0; i < indices.length; i++) {
      float expected = reference.score(indices[i]);
      if (expected != scores[i] || expected != scorer.score(indices[i])) {
        throw new AssertionError("scorers disagree on node " + indices[i]);
      }
    }
    IOUtils.close(in, dir);
    IOUtils.rm(path);
  }

  @Benchmark
  public float[] scoreScalar() throws IOException {
    for (int i = 0; i < indices.length; i++) {
      scores[i] = scorer.score(indices[i]);
    }
    return scores;
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float[] scoreVector() throws IOException {
    for (int i = 0; i < indices.length; i++) {
      scores[i] = scorer.score(indices[i]);
    }
    return scores;
  }

  @Benchmark
  public float bulkScoreScalar() throws IOException {
    return scorer.bulkScore(indices, scores, indices.length);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float bulkScoreVector() throws IOException {
    return scorer.bulkScore(indices, scores, indices.length);
  }
}
