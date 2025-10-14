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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules=jdk.incubator.vector"
    })
/*
 * Benchmark to compare the performance of float32 vector scoring using the default and optimized
 * scorers. While there are benchmark methods for each of the similarities, it is often most useful
 * to compare equivalent subsets, e.g. .*dot.*
 */
public class VectorScorerFloat32Benchmark {

  @Param({"1024"})
  public int size;

  @Param({"true", "false"})
  public boolean pollute = false;

  public int numVectors = 128_000;
  public int numVectorsToScore = 20_000;

  float[] scores;
  int[] indices;
  Path path;
  Directory dir;
  IndexInput in;
  KnnVectorValues values;
  UpdateableRandomVectorScorer defDotScorer, defCosScorer, defEucScorer, defMipScorer;
  UpdateableRandomVectorScorer optDotScorer, optCosScorer, optEucScorer, optMipScorer;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    var random = ThreadLocalRandom.current();
    path = Files.createTempDirectory("VectorScorerFloat32Benchmark");
    dir = new MMapDirectory(path);
    try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
      var ba = new byte[size * Float.BYTES];
      var buf = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
      for (int v = 0; v < numVectors; v++) {
        buf.put(0, randomVector(size, random));
        out.writeBytes(ba, 0, ba.length);
      }
    }
  }

  @Setup(Level.Iteration)
  public void perIterationInit() throws IOException {
    var random = ThreadLocalRandom.current();
    scores = new float[numVectorsToScore];
    in = dir.openInput("vector.data", IOContext.DEFAULT);
    int targetOrd = random.nextInt(numVectors);

    // default scorer
    values = vectorValues(size, numVectors, in, DOT_PRODUCT);
    var def = DefaultFlatVectorScorer.INSTANCE;
    defDotScorer = def.getRandomVectorScorerSupplier(DOT_PRODUCT, values.copy()).scorer();
    defCosScorer = def.getRandomVectorScorerSupplier(COSINE, values.copy()).scorer();
    defEucScorer = def.getRandomVectorScorerSupplier(EUCLIDEAN, values.copy()).scorer();
    defMipScorer = def.getRandomVectorScorerSupplier(MAXIMUM_INNER_PRODUCT, values.copy()).scorer();
    defDotScorer.setScoringOrdinal(targetOrd);
    defCosScorer.setScoringOrdinal(targetOrd);
    defEucScorer.setScoringOrdinal(targetOrd);
    defMipScorer.setScoringOrdinal(targetOrd);

    // optimized scorer
    var opt = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();
    optDotScorer = opt.getRandomVectorScorerSupplier(DOT_PRODUCT, values.copy()).scorer();
    optCosScorer = opt.getRandomVectorScorerSupplier(COSINE, values.copy()).scorer();
    optEucScorer = opt.getRandomVectorScorerSupplier(EUCLIDEAN, values.copy()).scorer();
    optMipScorer = opt.getRandomVectorScorerSupplier(MAXIMUM_INNER_PRODUCT, values.copy()).scorer();
    optDotScorer.setScoringOrdinal(targetOrd);
    optCosScorer.setScoringOrdinal(targetOrd);
    optEucScorer.setScoringOrdinal(targetOrd);
    optMipScorer.setScoringOrdinal(targetOrd);

    List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
    Collections.shuffle(list, random);
    indices = list.stream().limit(numVectorsToScore).mapToInt(i -> i).toArray();

    if (pollute) {
      pollute(random);
    }
  }

  @TearDown
  public void teardown() throws IOException {
    IOUtils.close(in);
    dir.deleteFile("vector.data");
    IOUtils.close(dir);
    Files.delete(path);
  }

  public void pollute(Random random) throws IOException {
    // exercise various similarities to ensure they don't have negative effects, e.g.,
    // type pollution on virtual calls, etc.
    float[] vec = randomVector(size, random);
    var opt = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

    for (int i = 0; i < 2; i++) {
      dotProductOptScorer();
      dotProductOptBulkScore();
      cosineOptScorer();
      cosineDefaultBulk();
      euclideanOptScorer();
      euclideanOptBulkScore();
      mipOptScorer();
      mipOptBulkScore();
      for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
        var scorer = opt.getRandomVectorScorer(sim, values.copy(), vec);
        for (int v = 0; v < numVectorsToScore; v++) {
          scores[v] = scorer.score(indices[v]);
        }
      }
    }
  }

  // -- dot product

  @Benchmark
  public float[] dotProductDefault() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = defDotScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] dotProductDefaultBulk() throws IOException {
    defDotScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  @Benchmark
  public float[] dotProductOptScorer() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = optDotScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] dotProductOptBulkScore() throws IOException {
    optDotScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  // -- euclidean

  @Benchmark
  public float[] euclideanDefault() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = defEucScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] euclideanDefaultBulk() throws IOException {
    defEucScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  @Benchmark
  public float[] euclideanOptScorer() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = optEucScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] euclideanOptBulkScore() throws IOException {
    optEucScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  // -- euclidean

  @Benchmark
  public float[] cosineDefault() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = defCosScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] cosineDefaultBulk() throws IOException {
    defCosScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  @Benchmark
  public float[] cosineOptScorer() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = optCosScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] cosineOptBulkScore() throws IOException {
    optCosScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  // -- max inner product

  @Benchmark
  public float[] mipDefault() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = defMipScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] mipDefaultBulk() throws IOException {
    defMipScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  @Benchmark
  public float[] mipOptScorer() throws IOException {
    for (int v = 0; v < numVectorsToScore; v++) {
      scores[v] = optMipScorer.score(indices[v]);
    }
    return scores;
  }

  @Benchmark
  public float[] mipOptBulkScore() throws IOException {
    optMipScorer.bulkScore(indices, scores, indices.length);
    return scores;
  }

  static float[] randomVector(int dims, Random random) {
    float[] fa = new float[dims];
    for (int i = 0; i < dims; ++i) {
      fa[i] = random.nextFloat();
    }
    return fa;
  }

  static KnnVectorValues vectorValues(
      int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
    int byteSize = dims * Float.BYTES;
    return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
        dims,
        size,
        in.slice("test", 0, in.length()),
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
