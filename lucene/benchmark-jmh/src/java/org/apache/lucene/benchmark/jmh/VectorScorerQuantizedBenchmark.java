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
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.LegacyQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
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
 * Benchmark to compare the performance of quantized (uint8) vector scoring using the default and
 * optimized (MemorySegment bulk) scorers.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {
      "-Xmx2g",
      "-Xms2g",
      "-XX:+AlwaysPreTouch",
      "--add-modules=jdk.incubator.vector"
    })
public class VectorScorerQuantizedBenchmark {

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
    path = Files.createTempDirectory("VectorScorerQuantizedBenchmark");
    dir = new MMapDirectory(path);

    // Create quantized uint8 vectors with score correction offsets
    // Format per vector: [byte0..byteN-1][float_offset]
    int nodeSize = size + Float.BYTES;
    byte[] nodeBytes = new byte[nodeSize];
    ByteBuffer nodeBuf = ByteBuffer.wrap(nodeBytes).order(ByteOrder.LITTLE_ENDIAN);

    try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
      for (int v = 0; v < numVectors; v++) {
        nodeBuf.clear();
        byte[] vec = randomVectorBytes(size, random);
        nodeBuf.put(vec);
        nodeBuf.putFloat(random.nextFloat() * 0.01f); // small offset
        out.writeBytes(nodeBytes, 0, nodeSize);
      }
    }
  }

  @Setup(Level.Iteration)
  public void perIterationInit() throws IOException {
    var random = ThreadLocalRandom.current();
    scores = new float[numVectorsToScore];
    if (in != null) {
      in.close();
    }
    in = dir.openInput("vector.data", IOContext.DEFAULT);
    int targetOrd = random.nextInt(numVectors);

    // default scorer
    values = quantizedVectorValues(size, numVectors, in);
    var def = new Lucene99ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE);
    defDotScorer = def.getRandomVectorScorerSupplier(DOT_PRODUCT, values.copy()).scorer();
    defCosScorer = def.getRandomVectorScorerSupplier(COSINE, values.copy()).scorer();
    defEucScorer = def.getRandomVectorScorerSupplier(EUCLIDEAN, values.copy()).scorer();
    defMipScorer = def.getRandomVectorScorerSupplier(MAXIMUM_INNER_PRODUCT, values.copy()).scorer();
    defDotScorer.setScoringOrdinal(targetOrd);
    defCosScorer.setScoringOrdinal(targetOrd);
    defEucScorer.setScoringOrdinal(targetOrd);
    defMipScorer.setScoringOrdinal(targetOrd);

    // optimized scorer (MemorySegment path)
    var opt = FlatVectorScorerUtil.getLucene99ScalarQuantizedVectorsScorer();
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
    float[] vec = randomVectorFloats(size, random);
    var opt = FlatVectorScorerUtil.getLucene99ScalarQuantizedVectorsScorer();

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

  // -- cosine

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

  static byte[] randomVectorBytes(int dims, Random random) {
    byte[] ba = new byte[dims];
    random.nextBytes(ba);
    // Make unsigned by clearing the sign bit, so values are 0-127
    // This matches the uint8 quantization range
    for (int i = 0; i < dims; i++) {
      ba[i] = (byte) ((ba[i] & 0xFF) % 128);
    }
    return ba;
  }

  static float[] randomVectorFloats(int dims, Random random) {
    float[] fa = new float[dims];
    for (int i = 0; i < dims; ++i) {
      fa[i] = random.nextFloat();
    }
    return fa;
  }

  static KnnVectorValues quantizedVectorValues(int dims, int size, IndexInput in)
      throws IOException {
    int nodeSize = dims + Float.BYTES;
    return new SimpleLegacyQuantizedByteVectorValues(dims, size, in, nodeSize);
  }

  /**
   * A minimal LegacyQuantizedByteVectorValues for benchmarking. Each "node" is [vector bytes][float
   * offset].
   */
  static final class SimpleLegacyQuantizedByteVectorValues extends LegacyQuantizedByteVectorValues {
    private final int dims;
    private final int size;
    private final IndexInput in;
    private final int nodeSize;
    private final byte[] scratch;
    private int ord = -1;

    SimpleLegacyQuantizedByteVectorValues(int dims, int size, IndexInput in, int nodeSize)
        throws IOException {
      this.dims = dims;
      this.size = size;
      this.in = in.slice("test", 0, in.length());
      this.nodeSize = nodeSize;
      this.scratch = new byte[dims];
    }

    @Override
    public int dimension() {
      return dims;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      this.ord = ord;
      ((RandomAccessInput) in).readBytes((long) ord * nodeSize, scratch, 0, dims);
      return scratch;
    }

    public byte[] vectorValue() throws IOException {
      return vectorValue(ord);
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    public SimpleLegacyQuantizedByteVectorValues copy() throws IOException {
      return new SimpleLegacyQuantizedByteVectorValues(dims, size, in, nodeSize);
    }

    @Override
    public IndexInput getSlice() {
      return in;
    }

    @Override
    public ScalarQuantizer getScalarQuantizer() {
      // Return a simple 7-bit quantizer (bits > 4, so uint8 path is used)
      return new ScalarQuantizer(0f, 1f, (byte) 7);
    }

    @Override
    public float getScoreCorrectionConstant(int ord) throws IOException {
      return Float.intBitsToFloat(((RandomAccessInput) in).readInt((long) ord * nodeSize + dims));
    }
  }
}
