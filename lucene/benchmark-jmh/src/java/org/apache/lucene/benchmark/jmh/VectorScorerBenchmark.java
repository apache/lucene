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
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
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
public class VectorScorerBenchmark {

  @Param({"1", "128", "207", "256", "300", "512", "702", "1024"})
  int size;

  Directory dir;
  IndexInput in;
  KnnVectorValues vectorValues;
  byte[] vec1, vec2;
  RandomVectorScorer scorer;

  @Setup(Level.Iteration)
  public void init() throws IOException {
    vec1 = new byte[size];
    vec2 = new byte[size];
    ThreadLocalRandom.current().nextBytes(vec1);
    ThreadLocalRandom.current().nextBytes(vec2);

    dir = new MMapDirectory(Files.createTempDirectory("VectorScorerBenchmark"));
    try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
      out.writeBytes(vec1, 0, vec1.length);
      out.writeBytes(vec2, 0, vec2.length);
    }
    in = dir.openInput("vector.data", IOContext.DEFAULT);
    vectorValues = vectorValues(size, 2, in, DOT_PRODUCT);
    scorer =
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
            .getRandomVectorScorerSupplier(DOT_PRODUCT, vectorValues)
            .scorer(0);
  }

  @TearDown
  public void teardown() throws IOException {
    IOUtils.close(dir, in);
  }

  @Benchmark
  public float binaryDotProductDefault() throws IOException {
    return scorer.score(1);
  }

  @Benchmark
  @Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
  public float binaryDotProductMemSeg() throws IOException {
    return scorer.score(1);
  }

  static KnnVectorValues vectorValues(
      int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
    return new OffHeapByteVectorValues.DenseOffHeapVectorValues(
        dims, size, in.slice("test", 0, in.length()), dims, new ThrowingFlatVectorScorer(), sim);
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
