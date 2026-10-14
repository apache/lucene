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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the time to insert one node into an HNSW graph that is created for {@code graphCapacity}
 * nodes, as a merge creates it. The setup inserts {@link #BUILT} nodes, and each operation inserts
 * {@link #BATCH} more. A cost that grows with the capacity, not with the graph's actual size, shows
 * as a slower insertion at a larger capacity.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, batchSize = 1)
@Measurement(iterations = 5, batchSize = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {
      "-Xmx8g",
      "-Xms8g",
      "-XX:+AlwaysPreTouch",
      "--add-modules=jdk.incubator.vector"
    })
public class HnswGraphInsertBenchmark {

  private static final int BUILT = 20_000;
  private static final int BATCH = 2_000;
  private static final int RESERVE = BATCH * 8; // warmup + measurement iterations
  private static final int M = 16;
  private static final int BEAM_WIDTH = 100;

  @Param({"1000000", "10000000", "100000000"})
  int graphCapacity;

  @Param({"32"})
  int dim;

  private HnswGraphBuilder builder;
  private int next;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    List<float[]> vectors = new ArrayList<>(BUILT + RESERVE);
    for (int i = 0; i < BUILT + RESERVE; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = random.nextFloat() * 2 - 1;
      }
      vectors.add(VectorUtil.l2normalize(v));
    }
    RandomVectorScorerSupplier supplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.DOT_PRODUCT, FloatVectorValues.fromFloats(vectors, dim));
    builder = HnswGraphBuilder.create(supplier, M, BEAM_WIDTH, 42, graphCapacity);
    for (next = 0; next < BUILT; next++) {
      builder.addGraphNode(next);
    }
  }

  @Benchmark
  @OperationsPerInvocation(BATCH)
  public void insert() throws IOException {
    for (int i = 0; i < BATCH; i++) {
      builder.addGraphNode(next++);
    }
  }
}
