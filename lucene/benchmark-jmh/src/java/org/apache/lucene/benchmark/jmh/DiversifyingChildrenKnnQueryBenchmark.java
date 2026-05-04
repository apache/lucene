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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
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
 * Benchmarks end-to-end latency of {@link DiversifyingChildrenFloatKnnVectorQuery} with sibling
 * expansion enabled across three sibling-correlation scenarios:
 *
 * <ul>
 *   <li><b>best</b> — siblings are nearly identical (small noise around a parent centroid).
 *       Expansion finds the best sibling immediately; HNSW terminates early.
 *   <li><b>standard</b> — siblings have moderate correlation (realistic use case).
 *   <li><b>worst</b> — siblings are fully independent random vectors. Expansion fires but adds no
 *       recall benefit; measures pure overhead.
 * </ul>
 *
 * Run with:
 *
 * <pre>
 *   ./gradlew -p lucene/benchmark-jmh assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar DiversifyingChildrenKnnQueryBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx4g", "-Xms4g", "-XX:+AlwaysPreTouch"})
public class DiversifyingChildrenKnnQueryBenchmark {

  private static final String FIELD = "vec";
  private static final String PARENT_FIELD = "docType";
  private static final String PARENT_VALUE = "_parent";
  private static final int NUM_QUERY_VECTORS = 256;

  /**
   * Sibling correlation scenario:
   * <ul>
   *   <li>{@code best} — siblings nearly identical (noise = 0.05); best case for expansion.
   *   <li>{@code standard} — siblings moderately correlated (noise = 0.3); realistic case.
   *   <li>{@code worst} — siblings fully random; pure overhead, no recall benefit.
   * </ul>
   */
  @Param({"best", "standard", "worst"})
  public String siblingCorrelation;

  @Param({"5000"})
  public int numParents;

  @Param({"4", "8"})
  public int childrenPerParent;

  @Param({"10"})
  public int k;

  @Param({"128"})
  public int dim;

  private Path tmpDir;
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private QueryBitSetProducer parentFilter;
  private float[][] queryVectors;
  private int queryIdx;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    tmpDir = Files.createTempDirectory("DiversifyingChildrenKnnQueryBenchmark");
    dir = MMapDirectory.open(tmpDir);

    float noiseLevel =
        switch (siblingCorrelation) {
          case "best"     -> 0.05f;
          case "standard" -> 0.30f;
          default         -> Float.NaN; // worst: fully random, no centroid
        };

    Random rnd = new Random(42);
    try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int p = 0; p < numParents; p++) {
        float[] centroid = Float.isNaN(noiseLevel) ? null : randomUnitVector(dim, rnd);
        List<Document> block = new ArrayList<>();
        for (int c = 0; c < childrenPerParent; c++) {
          float[] vec = centroid == null
              ? randomUnitVector(dim, rnd)
              : perturbedUnitVector(centroid, noiseLevel, rnd);
          Document child = new Document();
          child.add(new KnnFloatVectorField(FIELD, vec, VectorSimilarityFunction.DOT_PRODUCT));
          block.add(child);
        }
        Document parent = new Document();
        parent.add(new StringField(PARENT_FIELD, PARENT_VALUE, Field.Store.NO));
        block.add(parent);
        w.addDocuments(block);
      }
      w.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    parentFilter = new QueryBitSetProducer(new TermQuery(new Term(PARENT_FIELD, PARENT_VALUE)));

    Random qrnd = new Random(123);
    queryVectors = new float[NUM_QUERY_VECTORS][];
    for (int i = 0; i < NUM_QUERY_VECTORS; i++) {
      queryVectors[i] = randomUnitVector(dim, qrnd);
    }
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    IOUtils.close(reader, dir);
    IOUtils.rm(tmpDir);
  }

  @Benchmark
  public TopDocs search() throws IOException {
    float[] query = queryVectors[queryIdx++ & (NUM_QUERY_VECTORS - 1)];
    Query knnQuery =
        new DiversifyingChildrenFloatKnnVectorQuery(FIELD, query, null, k, parentFilter);
    return searcher.search(knnQuery, k);
  }

  private static float[] randomUnitVector(int dim, Random rnd) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) v[i] = rnd.nextFloat() * 2 - 1;
    return normalise(v);
  }

  /** Returns a unit vector near {@code centroid} with per-dimension noise scaled by noiseLevel. */
  private static float[] perturbedUnitVector(float[] centroid, float noiseLevel, Random rnd) {
    float[] v = new float[centroid.length];
    for (int i = 0; i < centroid.length; i++) {
      v[i] = centroid[i] + noiseLevel * (rnd.nextFloat() * 2 - 1);
    }
    return normalise(v);
  }

  private static float[] normalise(float[] v) {
    float norm = 0;
    for (float x : v) norm += x * x;
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < v.length; i++) v[i] /= norm;
    return v;
  }
}