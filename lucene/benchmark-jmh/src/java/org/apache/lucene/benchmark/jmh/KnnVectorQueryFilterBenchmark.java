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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
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
 * Benchmarks end-to-end latency of {@link KnnFloatVectorQuery} with a filter, across different
 * filter types and vector field densities.
 *
 * <p>The interesting comparison is between {@code filterType=range*} with {@code
 * vectorFraction=0.5}: this is the sparse-field + cheap-but-non-matchall-filter case. Before the
 * removal of {@code FieldExistsQuery} from the BooleanQuery in {@code
 * AbstractKnnVectorQuery.rewrite()}, this case paid a full vector-DISI scan on every query to
 * materialise the filter BitSet. After the fix, only the range DISI is scanned.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>{@code numDocs} — total documents in the index.
 *   <li>{@code vectorFraction} — fraction of documents that carry a vector; 1.0 = dense, 0.5 =
 *       sparse.
 *   <li>{@code filterType} — {@code none} (no filter), {@code matchAll} (MatchAllDocsQuery fast
 *       path), {@code range50} (IntPoint range covering ~50 % of docs), {@code range90} (covering
 *       ~90 %).
 *   <li>{@code k} — number of nearest-neighbour results requested.
 *   <li>{@code dim} — vector dimension.
 * </ul>
 *
 * Run with:
 *
 * <pre>
 *   ./gradlew -p lucene/benchmark-jmh assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar KnnVectorQueryFilterBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx4g", "-Xms4g", "-XX:+AlwaysPreTouch"})
public class KnnVectorQueryFilterBenchmark {

  private static final String VEC_FIELD = "vec";
  private static final String ID_FIELD = "id";
  private static final int NUM_QUERY_VECTORS = 256;

  @Param({"50000"})
  public int numDocs;

  /** 1.0 = every doc has a vector; 0.5 = every other doc has a vector (sparse). */
  @Param({"1.0", "0.5"})
  public float vectorFraction;

  /**
   * none = no filter (HNSW baseline); matchAll = MatchAllDocsQuery (takes the filterWeight=null
   * fast path); range50 / range90 = IntPoint range covering that fraction of docs (exercises the
   * filter materialisation path).
   */
  @Param({"none", "matchAll", "range50", "range90"})
  public String filterType;

  @Param({"10", "100"})
  public int k;

  @Param({"128"})
  public int dim;

  private Path tmpDir;
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private float[][] queryVectors;
  private int queryIdx;
  private Query filter;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    tmpDir = Files.createTempDirectory("KnnVectorQueryFilterBenchmark");
    dir = MMapDirectory.open(tmpDir);

    Random rng = new Random(42);
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new IntPoint(ID_FIELD, i));
        if (rng.nextFloat() < vectorFraction) {
          doc.add(
              new KnnFloatVectorField(
                  VEC_FIELD, randomUnitVector(dim, rng), VectorSimilarityFunction.DOT_PRODUCT));
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);

    filter =
        switch (filterType) {
          case "none" -> null;
          case "matchAll" -> new MatchAllDocsQuery();
          case "range50" -> IntPoint.newRangeQuery(ID_FIELD, 0, numDocs / 2 - 1);
          case "range90" -> IntPoint.newRangeQuery(ID_FIELD, 0, numDocs * 9 / 10 - 1);
          default -> throw new IllegalArgumentException("Unknown filterType: " + filterType);
        };

    Random queryRng = new Random(123);
    queryVectors = new float[NUM_QUERY_VECTORS][];
    for (int i = 0; i < NUM_QUERY_VECTORS; i++) {
      queryVectors[i] = randomUnitVector(dim, queryRng);
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
    return searcher.search(new KnnFloatVectorQuery(VEC_FIELD, query, k, filter), k);
  }

  private static float[] randomUnitVector(int dim, Random rng) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) v[i] = rng.nextFloat() * 2 - 1;
    float norm = 0;
    for (float x : v) norm += x * x;
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < v.length; i++) v[i] /= norm;
    return v;
  }
}
