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
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
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
 * Benchmarks end-to-end latency of {@link KnnFloatVectorQuery} whose pre-filter is a cached term
 * query, over a single segment.
 *
 * <p>The filter is warmed into {@link LRUQueryCache} before measurement, so every measured query
 * reuses the cached bit set, which is how a hot pre-filter reaches the kNN query in practice. The
 * corpus shape decides whether {@link org.apache.lucene.search.AcceptDocs} can load that filter in
 * bulk.
 *
 * <p>Parameters explore:
 *
 * <ul>
 *   <li>{@code numDocs} — total number of documents in the segment.
 *   <li>{@code dim} — vector dimension.
 *   <li>{@code k} — number of results requested.
 *   <li>{@code filterSelectivity} — fraction of docs matched by the pre-filter.
 *   <li>{@code vectorlessFraction} — fraction of docs indexed without a vector.
 *   <li>{@code deletedFraction} — fraction of docs deleted after indexing, which keeps the
 *       segment's liveDocs non-null.
 * </ul>
 *
 * Run with:
 *
 * <pre>
 *   ./gradlew -p lucene/benchmark-jmh assemble
 *   java -jar lucene/benchmark-jmh/build/benchmarks/lucene-benchmark-jmh-*.jar FilteredKnnVectorQueryBenchmark
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
public class FilteredKnnVectorQueryBenchmark {

  private static final String FIELD = "vec";
  private static final String FILTER_FIELD = "tag";
  private static final String ID_FIELD = "id";
  private static final String YES = "yes";
  private static final int NUM_QUERY_VECTORS = 256;

  @Param({"200000"})
  public int numDocs;

  @Param({"128"})
  public int dim;

  @Param({"100"})
  public int k;

  @Param({"0.95", "0.05"})
  public double filterSelectivity;

  @Param({"0", "0.1"})
  public double vectorlessFraction;

  @Param({"0", "0.05"})
  public double deletedFraction;

  private Path tmpDir;
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private LRUQueryCache queryCache;
  private Query filter;
  private float[][] queryVectors;
  private int queryIdx;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    tmpDir = Files.createTempDirectory("FilteredKnnVectorQueryBenchmark");
    dir = MMapDirectory.open(tmpDir);

    Random randomForDoc = new Random(42);
    try (IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, Integer.toString(i), Field.Store.NO));
        if (randomForDoc.nextDouble() < filterSelectivity) {
          doc.add(new StringField(FILTER_FIELD, YES, Field.Store.NO));
        }
        if (randomForDoc.nextDouble() >= vectorlessFraction) {
          doc.add(
              new KnnFloatVectorField(
                  FIELD,
                  randomUnitVector(dim, randomForDoc),
                  VectorSimilarityFunction.DOT_PRODUCT));
        }
        indexWriter.addDocument(doc);
      }
      indexWriter.forceMerge(1);
      if (deletedFraction > 0) {
        for (int i = 0; i < numDocs; i++) {
          if (randomForDoc.nextDouble() < deletedFraction) {
            indexWriter.deleteDocuments(new Term(ID_FIELD, Integer.toString(i)));
          }
        }
        indexWriter.commit();
      }
    }

    reader = DirectoryReader.open(dir);
    if (reader.leaves().size() != 1) {
      throw new AssertionError("expected one segment, got " + reader.leaves().size());
    }
    filter = new TermQuery(new Term(FILTER_FIELD, YES));
    queryCache = new LRUQueryCache(256, 64L * 1024 * 1024, context -> context.reader() != null, 1f);
    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(cacheOnly(filter));

    // Pull a scorer through the cache once: IndexSearcher#count would answer a term query from
    // index statistics without ever creating the scorer that populates the cache.
    Weight filterWeight =
        searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
    ScorerSupplier scorerSupplier = filterWeight.scorerSupplier(reader.leaves().get(0));
    if (scorerSupplier == null) {
      throw new AssertionError("filter matches nothing");
    }
    scorerSupplier.get(Long.MAX_VALUE).iterator().nextDoc();
    if (queryCache.getCacheSize() == 0) {
      throw new AssertionError("filter query was not cached");
    }

    Random randomForQuery = new Random(123);
    queryVectors = new float[NUM_QUERY_VECTORS][];
    for (int i = 0; i < NUM_QUERY_VECTORS; i++) {
      queryVectors[i] = randomUnitVector(dim, randomForQuery);
    }
  }

  @TearDown(Level.Trial)
  public void teardown() throws IOException {
    IOUtils.close(queryCache, reader, dir);
    IOUtils.rm(tmpDir);
  }

  @Benchmark
  public TopDocs search() throws IOException {
    float[] query = queryVectors[queryIdx++ & (NUM_QUERY_VECTORS - 1)];
    return searcher.search(new KnnFloatVectorQuery(FIELD, query, k, filter), k);
  }

  private static QueryCachingPolicy cacheOnly(Query queryToCache) {
    return new QueryCachingPolicy() {
      @Override
      public void onUse(Query query) {}

      @Override
      public boolean shouldCache(Query query) {
        return queryToCache.equals(query);
      }
    };
  }

  private static float[] randomUnitVector(int dim, Random rnd) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) v[i] = rnd.nextFloat() * 2 - 1;
    return l2Normalization(v);
  }

  private static float[] l2Normalization(float[] v) {
    float norm = 0;
    for (float x : v) norm += x * x;
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < v.length; i++) v[i] /= norm;
    return v;
  }
}
