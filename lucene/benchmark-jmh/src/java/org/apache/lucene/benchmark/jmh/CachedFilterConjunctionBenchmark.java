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
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
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
 * Benchmarks cached-filter conjunctions on no-score Boolean queries.
 *
 * <p>The filter is warmed into {@link LRUQueryCache} before measurement. Dense filter selectivities
 * are above the cache's FixedBitSet threshold, while very sparse selectivities exercise the
 * RoaringDocIdSet cache path.
 *
 * <p>The {@code filtered_optional} shape exercises the production path where a cached filter is
 * combined with a sparse optional disjunction through {@code
 * BooleanScorerSupplier.filteredOptionalBulkScorer()}, which can route to {@code
 * ConstantScoreBulkScorer}. The {@code filter_only} shape is retained as a control for the older
 * pure-filter conjunction shape. Very sparse filters below the query-cache bitset threshold and
 * very dense optional leads are controls too: they exercise RoaringDocIdSet and
 * DenseConjunctionBulkScorer paths rather than {@code BitSetConjunctionDISI.intoBitSet()}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 1,
    warmups = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+AlwaysPreTouch"})
public class CachedFilterConjunctionBenchmark {

  private static final String FILTER_FIELD = "filter";
  private static final String LEAD_FIELD = "lead";
  private static final String OPTIONAL_FIELD = "optional";
  private static final String OPTIONAL_A = "a";
  private static final String OPTIONAL_B = "b";
  private static final String YES = "yes";
  private static final String BASELINE = "baseline";
  private static final String BULKSCORER = "bulkscorer";
  private static final String FILTER_ONLY = "filter_only";
  private static final String FILTERED_OPTIONAL = "filtered_optional";

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private IndexSearcher cachedSearcher;
  private IndexSearcher uncachedSearcher;
  private LRUQueryCache queryCache;
  private Query filterQuery;
  private Query conjunctionQuery;
  private Query filterOnlyQuery;
  private Query filteredOptionalQuery;
  private LeafReaderContext context;
  private Weight cachedWeight;
  private Weight uncachedWeight;
  private boolean cachedFilterUsesBitSet;
  private int expectedHitCount;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"1000000"})
    public int docCount;

    @Param({"0.0001", "0.001", "0.002", "0.003", "0.005", "0.01", "0.03", "0.10", "0.50", "1.0"})
    public double filterSelectivity;

    @Param({"0.001", "0.03", "0.10"})
    public double leadSelectivity;

    @Param({FILTERED_OPTIONAL, FILTER_ONLY})
    public String queryShape;

    @Param({BASELINE, BULKSCORER})
    public String variant;
  }

  @Setup(Level.Trial)
  public void setup(Params params) throws Exception {
    path = Files.createTempDirectory("cachedFilterConjunctionBench");
    dir = MMapDirectory.open(path);

    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    Random random = new Random(42);
    for (int i = 0; i < params.docCount; ++i) {
      Document doc = new Document();
      if (random.nextDouble() < params.filterSelectivity) {
        doc.add(new StringField(FILTER_FIELD, YES, Store.NO));
      }
      if (random.nextDouble() < params.leadSelectivity) {
        doc.add(new StringField(LEAD_FIELD, YES, Store.NO));
        if (random.nextBoolean()) {
          doc.add(new StringField(OPTIONAL_FIELD, OPTIONAL_A, Store.NO));
        } else {
          doc.add(new StringField(OPTIONAL_FIELD, OPTIONAL_B, Store.NO));
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();

    filterQuery = new TermQuery(new Term(FILTER_FIELD, YES));
    Query leadQuery = new TermQuery(new Term(LEAD_FIELD, YES));
    filterOnlyQuery =
        new BooleanQuery.Builder()
            .add(leadQuery, Occur.FILTER)
            .add(filterQuery, Occur.FILTER)
            .build();
    filteredOptionalQuery =
        new BooleanQuery.Builder()
            .add(filterQuery, Occur.FILTER)
            .add(new TermQuery(new Term(OPTIONAL_FIELD, OPTIONAL_A)), Occur.SHOULD)
            .add(new TermQuery(new Term(OPTIONAL_FIELD, OPTIONAL_B)), Occur.SHOULD)
            .setMinimumNumberShouldMatch(1)
            .build();
    conjunctionQuery =
        switch (params.queryShape) {
          case FILTER_ONLY -> filterOnlyQuery;
          case FILTERED_OPTIONAL -> filteredOptionalQuery;
          default -> throw new AssertionError("Unknown query shape: " + params.queryShape);
        };

    queryCache = new LRUQueryCache(256, 32L * 1024 * 1024, context -> context.reader() != null, 1f);
    cachedSearcher = new IndexSearcher(reader);
    cachedSearcher.setQueryCache(queryCache);
    cachedSearcher.setQueryCachingPolicy(cacheOnly(filterQuery));

    uncachedSearcher = new IndexSearcher(reader);
    uncachedSearcher.setQueryCache(null);

    context = reader.leaves().get(0);

    // Warm only the filter query so the benchmark measures reuse of a cached FixedBitSet filter,
    // not first-use cache population or a fully cached top-level conjunction.
    Weight filterWeight =
        cachedSearcher.createWeight(
            cachedSearcher.rewrite(filterQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
    ScorerSupplier filterScorerSupplier = filterWeight.scorerSupplier(context);
    if (filterScorerSupplier != null) {
      cachedFilterUsesBitSet = filterScorerSupplier.cost() * 100 >= context.reader().maxDoc();
      filterScorerSupplier.get(Long.MAX_VALUE).iterator().nextDoc();
    }
    if (queryCache.getCacheSize() == 0) {
      throw new AssertionError("filter query was not cached");
    }

    cachedWeight =
        cachedSearcher.createWeight(
            cachedSearcher.rewrite(conjunctionQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
    uncachedWeight =
        uncachedSearcher.createWeight(
            uncachedSearcher.rewrite(conjunctionQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
    expectedHitCount = uncachedSearcher.count(conjunctionQuery);
    checkBulkScorerRoute(params, cachedWeight);
    checkHitCount(scoreVariant(cachedWeight, params.variant));
    checkHitCount(scoreVariant(uncachedWeight, params.variant));
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

  @Benchmark
  public int countCachedFilterConjunction(Params params) throws IOException {
    return scoreVariant(cachedWeight, params.variant);
  }

  @Benchmark
  public int countUncachedFilterConjunction(Params params) throws IOException {
    return scoreVariant(uncachedWeight, params.variant);
  }

  private int scoreVariant(Weight weight, String variant) throws IOException {
    switch (variant) {
      case BASELINE:
        Scorer scorer = scorer(weight);
        if (scorer == null) {
          return 0;
        }
        return scorePerDoc(scorer);
      case BULKSCORER:
        return scoreBulkScorer(weight);
      default:
        throw new AssertionError("Unknown variant: " + variant);
    }
  }

  private Scorer scorer(Weight weight) throws IOException {
    ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
    if (scorerSupplier == null) {
      return null;
    }
    return scorerSupplier.get(Long.MAX_VALUE);
  }

  private int scorePerDoc(Scorer scorer) throws IOException {
    DocIdSetIterator iterator = scorer.iterator();
    int maxDoc = context.reader().maxDoc();
    return scorePerDoc(iterator, maxDoc);
  }

  private int scorePerDoc(DocIdSetIterator iterator, int maxDoc) throws IOException {
    CountingLeafCollector collector = new CountingLeafCollector();
    for (int doc = iterator.nextDoc(); doc < maxDoc; doc = iterator.nextDoc()) {
      collector.collect(doc);
    }
    return collector.count;
  }

  private int scoreBulkScorer(Weight weight) throws IOException {
    BulkScorer bulkScorer = weight.bulkScorer(context);
    if (bulkScorer == null) {
      return 0;
    }
    CountingLeafCollector collector = new CountingLeafCollector();
    bulkScorer.score(collector, context.reader().getLiveDocs(), 0, context.reader().maxDoc());
    return collector.count;
  }

  private void checkBulkScorerRoute(Params params, Weight weight) throws IOException {
    ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
    if (scorerSupplier == null) {
      return;
    }
    BulkScorer bulkScorer = scorerSupplier.bulkScorer();
    String bulkScorerClassName = bulkScorer.getClass().getName();
    boolean isConstantScoreBulkScorer = bulkScorerClassName.endsWith(".ConstantScoreBulkScorer");
    boolean isDenseConjunctionBulkScorer =
        bulkScorerClassName.endsWith(".DenseConjunctionBulkScorer");
    boolean isDefaultBulkScorer = bulkScorerClassName.endsWith("$DefaultBulkScorer");
    if (FILTERED_OPTIONAL.equals(params.queryShape)
        && isConstantScoreBulkScorer == false
        && isDenseConjunctionBulkScorer == false
        && isDefaultBulkScorer == false) {
      throw new AssertionError(
          "filtered_optional should route through ConstantScoreBulkScorer, "
              + "DenseConjunctionBulkScorer, or DefaultBulkScorer but got "
              + bulkScorerClassName);
    }
    if (FILTERED_OPTIONAL.equals(params.queryShape)
        && isConstantScoreBulkScorer
        && cachedFilterUsesBitSet
        && params.filterSelectivity >= 0.02) {
      Scorer scorer = scorerSupplier.get(Long.MAX_VALUE);
      String iteratorClassName = scorer.iterator().getClass().getName();
      if (iteratorClassName.endsWith("$BitSetConjunctionDISI") == false) {
        throw new AssertionError(
            "cached filter should produce BitSetConjunctionDISI but got " + iteratorClassName);
      }
    }
    if (FILTER_ONLY.equals(params.queryShape) && isConstantScoreBulkScorer) {
      throw new AssertionError("filter_only unexpectedly routed through ConstantScoreBulkScorer");
    }
  }

  private void checkHitCount(int hitCount) {
    if (hitCount != expectedHitCount) {
      throw new AssertionError("hitCount=" + hitCount + " expected=" + expectedHitCount);
    }
  }

  private static class CountingLeafCollector implements LeafCollector {
    int count;

    @Override
    public void setScorer(Scorable scorer) {}

    @Override
    public void collect(int doc) {
      ++count;
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      // Match TotalHitCountCollector's batch path.
      for (int streamCount = stream.count(); streamCount != 0; streamCount = stream.count()) {
        count += streamCount;
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (queryCache != null) {
      queryCache.close();
    }
    if (reader != null) {
      reader.close();
    }
    if (dir != null) {
      dir.close();
    }
    if (path != null && Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.delete(p);
                  } catch (IOException _) {
                  }
                });
      }
    }
  }
}
