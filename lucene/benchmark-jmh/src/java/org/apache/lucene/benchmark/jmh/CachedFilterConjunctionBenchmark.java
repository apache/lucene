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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOIntConsumer;
import org.apache.lucene.util.MathUtil;
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
 * Benchmarks DefaultBulkScorer over a sparse FILTER clause conjoined with a dense cached FILTER
 * clause.
 *
 * <p>The filter is warmed into {@link LRUQueryCache} before measurement. Dense filter selectivities
 * are above the cache's FixedBitSet threshold, while very sparse selectivities exercise the
 * RoaringDocIdSet cache path. The lead selectivity is kept sparse so dense cached filters become
 * the random-access side of a BitSetConjunctionDISI when the BooleanQuery scorer is driven through
 * DefaultBulkScorer.
 *
 * <p>This benchmark deliberately obtains the BooleanQuery {@link Scorer} and wraps it with the
 * default scorer iteration shape. Normal {@link IndexSearcher} execution can pick more specialized
 * conjunction bulk scorers for this query shape.
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
  private static final String YES = "yes";
  private static final String BASELINE = "baseline";
  private static final String UNGATED = "ungated";
  private static final String GATED = "gated";
  private static final int WINDOW_SIZE = 1 << 12;

  private Directory dir;
  private IndexReader reader;
  private Path path;
  private IndexSearcher cachedSearcher;
  private IndexSearcher uncachedSearcher;
  private LRUQueryCache queryCache;
  private Query filterQuery;
  private Query conjunctionQuery;
  private LeafReaderContext context;
  private Weight cachedWeight;
  private Weight uncachedWeight;
  private int expectedHitCount;

  @State(Scope.Benchmark)
  public static class Params {
    @Param({"1000000"})
    public int docCount;

    @Param({"0.0001", "0.001", "0.002", "0.003", "0.005", "0.01", "0.03", "0.10", "0.50", "1.0"})
    public double filterSelectivity;

    @Param({"0.001", "0.01"})
    public double leadSelectivity;

    @Param({BASELINE, UNGATED, GATED})
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
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();

    filterQuery = new TermQuery(new Term(FILTER_FIELD, YES));
    Query leadQuery = new TermQuery(new Term(LEAD_FIELD, YES));
    conjunctionQuery =
        new BooleanQuery.Builder()
            .add(leadQuery, Occur.FILTER)
            .add(filterQuery, Occur.FILTER)
            .build();

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
    Scorer scorer = scorer(weight);
    if (scorer == null) {
      return 0;
    }
    switch (variant) {
      case BASELINE:
        return scorePerDoc(scorer);
      case UNGATED:
        return scoreIntoBitSet(scorer, false);
      case GATED:
        return scoreGated(scorer);
      default:
        throw new AssertionError("Unknown variant: " + variant);
    }
  }

  private int scoreGated(Scorer scorer) throws IOException {
    DocIdSetIterator iterator = scorer.iterator();
    int maxDoc = context.reader().maxDoc();
    if (isBitSetConjunction(iterator) == false || iterator.cost() < Math.ceilDiv(maxDoc, 512)) {
      return scorePerDoc(iterator, maxDoc);
    }
    return scoreIntoBitSet(iterator, maxDoc);
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

  private int scoreIntoBitSet(Scorer scorer, boolean gated) throws IOException {
    DocIdSetIterator iterator = scorer.iterator();
    int maxDoc = context.reader().maxDoc();
    if (gated && iterator.cost() < Math.ceilDiv(maxDoc, 512)) {
      return scorePerDoc(scorer);
    }
    return scoreIntoBitSet(iterator, maxDoc);
  }

  private int scoreIntoBitSet(DocIdSetIterator iterator, int maxDoc) throws IOException {
    FixedBitSet windowMatches = new FixedBitSet(WINDOW_SIZE);
    CountingLeafCollector collector = new CountingLeafCollector();
    for (int doc = iterator.nextDoc(); doc < maxDoc; ) {
      int windowBase = doc;
      int windowMax = MathUtil.unsignedMin(maxDoc, windowBase + WINDOW_SIZE);
      iterator.intoBitSet(windowMax, windowMatches, windowBase);
      if (windowMatches.scanIsEmpty() == false) {
        collector.collect(new BitSetDocIdStream(windowMatches, windowBase));
      }
      windowMatches.clear();
      doc = iterator.docID();
    }
    return collector.count;
  }

  private static boolean isBitSetConjunction(DocIdSetIterator iterator) {
    return iterator.getClass().getName().endsWith("$BitSetConjunctionDISI");
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

  private static class BitSetDocIdStream extends DocIdStream {

    private final FixedBitSet bitSet;
    private final int offset, max;
    private int upTo;

    BitSetDocIdStream(FixedBitSet bitSet, int offset) {
      this.bitSet = bitSet;
      this.offset = offset;
      upTo = offset;
      max = MathUtil.unsignedMin(Integer.MAX_VALUE, offset + bitSet.length());
    }

    @Override
    public boolean mayHaveRemaining() {
      return upTo < max;
    }

    @Override
    public void forEach(int upTo, IOIntConsumer consumer) throws IOException {
      if (upTo > this.upTo) {
        upTo = Math.min(upTo, max);
        bitSet.forEach(this.upTo - offset, upTo - offset, offset, consumer);
        this.upTo = upTo;
      }
    }

    @Override
    public int count(int upTo) {
      if (upTo > this.upTo) {
        upTo = Math.min(upTo, max);
        int count = bitSet.cardinality(this.upTo - offset, upTo - offset);
        this.upTo = upTo;
        return count;
      } else {
        return 0;
      }
    }

    @Override
    public int intoArray(int upTo, int[] array) {
      if (upTo > this.upTo) {
        upTo = Math.min(upTo, max);
        int count = bitSet.intoArray(this.upTo - offset, upTo - offset, offset, array);
        if (count == array.length) {
          upTo = array[array.length - 1] + 1;
        }
        this.upTo = upTo;
        return count;
      }
      return 0;
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
