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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;

public class TestMonitorConcurrency extends MonitorTestBase {

  /**
   * Commits every few queries and never merges, so that the queryindex has many segments, and uses
   * slices small enough to split segments into partitions
   */
  private static MonitorConfiguration manySegments() {
    return noMerges()
        .setQueryUpdateBufferSize(TestUtil.nextInt(random(), 1, 10))
        .setMaxQueriesPerSlice(TestUtil.nextInt(random(), 1, 5));
  }

  private static MonitorConfiguration noMerges() {
    return new MonitorConfiguration() {
      @Override
      protected IndexWriterConfig getIndexWriterConfig() {
        return super.getIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
      }
    };
  }

  /** Queries that all match {@link #matchingDoc()} */
  private static List<MonitorQuery> matchingQueries(int count) {
    List<MonitorQuery> queries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      queries.add(mq(Integer.toString(i), "t1"));
    }
    return queries;
  }

  private static Document matchingDoc() {
    Document doc = new Document();
    doc.add(newTextField(FIELD, "t1", Field.Store.NO));
    return doc;
  }

  private static MatcherFactory<QueryMatch> countingFactory(AtomicInteger matcherCount) {
    return searcher -> {
      matcherCount.incrementAndGet();
      return QueryMatch.SIMPLE_MATCHER.createMatcher(searcher);
    };
  }

  private static List<MonitorQuery> purgeQueries(int from, int to) {
    List<MonitorQuery> queries = new ArrayList<>();
    for (int i = from; i < to; i++) {
      queries.add(mq(Integer.toString(i), "+t1 +q" + i));
    }
    return queries;
  }

  private static String randomQuery() {
    String a = randomTerm();
    String b = randomTerm();
    String c = randomTerm();
    return switch (random().nextInt(5)) {
      case 0 -> a;
      case 1 -> "+" + a + " +" + b;
      case 2 -> a + " " + b + " " + c;
      case 3 -> "\"" + a + " " + b + "\"";
      default -> "(+" + a + " +" + b + ") " + c;
    };
  }

  private static String randomTerm() {
    return "t" + random().nextInt(30);
  }

  private static List<MonitorQuery> randomQueries() {
    List<MonitorQuery> queries = new ArrayList<>();
    int numQueries = atLeast(200);
    for (int i = 0; i < numQueries; i++) {
      queries.add(mq(Integer.toString(i), randomQuery()));
    }
    return queries;
  }

  private static Document[] randomDocs() {
    Document[] docs = new Document[TestUtil.nextInt(random(), 1, 5)];
    for (int i = 0; i < docs.length; i++) {
      StringBuilder text = new StringBuilder();
      int numTerms = TestUtil.nextInt(random(), 5, 20);
      for (int j = 0; j < numTerms; j++) {
        text.append(randomTerm()).append(' ');
      }
      docs[i] = new Document();
      docs[i].add(newTextField(FIELD, text.toString(), Field.Store.NO));
    }
    return docs;
  }

  public void testMatchesSameAsSequential() throws IOException {
    List<MonitorQuery> queries = randomQueries();
    try (Monitor sequential = new Monitor(ANALYZER, manySegments());
        Monitor concurrent = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      sequential.register(queries);
      concurrent.register(queries);
      assertEquals(sequential.getQueryIds(), concurrent.getQueryIds());

      for (int iter = 0; iter < 10; iter++) {
        Document[] docs = randomDocs();
        assertSameMatches(
            sequential.match(docs, QueryMatch.SIMPLE_MATCHER),
            concurrent.match(docs, QueryMatch.SIMPLE_MATCHER));
        assertSameMatches(
            sequential.match(docs, HighlightsMatch.MATCHER),
            concurrent.match(docs, HighlightsMatch.MATCHER));
        assertSameScores(
            sequential.match(docs, ScoringMatch.DEFAULT_MATCHER),
            concurrent.match(docs, ScoringMatch.DEFAULT_MATCHER));
        assertSameScores(
            sequential.match(docs, ExplainingMatch.MATCHER),
            concurrent.match(docs, ExplainingMatch.MATCHER),
            match -> match.getExplanation().getValue().doubleValue());
      }
    }
  }

  public void testLargeSegmentsAreSplit() throws IOException {
    AtomicInteger matcherCount = new AtomicInteger();
    MonitorConfiguration config = noMerges().setQueryUpdateBufferSize(10).setMaxQueriesPerSlice(5);
    try (Monitor sequential = new Monitor(ANALYZER, config)) {
      sequential.register(matchingQueries(100));
      assertEquals(
          100, sequential.match(matchingDoc(), countingFactory(matcherCount)).getMatchCount());
      assertEquals(1, matcherCount.getAndSet(0));
    }
    try (Monitor concurrent = new Monitor(ANALYZER, config.setExecutor(executor))) {
      concurrent.register(matchingQueries(100));
      assertEquals(
          100, concurrent.match(matchingDoc(), countingFactory(matcherCount)).getMatchCount());
      // register commits batches of 11 queries, and segments are split into partitions of 5
      assertTrue(matcherCount.get() >= 20);
    }
  }

  public void testSmallSegmentsShareASlice() throws IOException {
    AtomicInteger matcherCount = new AtomicInteger();
    // 50 segments of 2 queries, far below the default slice size
    MonitorConfiguration config = noMerges().setQueryUpdateBufferSize(1).setExecutor(executor);
    try (Monitor concurrent = new Monitor(ANALYZER, config)) {
      concurrent.register(matchingQueries(100));
      assertEquals(
          100, concurrent.match(matchingDoc(), countingFactory(matcherCount)).getMatchCount());
      assertEquals(1, matcherCount.get());
    }
  }

  public void testPartitionMatcher() throws IOException {
    List<MonitorQuery> queries = randomQueries();
    try (Monitor sequential = new Monitor(ANALYZER, manySegments());
        Monitor concurrent = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      sequential.register(queries);
      concurrent.register(queries);
      // sharing the Monitor's pool can't deadlock, since slice matchers finish on the calling
      // thread after the search
      MatcherFactory<QueryMatch> factory =
          PartitionMatcher.factory(executor, QueryMatch.SIMPLE_MATCHER, 2);

      for (int iter = 0; iter < 10; iter++) {
        Document[] docs = randomDocs();
        assertSameMatches(
            sequential.match(docs, QueryMatch.SIMPLE_MATCHER), concurrent.match(docs, factory));
      }
    }
  }

  public void testParallelMatcherIsRejected() throws Exception {
    List<MonitorQuery> queries = matchingQueries(100);
    Document doc = matchingDoc();
    ExecutorService matcherExecutor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("parallel-matcher"));
    // rejected even though a single segment is searched in a single slice
    try (Monitor concurrent =
            new Monitor(ANALYZER, new MonitorConfiguration().setExecutor(executor));
        Monitor sequential = new Monitor(ANALYZER)) {
      concurrent.register(queries);
      sequential.register(queries);
      MatcherFactory<QueryMatch> factory =
          ParallelMatcher.factory(matcherExecutor, QueryMatch.SIMPLE_MATCHER, 2);
      expectThrows(IllegalArgumentException.class, () -> concurrent.match(doc, factory));
      expectThrows(IllegalArgumentException.class, () -> concurrent.debug(doc, factory));
      // the workers of the rejected matchers were released, so the pool can run new ones
      assertEquals(100, sequential.match(doc, factory).getMatchCount());
    } finally {
      matcherExecutor.shutdown();
      assertTrue(matcherExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  public void testRejectionKeepsFinishFailure() throws Exception {
    ExecutorService matcherExecutor =
        Executors.newFixedThreadPool(1, new NamedThreadFactory("parallel-matcher"));
    try (Monitor monitor =
        new Monitor(ANALYZER, new MonitorConfiguration().setExecutor(executor))) {
      monitor.register(matchingQueries(1));
      MatcherFactory<QueryMatch> factory =
          ParallelMatcher.factory(matcherExecutor, QueryMatch.SIMPLE_MATCHER, 1);
      IllegalArgumentException e;
      // an interrupted thread makes ParallelMatcher.finish throw
      Thread.currentThread().interrupt();
      try {
        e =
            expectThrows(
                IllegalArgumentException.class, () -> monitor.match(matchingDoc(), factory));
      } finally {
        Thread.interrupted();
      }
      assertEquals(1, e.getSuppressed().length);
      assertTrue(e.getSuppressed()[0].getCause() instanceof InterruptedException);
    } finally {
      // the worker of the rejected matcher was never told to stop
      matcherExecutor.shutdownNow();
      assertTrue(matcherExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  public void testPurgeWhileRegistering() throws Exception {
    ExecutorService registerExecutor =
        Executors.newSingleThreadExecutor(new NamedThreadFactory("register"));
    try (Monitor monitor = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      monitor.register(purgeQueries(0, 200));
      List<String> deleted = new ArrayList<>();
      for (int i = 20; i < 80; i++) {
        deleted.add(Integer.toString(i));
      }
      monitor.deleteById(deleted);
      Future<?> registering =
          registerExecutor.submit(
              () -> {
                for (int i = 200; i < 400; i += 20) {
                  monitor.register(purgeQueries(i, i + 20));
                }
                return null;
              });
      monitor.purgeCache();
      registering.get();

      assertEquals(340, monitor.getQueryCacheStats().cachedQueries());
      Document doc = new Document();
      StringBuilder text = new StringBuilder("t1");
      for (int i = 0; i < 400; i++) {
        text.append(" q").append(i);
      }
      doc.add(newTextField(FIELD, text.toString(), Field.Store.NO));
      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getErrors().size());
      assertEquals(340, matches.getMatchCount());
    } finally {
      registerExecutor.shutdown();
      assertTrue(registerExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  public void testWrappingMatcher() throws IOException {
    List<MonitorQuery> queries = randomQueries();
    try (Monitor sequential = new Monitor(ANALYZER, manySegments());
        Monitor concurrent = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      sequential.register(queries);
      concurrent.register(queries);

      for (int iter = 0; iter < 10; iter++) {
        Document[] docs = randomDocs();
        Set<String> sequentialTimed = new HashSet<>();
        Set<String> concurrentTimed = ConcurrentHashMap.newKeySet();
        assertSameMatches(
            sequential.match(
                docs,
                QueryTimeListener.timingMatcher(
                    QueryMatch.SIMPLE_MATCHER, (id, _) -> sequentialTimed.add(id))),
            concurrent.match(
                docs,
                QueryTimeListener.timingMatcher(
                    QueryMatch.SIMPLE_MATCHER, (id, _) -> concurrentTimed.add(id))));
        assertEquals(sequentialTimed, concurrentTimed);
      }
    }
  }

  public void testMatchesFromOtherSlicesAreResolved() throws IOException {
    Document doc = new Document();
    doc.add(newTextField(FIELD, "t1 t2", Field.Store.NO));
    try (DocumentBatch batch = DocumentBatch.of(ANALYZER, doc)) {
      IndexSearcher searcher = new IndexSearcher(batch.get());

      CandidateMatcher<ScoringMatch> sequential =
          ScoringMatch.DEFAULT_MATCHER.createMatcher(searcher);
      sequential.matchQuery("q", parse("t1"), Map.of());
      sequential.matchQuery("q", parse("t2"), Map.of());

      CandidateMatcher<ScoringMatch> slice1 = ScoringMatch.DEFAULT_MATCHER.createMatcher(searcher);
      slice1.matchQuery("q", parse("t1"), Map.of());
      CandidateMatcher<ScoringMatch> slice2 = ScoringMatch.DEFAULT_MATCHER.createMatcher(searcher);
      slice2.matchQuery("q", parse("t2"), Map.of());
      slice2.reportError("failing", new IOException("error"));
      MultiMatchingQueries<ScoringMatch> merged = slice1.finish(0, 2, List.of(slice2));

      assertSameScores(sequential.finish(0, 2), merged);
      assertEquals(Set.of("failing"), merged.getErrors().keySet());
    }
  }

  public void testErrorsFromAllSlices() throws IOException {
    Set<String> failingIds = new HashSet<>();
    List<MonitorQuery> queries = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      if (i % 7 == 0) {
        failingIds.add("error" + i);
        queries.add(new MonitorQuery("error" + i, new ThrowOnRewriteQuery()));
      } else {
        queries.add(mq(Integer.toString(i), randomQuery()));
      }
    }
    try (Monitor monitor = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      monitor.register(queries);
      MultiMatchingQueries<QueryMatch> matches =
          monitor.match(randomDocs(), QueryMatch.SIMPLE_MATCHER);
      assertEquals(failingIds, matches.getErrors().keySet());
    }
  }

  public void testDebugSameAsSequential() throws IOException {
    List<MonitorQuery> queries = randomQueries();
    try (Monitor sequential = new Monitor(ANALYZER, manySegments());
        Monitor concurrent = new Monitor(ANALYZER, manySegments().setExecutor(executor))) {
      sequential.register(queries);
      concurrent.register(queries);

      Document[] docs = randomDocs();
      PresearcherMatches<QueryMatch> expected = sequential.debug(docs, QueryMatch.SIMPLE_MATCHER);
      PresearcherMatches<QueryMatch> actual = concurrent.debug(docs, QueryMatch.SIMPLE_MATCHER);
      assertSameMatches(expected.matcher, actual.matcher);
      for (MonitorQuery query : queries) {
        for (int doc = 0; doc < docs.length; doc++) {
          PresearcherMatch<QueryMatch> expectedMatch = expected.match(query.getId(), doc);
          PresearcherMatch<QueryMatch> actualMatch = actual.match(query.getId(), doc);
          if (expectedMatch == null) {
            assertNull(actualMatch);
          } else {
            assertEquals(
                presearcherTerms(expectedMatch.presearcherMatches),
                presearcherTerms(actualMatch.presearcherMatches));
          }
        }
      }
    }
  }

  public void testPersistentAndReadonlyMonitors() throws IOException {
    List<MonitorQuery> queries = randomQueries();
    Path indexPath = createTempDir();
    MonitorQuerySerializer serializer = MonitorQuerySerializer.fromParser(MonitorTestBase::parse);
    try (Monitor sequential = new Monitor(ANALYZER, manySegments())) {
      sequential.register(queries);
      Document[] docs = randomDocs();
      MultiMatchingQueries<QueryMatch> expected = sequential.match(docs, QueryMatch.SIMPLE_MATCHER);

      MonitorConfiguration writeConfig =
          manySegments().setIndexPath(indexPath, serializer).setExecutor(executor);
      try (Monitor writer = new Monitor(ANALYZER, writeConfig)) {
        writer.register(queries);
        assertEquals(sequential.getQueryCount(), writer.getQueryCount());
        assertEquals(queries.get(0).getQueryString(), writer.getQuery("0").getQueryString());
        assertSameMatches(expected, writer.match(docs, QueryMatch.SIMPLE_MATCHER));
      }

      // reopening the monitor reloads the query cache from the queryindex
      try (Monitor reopened = new Monitor(ANALYZER, writeConfig)) {
        assertEquals(sequential.getQueryCount(), reopened.getQueryCount());
        assertSameMatches(expected, reopened.match(docs, QueryMatch.SIMPLE_MATCHER));
      }

      MonitorConfiguration readConfig =
          new MonitorConfiguration()
              .setDirectoryProvider(() -> FSDirectory.open(indexPath), serializer, true)
              .setExecutor(executor)
              .setMaxQueriesPerSlice(TestUtil.nextInt(random(), 1, 5));
      try (Monitor readonly = new Monitor(ANALYZER, readConfig)) {
        assertEquals(sequential.getQueryCount(), readonly.getQueryCount());
        assertEquals(queries.get(0).getQueryString(), readonly.getQuery("0").getQueryString());
        assertSameMatches(expected, readonly.match(docs, QueryMatch.SIMPLE_MATCHER));
      }
    }
  }

  private static Set<String> presearcherTerms(String presearcherMatches) {
    return new HashSet<>(Arrays.asList(presearcherMatches.trim().split(" ")));
  }

  private static <T extends QueryMatch> void assertSameMatches(
      MultiMatchingQueries<T> expected, MultiMatchingQueries<T> actual) {
    assertEquals(expected.getQueriesRun(), actual.getQueriesRun());
    assertEquals(expected.getErrors().keySet(), actual.getErrors().keySet());
    assertEquals(expected.getBatchSize(), actual.getBatchSize());
    for (int doc = 0; doc < expected.getBatchSize(); doc++) {
      assertEquals(byQueryId(expected.getMatches(doc)), byQueryId(actual.getMatches(doc)));
    }
  }

  private static void assertSameScores(
      MultiMatchingQueries<ScoringMatch> expected, MultiMatchingQueries<ScoringMatch> actual) {
    assertSameScores(expected, actual, ScoringMatch::getScore);
  }

  // scores of a query's disjuncts are summed in a different order, so allow a small delta
  private static <T extends QueryMatch> void assertSameScores(
      MultiMatchingQueries<T> expected, MultiMatchingQueries<T> actual, ToDoubleFunction<T> score) {
    assertEquals(expected.getQueriesRun(), actual.getQueriesRun());
    for (int doc = 0; doc < expected.getBatchSize(); doc++) {
      Map<String, T> expectedMatches = byQueryId(expected.getMatches(doc));
      Map<String, T> actualMatches = byQueryId(actual.getMatches(doc));
      assertEquals(expectedMatches.keySet(), actualMatches.keySet());
      for (Map.Entry<String, T> entry : expectedMatches.entrySet()) {
        double expectedScore = score.applyAsDouble(entry.getValue());
        assertEquals(
            expectedScore,
            score.applyAsDouble(actualMatches.get(entry.getKey())),
            expectedScore * 1e-5);
      }
    }
  }

  private static <T extends QueryMatch> Map<String, T> byQueryId(Iterable<T> matches) {
    Map<String, T> byId = new HashMap<>();
    for (T match : matches) {
      byId.put(match.getQueryId(), match);
    }
    return byId;
  }
}
