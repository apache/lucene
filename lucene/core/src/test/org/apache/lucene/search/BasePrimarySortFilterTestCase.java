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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Assert;

/**
 * Base test case for {@link PrimarySortAlignable} filter queries used with {@link
 * FilteredOnPrimaryIndexSortFieldQuery}. Subclasses supply the index schema, filter query, and
 * expected hit counts; common tests verify rewrite, hit count, multi-segment correctness vs a
 * simpler boolean shape, two FILTER clauses, MUST_NOT interaction, and (when {@link
 * #densePrimarySortBulkChecksOrNull()} is non-null) bulk-scorer narrowing, cost, and recorded
 * scoring windows.
 *
 * <p>Concrete implementations exist for each supported filter type (numeric range, point range,
 * sorted-set range, term query, etc.). Shared helpers live in {@link RecordingMatchAllQuery}.
 */
public abstract class BasePrimarySortFilterTestCase extends LuceneTestCase {

  /** Number of documents indexed; randomized per test run in {@link #setUp()}. */
  protected int numDocs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    numDocs = TestUtil.nextInt(random(), 50, 200);
  }

  /**
   * When the index maps doc {@code n} to the {@code n}-th sort key in order (so the filter's dense
   * doc-id interval is known), return the interval and expected stats for bulk-scorer checks.
   *
   * <p>{@link #testBulkScorerNarrowingCostAndRecording} assumes a force-merged single segment.
   */
  protected record DensePrimarySortBulkChecks(
      int denseMinDocInclusive, int denseMaxDocExclusive, int expectedMatchingDocs) {}

  /** Returns {@code null} if bulk-scorer slice tests do not apply to this fixture. */
  protected DensePrimarySortBulkChecks densePrimarySortBulkChecksOrNull() {
    return null;
  }

  /** Returns the index sort for the primary sort field under test. */
  protected abstract Sort buildIndexSort();

  /** Adds a single document at logical position {@code i} (0-based) to the writer. */
  protected abstract void addDocument(IndexWriter writer, int i) throws IOException;

  /**
   * Returns the FILTER query that selects a contiguous sub-range of the sorted index. The query
   * must implement {@link PrimarySortAlignable}.
   */
  protected abstract Query buildFilterQuery();

  /** Expected number of documents matched by {@link #buildFilterQuery()}. */
  protected abstract int expectedFilteredHitCount();

  /**
   * If the filter type supports a second, wider range for the two-filter intersection test, return
   * it here. Return {@code null} to skip {@link #testTwoOptimizableFilters()}.
   */
  protected Query buildWiderFilterQuery() {
    return null;
  }

  // ---- index helpers ----

  private IndexWriterConfig buildIndexWriterConfig() {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(buildIndexSort());
    iwc.setMergePolicy(newMergePolicy());
    return iwc;
  }

  // ---- tests ----

  /** The filter query must implement {@link PrimarySortAlignable}. */
  public void testFilterImplementsPrimarySortAlignable() {
    Query filter = buildFilterQuery();
    assertTrue(
        filter.getClass().getSimpleName() + " must implement PrimarySortAlignable",
        filter instanceof PrimarySortAlignable);
  }

  /**
   * BooleanQuery with the filter as FILTER rewrites to {@link FilteredOnPrimaryIndexSortFieldQuery}
   * (possibly wrapped, e.g. in {@link ConstantScoreQuery}).
   *
   * <p>Uses {@link RecordingMatchAllQuery} as the MUST clause: {@link MatchAllDocsQuery} + FILTER
   * is rewritten to {@link ConstantScoreQuery} earlier in {@link BooleanQuery#rewrite}, which
   * prevents the primary-sort optimization from applying at the query shape we need to assert here.
   */
  public void testRewriteProducesFilteredQuery() throws IOException {
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        Query query = buildBooleanQuery(new RecordingMatchAllQuery(), buildFilterQuery());
        Query rewritten = searcher.rewrite(query);
        assertNotNull(
            "expected FilteredOnPrimaryIndexSortFieldQuery in rewrite chain",
            unwrapFilteredOnPrimaryIndexSortFieldQuery(rewritten));
      }
    }
  }

  /** Filtered search returns exactly the expected number of hits. */
  public void testFilteredHitCount() throws IOException {
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        Query query = buildBooleanQuery(new MatchAllDocsQuery(), buildFilterQuery());
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(expectedFilteredHitCount(), topDocs.totalHits.value());
      }
    }
  }

  /**
   * On a multi-segment index, the primary-sort rewrite path must agree with a plain {@code
   * MUST}&nbsp;{@link MatchAllDocsQuery} + {@code FILTER} boolean (which does not take the {@link
   * FilteredOnPrimaryIndexSortFieldQuery} shape because of {@link BooleanQuery#rewrite}).
   */
  public void testMultiSegmentEquivalentToSimpleBoolean() throws IOException {
    try (Directory dir = newDirectory()) {
      // NoMergePolicy preserves the explicit segment boundaries we create below.
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      iwc.setIndexSort(buildIndexSort());
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        // Commit at a random split point to guarantee at least two segments.
        int split = TestUtil.nextInt(random(), 1, numDocs - 1);
        for (int i = 0; i < numDocs; i++) {
          addDocument(iw, i);
          if (i == split - 1) {
            iw.commit();
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertTrue("expected multi-segment", reader.leaves().size() > 1);
        IndexSearcher searcher = newSearcher(reader);

        Query filter = buildFilterQuery();
        Query withFilteredOnPrimary = buildBooleanQuery(new RecordingMatchAllQuery(), filter);
        Query simpleMatchAllAndFilter = buildBooleanQuery(new MatchAllDocsQuery(), filter);

        TopDocs tdOpt = searcher.search(withFilteredOnPrimary, numDocs, Sort.INDEXORDER, true);
        TopDocs tdSimple = searcher.search(simpleMatchAllAndFilter, numDocs, Sort.INDEXORDER, true);
        assertEquals(tdSimple.totalHits.value(), tdOpt.totalHits.value());
        for (int i = 0; i < tdSimple.scoreDocs.length; i++) {
          assertEquals(tdSimple.scoreDocs[i].doc, tdOpt.scoreDocs[i].doc);
        }
      }
    }
  }

  /**
   * Two optimizable FILTERs on the same field: intersection semantics must match regardless of
   * clause order.
   */
  public void testTwoOptimizableFilters() throws IOException {
    Query widerFilter = buildWiderFilterQuery();
    if (widerFilter == null) {
      return; // subclass does not support this test
    }
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        assertTwoFiltersMatch(searcher, widerFilter, buildFilterQuery());
        assertTwoFiltersMatch(searcher, buildFilterQuery(), widerFilter);
      }
    }
  }

  /**
   * {@link FilteredOnPrimaryIndexSortFieldQuery} narrows bulk scoring to the dense doc-id interval:
   * disjoint windows collect nothing; adjacent slices compose; {@link ScorerSupplier#cost()}
   * matches the narrowed span; {@link RecordingMatchAllQuery} sees only intervals inside the dense
   * range. Uses a plain {@link IndexSearcher} for the recording part so asserting bulk scorers run
   * (see {@link org.apache.lucene.tests.search.AssertingIndexSearcher}).
   */
  public void testBulkScorerNarrowingCostAndRecording() throws IOException {
    DensePrimarySortBulkChecks d = densePrimarySortBulkChecksOrNull();
    if (d == null) {
      return;
    }
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        RecordingMatchAllQuery recordingQuery = new RecordingMatchAllQuery();
        Query filter = buildFilterQuery();
        Query booleanQuery = buildBooleanQuery(recordingQuery, filter);

        IndexSearcher plainSearcher = new IndexSearcher(reader);
        Query plainRewritten = plainSearcher.rewrite(booleanQuery);
        assertNotNull(
            "expected FilteredOnPrimaryIndexSortFieldQuery in rewrite chain",
            unwrapFilteredOnPrimaryIndexSortFieldQuery(plainRewritten));

        recordingQuery.clearRecordedRanges();
        TopDocs topDocs = plainSearcher.search(booleanQuery, numDocs);
        assertEquals(d.expectedMatchingDocs(), topDocs.totalHits.value());
        assertFalse(recordingQuery.scoredRanges.isEmpty());
        for (DocIdRange range : recordingQuery.scoredRanges) {
          assertTrue(range.minDoc() >= d.denseMinDocInclusive());
          assertTrue(range.maxDoc() <= d.denseMaxDocExclusive());
        }

        recordingQuery.clearRecordedRanges();

        IndexSearcher searcher = newSearcher(reader);
        Query rewritten = searcher.rewrite(booleanQuery);
        assertNotNull(
            "expected FilteredOnPrimaryIndexSortFieldQuery in rewrite chain",
            unwrapFilteredOnPrimaryIndexSortFieldQuery(rewritten));

        Weight optWeight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        Weight origWeight = searcher.createWeight(booleanQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext ctx : reader.leaves()) {
          assertBulkScorerMatchesDenseChecks(d, optWeight, origWeight, ctx);
        }
      }
    }
  }

  /** MUST_NOT clause must still be respected after primary-sort FILTER rewrite. */
  public void testMustNotInteraction() throws IOException {
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, true);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        Query filter = buildFilterQuery();
        // Add a MUST_NOT that excludes all docs — should return 0 hits
        Query query =
            new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), Occur.MUST)
                .add(filter, Occur.FILTER)
                .add(new MatchAllDocsQuery(), Occur.MUST_NOT)
                .build();
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(0, topDocs.totalHits.value());
      }
    }
  }

  /**
   * On an unsorted index the filter must NOT trigger the optimization (no rewrite to
   * FilteredOnPrimaryIndexSortFieldQuery).
   */
  public void testUnsortedIndexDoesNotOptimize() throws IOException {
    try (Directory dir = newDirectory()) {
      buildAndPopulateIndex(dir, false);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        Query query = buildBooleanQuery(new MatchAllDocsQuery(), buildFilterQuery());
        Query rewritten = searcher.rewrite(query);
        assertNull(
            "should not optimize on unsorted index",
            unwrapFilteredOnPrimaryIndexSortFieldQuery(rewritten));
        // But must still return correct results
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(expectedFilteredHitCount(), topDocs.totalHits.value());
      }
    }
  }

  // ---- helpers ----

  private void buildAndPopulateIndex(Directory dir, boolean sorted) throws IOException {
    IndexWriterConfig iwc =
        sorted ? buildIndexWriterConfig() : new IndexWriterConfig(new MockAnalyzer(random()));
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < numDocs; i++) {
        addDocument(writer, i);
      }
      writer.forceMerge(1);
    }
  }

  private static Query buildBooleanQuery(Query must, Query filter) {
    return new BooleanQuery.Builder().add(must, Occur.MUST).add(filter, Occur.FILTER).build();
  }

  private void assertTwoFiltersMatch(IndexSearcher searcher, Query first, Query second)
      throws IOException {
    Query query =
        new BooleanQuery.Builder()
            .add(new RecordingMatchAllQuery(), Occur.MUST)
            .add(first, Occur.FILTER)
            .add(second, Occur.FILTER)
            .build();
    Query rewritten = searcher.rewrite(query);
    assertNotNull(
        "expected FilteredOnPrimaryIndexSortFieldQuery in rewrite chain",
        unwrapFilteredOnPrimaryIndexSortFieldQuery(rewritten));
    TopDocs topDocs = searcher.search(query, numDocs);
    assertEquals(expectedFilteredHitCount(), topDocs.totalHits.value());
  }

  private static void assertBulkScorerMatchesDenseChecks(
      DensePrimarySortBulkChecks d, Weight optWeight, Weight origWeight, LeafReaderContext ctx)
      throws IOException {
    ScorerSupplier optSs = optWeight.scorerSupplier(ctx);
    ScorerSupplier origSs = origWeight.scorerSupplier(ctx);
    assertNotNull(optSs);
    assertNotNull(origSs);
    assertTrue(optSs.cost() <= origSs.cost());
    long span = (long) (d.denseMaxDocExclusive() - d.denseMinDocInclusive());
    assertTrue(
        "expected cost <= span (" + span + ") but was " + optSs.cost(), optSs.cost() <= span);

    final int leafMax = ctx.reader().maxDoc();

    assertTrue(bulkCollectDocs(optWeight, ctx, 0, d.denseMinDocInclusive()).isEmpty());

    List<Integer> expected = new ArrayList<>();
    for (int doc = d.denseMinDocInclusive(); doc < d.denseMaxDocExclusive(); ++doc) {
      expected.add(doc);
    }
    List<Integer> fullPass = bulkCollectDocs(optWeight, ctx, 0, leafMax);
    Collections.sort(fullPass);
    assertEquals(expected, fullPass);

    final int rangeLen = d.denseMaxDocExclusive() - d.denseMinDocInclusive();
    int split = d.denseMinDocInclusive() + Math.max(1, rangeLen * 3 / 4);
    if (split >= d.denseMaxDocExclusive()) {
      split = d.denseMaxDocExclusive() - 1;
    }

    List<Integer> chunked = new ArrayList<>();
    chunked.addAll(bulkCollectDocs(optWeight, ctx, 0, split));
    chunked.addAll(bulkCollectDocs(optWeight, ctx, split, leafMax));
    Collections.sort(chunked);
    assertEquals(expected, chunked);
  }

  /**
   * Finds {@link FilteredOnPrimaryIndexSortFieldQuery} under common rewrite wrappers ({@link
   * ConstantScoreQuery}, {@link BoostQuery}, outer {@link BooleanQuery} with a single MUST child).
   */
  static FilteredOnPrimaryIndexSortFieldQuery unwrapFilteredOnPrimaryIndexSortFieldQuery(Query q) {
    if (q == null) {
      return null;
    }
    if (q instanceof FilteredOnPrimaryIndexSortFieldQuery f) {
      return f;
    }
    if (q instanceof ConstantScoreQuery cs) {
      return unwrapFilteredOnPrimaryIndexSortFieldQuery(cs.getQuery());
    }
    if (q instanceof BoostQuery bq) {
      return unwrapFilteredOnPrimaryIndexSortFieldQuery(bq.getQuery());
    }
    if (q instanceof BooleanQuery b && b.clauses().size() == 1) {
      return unwrapFilteredOnPrimaryIndexSortFieldQuery(b.clauses().get(0).query());
    }
    return null;
  }

  static List<Integer> bulkCollectDocs(Weight weight, LeafReaderContext ctx, int min, int max)
      throws IOException {
    ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
    Assert.assertNotNull(scorerSupplier);
    BulkScorer bulkScorer = scorerSupplier.bulkScorer();
    Assert.assertNotNull(bulkScorer);
    List<Integer> docs = new ArrayList<>();
    bulkScorer.score(
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            docs.add(doc);
          }
        },
        null,
        min,
        max);
    return docs;
  }
}
