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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * An exhaustive {@link ScoreMode} ({@link ScoreMode#COMPLETE} or {@link
 * ScoreMode#COMPLETE_NO_SCORES}) must visit every match, even when a nested collector calls {@link
 * Scorable#setMinCompetitiveScore(float)} in violation of the contract documented on that method.
 *
 * <p>These tests exercise every bulk scorer that reads {@code SimpleScorable#minCompetitiveScore}:
 * {@code BatchScoreBulkScorer} (term and combined-field queries) and {@code
 * DenseConjunctionBulkScorer}, which is reached both from {@code BooleanScorerSupplier} for a
 * FILTER-only conjunction and from {@code ConstantScoreScorerSupplier}, which backs {@link
 * MatchAllDocsQuery}, {@link FieldExistsQuery} and point range queries.
 */
public class TestExhaustiveScoreModeNoPruning extends LuceneTestCase {

  /**
   * Index size used throughout. Comfortably above {@code DenseConjunctionBulkScorer.WINDOW_SIZE}
   * (4096) so that collection spans several windows, which is all these tests need: the failure
   * mode is identical at 20k and at 100k documents.
   */
  private static final int NUM_DOCS = 20_000;

  /**
   * Outer collector that declares an exhaustive score mode and forwards to an inner
   * TopScoreDocCollector, which will call setMinCompetitiveScore once its queue is full. Counts
   * every collect() call so that we can assert nothing was pruned.
   */
  private static final class CountingWrapper extends SimpleCollector {
    final AtomicInteger totalCalls = new AtomicInteger();
    private final TopScoreDocCollector in;
    private final ScoreMode scoreMode;
    private LeafCollector leafIn;

    CountingWrapper(TopScoreDocCollector in, ScoreMode scoreMode) {
      this.in = in;
      this.scoreMode = scoreMode;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      leafIn = in.getLeafCollector(context);
    }

    @Override
    public void collect(int doc) throws IOException {
      totalCalls.incrementAndGet();
      leafIn.collect(doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      super.setScorer(scorer);
      leafIn.setScorer(scorer);
    }

    @Override
    public ScoreMode scoreMode() {
      return scoreMode;
    }
  }

  private static int runCollect(IndexSearcher searcher, Query query, ScoreMode mode)
      throws IOException {
    TopScoreDocCollector in = new TopScoreDocCollectorManager(1, 1).newCollector();
    CountingWrapper out = new CountingWrapper(in, mode);
    searcher.search(query, out);
    return out.totalCalls.intValue();
  }

  /** Single-segment, single-threaded searcher so that bulk scorer selection is deterministic. */
  private static IndexSearcher plainSearcher(IndexReader reader) {
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    return searcher;
  }

  /** A body value whose term count varies with {@code i}, so documents get different norms. */
  private static String body(int i) {
    return ("hit " + "pad ".repeat(1 + (i % 8))).trim();
  }

  // ---------------------------------------------------------------------------------------------
  // 1. BatchScoreBulkScorer via TermQuery
  // ---------------------------------------------------------------------------------------------

  public void testTermQueryCompleteVisitsEveryMatch() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        // Varying term counts give the docs different norms, so scores differ and the inner
        // TopScoreDocCollector really does raise a competitive threshold.
        doc.add(new TextField("body", body(i), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        Query q = new TermQuery(new Term("body", "hit"));
        assertEquals(
            "COMPLETE pruned matches", NUM_DOCS, runCollect(searcher, q, ScoreMode.COMPLETE));
        assertEquals(
            "COMPLETE_NO_SCORES pruned matches",
            NUM_DOCS,
            runCollect(searcher, q, ScoreMode.COMPLETE_NO_SCORES));
        // TOP_DOCS_WITH_SCORES is not exhaustive, so pruning is expected there.
        int topDocsWithScores = runCollect(searcher, q, ScoreMode.TOP_DOCS_WITH_SCORES);
        assertTrue(
            "TOP_DOCS_WITH_SCORES unexpectedly exhaustive, collected=" + topDocsWithScores,
            topDocsWithScores < NUM_DOCS);
      }
    }
  }

  /**
   * Same as above, but through newSearcher so that reader wrapping and intra-segment concurrency
   * are exercised too.
   *
   * <p>wrapWithAssertions is deliberately false: {@link CountingWrapper} violates the {@link
   * Scorable#setMinCompetitiveScore(float)} contract on purpose, and AssertingScorer asserts that
   * only {@link ScoreMode#TOP_SCORES} may call it.
   */
  public void testTermQueryCompleteWithConcurrentSearcher() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new TextField("body", body(i), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = newSearcher(reader, true, false, true);
        searcher.setQueryCache(null);
        assertEquals(
            NUM_DOCS,
            runCollect(searcher, new TermQuery(new Term("body", "hit")), ScoreMode.COMPLETE));
      }
    }
  }

  /** TOP_SCORES must keep pruning: the fix must not disable the optimization. */
  public void testTopScoresStillPrunes() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new TextField("body", body(i), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        int collected =
            runCollect(searcher, new TermQuery(new Term("body", "hit")), ScoreMode.TOP_SCORES);
        assertTrue(
            "TOP_SCORES should still skip non-competitive hits, collected=" + collected,
            collected < NUM_DOCS);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 2. DenseConjunctionBulkScorer via a FILTER-only BooleanQuery
  // ---------------------------------------------------------------------------------------------

  public void testFilterOnlyBooleanQueryCompleteVisitsEveryMatch() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", "y", Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        BooleanQuery q =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("a", "x")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("b", "y")), BooleanClause.Occur.FILTER)
                .build();
        assertEquals(
            "FILTER-only COMPLETE pruned matches",
            NUM_DOCS,
            runCollect(searcher, q, ScoreMode.COMPLETE));
      }
    }
  }

  /** Same, with three FILTER clauses and a sparser match set that still clears the density bar. */
  public void testFilterOnlyBooleanQueryThreeClausesSparse() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      int expected = 0;
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", i % 2 == 0 ? "y" : "n", Store.NO));
        doc.add(new StringField("c", i % 3 == 0 ? "z" : "n", Store.NO));
        if (i % 2 == 0 && i % 3 == 0) {
          expected++;
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        BooleanQuery q =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("a", "x")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("b", "y")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("c", "z")), BooleanClause.Occur.FILTER)
                .build();
        assertEquals(expected, runCollect(searcher, q, ScoreMode.COMPLETE));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 3. BatchScoreBulkScorer via CombinedFieldQuery (the second call site the PR patches)
  // ---------------------------------------------------------------------------------------------

  public void testCombinedFieldQueryCompleteVisitsEveryMatch() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new TextField("t", body(i), Store.NO));
        doc.add(new TextField("u", ("hit " + "pad ".repeat(1 + (i % 5))).trim(), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        Query q =
            new CombinedFieldQuery.Builder(new BytesRef("hit"))
                .addField("t", 1.0f)
                .addField("u", 1.0f)
                .build();
        assertEquals(
            "CombinedFieldQuery under COMPLETE pruned matches",
            NUM_DOCS,
            runCollect(searcher, q, ScoreMode.COMPLETE));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 4. Cross-check: totalHits from a COMPLETE count must equal the number of collect() calls
  // ---------------------------------------------------------------------------------------------

  public void testCountAgreesWithCollectCalls() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", "y", Store.NO));
        doc.add(new TextField("body", body(i), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        Query bq =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("a", "x")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("b", "y")), BooleanClause.Occur.FILTER)
                .build();
        Query tq = new TermQuery(new Term("body", "hit"));
        for (Query q : new Query[] {bq, tq}) {
          assertEquals(
              "count() and COMPLETE collection disagree for " + q,
              searcher.count(q),
              runCollect(searcher, q, ScoreMode.COMPLETE));
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 5. Constant-score queries that reach DenseConjunctionBulkScorer via
  //    ConstantScoreScorerSupplier#bulkScorer with a non-zero constant score.
  // ---------------------------------------------------------------------------------------------

  public void testConstantScoreQueriesVisitEveryMatch() throws Exception {
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new NumericDocValuesField("n", i));
        doc.add(new IntPoint("p", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        Query[] queries = {
          new MatchAllDocsQuery(),
          new FieldExistsQuery("n"),
          IntPoint.newRangeQuery("p", 0, NUM_DOCS),
          // ConstantScoreQuery is safe on its own: it wraps the inner bulk scorer in
          // ConstantBulkScorer. Kept here as a regression guard on that wrapping.
          new ConstantScoreQuery(new TermQuery(new Term("a", "x"))),
        };
        for (Query q : queries) {
          assertEquals(
              "COMPLETE pruned matches for " + q,
              NUM_DOCS,
              runCollect(searcher, q, ScoreMode.COMPLETE));
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 6. With deletions (acceptDocs != null) the "whole run matches" shortcut in
  //    DenseConjunctionBulkScorer#scoreWindow no longer applies, so MatchAllDocsQuery,
  //    FieldExistsQuery and point ranges go window-by-window and hit the pruning check.
  //    A single deleted document is enough.
  //
  //    These two tests use a plain IndexWriter rather than RandomIndexWriter: the latter's
  //    getReader() calls doRandomForceMerge(), which may expunge the deletion and put us back on
  //    the collectRange() shortcut, so the deletions coverage would silently disappear. The
  //    hasDeletions() assertions below make that failure loud if it ever comes back.
  // ---------------------------------------------------------------------------------------------

  public void testConstantScoreQueriesWithDeletionsUnderComplete() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", "y", Store.NO));
        doc.add(new NumericDocValuesField("n", i));
        doc.add(new IntPoint("p", i));
        if (i == 7) {
          doc.add(new StringField("del", "yes", Store.NO));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.deleteDocuments(new Term("del", "yes"));
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertTrue("the deletion must survive: it is what this test covers", reader.hasDeletions());
        IndexSearcher searcher = plainSearcher(reader);
        final int live = NUM_DOCS - 1;
        assertEquals(live, reader.numDocs());
        Query[] queries = {
          new MatchAllDocsQuery(),
          new FieldExistsQuery("n"),
          IntPoint.newRangeQuery("p", 0, NUM_DOCS),
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term("a", "x")), BooleanClause.Occur.FILTER)
              .add(new TermQuery(new Term("b", "y")), BooleanClause.Occur.FILTER)
              .build(),
        };
        for (Query q : queries) {
          assertEquals(
              "COMPLETE pruned matches with deletions for " + q,
              live,
              runCollect(searcher, q, ScoreMode.COMPLETE));
          assertEquals(
              "COMPLETE_NO_SCORES pruned matches with deletions for " + q,
              live,
              runCollect(searcher, q, ScoreMode.COMPLETE_NO_SCORES));
        }
      }
    }
  }

  /** Deletions + TOP_SCORES: pruning must still happen, so the fix costs nothing. */
  public void testMatchAllWithDeletionsStillPrunesUnderTopScores() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        if (i == 7) {
          doc.add(new StringField("del", "yes", Store.NO));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.deleteDocuments(new Term("del", "yes"));
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertTrue("the deletion must survive: it is what this test covers", reader.hasDeletions());
        IndexSearcher searcher = plainSearcher(reader);
        int collected = runCollect(searcher, new MatchAllDocsQuery(), ScoreMode.TOP_SCORES);
        assertTrue(
            "TOP_SCORES should still prune, collected=" + collected, collected < NUM_DOCS - 1);
      }
    }
  }
}
