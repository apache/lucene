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
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Verification harness for GITHUB#15239 / PR #16542.
 *
 * <p>An exhaustive {@link ScoreMode} (COMPLETE / COMPLETE_NO_SCORES / TOP_DOCS) must visit every
 * match, even when a nested collector calls {@link Scorable#setMinCompetitiveScore(float)}. These
 * tests exercise every bulk scorer that reads {@code SimpleScorable#minCompetitiveScore}.
 */
public class TestExhaustiveScoreModeNoPruning extends LuceneTestCase {

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

  // ---------------------------------------------------------------------------------------------
  // 1. BatchScoreBulkScorer via TermQuery (the case PR #16542 fixes)
  // ---------------------------------------------------------------------------------------------

  public void testTermQueryCompleteVisitsEveryMatchAtScale() throws Exception {
    for (int numDocs : new int[] {1000, 5000, 20000, 65536, 100000}) {
      try (Directory dir = newDirectory();
          RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          // Varying term counts give the docs different norms, so scores differ and the inner
          // TopScoreDocCollector really does raise a competitive threshold.
          doc.add(new TextField("body", ("hit " + "pad ".repeat(1 + (i % 8))).trim(), Store.NO));
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (IndexReader reader = w.getReader()) {
          IndexSearcher searcher = plainSearcher(reader);
          Query q = new TermQuery(new Term("body", "hit"));
          assertEquals(
              "COMPLETE pruned matches at numDocs=" + numDocs,
              numDocs,
              runCollect(searcher, q, ScoreMode.COMPLETE));
          // TOP_DOCS_WITH_SCORES is NOT exhaustive, so pruning is honored there. This documents
          // the mismatch with the ScoreMode.COMPLETE javadoc, which claims setMinCompetitiveScore
          // is "only honored for TOP_SCORES".
          int topDocsWithScores = runCollect(searcher, q, ScoreMode.TOP_DOCS_WITH_SCORES);
          assertTrue(
              "TOP_DOCS_WITH_SCORES unexpectedly exhaustive at numDocs=" + numDocs,
              topDocsWithScores < numDocs);
          assertEquals(
              "COMPLETE_NO_SCORES pruned matches at numDocs=" + numDocs,
              numDocs,
              runCollect(searcher, q, ScoreMode.COMPLETE_NO_SCORES));
        }
      }
    }
  }

  /** Same as above, but through newSearcher with asserting wrappers and concurrency enabled. */
  public void testTermQueryCompleteWithAssertingSearcher() throws Exception {
    final int numDocs = 20000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new TextField("body", ("hit " + "pad ".repeat(1 + (i % 8))).trim(), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = newSearcher(reader, true, true, true);
        searcher.setQueryCache(null);
        assertEquals(
            numDocs,
            runCollect(searcher, new TermQuery(new Term("body", "hit")), ScoreMode.COMPLETE));
      }
    }
  }

  /** TOP_SCORES must keep pruning: the fix must not disable the optimization. */
  public void testTopScoresStillPrunes() throws Exception {
    final int numDocs = 50000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new TextField("body", ("hit " + "pad ".repeat(1 + (i % 8))).trim(), Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        int collected =
            runCollect(searcher, new TermQuery(new Term("body", "hit")), ScoreMode.TOP_SCORES);
        assertTrue(
            "TOP_SCORES should still skip non-competitive hits, collected=" + collected,
            collected < numDocs);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 2. DenseConjunctionBulkScorer via a FILTER-only BooleanQuery (the reviewer's finding)
  // ---------------------------------------------------------------------------------------------

  public void testFilterOnlyBooleanQueryCompleteVisitsEveryMatch() throws Exception {
    for (int numDocs : new int[] {5000, 20000, 65536, 100000}) {
      try (Directory dir = newDirectory();
          RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
        for (int i = 0; i < numDocs; ++i) {
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
              "FILTER-only COMPLETE pruned matches at numDocs=" + numDocs,
              numDocs,
              runCollect(searcher, q, ScoreMode.COMPLETE));
        }
      }
    }
  }

  /** Same, with three FILTER clauses and a sparser match set that still clears the density bar. */
  public void testFilterOnlyBooleanQueryThreeClausesSparse() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      int expected = 0;
      for (int i = 0; i < numDocs; ++i) {
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
  // 3. DenseConjunctionBulkScorer via ConstantScoreScorerSupplier (single constant-score clause)
  // ---------------------------------------------------------------------------------------------

  public void testConstantScoreQueryCompleteVisitsEveryMatchDup() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        // A non-zero constant score, so nextUp(score) really does exceed it.
        Query q = new ConstantScoreQuery(new TermQuery(new Term("a", "x")));
        assertEquals(
            "ConstantScoreQuery under COMPLETE pruned matches",
            numDocs,
            runCollect(searcher, q, ScoreMode.COMPLETE));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 4. BatchScoreBulkScorer via CombinedFieldQuery (the second call site the PR patches)
  // ---------------------------------------------------------------------------------------------

  public void testCombinedFieldQueryCompleteVisitsEveryMatch() throws Exception {
    for (int numDocs : new int[] {1000, 20000, 65536}) {
      try (Directory dir = newDirectory();
          RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          doc.add(new TextField("t", ("hit " + "pad ".repeat(1 + (i % 8))).trim(), Store.NO));
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
              "CombinedFieldQuery under COMPLETE pruned matches at numDocs=" + numDocs,
              numDocs,
              runCollect(searcher, q, ScoreMode.COMPLETE));
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 5. Cross-check: totalHits from a COMPLETE count must equal the number of collect() calls
  // ---------------------------------------------------------------------------------------------

  public void testCountAgreesWithCollectCalls() throws Exception {
    final int numDocs = 30000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", "y", Store.NO));
        doc.add(new TextField("body", ("hit " + "pad ".repeat(1 + (i % 8))).trim(), Store.NO));
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
  // 6. Other constant-score queries that reach DenseConjunctionBulkScorer via
  //    ConstantScoreScorerSupplier#bulkScorer with a non-zero constant score.
  // ---------------------------------------------------------------------------------------------

  public void testMatchAllDocsQueryCompleteVisitsEveryMatch() throws Exception {
    for (int numDocs : new int[] {5000, 20000, 100000}) {
      try (Directory dir = newDirectory();
          RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          doc.add(new StringField("a", "x", Store.NO));
          w.addDocument(doc);
        }
        w.forceMerge(1);
        try (IndexReader reader = w.getReader()) {
          IndexSearcher searcher = plainSearcher(reader);
          assertEquals(
              "MatchAllDocsQuery under COMPLETE pruned matches at numDocs=" + numDocs,
              numDocs,
              runCollect(searcher, new MatchAllDocsQuery(), ScoreMode.COMPLETE));
        }
      }
    }
  }

  public void testFieldExistsQueryCompleteVisitsEveryMatch() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new org.apache.lucene.document.NumericDocValuesField("n", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        assertEquals(
            "FieldExistsQuery under COMPLETE pruned matches",
            numDocs,
            runCollect(searcher, new FieldExistsQuery("n"), ScoreMode.COMPLETE));
      }
    }
  }

  public void testPointRangeQueryCompleteVisitsEveryMatch() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new org.apache.lucene.document.IntPoint("p", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        assertEquals(
            "IntPoint range under COMPLETE pruned matches",
            numDocs,
            runCollect(
                searcher,
                org.apache.lucene.document.IntPoint.newRangeQuery("p", 0, numDocs),
                ScoreMode.COMPLETE));
      }
    }
  }

  /** ConstantScoreQuery is safe: it wraps the inner bulk scorer in ConstantBulkScorer. */
  public void testConstantScoreQueryIsUnaffected() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        assertEquals(
            numDocs,
            runCollect(
                searcher,
                new ConstantScoreQuery(new TermQuery(new Term("a", "x"))),
                ScoreMode.COMPLETE));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 7. With deletions (acceptDocs != null) the "whole run matches" shortcut in
  //    DenseConjunctionBulkScorer#scoreWindow no longer applies, so MatchAllDocsQuery,
  //    FieldExistsQuery and point ranges go window-by-window and hit the pruning check.
  //    A single deleted document is enough.
  // ---------------------------------------------------------------------------------------------

  public void testConstantScoreQueriesWithDeletionsUnderComplete() throws Exception {
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        doc.add(new StringField("b", "y", Store.NO));
        doc.add(new org.apache.lucene.document.NumericDocValuesField("n", i));
        doc.add(new org.apache.lucene.document.IntPoint("p", i));
        if (i == 7) {
          doc.add(new StringField("del", "yes", Store.NO));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.deleteDocuments(new Term("del", "yes"));
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        final int live = numDocs - 1;
        assertEquals(live, reader.numDocs());
        Query[] queries = {
          new MatchAllDocsQuery(),
          new FieldExistsQuery("n"),
          org.apache.lucene.document.IntPoint.newRangeQuery("p", 0, numDocs),
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
    final int numDocs = 100000;
    try (Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new StringField("a", "x", Store.NO));
        if (i == 7) {
          doc.add(new StringField("del", "yes", Store.NO));
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.deleteDocuments(new Term("del", "yes"));
      try (IndexReader reader = w.getReader()) {
        IndexSearcher searcher = plainSearcher(reader);
        int collected = runCollect(searcher, new MatchAllDocsQuery(), ScoreMode.TOP_SCORES);
        assertTrue(
            "TOP_SCORES should still prune, collected=" + collected, collected < numDocs - 1);
      }
    }
  }
}
