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

package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFunctionScoreQuery extends FunctionTestSetup {

  static IndexReader reader;
  static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
  }

  public void testEqualities() {

    Query q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(1));
    Query q2 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(1));
    Query q3 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));
    Query q4 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));

    QueryUtils.check(q1);
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, q2);
    QueryUtils.checkUnequal(q2, q3);
    QueryUtils.checkEqual(q3, q4);

    Query bq1 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(2));
    QueryUtils.check(bq1);
    Query bq2 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq2);
    Query bq3 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq3);
    QueryUtils.checkUnequal(bq2, bq3);
    Query bq4 =
        FunctionScoreQuery.boostByValue(
            new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkEqual(bq3, bq4);

    Query qq1 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.check(qq1);
    Query qq2 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.2f);
    QueryUtils.checkUnequal(qq1, qq2);
    Query qq3 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "b")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.checkUnequal(qq1, qq3);
    QueryUtils.checkUnequal(qq2, qq3);
    Query qq4 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "zz")),
            0.1f);
    QueryUtils.checkUnequal(qq1, qq4);
    QueryUtils.checkUnequal(qq2, qq4);
    QueryUtils.checkUnequal(qq3, qq4);
    Query qq5 =
        FunctionScoreQuery.boostByQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")),
            new TermQuery(new Term(TEXT_FIELD, "z")),
            0.1f);
    QueryUtils.checkEqual(qq1, qq5);
  }

  // FunctionQuery equivalent
  public void testSimpleSourceScore() throws Exception {

    FunctionScoreQuery q =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "first")),
            DoubleValuesSource.fromIntField(INT_FIELD));

    QueryUtils.check(random(), q, searcher, rarely());

    int[] expectedDocs = new int[] {4, 7, 9};
    TopDocs docs = searcher.search(q, 4);
    assertEquals(expectedDocs.length, docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(docs.scoreDocs[i].doc, expectedDocs[i]);
    }
  }

  // CustomScoreQuery and BoostedQuery equivalent
  public void testScoreModifyingSource() throws Exception {

    BooleanQuery bq =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD)
            .build();
    TopDocs plain = searcher.search(bq, 1);

    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByValue(bq, DoubleValuesSource.fromIntField("iii"));

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[] {4, 7, 9, 8, 12};
    TopDocs docs = searcher.search(fq, 5);
    assertEquals(plain.totalHits.value(), docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);
    }

    Explanation expl = searcher.explain(fq, 4);
    assertTrue(expl.toString().contains("first"));
    assertTrue(expl.toString().contains("iii"));
  }

  // BoostingQuery equivalent
  public void testCombiningMultipleQueryScores() throws Exception {

    TermQuery q = new TermQuery(new Term(TEXT_FIELD, "text"));
    TopDocs plain = searcher.search(q, 1);

    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByQuery(q, new TermQuery(new Term(TEXT_FIELD, "rechecking")), 100f);

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[] {6, 1, 0, 2, 8};
    TopDocs docs = searcher.search(fq, 20);
    assertEquals(plain.totalHits.value(), docs.totalHits.value());
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);
    }

    Explanation expl = searcher.explain(fq, 6);
    assertTrue(expl.toString().contains("rechecking"));
    assertTrue(expl.toString().contains("text"));
  }

  // check boosts with non-distributive score source
  public void testBoostsAreAppliedLast() throws Exception {

    SimpleBindings bindings = new SimpleBindings();
    bindings.add("score", DoubleValuesSource.SCORES);
    Expression expr = JavascriptCompiler.compile("ln(score + 4)");

    Query q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "text")), expr.getDoubleValuesSource(bindings));
    TopDocs plain = searcher.search(q1, 5);

    Query boosted = new BoostQuery(q1, 2);
    TopDocs afterboost = searcher.search(boosted, 5);
    assertEquals(plain.totalHits.value(), afterboost.totalHits.value());
    for (int i = 0; i < 5; i++) {
      assertEquals(plain.scoreDocs[i].doc, afterboost.scoreDocs[i].doc);
      assertEquals(plain.scoreDocs[i].score, afterboost.scoreDocs[i].score / 2, 0.0001);
    }
  }

  public void testTruncateNegativeScores() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", -2));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q =
        new FunctionScoreQuery(MatchAllDocsQuery.INSTANCE, DoubleValuesSource.fromLongField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(expl.toString(), expl.getDetails()[0].getDescription().contains("truncated score"));
    reader.close();
    dir.close();
  }

  public void testNaN() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", Double.doubleToLongBits(Double.NaN)));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q =
        new FunctionScoreQuery(
            MatchAllDocsQuery.INSTANCE, DoubleValuesSource.fromDoubleField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(
        expl.toString(), expl.getDetails()[0].getDescription().contains("NaN is an illegal score"));
    reader.close();
    dir.close();
  }

  // check access to the score source of a functionScoreQuery
  public void testAccessToValueSource() throws Exception {

    FunctionScoreQuery q1 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(31));
    Query q2 = new FunctionScoreQuery(q1.getWrappedQuery(), q1.getSource());
    QueryUtils.check(q2);
    QueryUtils.checkEqual(q2, q1);

    FunctionScoreQuery q3 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "first")),
            DoubleValuesSource.fromIntField(INT_FIELD));
    Query q4 = new FunctionScoreQuery(q3.getWrappedQuery(), q3.getSource());
    QueryUtils.checkEqual(q3, q4);

    SimpleBindings bindings = new SimpleBindings();
    bindings.add("score", DoubleValuesSource.SCORES);
    Expression expr = JavascriptCompiler.compile("ln(score + 4)");
    FunctionScoreQuery q5 =
        new FunctionScoreQuery(
            new TermQuery(new Term(TEXT_FIELD, "text")), expr.getDoubleValuesSource(bindings));
    Query q6 = new FunctionScoreQuery(q5.getWrappedQuery(), q5.getSource());
    QueryUtils.checkEqual(q5, q6);
  }

  public void testScoreMode() throws Exception {
    // Value Source doesn't need scores
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.COMPLETE,
        DoubleValuesSource.fromDoubleField("foo"));
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.COMPLETE_NO_SCORES,
        DoubleValuesSource.fromDoubleField("foo"));
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES,
        ScoreMode.TOP_SCORES,
        DoubleValuesSource.fromDoubleField("foo"));

    // Value Source needs scores
    assertInnerScoreMode(ScoreMode.COMPLETE, ScoreMode.COMPLETE, DoubleValuesSource.SCORES);
    assertInnerScoreMode(
        ScoreMode.COMPLETE_NO_SCORES, ScoreMode.COMPLETE_NO_SCORES, DoubleValuesSource.SCORES);
    assertInnerScoreMode(ScoreMode.COMPLETE, ScoreMode.TOP_SCORES, DoubleValuesSource.SCORES);
  }

  private void assertInnerScoreMode(
      ScoreMode expectedScoreMode, ScoreMode inputScoreMode, DoubleValuesSource valueSource)
      throws IOException {
    final AtomicReference<ScoreMode> scoreModeInWeight = new AtomicReference<>();
    Query innerQ =
        new TermQuery(new Term(TEXT_FIELD, "a")) {

          @Override
          public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
              throws IOException {
            scoreModeInWeight.set(scoreMode);
            return super.createWeight(searcher, scoreMode, boost);
          }
        };

    FunctionScoreQuery fq = new FunctionScoreQuery(innerQ, valueSource);
    fq.createWeight(searcher, inputScoreMode, 1f);
    assertEquals(expectedScoreMode, scoreModeInWeight.get());
  }

  /** The FunctionScoreQuery's Scorer score() is going to be called twice for the same doc. */
  public void testScoreCalledTwice() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      IndexWriter indexWriter = new IndexWriter(dir, conf);
      Document doc = new Document();
      doc.add(new TextField("ExampleText", "periodic function", Field.Store.NO));
      doc.add(new TextField("ExampleText", "plot of the original function", Field.Store.NO));
      indexWriter.addDocument(doc);
      indexWriter.commit();
      indexWriter.close();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Query q = new TermQuery(new Term("ExampleText", "function"));

        q =
            FunctionScoreQuery.boostByQuery(
                q, new PhraseQuery(1, "ExampleText", "function", "plot"), 2);
        q = FunctionScoreQuery.boostByValue(q, DoubleValuesSource.SCORES);

        assertEquals(1, new IndexSearcher(reader).search(q, 10).totalHits.value());
      }
    }
  }

  // Weight#count is delegated to the inner weight
  public void testQueryMatchesCount() throws Exception {
    TermQuery query = new TermQuery(new Term(TEXT_FIELD, "first"));
    FunctionScoreQuery fq =
        FunctionScoreQuery.boostByValue(query, DoubleValuesSource.fromIntField("iii"));

    final int searchCount = searcher.count(fq);
    final Weight weight = searcher.createWeight(fq, ScoreMode.COMPLETE, 1);
    int weightCount = 0;
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      weightCount += weight.count(leafReaderContext);
    }
    assertEquals(searchCount, weightCount);
  }

  public void testMaxScoreDelegationWithDocValuesSkipper() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 500; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "test doc", Field.Store.NO));
          doc.add(new NumericDocValuesField("val", i * 10));
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        DoubleValuesSource valueSource = DoubleValuesSource.fromLongField("val");
        Query q = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "test")), valueSource);

        Weight weight = searcher.createWeight(q, ScoreMode.TOP_SCORES, 1f);
        LeafReaderContext ctx = reader.leaves().get(0);
        var scorerSupplier = weight.scorerSupplier(ctx);
        assertNotNull(scorerSupplier);
        var scorer = scorerSupplier.get(Long.MAX_VALUE);
        assertNotNull(scorer);

        int maxDoc = ctx.reader().maxDoc();
        int shallowEnd = scorer.advanceShallow(0);
        assertTrue(shallowEnd >= 0);

        float maxScore = scorer.getMaxScore(maxDoc);
        if (ctx.reader().getDocValuesSkipper("val") != null) {
          assertFalse(Float.isInfinite(maxScore));
          assertTrue(maxScore >= 5000f);
        } else {
          assertTrue(Float.isInfinite(maxScore));
        }
      }
    }
  }

  public void testMaxScorePruningTopDocs() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 200; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "prune search", Field.Store.NO));
          doc.add(new NumericDocValuesField("score_val", i));
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query baseQuery = new TermQuery(new Term(TEXT_FIELD, "prune"));
        DoubleValuesSource valSource = DoubleValuesSource.fromLongField("score_val");
        Query scriptQuery = new FunctionScoreQuery(baseQuery, valSource);

        TopDocs topDocs = searcher.search(scriptQuery, 5);
        assertEquals(5, topDocs.scoreDocs.length);
        // Highest numeric values (200, 199, 198, 197, 196) must be returned
        assertTrue(topDocs.scoreDocs[0].score >= 196f);
      }
    }
  }

  public void testMaxScoreMonotonicDecreasingFunction() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 100; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "decreasing", Field.Store.NO));
          doc.add(new NumericDocValuesField("val", i * 10)); // 10..1000
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query baseQuery = new TermQuery(new Term(TEXT_FIELD, "decreasing"));
        // Monotonically decreasing function: f(x) = 10000.0 / x (Monotonicity.DECREASING)
        DoubleValuesSource valSource =
            DoubleValuesSource.fromField(
                "val", (v) -> 10000.0 / v, DoubleValuesSource.Monotonicity.DECREASING);
        Query scriptQuery = new FunctionScoreQuery(baseQuery, valSource);

        LeafReaderContext ctx = reader.leaves().get(0);
        Weight weight = scriptQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        ScorerSupplier supplier = weight.scorerSupplier(ctx);
        assertNotNull(supplier);
        Scorer scorer = supplier.get(Long.MAX_VALUE);

        int maxDoc = ctx.reader().maxDoc();
        scorer.advanceShallow(0);
        float maxScore = scorer.getMaxScore(maxDoc);

        if (ctx.reader().getDocValuesSkipper("val") != null) {
          // Minimum raw value (10) produces max score: 10000 / 10 = 1000.0
          assertFalse(Float.isInfinite(maxScore));
          assertTrue("Expected maxScore >= 1000f but got " + maxScore, maxScore >= 1000f);
        }
      }
    }
  }

  public void testCustomHomemadeIncreasingAndDecreasingFunction() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 100; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "custom test", Field.Store.NO));
          doc.add(new NumericDocValuesField("val", i * 5)); // 5..500
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query baseQuery = new TermQuery(new Term(TEXT_FIELD, "custom"));

        // Homemade custom DoubleValuesSource class: f(x) = sqrt(x) [Increasing]
        DoubleValuesSource customIncreasing =
            new DoubleValuesSource() {
              @Override
              public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores)
                  throws IOException {
                DoubleValues in = DoubleValuesSource.fromLongField("val").getValues(ctx, scores);
                return new DoubleValues() {
                  @Override
                  public double doubleValue() throws IOException {
                    return Math.sqrt(in.doubleValue());
                  }

                  @Override
                  public boolean advanceExact(int doc) throws IOException {
                    return in.advanceExact(doc);
                  }

                  @Override
                  public int advanceShallow(int target) throws IOException {
                    return in.advanceShallow(target);
                  }

                  @Override
                  public float getMaxScore(int upTo) throws IOException {
                    float innerMax = in.getMaxScore(upTo);
                    return Float.isInfinite(innerMax)
                        ? Float.POSITIVE_INFINITY
                        : (float) Math.sqrt(innerMax);
                  }
                };
              }

              @Override
              public boolean needsScores() {
                return false;
              }

              @Override
              public boolean isCacheable(LeafReaderContext ctx) {
                return true;
              }

              @Override
              public DoubleValuesSource rewrite(IndexSearcher searcher) {
                return this;
              }

              @Override
              public boolean equals(Object o) {
                return o == this;
              }

              @Override
              public int hashCode() {
                return System.identityHashCode(this);
              }

              @Override
              public String toString() {
                return "customSqrt(val)";
              }
            };

        Query q = new FunctionScoreQuery(baseQuery, customIncreasing);
        LeafReaderContext ctx = reader.leaves().get(0);
        Weight weight = q.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        ScorerSupplier supplier = weight.scorerSupplier(ctx);
        assertNotNull(supplier);
        Scorer scorer = supplier.get(Long.MAX_VALUE);
        scorer.advanceShallow(0);
        float maxScore = scorer.getMaxScore(ctx.reader().maxDoc());

        if (ctx.reader().getDocValuesSkipper("val") != null) {
          // Raw max is 500, sqrt(500) ~ 22.36
          assertFalse(Float.isInfinite(maxScore));
          assertTrue(maxScore >= 22.3f);
        }
      }
    }
  }

  public void testMaxScoreNonMonotonicFunction() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 100; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "nonmonotonic", Field.Store.NO));
          doc.add(new NumericDocValuesField("val", i * 10));
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query baseQuery = new TermQuery(new Term(TEXT_FIELD, "nonmonotonic"));

        // A non-monotonic function (neither increasing nor decreasing)
        // specifying Monotonicity.NONE
        DoubleValuesSource valSource =
            DoubleValuesSource.fromField(
                "val", (v) -> Math.sin((double) v), DoubleValuesSource.Monotonicity.NONE);
        Query scriptQuery = new FunctionScoreQuery(baseQuery, valSource);

        LeafReaderContext ctx = reader.leaves().get(0);
        Weight weight = scriptQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        ScorerSupplier supplier = weight.scorerSupplier(ctx);
        assertNotNull(supplier);
        Scorer scorer = supplier.get(Long.MAX_VALUE);

        int maxDoc = ctx.reader().maxDoc();
        scorer.advanceShallow(0);
        float maxScore = scorer.getMaxScore(maxDoc);

        // Since the function is neither increasing nor decreasing (Monotonicity.NONE),
        // we cannot compute a tight block-level max score bound using the skipper.
        // Therefore, it must return Float.POSITIVE_INFINITY, meaning the doc block
        // cannot be pruned or skipped.
        assertEquals(Float.POSITIVE_INFINITY, maxScore, 0f);
      }
    }
  }

  public void testConstantLambdaMonotonicity() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig conf = newIndexWriterConfig();
      try (IndexWriter indexWriter = new IndexWriter(dir, conf)) {
        for (int i = 1; i <= 100; i++) {
          Document doc = new Document();
          doc.add(new TextField(TEXT_FIELD, "constant test", Field.Store.NO));
          doc.add(new NumericDocValuesField("val", i * 10));
          indexWriter.addDocument(doc);
        }
        indexWriter.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query baseQuery = new TermQuery(new Term(TEXT_FIELD, "constant"));

        // Create the three value sources with different monotonicity settings,
        // all using the same constant lambda function: f(v) = 5.0
        DoubleValuesSource sourceIncreasing =
            DoubleValuesSource.fromField(
                "val", (_) -> 5.0, DoubleValuesSource.Monotonicity.INCREASING);
        DoubleValuesSource sourceDecreasing =
            DoubleValuesSource.fromField(
                "val", (_) -> 5.0, DoubleValuesSource.Monotonicity.DECREASING);
        DoubleValuesSource sourceNone =
            DoubleValuesSource.fromField("val", (_) -> 5.0, DoubleValuesSource.Monotonicity.NONE);

        Query qIncreasing = new FunctionScoreQuery(baseQuery, sourceIncreasing);
        Query qDecreasing = new FunctionScoreQuery(baseQuery, sourceDecreasing);
        Query qNone = new FunctionScoreQuery(baseQuery, sourceNone);

        // 1. Verify that all three queries produce the exact same top docs and scores
        TopDocs tdIncreasing = searcher.search(qIncreasing, 10);
        TopDocs tdDecreasing = searcher.search(qDecreasing, 10);
        TopDocs tdNone = searcher.search(qNone, 10);

        assertEquals(tdIncreasing.totalHits.value(), tdDecreasing.totalHits.value());
        assertEquals(tdIncreasing.totalHits.value(), tdNone.totalHits.value());
        assertEquals(tdIncreasing.scoreDocs.length, tdDecreasing.scoreDocs.length);
        assertEquals(tdIncreasing.scoreDocs.length, tdNone.scoreDocs.length);

        for (int i = 0; i < tdIncreasing.scoreDocs.length; i++) {
          assertEquals(tdIncreasing.scoreDocs[i].doc, tdDecreasing.scoreDocs[i].doc);
          assertEquals(tdIncreasing.scoreDocs[i].doc, tdNone.scoreDocs[i].doc);
          assertEquals(tdIncreasing.scoreDocs[i].score, tdDecreasing.scoreDocs[i].score, 1e-5f);
          assertEquals(tdIncreasing.scoreDocs[i].score, tdNone.scoreDocs[i].score, 1e-5f);
        }

        // 2. Verify that they calculate appropriate maxScore bounds
        LeafReaderContext ctx = reader.leaves().get(0);

        // INCREASING
        Weight wInc = qIncreasing.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        Scorer scorerInc = wInc.scorerSupplier(ctx).get(Long.MAX_VALUE);
        scorerInc.advanceShallow(0);
        float maxScoreInc = scorerInc.getMaxScore(ctx.reader().maxDoc());

        // DECREASING
        Weight wDec = qDecreasing.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        Scorer scorerDec = wDec.scorerSupplier(ctx).get(Long.MAX_VALUE);
        scorerDec.advanceShallow(0);
        float maxScoreDec = scorerDec.getMaxScore(ctx.reader().maxDoc());

        // NONE
        Weight wNone = qNone.createWeight(searcher, ScoreMode.TOP_SCORES, 1f);
        Scorer scorerNone = wNone.scorerSupplier(ctx).get(Long.MAX_VALUE);
        scorerNone.advanceShallow(0);
        float maxScoreNone = scorerNone.getMaxScore(ctx.reader().maxDoc());

        if (ctx.reader().getDocValuesSkipper("val") != null) {
          // If skipper exists, INCREASING and DECREASING should compute a tight finite max score
          // (5.0 * innerMaxScore)
          assertFalse(Float.isInfinite(maxScoreInc));
          assertFalse(Float.isInfinite(maxScoreDec));
          assertEquals(maxScoreInc, maxScoreDec, 1e-5f);
        }

        // NONE must always return Float.POSITIVE_INFINITY
        assertEquals(Float.POSITIVE_INFINITY, maxScoreNone, 0f);
      }
    }
  }
}
