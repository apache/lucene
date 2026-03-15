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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BayesianBM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for {@link LogOddsConjunctionQuery}. */
public class TestLogOddsConjunctionQuery extends LuceneTestCase {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter writer = new IndexWriter(dir, config);

    // doc0: matches "alpha" and "beta" in body
    Document doc0 = new Document();
    doc0.add(new TextField("body", "alpha beta gamma", Field.Store.YES));
    writer.addDocument(doc0);

    // doc1: matches only "alpha"
    Document doc1 = new Document();
    doc1.add(new TextField("body", "alpha gamma delta", Field.Store.YES));
    writer.addDocument(doc1);

    // doc2: matches only "beta"
    Document doc2 = new Document();
    doc2.add(new TextField("body", "beta gamma delta", Field.Store.YES));
    writer.addDocument(doc2);

    // doc3: matches neither
    Document doc3 = new Document();
    doc3.add(new TextField("body", "gamma delta epsilon", Field.Store.YES));
    writer.addDocument(doc3);

    writer.forceMerge(1);
    reader = DirectoryReader.open(writer);
    writer.close();
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new BayesianBM25Similarity());
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testBasicFusion() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    QueryUtils.check(random(), loq, searcher);

    ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;

    // doc0 matches both terms, doc1 matches only alpha, doc2 matches only beta
    assertTrue("should have at least 1 hit", hits.length >= 1);

    // All scores should be in (0, 1) since they are probabilities
    for (ScoreDoc hit : hits) {
      assertTrue("score should be > 0, got " + hit.score, hit.score > 0);
      assertTrue("score should be < 1, got " + hit.score, hit.score < 1);
    }

    // doc0 (matching both) should score highest
    if (hits.length >= 2) {
      assertTrue(
          "doc matching both terms should score higher than doc matching one",
          hits[0].score >= hits[1].score);
    }
  }

  public void testSingleClauseRewrite() throws Exception {
    Query sub = new TermQuery(new Term("body", "alpha"));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Collections.singletonList(sub));

    Query rewritten = searcher.rewrite(loq);
    // Single clause should rewrite to the inner query
    assertTrue(
        "single clause should rewrite to TermQuery, got " + rewritten.getClass().getName(),
        rewritten instanceof TermQuery);
  }

  public void testEmptyClauseRewrite() throws Exception {
    LogOddsConjunctionQuery loq =
        new LogOddsConjunctionQuery(Collections.emptyList(), 0.5f);

    Query rewritten = searcher.rewrite(loq);
    assertTrue(
        "empty should rewrite to MatchNoDocsQuery",
        rewritten instanceof MatchNoDocsQuery);
  }

  public void testAlphaValues() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    // alpha=0 means no scaling: meanLogit * n^0 = meanLogit * 1
    LogOddsConjunctionQuery loq0 = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.0f);
    ScoreDoc[] hits0 = searcher.search(loq0, 10).scoreDocs;

    // alpha=0.5 means sqrt(n) scaling
    LogOddsConjunctionQuery loq05 = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    ScoreDoc[] hits05 = searcher.search(loq05, 10).scoreDocs;

    // alpha=1.0 means n scaling (strongest agreement bonus)
    LogOddsConjunctionQuery loq1 = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 1.0f);
    ScoreDoc[] hits1 = searcher.search(loq1, 10).scoreDocs;

    // All should produce valid results
    assertTrue("alpha=0 should have hits", hits0.length > 0);
    assertTrue("alpha=0.5 should have hits", hits05.length > 0);
    assertTrue("alpha=1.0 should have hits", hits1.length > 0);

    // Scores should be valid probabilities
    for (ScoreDoc[] hits : new ScoreDoc[][] {hits0, hits05, hits1}) {
      for (ScoreDoc hit : hits) {
        assertTrue("score should be finite", Float.isFinite(hit.score));
        assertTrue("score should be > 0", hit.score > 0);
        assertTrue("score should be < 1", hit.score < 1);
      }
    }
  }

  public void testIllegalAlpha() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogOddsConjunctionQuery(Collections.emptyList(), -0.1f));
    assertTrue(expected.getMessage().contains("alpha"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogOddsConjunctionQuery(Collections.emptyList(), 1.1f));
    assertTrue(expected.getMessage().contains("alpha"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogOddsConjunctionQuery(Collections.emptyList(), Float.NaN));
    assertTrue(expected.getMessage().contains("alpha"));
  }

  public void testNonMatchingSubQueries() throws Exception {
    Query q1 = new TermQuery(new Term("body", "nonexistent1"));
    Query q2 = new TermQuery(new Term("body", "nonexistent2"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;
    assertEquals("no docs should match nonexistent terms", 0, hits.length);
  }

  public void testExplanation() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);

    Weight w = searcher.createWeight(searcher.rewrite(loq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    // doc0 matches both
    Explanation explanation = w.explain(context, 0);
    assertTrue("doc0 should match", explanation.isMatch());
    assertTrue(
        "explanation should mention log-odds",
        explanation.getDescription().contains("log-odds"));

    // doc3 matches neither
    Explanation noMatchExpl = w.explain(context, 3);
    assertFalse("doc3 should not match", noMatchExpl.isMatch());
  }

  public void testEqualsAndHashCode() {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery a = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    LogOddsConjunctionQuery b = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    LogOddsConjunctionQuery c = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.3f);
    LogOddsConjunctionQuery d = new LogOddsConjunctionQuery(Arrays.asList(q2, q1), 0.5f);

    // Same clauses, same alpha
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    // Different alpha
    assertNotEquals(a, c);

    // Different clause order should still be equal (Multiset-based)
    assertEquals(a, d);
    assertEquals(a.hashCode(), d.hashCode());
  }

  public void testQueryUtils() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    QueryUtils.check(random(), loq, searcher);
  }

  public void testMaxScoreCorrectness() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);

    // Use CheckHits to validate that max scores are correct
    CheckHits.checkTopScores(random(), loq, searcher);
  }

  public void testToString() {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    String str = loq.toString("body");
    assertTrue("toString should contain LogOdds", str.contains("LogOdds"));
    assertTrue("toString should contain alpha", str.contains("0.5"));
  }

  public void testThreeWayCombination() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));
    Query q3 = new TermQuery(new Term("body", "gamma"));

    LogOddsConjunctionQuery loq =
        new LogOddsConjunctionQuery(Arrays.asList(q1, q2, q3), 0.5f);
    QueryUtils.check(random(), loq, searcher);

    ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;
    assertTrue("should have hits", hits.length > 0);

    // All scores should be valid probabilities
    for (ScoreDoc hit : hits) {
      assertTrue("score should be > 0", hit.score > 0);
      assertTrue("score should be < 1", hit.score < 1);
    }
  }

  public void testRandomTopDocs() throws Exception {
    // Build a larger index for more thorough testing
    Directory testDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter w = new IndexWriter(testDir, config);

    int numDocs = atLeast(100);
    String[] terms = {"alpha", "beta", "gamma", "delta", "epsilon"};
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      StringBuilder sb = new StringBuilder();
      for (String term : terms) {
        if (random().nextDouble() < 0.3) {
          int count = 1 + random().nextInt(5);
          for (int j = 0; j < count; j++) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(term);
          }
        }
      }
      if (sb.length() == 0) {
        sb.append("filler");
      }
      doc.add(new TextField("body", sb.toString(), Field.Store.NO));
      w.addDocument(doc);
    }

    IndexReader testReader = DirectoryReader.open(w);
    w.close();
    IndexSearcher testSearcher = newSearcher(testReader);
    testSearcher.setSimilarity(new BayesianBM25Similarity());

    // Test various combinations
    for (int i = 0; i < 4; i++) {
      List<Query> clauses = new java.util.ArrayList<>();
      int numClauses = 2 + random().nextInt(3);
      for (int j = 0; j < numClauses; j++) {
        clauses.add(new TermQuery(new Term("body", terms[random().nextInt(terms.length)])));
      }
      float alpha = random().nextFloat();
      LogOddsConjunctionQuery query = new LogOddsConjunctionQuery(clauses, alpha);
      CheckHits.checkTopScores(random(), query, testSearcher);
    }

    testReader.close();
    testDir.close();
  }

  // ---- Hybrid search tests: BayesianBM25 + LogOddsConjunction end-to-end ----

  /**
   * Verifies that BayesianBM25Similarity produces scores strictly in (0, 1) before they are fed
   * into LogOddsConjunctionQuery. This is a prerequisite for the log-odds combination to work
   * correctly, since logit(p) is only defined for p in (0, 1).
   */
  public void testBayesianBM25ProducesProbabilityScores() throws Exception {
    Query q = new TermQuery(new Term("body", "alpha"));
    // Search with BayesianBM25Similarity (set in setUp)
    TopDocs topDocs = searcher.search(q, 10);
    assertTrue("should have hits", topDocs.scoreDocs.length > 0);

    for (ScoreDoc hit : topDocs.scoreDocs) {
      assertTrue(
          "BayesianBM25 score should be > 0 (got " + hit.score + ")", hit.score > 0f);
      assertTrue(
          "BayesianBM25 score should be < 1 (got " + hit.score + ")", hit.score < 1f);
    }
  }

  /**
   * Multi-field hybrid search: combines title and body field queries via LogOddsConjunction.
   * Verifies that a document matching in both fields ranks higher than documents matching in only
   * one field. With softplus gating, any match contributes a positive value (softplus(logit) &gt; 0),
   * so matching in more fields always helps regardless of corpus size or IDF values.
   */
  public void testMultiFieldHybridSearch() throws Exception {
    Directory hybridDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter writer = new IndexWriter(hybridDir, config);

    // doc0: "lucene" in both title and body
    Document doc0 = new Document();
    doc0.add(new TextField("title", "lucene search engine", Field.Store.YES));
    doc0.add(new TextField("body", "lucene is a full-text search library", Field.Store.YES));
    writer.addDocument(doc0);

    // doc1: "lucene" in title only (same title content as doc0 for fair comparison)
    Document doc1 = new Document();
    doc1.add(new TextField("title", "lucene search engine", Field.Store.YES));
    doc1.add(new TextField("body", "this is about information retrieval", Field.Store.YES));
    writer.addDocument(doc1);

    // doc2: "lucene" in body only
    Document doc2 = new Document();
    doc2.add(new TextField("title", "search technology overview", Field.Store.YES));
    doc2.add(new TextField("body", "lucene provides inverted index capabilities", Field.Store.YES));
    writer.addDocument(doc2);

    // doc3: "lucene" in neither
    Document doc3 = new Document();
    doc3.add(new TextField("title", "database systems", Field.Store.YES));
    doc3.add(new TextField("body", "relational databases use SQL", Field.Store.YES));
    writer.addDocument(doc3);

    writer.forceMerge(1);
    IndexReader hybridReader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher hybridSearcher = new IndexSearcher(hybridReader);
    hybridSearcher.setSimilarity(new BayesianBM25Similarity());

    // Combine title and body queries via LogOddsConjunction
    Query titleQuery = new TermQuery(new Term("title", "lucene"));
    Query bodyQuery = new TermQuery(new Term("body", "lucene"));
    LogOddsConjunctionQuery hybridQuery =
        new LogOddsConjunctionQuery(Arrays.asList(titleQuery, bodyQuery), 0.5f);

    ScoreDoc[] hits = hybridSearcher.search(hybridQuery, 10).scoreDocs;

    // Should match 3 docs (doc0, doc1, doc2); doc3 matches neither field
    assertEquals("should have 3 hits", 3, hits.length);

    // All scores in (0, 1)
    for (ScoreDoc hit : hits) {
      assertTrue("hybrid score should be > 0, got " + hit.score, hit.score > 0f);
      assertTrue("hybrid score should be < 1, got " + hit.score, hit.score < 1f);
    }

    // doc0 matches both fields. With softplus gating, each match contributes
    // softplus(logit(p)) > 0, so matching in more fields always increases the score.
    // doc0 and doc1 have identical titles, so doc0's extra body match must rank it higher.
    assertEquals("doc matching both fields should rank first", 0, hits[0].doc);
    assertTrue(
        "doc matching both fields (" + hits[0].score + ") should score > "
            + "doc matching one field (" + hits[1].score + ")",
        hits[0].score > hits[1].score);

    // doc3 should not appear
    for (ScoreDoc hit : hits) {
      assertNotEquals("doc3 should not match", 3, hit.doc);
    }

    hybridReader.close();
    hybridDir.close();
  }

  /**
   * Tests that adding more matching signals never decreases the combined score. With softplus
   * gating, matching sub-scorers contribute softplus(logit(p)) &gt; 0 -- even weak matches
   * contribute a positive value. This means more matches can only help, never hurt.
   *
   * <p>This is the key semantic property: "absence of evidence is not evidence of absence."
   * A document that matches a query term (even weakly) should never score worse than if it
   * did not match that term at all.
   */
  public void testMoreSignalsIncreaseScore() throws Exception {
    Directory signalDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter writer = new IndexWriter(signalDir, config);

    // Target document: matches "search" in three fields
    Document doc = new Document();
    doc.add(new TextField("f1", "search engine optimization", Field.Store.NO));
    doc.add(new TextField("f2", "search algorithms and data structures", Field.Store.NO));
    doc.add(new TextField("f3", "search relevance tuning", Field.Store.NO));
    writer.addDocument(doc);

    writer.forceMerge(1);
    IndexReader signalReader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher signalSearcher = new IndexSearcher(signalReader);
    signalSearcher.setSimilarity(new BayesianBM25Similarity());

    Query q1 = new TermQuery(new Term("f1", "search"));
    Query q2 = new TermQuery(new Term("f2", "search"));
    Query q3 = new TermQuery(new Term("f3", "search"));

    // Single signal (raw BayesianBM25 score)
    float score1 = signalSearcher.search(q1, 1).scoreDocs[0].score;

    // 2-signal combination
    LogOddsConjunctionQuery twoSignals =
        new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    float score2 = signalSearcher.search(twoSignals, 1).scoreDocs[0].score;

    // 3-signal combination
    LogOddsConjunctionQuery threeSignals =
        new LogOddsConjunctionQuery(Arrays.asList(q1, q2, q3), 0.5f);
    float score3 = signalSearcher.search(threeSignals, 1).scoreDocs[0].score;

    // With softplus gating, more matching signals always produce >= score.
    // Even if individual BayesianBM25 scores are < 0.5 (low IDF in a 1-doc index),
    // softplus gating ensures they contribute a positive value rather than negative evidence.
    assertTrue(
        "3 signals (" + score3 + ") should score >= 2 signals (" + score2 + ")",
        score3 >= score2);
    assertTrue(
        "2 signals (" + score2 + ") should score >= 1 signal (" + score1 + ")",
        score2 >= score1);

    // All remain valid probabilities
    assertTrue("1-signal score in (0,1)", score1 > 0 && score1 < 1);
    assertTrue("2-signal score in (0,1)", score2 > 0 && score2 < 1);
    assertTrue("3-signal score in (0,1)", score3 > 0 && score3 < 1);

    signalReader.close();
    signalDir.close();
  }

  /**
   * Verifies the full explanation tree from BayesianBM25 posterior through LogOdds sigmoid.
   * Weight.explain() wraps the SimScorer explanation, so the structure is:
   *
   * <pre>
   *   LogOdds conjunction (top-level)
   *     weight(body:alpha) [BayesianBM25Similarity]  (per-clause weight explanation)
   *       Bayesian posterior                          (SimScorer explanation)
   *         likelihood
   *         tfPrior
   * </pre>
   */
  public void testHybridExplanationPipeline() throws Exception {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);

    Weight w = searcher.createWeight(searcher.rewrite(loq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    // doc0 matches both alpha and beta
    Explanation expl = w.explain(context, 0);
    assertTrue("should match", expl.isMatch());

    // Top-level: LogOdds conjunction
    String topDesc = expl.getDescription();
    assertTrue(
        "top-level should describe log-odds conjunction, got: " + topDesc,
        topDesc.contains("log-odds conjunction"));

    // Score should be in (0, 1)
    float topScore = expl.getValue().floatValue();
    assertTrue("top score should be > 0", topScore > 0f);
    assertTrue("top score should be < 1", topScore < 1f);

    // Sub-explanations: one per matching clause (weight-level explanations)
    Explanation[] details = expl.getDetails();
    assertTrue("should have at least 2 sub-explanations", details.length >= 2);

    for (Explanation detail : details) {
      assertTrue("sub should match", detail.isMatch());

      // Sub-score should be in (0, 1) since BayesianBM25 produces probabilities
      float subScore = detail.getValue().floatValue();
      assertTrue("sub score should be > 0, got " + subScore, subScore > 0f);
      assertTrue("sub score should be < 1, got " + subScore, subScore < 1f);

      // The weight-level explanation wraps the BayesianBM25 SimScorer explanation.
      // Search recursively for "Bayesian posterior", "likelihood", and "tfPrior".
      assertTrue(
          "explanation tree should contain 'Bayesian posterior' somewhere",
          containsDescription(detail, "Bayesian posterior"));
      assertTrue(
          "explanation tree should contain 'likelihood' somewhere",
          containsDescription(detail, "likelihood"));
      assertTrue(
          "explanation tree should contain 'tfPrior' somewhere",
          containsDescription(detail, "tfPrior"));
    }
  }

  /** Recursively checks if any explanation in the tree contains the given text in its description. */
  private static boolean containsDescription(Explanation expl, String text) {
    if (expl.getDescription().contains(text)) {
      return true;
    }
    for (Explanation detail : expl.getDetails()) {
      if (containsDescription(detail, text)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Tests hybrid search at scale with randomized multi-field documents and queries. Validates that
   * the full BayesianBM25 + LogOddsConjunction pipeline produces correct WAND upper bounds via
   * CheckHits.checkTopScores.
   */
  public void testRandomMultiFieldHybrid() throws Exception {
    Directory testDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter w = new IndexWriter(testDir, config);

    String[] fields = {"title", "body", "tags"};
    String[] vocabulary = {"search", "lucene", "index", "query", "score", "rank", "term", "field"};

    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (String field : fields) {
        StringBuilder sb = new StringBuilder();
        int numTokens = 1 + random().nextInt(10);
        for (int t = 0; t < numTokens; t++) {
          if (sb.length() > 0) sb.append(' ');
          sb.append(vocabulary[random().nextInt(vocabulary.length)]);
        }
        doc.add(new TextField(field, sb.toString(), Field.Store.NO));
      }
      w.addDocument(doc);
    }

    IndexReader testReader = DirectoryReader.open(w);
    w.close();
    IndexSearcher testSearcher = newSearcher(testReader);
    testSearcher.setSimilarity(new BayesianBM25Similarity());

    for (int iter = 0; iter < 5; iter++) {
      // Pick a random query term
      String queryTerm = vocabulary[random().nextInt(vocabulary.length)];

      // Build a multi-field LogOddsConjunction query
      List<Query> fieldQueries = new java.util.ArrayList<>();
      for (String field : fields) {
        fieldQueries.add(new TermQuery(new Term(field, queryTerm)));
      }

      float alpha = random().nextFloat();
      LogOddsConjunctionQuery hybridQuery = new LogOddsConjunctionQuery(fieldQueries, alpha);

      // Validate WAND correctness
      CheckHits.checkTopScores(random(), hybridQuery, testSearcher);

      // Validate all scores are proper probabilities
      ScoreDoc[] hits = testSearcher.search(hybridQuery, 10).scoreDocs;
      for (ScoreDoc hit : hits) {
        assertTrue(
            "hybrid score should be > 0, got " + hit.score, hit.score > 0f);
        assertTrue(
            "hybrid score should be < 1, got " + hit.score, hit.score < 1f);
      }
    }

    testReader.close();
    testDir.close();
  }

  /**
   * Tests that a BooleanQuery wrapping LogOddsConjunctionQuery clauses works correctly. This
   * simulates a real-world multi-term hybrid search: each term is searched across multiple fields
   * via LogOdds, and the terms are combined via BooleanQuery.
   */
  public void testBooleanWrappedHybridQuery() throws Exception {
    Directory testDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BayesianBM25Similarity());
    IndexWriter writer = new IndexWriter(testDir, config);

    // doc0: best match for "alpha beta" across title+body
    Document doc0 = new Document();
    doc0.add(new TextField("title", "alpha beta", Field.Store.YES));
    doc0.add(new TextField("body", "alpha beta gamma", Field.Store.YES));
    writer.addDocument(doc0);

    // doc1: matches "alpha" well but not "beta"
    Document doc1 = new Document();
    doc1.add(new TextField("title", "alpha gamma", Field.Store.YES));
    doc1.add(new TextField("body", "alpha delta epsilon", Field.Store.YES));
    writer.addDocument(doc1);

    // doc2: matches "beta" well but not "alpha"
    Document doc2 = new Document();
    doc2.add(new TextField("title", "beta gamma", Field.Store.YES));
    doc2.add(new TextField("body", "beta delta epsilon", Field.Store.YES));
    writer.addDocument(doc2);

    writer.forceMerge(1);
    IndexReader testReader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher testSearcher = new IndexSearcher(testReader);
    testSearcher.setSimilarity(new BayesianBM25Similarity());

    // For each search term, combine across title+body via LogOdds
    LogOddsConjunctionQuery alphaHybrid =
        new LogOddsConjunctionQuery(
            Arrays.asList(
                new TermQuery(new Term("title", "alpha")),
                new TermQuery(new Term("body", "alpha"))),
            0.5f);

    LogOddsConjunctionQuery betaHybrid =
        new LogOddsConjunctionQuery(
            Arrays.asList(
                new TermQuery(new Term("title", "beta")),
                new TermQuery(new Term("body", "beta"))),
            0.5f);

    // Combine term-level hybrids via BooleanQuery
    BooleanQuery booleanHybrid =
        new BooleanQuery.Builder()
            .add(alphaHybrid, BooleanClause.Occur.SHOULD)
            .add(betaHybrid, BooleanClause.Occur.SHOULD)
            .build();

    ScoreDoc[] hits = testSearcher.search(booleanHybrid, 10).scoreDocs;

    assertTrue("should have 3 hits", hits.length == 3);

    // doc0 matches both terms in both fields, should rank first
    assertEquals("doc0 should rank first", 0, hits[0].doc);
    assertTrue(
        "doc0 (" + hits[0].score + ") should score higher than doc1 (" + hits[1].score + ")",
        hits[0].score > hits[1].score);

    // Validate WAND correctness for the nested query structure
    CheckHits.checkTopScores(random(), booleanHybrid, testSearcher);

    testReader.close();
    testDir.close();
  }
}
