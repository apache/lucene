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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for {@link BayesianScoreQuery}. */
public class TestBayesianScoreQuery extends LuceneTestCase {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    // doc0: high tf for "alpha"
    Document doc0 = new Document();
    doc0.add(new TextField("body", "alpha alpha alpha alpha alpha beta", Field.Store.NO));
    writer.addDocument(doc0);

    // doc1: low tf for "alpha"
    Document doc1 = new Document();
    doc1.add(new TextField("body", "alpha gamma delta epsilon", Field.Store.NO));
    writer.addDocument(doc1);

    // doc2: no "alpha"
    Document doc2 = new Document();
    doc2.add(new TextField("body", "beta gamma delta epsilon zeta", Field.Store.NO));
    writer.addDocument(doc2);

    // Filler docs for meaningful IDF
    for (int i = 0; i < 50; i++) {
      Document filler = new Document();
      filler.add(new TextField("body", "filler content number " + i, Field.Store.NO));
      writer.addDocument(filler);
    }

    writer.forceMerge(1);
    reader = DirectoryReader.open(writer);
    writer.close();
    searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new BM25Similarity());
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testScoresInProbabilityRange() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);

    ScoreDoc[] hits = searcher.search(bsq, 10).scoreDocs;
    assertTrue("should have hits", hits.length > 0);

    for (ScoreDoc hit : hits) {
      assertTrue("score should be > 0, got " + hit.score, hit.score > 0f);
      assertTrue("score should be < 1, got " + hit.score, hit.score < 1f);
    }
  }

  public void testRankingPreserved() throws Exception {
    // BM25 ranking for "alpha": doc0 (high tf) > doc1 (low tf)
    Query bm25Query = new TermQuery(new Term("body", "alpha"));
    ScoreDoc[] bm25Hits = searcher.search(bm25Query, 10).scoreDocs;
    assertEquals("doc0 should rank first in BM25", 0, bm25Hits[0].doc);
    assertEquals("doc1 should rank second in BM25", 1, bm25Hits[1].doc);

    // BayesianScoreQuery should preserve this ranking (sigmoid is monotone)
    BayesianScoreQuery bsq = new BayesianScoreQuery(bm25Query, 0.5f, 5.0f);
    ScoreDoc[] bsqHits = searcher.search(bsq, 10).scoreDocs;
    assertEquals("doc0 should still rank first", 0, bsqHits[0].doc);
    assertEquals("doc1 should still rank second", 1, bsqHits[1].doc);
    assertTrue(
        "doc0 (" + bsqHits[0].score + ") > doc1 (" + bsqHits[1].score + ")",
        bsqHits[0].score > bsqHits[1].score);
  }

  public void testMultiTermQueryRankingPreserved() throws Exception {
    // Multi-term BooleanQuery
    BooleanQuery bm25Query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("body", "alpha")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("body", "beta")), BooleanClause.Occur.SHOULD)
            .build();

    ScoreDoc[] bm25Hits = searcher.search(bm25Query, 10).scoreDocs;

    // Wrap with BayesianScoreQuery
    BayesianScoreQuery bsq = new BayesianScoreQuery(bm25Query, 0.3f, 8.0f);
    ScoreDoc[] bsqHits = searcher.search(bsq, 10).scoreDocs;

    // Same number of hits
    assertEquals("same number of hits", bm25Hits.length, bsqHits.length);

    // Same document ordering (sigmoid preserves ranking)
    for (int i = 0; i < bm25Hits.length; i++) {
      assertEquals("ranking should be preserved at position " + i, bm25Hits[i].doc, bsqHits[i].doc);
    }

    // All BayesianScore outputs in (0, 1)
    for (ScoreDoc hit : bsqHits) {
      assertTrue("score should be > 0", hit.score > 0f);
      assertTrue("score should be < 1", hit.score < 1f);
    }
  }

  public void testSigmoidProperties() throws Exception {
    // sigmoid(0) = 0.5
    assertEquals(0.5f, BayesianScoreQuery.sigmoid(0f), 1e-6f);

    // sigmoid is monotone
    float prev = BayesianScoreQuery.sigmoid(-50f);
    for (float x = -49f; x <= 50f; x += 1f) {
      float curr = BayesianScoreQuery.sigmoid(x);
      assertTrue("sigmoid should be monotone", curr >= prev);
      prev = curr;
    }

    // sigmoid is numerically stable at extremes
    float large = BayesianScoreQuery.sigmoid(100f);
    assertTrue("sigmoid(100) near 1", large > 0.99f && large <= 1.0f);
    float small = BayesianScoreQuery.sigmoid(-100f);
    assertTrue("sigmoid(-100) near 0", small >= 0f && small < 0.01f);
  }

  public void testIllegalParameters() {
    Query q = new TermQuery(new Term("body", "alpha"));

    expectThrows(IllegalArgumentException.class, () -> new BayesianScoreQuery(q, -1f, 5f));
    expectThrows(IllegalArgumentException.class, () -> new BayesianScoreQuery(q, 0f, 5f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new BayesianScoreQuery(q, Float.POSITIVE_INFINITY, 5f));
    expectThrows(IllegalArgumentException.class, () -> new BayesianScoreQuery(q, 1f, Float.NaN));
  }

  public void testRewrite() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);

    Query rewritten = searcher.rewrite(bsq);
    // Should rewrite to a BayesianScoreQuery wrapping the rewritten inner query
    assertTrue(
        "should rewrite to BayesianScoreQuery, got " + rewritten.getClass().getName(),
        rewritten instanceof BayesianScoreQuery);
  }

  public void testEqualsAndHashCode() {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));

    BayesianScoreQuery a = new BayesianScoreQuery(q1, 0.5f, 5.0f);
    BayesianScoreQuery b = new BayesianScoreQuery(q1, 0.5f, 5.0f);
    BayesianScoreQuery c = new BayesianScoreQuery(q1, 0.3f, 5.0f);
    BayesianScoreQuery d = new BayesianScoreQuery(q2, 0.5f, 5.0f);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, d);
  }

  public void testExplanation() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);

    Weight w = searcher.createWeight(searcher.rewrite(bsq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    // doc0 matches
    Explanation expl = w.explain(context, 0);
    assertTrue("should match", expl.isMatch());
    assertTrue("should mention sigmoid", expl.getDescription().contains("sigmoid"));
    float score = expl.getValue().floatValue();
    assertTrue("score in (0,1)", score > 0f && score < 1f);

    // doc2 does not match "alpha"
    Explanation noMatch = w.explain(context, 2);
    assertFalse("should not match", noMatch.isMatch());
  }

  public void testQueryUtils() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);
    QueryUtils.check(random(), bsq, searcher);
  }

  public void testMaxScoreCorrectness() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);
    CheckHits.checkTopScores(random(), bsq, searcher);
  }

  public void testToString() {
    Query inner = new TermQuery(new Term("body", "alpha"));
    BayesianScoreQuery bsq = new BayesianScoreQuery(inner, 0.5f, 5.0f);
    String s = bsq.toString("body");
    assertTrue("should contain BayesianScore", s.contains("BayesianScore"));
    assertTrue("should contain alpha", s.contains("alpha"));
  }

  public void testDifferentAlphaBeta() throws Exception {
    Query inner = new TermQuery(new Term("body", "alpha"));

    // Gentle calibration: scores close to 0.5
    BayesianScoreQuery gentle = new BayesianScoreQuery(inner, 0.1f, 5.0f);
    ScoreDoc[] gentleHits = searcher.search(gentle, 10).scoreDocs;

    // Steep calibration: scores pushed toward 0 or 1
    BayesianScoreQuery steep = new BayesianScoreQuery(inner, 2.0f, 5.0f);
    ScoreDoc[] steepHits = searcher.search(steep, 10).scoreDocs;

    // Both should produce valid probabilities
    for (ScoreDoc hit : gentleHits) {
      assertTrue("gentle score in (0,1)", hit.score > 0f && hit.score < 1f);
    }
    for (ScoreDoc hit : steepHits) {
      assertTrue("steep score in (0,1)", hit.score > 0f && hit.score < 1f);
    }

    // Same ranking order (sigmoid is monotone regardless of alpha/beta)
    assertEquals("same top doc", gentleHits[0].doc, steepHits[0].doc);
  }
}
