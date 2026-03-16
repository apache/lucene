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

import java.util.Arrays;
import java.util.Collections;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests for {@link LogOddsConjunctionQuery}. Clauses are wrapped with {@link BayesianScoreQuery} to
 * produce (0,1) probability scores, matching the paper's query-level calibration design.
 */
public class TestLogOddsConjunctionQuery extends LuceneTestCase {

  private static final float BSQ_ALPHA = 0.5f;
  private static final float BSQ_BETA = 3.0f;

  /** Wraps a query with BayesianScoreQuery for sigmoid calibration to (0,1). */
  private static Query bayesian(Query q) {
    return new BayesianScoreQuery(q, BSQ_ALPHA, BSQ_BETA);
  }

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc0 = new Document();
    doc0.add(new TextField("body", "alpha beta gamma", Field.Store.YES));
    writer.addDocument(doc0);

    Document doc1 = new Document();
    doc1.add(new TextField("body", "alpha gamma delta", Field.Store.YES));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new TextField("body", "beta gamma delta", Field.Store.YES));
    writer.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new TextField("body", "gamma delta epsilon", Field.Store.YES));
    writer.addDocument(doc3);

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

  // ---- Core query behavior ----

  public void testBasicFusion() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));

    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    QueryUtils.check(random(), loq, searcher);

    ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;
    assertTrue("should have at least 1 hit", hits.length >= 1);

    for (ScoreDoc hit : hits) {
      assertTrue("score should be > 0, got " + hit.score, hit.score > 0);
      assertTrue("score should be < 1, got " + hit.score, hit.score < 1);
    }

    if (hits.length >= 2) {
      assertTrue(
          "doc matching both terms should score higher than doc matching one",
          hits[0].score >= hits[1].score);
    }
  }

  public void testSingleClauseRewrite() throws Exception {
    Query sub = bayesian(new TermQuery(new Term("body", "alpha")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Collections.singletonList(sub));

    Query rewritten = searcher.rewrite(loq);
    assertTrue(
        "single clause should rewrite to BayesianScoreQuery, got " + rewritten.getClass().getName(),
        rewritten instanceof BayesianScoreQuery);
  }

  public void testEmptyClauseRewrite() throws Exception {
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Collections.emptyList(), 0.5f);
    Query rewritten = searcher.rewrite(loq);
    assertTrue("empty should rewrite to MatchNoDocsQuery", rewritten instanceof MatchNoDocsQuery);
  }

  public void testAlphaValues() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));

    for (float alpha : new float[] {0.0f, 0.5f, 1.0f}) {
      LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), alpha);
      ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;
      assertTrue("alpha=" + alpha + " should have hits", hits.length > 0);
      for (ScoreDoc hit : hits) {
        assertTrue("score should be finite and > 0", Float.isFinite(hit.score) && hit.score > 0);
        assertTrue("score should be < 1", hit.score < 1);
      }
    }
  }

  public void testIllegalAlpha() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new LogOddsConjunctionQuery(Collections.emptyList(), -0.1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new LogOddsConjunctionQuery(Collections.emptyList(), 1.1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new LogOddsConjunctionQuery(Collections.emptyList(), Float.NaN));
  }

  public void testNonMatchingSubQueries() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "nonexistent1")));
    Query q2 = bayesian(new TermQuery(new Term("body", "nonexistent2")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    assertEquals("no docs should match", 0, searcher.search(loq, 10).scoreDocs.length);
  }

  public void testExplanation() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);

    Weight w = searcher.createWeight(searcher.rewrite(loq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    Explanation expl = w.explain(context, 0);
    assertTrue("doc0 should match", expl.isMatch());
    assertTrue("should mention log-odds", expl.getDescription().contains("log-odds"));

    Explanation noMatch = w.explain(context, 3);
    assertFalse("doc3 should not match", noMatch.isMatch());
  }

  public void testEqualsAndHashCode() {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));

    LogOddsConjunctionQuery a = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    LogOddsConjunctionQuery b = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    LogOddsConjunctionQuery c = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.3f);
    LogOddsConjunctionQuery d = new LogOddsConjunctionQuery(Arrays.asList(q2, q1), 0.5f);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertEquals(a, d);
  }

  public void testQueryUtils() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    QueryUtils.check(random(), loq, searcher);
  }

  public void testMaxScoreCorrectness() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    CheckHits.checkTopScores(random(), loq, searcher);
  }

  public void testToString() {
    Query q1 = new TermQuery(new Term("body", "alpha"));
    Query q2 = new TermQuery(new Term("body", "beta"));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    String str = loq.toString("body");
    assertTrue("should contain LogOdds", str.contains("LogOdds"));
    assertTrue("should contain alpha", str.contains("0.5"));
  }

  public void testThreeWayCombination() throws Exception {
    Query q1 = bayesian(new TermQuery(new Term("body", "alpha")));
    Query q2 = bayesian(new TermQuery(new Term("body", "beta")));
    Query q3 = bayesian(new TermQuery(new Term("body", "gamma")));
    LogOddsConjunctionQuery loq = new LogOddsConjunctionQuery(Arrays.asList(q1, q2, q3), 0.5f);
    QueryUtils.check(random(), loq, searcher);

    ScoreDoc[] hits = searcher.search(loq, 10).scoreDocs;
    assertTrue("should have hits", hits.length > 0);
    for (ScoreDoc hit : hits) {
      assertTrue("score in (0,1)", hit.score > 0 && hit.score < 1);
    }
  }

  // ---- Multi-field hybrid text tests ----

  public void testMultiFieldHybridSearch() throws Exception {
    Directory hybridDir = newDirectory();
    IndexWriter writer = new IndexWriter(hybridDir, new IndexWriterConfig());

    Document doc0 = new Document();
    doc0.add(new TextField("title", "lucene search engine", Field.Store.YES));
    doc0.add(new TextField("body", "lucene is a full-text search library", Field.Store.YES));
    writer.addDocument(doc0);

    Document doc1 = new Document();
    doc1.add(new TextField("title", "lucene search engine", Field.Store.YES));
    doc1.add(new TextField("body", "this is about information retrieval", Field.Store.YES));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new TextField("title", "database systems overview", Field.Store.YES));
    doc2.add(new TextField("body", "relational databases use SQL", Field.Store.YES));
    writer.addDocument(doc2);

    writer.forceMerge(1);
    writer.close();
    IndexReader hybridReader = DirectoryReader.open(hybridDir);
    IndexSearcher hybridSearcher = new IndexSearcher(hybridReader);
    hybridSearcher.setSimilarity(new BM25Similarity());

    Query titleQuery = bayesian(new TermQuery(new Term("title", "lucene")));
    Query bodyQuery = bayesian(new TermQuery(new Term("body", "lucene")));
    LogOddsConjunctionQuery hybridQuery =
        new LogOddsConjunctionQuery(Arrays.asList(titleQuery, bodyQuery), 0.5f);

    ScoreDoc[] hits = hybridSearcher.search(hybridQuery, 10).scoreDocs;
    assertEquals("should have 2 hits", 2, hits.length);

    for (ScoreDoc hit : hits) {
      assertTrue("score in (0,1)", hit.score > 0f && hit.score < 1f);
    }

    // doc0 matches both fields, doc1 matches title only
    assertEquals("doc0 should rank first", 0, hits[0].doc);
    assertTrue(
        "doc0 (" + hits[0].score + ") > doc1 (" + hits[1].score + ")",
        hits[0].score > hits[1].score);

    hybridReader.close();
    hybridDir.close();
  }

  public void testMoreSignalsIncreaseScore() throws Exception {
    Directory signalDir = newDirectory();
    IndexWriter writer = new IndexWriter(signalDir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("f1", "search engine optimization", Field.Store.NO));
    doc.add(new TextField("f2", "search algorithms and data structures", Field.Store.NO));
    doc.add(new TextField("f3", "search relevance tuning", Field.Store.NO));
    writer.addDocument(doc);

    writer.forceMerge(1);
    writer.close();
    IndexReader signalReader = DirectoryReader.open(signalDir);
    IndexSearcher signalSearcher = new IndexSearcher(signalReader);
    signalSearcher.setSimilarity(new BM25Similarity());

    Query q1 = bayesian(new TermQuery(new Term("f1", "search")));
    Query q2 = bayesian(new TermQuery(new Term("f2", "search")));
    Query q3 = bayesian(new TermQuery(new Term("f3", "search")));

    float score1 = signalSearcher.search(q1, 1).scoreDocs[0].score;

    LogOddsConjunctionQuery twoSignals = new LogOddsConjunctionQuery(Arrays.asList(q1, q2), 0.5f);
    float score2 = signalSearcher.search(twoSignals, 1).scoreDocs[0].score;

    LogOddsConjunctionQuery threeSignals =
        new LogOddsConjunctionQuery(Arrays.asList(q1, q2, q3), 0.5f);
    float score3 = signalSearcher.search(threeSignals, 1).scoreDocs[0].score;

    // With softplus gating, more matching signals always produce >= score.
    assertTrue("3 >= 2 signals", score3 >= score2);
    assertTrue("2 >= 1 signal", score2 >= score1);
    assertTrue("all in (0,1)", score1 > 0 && score1 < 1 && score2 > 0 && score3 < 1);

    signalReader.close();
    signalDir.close();
  }

  // ---- Vector + Text hybrid search tests ----

  private static final int HYBRID_NUM_FILLER = 196;
  private static final float[] QUERY_A = normalize(new float[] {1.0f, 0.0f, 0.0f});

  private Directory hybridDir;
  private IndexReader hybridReader;
  private IndexSearcher hybridSearcher;

  private void buildHybridIndex() throws Exception {
    hybridDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(TestUtil.getDefaultCodec());
    IndexWriter writer = new IndexWriter(hybridDir, config);

    // doc0: text match + vector near queryA
    Document doc0 = new Document();
    doc0.add(new TextField("body", "lucene search engine optimization library", Field.Store.NO));
    doc0.add(
        new KnnFloatVectorField(
            "embedding",
            normalize(new float[] {0.95f, 0.05f, 0.0f}),
            VectorSimilarityFunction.COSINE));
    writer.addDocument(doc0);

    // doc1: text match + vector far from queryA
    Document doc1 = new Document();
    doc1.add(new TextField("body", "lucene full text search library index", Field.Store.NO));
    doc1.add(
        new KnnFloatVectorField(
            "embedding",
            normalize(new float[] {0.0f, 0.0f, 1.0f}),
            VectorSimilarityFunction.COSINE));
    writer.addDocument(doc1);

    // doc2: no text match + vector near queryA
    Document doc2 = new Document();
    doc2.add(new TextField("body", "unrelated topic about cooking recipes", Field.Store.NO));
    doc2.add(
        new KnnFloatVectorField(
            "embedding",
            normalize(new float[] {0.90f, 0.10f, 0.0f}),
            VectorSimilarityFunction.COSINE));
    writer.addDocument(doc2);

    // doc3: neither
    Document doc3 = new Document();
    doc3.add(new TextField("body", "gardening tips for spring planting", Field.Store.NO));
    doc3.add(
        new KnnFloatVectorField(
            "embedding",
            normalize(new float[] {0.0f, 1.0f, 0.0f}),
            VectorSimilarityFunction.COSINE));
    writer.addDocument(doc3);

    // Filler docs with vectors away from queryA
    for (int i = 0; i < HYBRID_NUM_FILLER; i++) {
      Document filler = new Document();
      filler.add(new TextField("body", "filler document about topic " + i, Field.Store.NO));
      float y = (float) Math.sin(i * 0.1);
      float z = (float) Math.cos(i * 0.1);
      filler.add(
          new KnnFloatVectorField(
              "embedding", normalize(new float[] {0.0f, y, z}), VectorSimilarityFunction.COSINE));
      writer.addDocument(filler);
    }

    writer.forceMerge(1);
    writer.close();
    hybridReader = DirectoryReader.open(hybridDir);
    hybridSearcher = new IndexSearcher(hybridReader);
    hybridSearcher.setSimilarity(new BM25Similarity());
  }

  private void closeHybridIndex() throws Exception {
    hybridReader.close();
    hybridDir.close();
  }

  public void testTextAndVectorHybridSearch() throws Exception {
    buildHybridIndex();
    try {
      // Text query wrapped with BayesianScoreQuery -> probability in (0,1)
      Query textQuery = bayesian(new TermQuery(new Term("body", "lucene")));
      // KNN COSINE already produces (0,1) scores via (1+cos)/2
      Query vectorQuery = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      LogOddsConjunctionQuery hybridQuery =
          new LogOddsConjunctionQuery(Arrays.asList(textQuery, vectorQuery), 0.5f);

      ScoreDoc[] hits = hybridSearcher.search(hybridQuery, 10).scoreDocs;
      assertTrue("should have hits", hits.length >= 3);

      for (ScoreDoc hit : hits) {
        assertTrue("score > 0", hit.score > 0f);
        assertTrue("score < 1", hit.score < 1f);
      }

      // doc0 matches both text and vector
      assertEquals("doc0 (text+vector) should rank first", 0, hits[0].doc);
      assertTrue("doc0 > second", hits[0].score > hits[1].score);
    } finally {
      closeHybridIndex();
    }
  }

  public void testVectorAndTextBothContribute() throws Exception {
    buildHybridIndex();
    try {
      Query textQuery = bayesian(new TermQuery(new Term("body", "lucene")));
      Query vectorQuery = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      // Verify KNN returns doc0 and doc2
      ScoreDoc[] knnHits = hybridSearcher.search(vectorQuery, 10).scoreDocs;
      boolean knnFoundDoc0 = false;
      boolean knnFoundDoc2 = false;
      for (ScoreDoc hit : knnHits) {
        if (hit.doc == 0) knnFoundDoc0 = true;
        if (hit.doc == 2) knnFoundDoc2 = true;
      }
      assertTrue("KNN should return doc0", knnFoundDoc0);
      assertTrue("KNN should return doc2", knnFoundDoc2);

      LogOddsConjunctionQuery hybridQuery =
          new LogOddsConjunctionQuery(Arrays.asList(textQuery, vectorQuery), 0.5f);
      ScoreDoc[] hits = hybridSearcher.search(hybridQuery, 20).scoreDocs;

      boolean foundTextOnly = false;
      boolean foundVectorOnly = false;
      for (ScoreDoc hit : hits) {
        if (hit.doc == 1) foundTextOnly = true;
        if (hit.doc == 2) foundVectorOnly = true;
      }
      assertTrue("text-only doc1 should appear", foundTextOnly);
      assertTrue("vector-only doc2 should appear", foundVectorOnly);
    } finally {
      closeHybridIndex();
    }
  }

  public void testMultipleVectorFields() throws Exception {
    float[] queryA = normalize(new float[] {1.0f, 0.0f});
    float[] queryB = normalize(new float[] {0.0f, 1.0f});

    Directory testDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(TestUtil.getDefaultCodec());
    IndexWriter writer = new IndexWriter(testDir, config);

    Document doc0 = new Document();
    doc0.add(
        new KnnFloatVectorField(
            "emb_a", normalize(new float[] {0.95f, 0.05f}), VectorSimilarityFunction.COSINE));
    doc0.add(
        new KnnFloatVectorField(
            "emb_b", normalize(new float[] {0.05f, 0.95f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc0);

    Document doc1 = new Document();
    doc1.add(
        new KnnFloatVectorField(
            "emb_a", normalize(new float[] {0.90f, 0.10f}), VectorSimilarityFunction.COSINE));
    doc1.add(
        new KnnFloatVectorField(
            "emb_b", normalize(new float[] {1.0f, 0.0f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(
        new KnnFloatVectorField(
            "emb_a", normalize(new float[] {0.0f, 1.0f}), VectorSimilarityFunction.COSINE));
    doc2.add(
        new KnnFloatVectorField(
            "emb_b", normalize(new float[] {0.10f, 0.90f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc2);

    writer.forceMerge(1);
    writer.close();
    IndexReader testReader = DirectoryReader.open(testDir);
    IndexSearcher testSearcher = newSearcher(testReader);

    Query vecQueryA = new KnnFloatVectorQuery("emb_a", queryA, 3);
    Query vecQueryB = new KnnFloatVectorQuery("emb_b", queryB, 3);

    LogOddsConjunctionQuery multiVecQuery =
        new LogOddsConjunctionQuery(Arrays.asList(vecQueryA, vecQueryB), 0.5f);

    ScoreDoc[] hits = testSearcher.search(multiVecQuery, 3).scoreDocs;
    assertEquals("all 3 docs should match", 3, hits.length);
    assertEquals("doc0 (close to both) should rank first", 0, hits[0].doc);

    testReader.close();
    testDir.close();
  }

  // ---- Boolean logic combinations ----

  public void testBooleanMustWithHybrid() throws Exception {
    buildHybridIndex();
    try {
      Query textQuery = bayesian(new TermQuery(new Term("body", "lucene")));
      Query vectorQuery = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      LogOddsConjunctionQuery hybridQuery =
          new LogOddsConjunctionQuery(Arrays.asList(textQuery, vectorQuery), 0.5f);

      BooleanQuery filtered =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term("body", "lucene")), BooleanClause.Occur.FILTER)
              .add(hybridQuery, BooleanClause.Occur.MUST)
              .build();

      ScoreDoc[] hits = hybridSearcher.search(filtered, 10).scoreDocs;
      for (ScoreDoc hit : hits) {
        assertTrue("only docs 0 and 1 match text", hit.doc == 0 || hit.doc == 1);
      }
      assertTrue("should have at least 1 hit", hits.length >= 1);
    } finally {
      closeHybridIndex();
    }
  }

  public void testBooleanMustNotWithHybrid() throws Exception {
    buildHybridIndex();
    try {
      Query textQuery = bayesian(new TermQuery(new Term("body", "lucene")));
      Query vectorQuery = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      LogOddsConjunctionQuery hybridQuery =
          new LogOddsConjunctionQuery(Arrays.asList(textQuery, vectorQuery), 0.5f);

      Query excludeFilter = new TermQuery(new Term("body", "cooking"));
      BooleanQuery excludeQuery =
          new BooleanQuery.Builder()
              .add(hybridQuery, BooleanClause.Occur.MUST)
              .add(excludeFilter, BooleanClause.Occur.MUST_NOT)
              .build();

      ScoreDoc[] hits = hybridSearcher.search(excludeQuery, 10).scoreDocs;
      for (ScoreDoc hit : hits) {
        assertNotEquals("doc2 (cooking) should be excluded", 2, hit.doc);
      }
    } finally {
      closeHybridIndex();
    }
  }

  public void testBooleanShouldWithHybrid() throws Exception {
    buildHybridIndex();
    try {
      Query textLucene = bayesian(new TermQuery(new Term("body", "lucene")));
      Query textCooking = bayesian(new TermQuery(new Term("body", "cooking")));
      Query vecA = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      LogOddsConjunctionQuery hybrid1 =
          new LogOddsConjunctionQuery(Arrays.asList(textLucene, vecA), 0.5f);
      LogOddsConjunctionQuery hybrid2 =
          new LogOddsConjunctionQuery(Arrays.asList(textCooking, vecA), 0.5f);

      BooleanQuery orQuery =
          new BooleanQuery.Builder()
              .add(hybrid1, BooleanClause.Occur.SHOULD)
              .add(hybrid2, BooleanClause.Occur.SHOULD)
              .build();

      ScoreDoc[] hits = hybridSearcher.search(orQuery, 20).scoreDocs;
      assertTrue("should have hits", hits.length > 0);

      boolean foundDoc0 = false;
      boolean foundDoc2 = false;
      for (ScoreDoc hit : hits) {
        if (hit.doc == 0) foundDoc0 = true;
        if (hit.doc == 2) foundDoc2 = true;
      }
      assertTrue("doc0 (lucene + near vector) should appear", foundDoc0);
      assertTrue("doc2 (cooking + near vector) should appear", foundDoc2);
    } finally {
      closeHybridIndex();
    }
  }

  public void testVectorAndVector() throws Exception {
    float[] queryX = normalize(new float[] {1.0f, 0.0f});
    float[] queryY = normalize(new float[] {0.0f, 1.0f});

    Directory testDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(TestUtil.getDefaultCodec());
    IndexWriter writer = new IndexWriter(testDir, config);

    Document doc0 = new Document();
    doc0.add(
        new KnnFloatVectorField(
            "emb", normalize(new float[] {0.7f, 0.7f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc0);

    Document doc1 = new Document();
    doc1.add(
        new KnnFloatVectorField(
            "emb", normalize(new float[] {0.95f, 0.05f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(
        new KnnFloatVectorField(
            "emb", normalize(new float[] {0.05f, 0.95f}), VectorSimilarityFunction.COSINE));
    writer.addDocument(doc2);

    writer.forceMerge(1);
    writer.close();
    IndexReader testReader = DirectoryReader.open(testDir);
    IndexSearcher testSearcher = newSearcher(testReader);

    Query vecX = new KnnFloatVectorQuery("emb", queryX, 3);
    Query vecY = new KnnFloatVectorQuery("emb", queryY, 3);

    BooleanQuery vecAnd =
        new BooleanQuery.Builder()
            .add(vecX, BooleanClause.Occur.MUST)
            .add(vecY, BooleanClause.Occur.MUST)
            .build();

    ScoreDoc[] hits = testSearcher.search(vecAnd, 3).scoreDocs;
    assertEquals("all 3 docs should match", 3, hits.length);
    assertEquals("doc0 (near both) should rank first", 0, hits[0].doc);

    testReader.close();
    testDir.close();
  }

  public void testVectorNot() throws Exception {
    buildHybridIndex();
    try {
      Query textQuery = new TermQuery(new Term("body", "lucene"));
      Query vecNear = new KnnFloatVectorQuery("embedding", QUERY_A, 10);

      BooleanQuery textNotVector =
          new BooleanQuery.Builder()
              .add(textQuery, BooleanClause.Occur.MUST)
              .add(vecNear, BooleanClause.Occur.MUST_NOT)
              .build();

      ScoreDoc[] hits = hybridSearcher.search(textNotVector, 10).scoreDocs;
      for (ScoreDoc hit : hits) {
        assertNotEquals("doc0 should be excluded (near QUERY_A)", 0, hit.doc);
      }
    } finally {
      closeHybridIndex();
    }
  }

  /** L2-normalize a float vector. */
  private static float[] normalize(float[] v) {
    double norm = 0;
    for (float x : v) {
      norm += x * x;
    }
    norm = Math.sqrt(norm);
    float[] result = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      result[i] = (float) (v[i] / norm);
    }
    return result;
  }
}
