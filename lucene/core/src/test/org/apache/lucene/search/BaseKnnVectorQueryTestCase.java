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

import static com.carrotsearch.randomizedtesting.RandomizedTest.frequently;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/** Test cases for AbstractKnnVectorQuery objects. */
abstract class BaseKnnVectorQueryTestCase extends LuceneTestCase {

  abstract AbstractKnnVectorQuery getKnnVectorQuery(
      String field, float[] query, int k, Query queryFilter);

  abstract AbstractKnnVectorQuery getThrowingKnnVectorQuery(
      String field, float[] query, int k, Query queryFilter);

  AbstractKnnVectorQuery getKnnVectorQuery(String field, float[] query, int k) {
    return getKnnVectorQuery(field, query, k, null);
  }

  abstract float[] randomVector(int dim);

  abstract Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction similarityFunction);

  abstract Field getKnnVectorField(String name, float[] vector);

  public void testEquals() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 10);
    Query filter1 = new TermQuery(new Term("id", "id1"));
    AbstractKnnVectorQuery q2 = getKnnVectorQuery("f1", new float[] {0, 1}, 10, filter1);

    assertNotEquals(q2, q1);
    assertNotEquals(q1, q2);
    assertEquals(q2, getKnnVectorQuery("f1", new float[] {0, 1}, 10, filter1));

    Query filter2 = new TermQuery(new Term("id", "id2"));
    assertNotEquals(q2, getKnnVectorQuery("f1", new float[] {0, 1}, 10, filter2));

    assertEquals(q1, getKnnVectorQuery("f1", new float[] {0, 1}, 10));

    assertNotEquals(null, q1);

    assertNotEquals(q1, new TermQuery(new Term("f1", "x")));

    assertNotEquals(q1, getKnnVectorQuery("f2", new float[] {0, 1}, 10));
    assertNotEquals(q1, getKnnVectorQuery("f1", new float[] {1, 1}, 10));
    assertNotEquals(q1, getKnnVectorQuery("f1", new float[] {0, 1}, 2));
    assertNotEquals(q1, getKnnVectorQuery("f1", new float[] {0}, 10));
  }

  public void testGetField() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 10);
    Query filter1 = new TermQuery(new Term("id", "id1"));
    AbstractKnnVectorQuery q2 = getKnnVectorQuery("f2", new float[] {0, 1}, 10, filter1);

    assertEquals("f1", q1.getField());
    assertEquals("f2", q2.getField());
  }

  public void testGetK() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 6);
    Query filter1 = new TermQuery(new Term("id", "id1"));
    AbstractKnnVectorQuery q2 = getKnnVectorQuery("f2", new float[] {0, 1}, 7, filter1);

    assertEquals(6, q1.getK());
    assertEquals(7, q2.getK());
  }

  public void testGetFilter() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 6);
    Query filter1 = new TermQuery(new Term("id", "id1"));
    AbstractKnnVectorQuery q2 = getKnnVectorQuery("f2", new float[] {0, 1}, 7, filter1);

    assertNull(q1.getFilter());
    assertEquals(filter1, q2.getFilter());
  }

  /**
   * Tests if a AbstractKnnVectorQuery is rewritten to a MatchNoDocsQuery when there are no
   * documents to match.
   */
  public void testEmptyIndex() throws IOException {
    try (Directory indexStore = getIndexStore("field");
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      AbstractKnnVectorQuery kvq = getKnnVectorQuery("field", new float[] {1, 2}, 10);
      assertMatches(searcher, kvq, 0);
      Query q = searcher.rewrite(kvq);
      assertTrue(q instanceof MatchNoDocsQuery);
    }
  }

  /**
   * Tests that a AbstractKnnVectorQuery whose topK &gt;= numDocs returns all the documents in score
   * order
   */
  public void testFindAll() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      AbstractKnnVectorQuery kvq = getKnnVectorQuery("field", new float[] {0, 0}, 10);
      assertMatches(searcher, kvq, 3);
      ScoreDoc[] scoreDocs = searcher.search(kvq, 3).scoreDocs;
      assertIdMatches(reader, "id2", scoreDocs[0]);
      assertIdMatches(reader, "id0", scoreDocs[1]);
      assertIdMatches(reader, "id1", scoreDocs[2]);
    }
  }

  public void testFindFewer() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      AbstractKnnVectorQuery kvq = getKnnVectorQuery("field", new float[] {0, 0}, 2);
      assertMatches(searcher, kvq, 2);
      ScoreDoc[] scoreDocs = searcher.search(kvq, 3).scoreDocs;
      assertEquals(scoreDocs.length, 2);
      assertIdMatches(reader, "id2", scoreDocs[0]);
      assertIdMatches(reader, "id0", scoreDocs[1]);
    }
  }

  public void testSearchBoost() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query vectorQuery = getKnnVectorQuery("field", new float[] {0, 0}, 10);
      ScoreDoc[] scoreDocs = searcher.search(vectorQuery, 3).scoreDocs;

      Query boostQuery = new BoostQuery(vectorQuery, 3.0f);
      ScoreDoc[] boostScoreDocs = searcher.search(boostQuery, 3).scoreDocs;
      assertEquals(scoreDocs.length, boostScoreDocs.length);

      for (int i = 0; i < scoreDocs.length; i++) {
        ScoreDoc scoreDoc = scoreDocs[i];
        ScoreDoc boostScoreDoc = boostScoreDocs[i];

        assertEquals(scoreDoc.doc, boostScoreDoc.doc);
        assertEquals(scoreDoc.score * 3.0f, boostScoreDoc.score, 0.001f);
      }
    }
  }

  /** Tests that a AbstractKnnVectorQuery applies the filter query */
  public void testSimpleFilter() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      Query filter = new TermQuery(new Term("id", "id2"));
      Query kvq = getKnnVectorQuery("field", new float[] {0, 0}, 10, filter);
      TopDocs topDocs = searcher.search(kvq, 3);
      assertEquals(1, topDocs.totalHits.value);
      assertIdMatches(reader, "id2", topDocs.scoreDocs[0]);
    }
  }

  public void testFilterWithNoVectorMatches() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query filter = new TermQuery(new Term("other", "value"));
      Query kvq = getKnnVectorQuery("field", new float[] {0, 0}, 10, filter);
      TopDocs topDocs = searcher.search(kvq, 3);
      assertEquals(0, topDocs.totalHits.value);
    }
  }

  /** testDimensionMismatch */
  public void testDimensionMismatch() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      AbstractKnnVectorQuery kvq = getKnnVectorQuery("field", new float[] {0}, 10);
      IllegalArgumentException e =
          expectThrows(
              RuntimeException.class,
              IllegalArgumentException.class,
              () -> searcher.search(kvq, 10));
      assertEquals("vector query dimension: 1 differs from field dimension: 2", e.getMessage());
    }
  }

  /** testNonVectorField */
  public void testNonVectorField() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      assertMatches(searcher, getKnnVectorQuery("xyzzy", new float[] {0}, 10), 0);
      assertMatches(searcher, getKnnVectorQuery("id", new float[] {0}, 10), 0);
    }
  }

  /** Test bad parameters */
  public void testIllegalArguments() throws IOException {
    expectThrows(IllegalArgumentException.class, () -> getKnnVectorQuery("xx", new float[] {1}, 0));
  }

  public void testDifferentReader() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {2, 3}, 3);
      Query dasq = query.rewrite(newSearcher(reader));
      IndexSearcher leafSearcher = newSearcher(reader.leaves().get(0).reader());
      expectThrows(
          IllegalStateException.class,
          () -> dasq.createWeight(leafSearcher, ScoreMode.COMPLETE, 1));
    }
  }

  public void testScoreEuclidean() throws IOException {
    float[][] vectors = new float[5][];
    for (int j = 0; j < 5; j++) {
      vectors[j] = new float[] {j, j};
    }
    try (Directory d = getStableIndexStore("field", vectors);
        IndexReader reader = DirectoryReader.open(d)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {2, 3}, 3);
      Query rewritten = query.rewrite(searcher);
      Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
      Scorer scorer = weight.scorer(reader.leaves().get(0));

      // prior to advancing, score is 0
      assertEquals(-1, scorer.docID());
      expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);

      // This is 1 / ((l2distance((2,3), (2, 2)) = 1) + 1) = 0.5
      assertEquals(1 / 2f, scorer.getMaxScore(2), 0);
      assertEquals(1 / 2f, scorer.getMaxScore(Integer.MAX_VALUE), 0);

      DocIdSetIterator it = scorer.iterator();
      assertEquals(3, it.cost());
      assertEquals(1, it.nextDoc());
      assertEquals(1 / 6f, scorer.score(), 0);
      assertEquals(3, it.advance(3));
      assertEquals(1 / 2f, scorer.score(), 0);

      assertEquals(NO_MORE_DOCS, it.advance(4));
      expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
    }
  }

  public void testScoreCosine() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 1; j <= 5; j++) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j * j}, COSINE));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {2, 3}, 3);
        Query rewritten = query.rewrite(searcher);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.leaves().get(0));

        // prior to advancing, score is undefined
        assertEquals(-1, scorer.docID());
        expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);

        /* score0 = ((2,3) * (1, 1) = 5) / (||2, 3|| * ||1, 1|| = sqrt(26)), then
         * normalized by (1 + x) /2.
         */
        float score0 =
            (float) ((1 + (2 * 1 + 3 * 1) / Math.sqrt((2 * 2 + 3 * 3) * (1 * 1 + 1 * 1))) / 2);

        /* score1 = ((2,3) * (2, 4) = 16) / (||2, 3|| * ||2, 4|| = sqrt(260)), then
         * normalized by (1 + x) /2
         */
        float score1 =
            (float) ((1 + (2 * 2 + 3 * 4) / Math.sqrt((2 * 2 + 3 * 3) * (2 * 2 + 4 * 4))) / 2);

        // doc 1 happens to have the maximum score
        assertEquals(score1, scorer.getMaxScore(2), 0.0001);
        assertEquals(score1, scorer.getMaxScore(Integer.MAX_VALUE), 0.0001);

        DocIdSetIterator it = scorer.iterator();
        assertEquals(3, it.cost());
        assertEquals(0, it.nextDoc());
        // doc 0 has (1, 1)
        assertEquals(score0, scorer.score(), 0.0001);
        assertEquals(1, it.advance(1));
        assertEquals(score1, scorer.score(), 0.0001);

        // since topK was 3
        assertEquals(NO_MORE_DOCS, it.advance(4));
        expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
      }
    }
  }

  public void testExplain() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 0; j < 5; j++) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j}));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {2, 3}, 3);
        Explanation matched = searcher.explain(query, 2);
        assertTrue(matched.isMatch());
        assertEquals(1 / 2f, matched.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("within top 3 docs", matched.getDescription());

        Explanation nomatch = searcher.explain(query, 4);
        assertFalse(nomatch.isMatch());
        assertEquals(0f, nomatch.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("not in top 3 docs", nomatch.getDescription());
      }
    }
  }

  public void testExplainMultipleSegments() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 0; j < 5; j++) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j}));
          w.addDocument(doc);
          w.commit();
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {2, 3}, 3);
        Explanation matched = searcher.explain(query, 2);
        assertTrue(matched.isMatch());
        assertEquals(1 / 2f, matched.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("within top 3 docs", matched.getDescription());

        Explanation nomatch = searcher.explain(query, 4);
        assertFalse(nomatch.isMatch());
        assertEquals(0f, nomatch.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("not in top 3 docs", nomatch.getDescription());
      }
    }
  }

  /** Test that when vectors are abnormally distributed among segments, we still find the top K */
  public void testSkewedIndex() throws IOException {
    /* We have to choose the numbers carefully here so that some segment has more than the expected
     * number of top K documents, but no more than K documents in total (otherwise we might occasionally
     * randomly fail to find one).
     */
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        int r = 0;
        for (int i = 0; i < 5; i++) {
          for (int j = 0; j < 5; j++) {
            Document doc = new Document();
            doc.add(getKnnVectorField("field", new float[] {r, r}));
            doc.add(new StringField("id", "id" + r, Field.Store.YES));
            w.addDocument(doc);
            ++r;
          }
          w.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        TopDocs results = searcher.search(getKnnVectorQuery("field", new float[] {0, 0}, 8), 10);
        assertEquals(8, results.scoreDocs.length);
        assertIdMatches(reader, "id0", results.scoreDocs[0]);
        assertIdMatches(reader, "id7", results.scoreDocs[7]);

        // test some results in the middle of the sequence - also tests docid tiebreaking
        results = searcher.search(getKnnVectorQuery("field", new float[] {10, 10}, 8), 10);
        assertEquals(8, results.scoreDocs.length);
        assertIdMatches(reader, "id10", results.scoreDocs[0]);
        assertIdMatches(reader, "id6", results.scoreDocs[7]);
      }
    }
  }

  /** Tests with random vectors, number of documents, etc. Uses RandomIndexWriter. */
  public void testRandom() throws IOException {
    int numDocs = atLeast(100);
    int dimension = atLeast(5);
    int numIters = atLeast(10);
    boolean everyDocHasAVector = random().nextBoolean();
    try (Directory d = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), d);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (everyDocHasAVector || random().nextInt(10) != 2) {
          doc.add(getKnnVectorField("field", randomVector(dimension)));
        }
        w.addDocument(doc);
      }
      w.close();
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i < numIters; i++) {
          int k = random().nextInt(80) + 1;
          AbstractKnnVectorQuery query = getKnnVectorQuery("field", randomVector(dimension), k);
          int n = random().nextInt(100) + 1;
          TopDocs results = searcher.search(query, n);
          int expected = Math.min(Math.min(n, k), reader.numDocs());
          // we may get fewer results than requested if there are deletions, but this test doesn't
          // test that
          assert reader.hasDeletions() == false;
          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value >= results.scoreDocs.length);
          // verify the results are in descending score order
          float last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }
        }
      }
    }
  }

  /** Tests with random vectors and a random filter. Uses RandomIndexWriter. */
  public void testRandomWithFilter() throws IOException {
    int numDocs = 1000;
    int dimension = atLeast(5);
    int numIters = atLeast(10);
    try (Directory d = newDirectory()) {
      // Always use the default kNN format to have predictable behavior around when it hits
      // visitedLimit. This is fine since the test targets AbstractKnnVectorQuery logic, not the kNN
      // format
      // implementation.
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
      RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(getKnnVectorField("field", randomVector(dimension)));
        doc.add(new NumericDocValuesField("tag", i));
        doc.add(new IntPoint("tag", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.close();

      try (DirectoryReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i < numIters; i++) {
          int lower = random().nextInt(500);

          // Test a filter with cost less than k and check we use exact search
          Query filter1 = IntPoint.newRangeQuery("tag", lower, lower + 8);
          TopDocs results =
              searcher.search(
                  getKnnVectorQuery("field", randomVector(dimension), 10, filter1), numDocs);
          assertEquals(9, results.totalHits.value);
          assertEquals(results.totalHits.value, results.scoreDocs.length);
          expectThrows(
              RuntimeException.class,
              UnsupportedOperationException.class,
              () ->
                  searcher.search(
                      getThrowingKnnVectorQuery("field", randomVector(dimension), 10, filter1),
                      numDocs));

          // Test a restrictive filter and check we use exact search
          Query filter2 = IntPoint.newRangeQuery("tag", lower, lower + 6);
          results =
              searcher.search(
                  getKnnVectorQuery("field", randomVector(dimension), 5, filter2), numDocs);
          assertEquals(5, results.totalHits.value);
          assertEquals(results.totalHits.value, results.scoreDocs.length);
          expectThrows(
              RuntimeException.class,
              UnsupportedOperationException.class,
              () ->
                  searcher.search(
                      getThrowingKnnVectorQuery("field", randomVector(dimension), 5, filter2),
                      numDocs));

          // Test an unrestrictive filter and check we use approximate search
          Query filter3 = IntPoint.newRangeQuery("tag", lower, numDocs);
          results =
              searcher.search(
                  getThrowingKnnVectorQuery("field", randomVector(dimension), 5, filter3),
                  numDocs,
                  new Sort(new SortField("tag", SortField.Type.INT)));
          assertEquals(5, results.totalHits.value);
          assertEquals(results.totalHits.value, results.scoreDocs.length);

          for (ScoreDoc scoreDoc : results.scoreDocs) {
            FieldDoc fieldDoc = (FieldDoc) scoreDoc;
            assertEquals(1, fieldDoc.fields.length);

            int tag = (int) fieldDoc.fields[0];
            assertTrue(lower <= tag && tag <= numDocs);
          }

          // Test a filter that exhausts visitedLimit in upper levels, and switches to exact search
          Query filter4 = IntPoint.newRangeQuery("tag", lower, lower + 2);
          expectThrows(
              RuntimeException.class,
              UnsupportedOperationException.class,
              () ->
                  searcher.search(
                      getThrowingKnnVectorQuery("field", randomVector(dimension), 1, filter4),
                      numDocs));
        }
      }
    }
  }

  /** Tests filtering when all vectors have the same score. */
  @AwaitsFix(bugUrl = "https://github.com/apache/lucene/issues/11787")
  public void testFilterWithSameScore() throws IOException {
    int numDocs = 100;
    int dimension = atLeast(5);
    try (Directory d = newDirectory()) {
      // Always use the default kNN format to have predictable behavior around when it hits
      // visitedLimit. This is fine since the test targets AbstractKnnVectorQuery logic, not the kNN
      // format
      // implementation.
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
      IndexWriter w = new IndexWriter(d, iwc);
      float[] vector = randomVector(dimension);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(getKnnVectorField("field", vector));
        doc.add(new IntPoint("tag", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.close();

      try (DirectoryReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        int lower = random().nextInt(50);
        int size = 5;

        // Test a restrictive filter, which usually performs exact search
        Query filter1 = IntPoint.newRangeQuery("tag", lower, lower + 6);
        TopDocs results =
            searcher.search(
                getKnnVectorQuery("field", randomVector(dimension), size, filter1), size);
        assertEquals(size, results.scoreDocs.length);

        // Test an unrestrictive filter, which usually performs approximate search
        Query filter2 = IntPoint.newRangeQuery("tag", lower, numDocs);
        results =
            searcher.search(
                getKnnVectorQuery("field", randomVector(dimension), size, filter2), size);
        assertEquals(size, results.scoreDocs.length);
      }
    }
  }

  public void testDeletes() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final int numDocs = atLeast(100);
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(new StringField("index", String.valueOf(i), Field.Store.YES));
        if (frequently()) {
          d.add(getKnnVectorField("vector", randomVector(dim)));
        }
        w.addDocument(d);
      }
      w.commit();

      // Delete some documents at random, both those with and without vectors
      Set<Term> toDelete = new HashSet<>();
      for (int i = 0; i < 25; i++) {
        int index = random().nextInt(numDocs);
        toDelete.add(new Term("index", String.valueOf(index)));
      }
      w.deleteDocuments(toDelete.toArray(new Term[0]));
      w.commit();

      int hits = 50;
      try (IndexReader reader = DirectoryReader.open(dir)) {
        Set<String> allIds = new HashSet<>();
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("vector", randomVector(dim), hits);
        TopDocs topDocs = searcher.search(query, numDocs);
        StoredFields storedFields = reader.storedFields();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          Document doc = storedFields.document(scoreDoc.doc, Set.of("index"));
          String index = doc.get("index");
          assertFalse(
              "search returned a deleted document: " + index,
              toDelete.contains(new Term("index", index)));
          allIds.add(index);
        }
        assertEquals("search missed some documents", hits, allIds.size());
      }
    }
  }

  public void testAllDeletes() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final int numDocs = atLeast(100);
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(getKnnVectorField("vector", randomVector(dim)));
        w.addDocument(d);
      }
      w.commit();

      w.deleteDocuments(new MatchAllDocsQuery());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("vector", randomVector(dim), numDocs);
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(0, topDocs.scoreDocs.length);
      }
    }
  }

  /**
   * Check that the query behaves reasonably when using a custom filter reader where there are no
   * live docs.
   */
  public void testNoLiveDocsReader() throws IOException {
    IndexWriterConfig iwc = newIndexWriterConfig();
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      final int numDocs = 10;
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(new StringField("index", String.valueOf(i), Field.Store.NO));
        d.add(getKnnVectorField("vector", randomVector(dim)));
        w.addDocument(d);
      }
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        DirectoryReader wrappedReader = new NoLiveDocsDirectoryReader(reader);
        IndexSearcher searcher = new IndexSearcher(wrappedReader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("vector", randomVector(dim), numDocs);
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(0, topDocs.scoreDocs.length);
      }
    }
  }

  /**
   * Test that AbstractKnnVectorQuery optimizes the case where the filter query is backed by {@link
   * BitSetIterator}.
   */
  public void testBitSetQuery() throws IOException {
    IndexWriterConfig iwc = newIndexWriterConfig();
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, iwc)) {
      final int numDocs = 100;
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(getKnnVectorField("vector", randomVector(dim)));
        w.addDocument(d);
      }
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        Query filter = new ThrowingBitSetQuery(new FixedBitSet(numDocs));
        expectThrows(
            RuntimeException.class,
            UnsupportedOperationException.class,
            () ->
                searcher.search(
                    getKnnVectorQuery("vector", randomVector(dim), 10, filter), numDocs));
      }
    }
  }

  /** Creates a new directory and adds documents with the given vectors as kNN vector fields */
  Directory getIndexStore(String field, float[]... contents) throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      doc.add(getKnnVectorField(field, contents[i]));
      doc.add(new StringField("id", "id" + i, Field.Store.YES));
      writer.addDocument(doc);
    }
    // Add some documents without a vector
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StringField("other", "value", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.close();
    return indexStore;
  }

  /**
   * Creates a new directory and adds documents with the given vectors as kNN vector fields,
   * preserving the order of the added documents.
   */
  private Directory getStableIndexStore(String field, float[]... contents) throws IOException {
    Directory indexStore = newDirectory();
    try (IndexWriter writer = new IndexWriter(indexStore, new IndexWriterConfig())) {
      for (int i = 0; i < contents.length; ++i) {
        Document doc = new Document();
        doc.add(getKnnVectorField(field, contents[i]));
        doc.add(new StringField("id", "id" + i, Field.Store.YES));
        writer.addDocument(doc);
      }
      // Add some documents without a vector
      for (int i = 0; i < 5; i++) {
        Document doc = new Document();
        doc.add(new StringField("other", "value", Field.Store.NO));
        writer.addDocument(doc);
      }
    }
    return indexStore;
  }

  private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
      throws IOException {
    ScoreDoc[] result = searcher.search(q, 1000).scoreDocs;
    assertEquals(expectedMatches, result.length);
  }

  void assertIdMatches(IndexReader reader, String expectedId, ScoreDoc scoreDoc)
      throws IOException {
    String actualId = reader.storedFields().document(scoreDoc.doc).get("id");
    assertEquals(expectedId, actualId);
  }

  void assertDocScoreQueryToString(Query query) {
    String queryString = query.toString("ignored");
    // The string should contain matching docIds and their score.
    // Since a forceMerge could occur in this test, we must not assert that a specific doc_id is
    // matched
    // But that instead the string format is expected and that the score is 1.0
    assertTrue(queryString.matches("DocAndScoreQuery\\[\\d+,...]\\[1.0,...]"));
  }

  /**
   * A version of {@link AbstractKnnVectorQuery} that throws an error when an exact search is run.
   * This allows us to check what search strategy is being used.
   */
  private static class NoLiveDocsDirectoryReader extends FilterDirectoryReader {

    private NoLiveDocsDirectoryReader(DirectoryReader in) throws IOException {
      super(
          in,
          new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
              return new NoLiveDocsLeafReader(reader);
            }
          });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new NoLiveDocsDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  private static class NoLiveDocsLeafReader extends FilterLeafReader {
    private NoLiveDocsLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public int numDocs() {
      return 0;
    }

    @Override
    public Bits getLiveDocs() {
      return new Bits.MatchNoBits(in.maxDoc());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }
  }

  static class ThrowingBitSetQuery extends Query {

    private final FixedBitSet docs;

    ThrowingBitSetQuery(FixedBitSet docs) {
      this.docs = docs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          BitSetIterator bitSetIterator =
              new BitSetIterator(docs, docs.approximateCardinality()) {
                @Override
                public BitSet getBitSet() {
                  throw new UnsupportedOperationException("reusing BitSet is not supported");
                }
              };
          return new ConstantScoreScorer(this, score(), scoreMode, bitSetIterator);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public String toString(String field) {
      return "throwingBitSetQuery";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && docs.equals(((ThrowingBitSetQuery) other).docs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + docs.hashCode();
    }
  }
}
