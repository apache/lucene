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
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.TestVectorUtil.randomVector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** TestKnnVectorQuery tests KnnVectorQuery. */
public class TestKnnVectorQuery extends LuceneTestCase {

  public void testEquals() {
    KnnVectorQuery q1 = new KnnVectorQuery("f1", new float[] {0, 1}, 10);

    assertEquals(q1, new KnnVectorQuery("f1", new float[] {0, 1}, 10));

    assertNotEquals(null, q1);

    assertNotEquals(q1, new TermQuery(new Term("f1", "x")));

    assertNotEquals(q1, new KnnVectorQuery("f2", new float[] {0, 1}, 10));
    assertNotEquals(q1, new KnnVectorQuery("f1", new float[] {1, 1}, 10));
    assertNotEquals(q1, new KnnVectorQuery("f1", new float[] {0, 1}, 2));
    assertNotEquals(q1, new KnnVectorQuery("f1", new float[] {0}, 10));
  }

  public void testToString() {
    KnnVectorQuery q1 = new KnnVectorQuery("f1", new float[] {0, 1}, 10);
    assertEquals("KnnVectorQuery:f1[0.0,...][10]", q1.toString("ignored"));
  }

  /**
   * Tests if a KnnVectorQuery is rewritten to a MatchNoDocsQuery when there are no documents to
   * match.
   */
  public void testEmptyIndex() throws IOException {
    try (Directory indexStore = getIndexStore("field");
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      KnnVectorQuery kvq = new KnnVectorQuery("field", new float[] {1, 2}, 10);
      assertMatches(searcher, kvq, 0);
      Query q = searcher.rewrite(kvq);
      assertTrue(q instanceof MatchNoDocsQuery);
    }
  }

  /**
   * Tests that a KnnVectorQuery whose topK &gt;= numDocs returns all the documents in score order
   */
  public void testFindAll() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      KnnVectorQuery kvq = new KnnVectorQuery("field", new float[] {0, 0}, 10);
      assertMatches(searcher, kvq, reader.numDocs());
      TopDocs topDocs = searcher.search(kvq, 3);
      assertEquals(2, topDocs.scoreDocs[0].doc);
      assertEquals(0, topDocs.scoreDocs[1].doc);
      assertEquals(1, topDocs.scoreDocs[2].doc);
    }
  }

  /** testDimensionMismatch */
  public void testDimensionMismatch() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      KnnVectorQuery kvq = new KnnVectorQuery("field", new float[] {0}, 10);
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> searcher.search(kvq, 10));
      assertEquals("vector dimensions differ: 1!=2", e.getMessage());
    }
  }

  /** testNonVectorField */
  public void testNonVectorField() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      assertMatches(searcher, new KnnVectorQuery("xyzzy", new float[] {0}, 10), 0);
      assertMatches(searcher, new KnnVectorQuery("id", new float[] {0}, 10), 0);
    }
  }

  /** Test bad parameters */
  public void testIllegalArguments() throws IOException {
    expectThrows(
        IllegalArgumentException.class, () -> new KnnVectorQuery("xx", new float[] {1}, 0));
  }

  public void testDifferentReader() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      KnnVectorQuery query = new KnnVectorQuery("field", new float[] {2, 3}, 3);
      Query dasq = query.rewrite(reader);
      IndexSearcher leafSearcher = newSearcher(reader.leaves().get(0).reader());
      expectThrows(
          IllegalStateException.class,
          () -> dasq.createWeight(leafSearcher, ScoreMode.COMPLETE, 1));
    }
  }

  public void testAdvanceShallow() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 0; j < 5; j++) {
          Document doc = new Document();
          doc.add(new KnnVectorField("field", new float[] {j, j}));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        KnnVectorQuery query = new KnnVectorQuery("field", new float[] {2, 3}, 3);
        Query dasq = query.rewrite(reader);
        Scorer scorer =
            dasq.createWeight(searcher, ScoreMode.COMPLETE, 1).scorer(reader.leaves().get(0));
        // before advancing the iterator
        assertEquals(1, scorer.advanceShallow(0));
        assertEquals(1, scorer.advanceShallow(1));
        assertEquals(NO_MORE_DOCS, scorer.advanceShallow(10));

        // after advancing the iterator
        scorer.iterator().advance(2);
        assertEquals(2, scorer.advanceShallow(0));
        assertEquals(2, scorer.advanceShallow(2));
        assertEquals(3, scorer.advanceShallow(3));
        assertEquals(NO_MORE_DOCS, scorer.advanceShallow(10));
      }
    }
  }

  public void testScore() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 0; j < 5; j++) {
          Document doc = new Document();
          doc.add(new KnnVectorField("field", new float[] {j, j}));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        KnnVectorQuery query = new KnnVectorQuery("field", new float[] {2, 3}, 3);
        Query rewritten = query.rewrite(reader);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.leaves().get(0));

        // prior to advancing, score is 0
        assertEquals(-1, scorer.docID());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> scorer.score());

        // test getMaxScore
        assertEquals(0, scorer.getMaxScore(-1), 0);
        assertEquals(0, scorer.getMaxScore(0), 0);
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
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> scorer.score());
      }
    }
  }

  public void testExplain() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 0; j < 5; j++) {
          Document doc = new Document();
          doc.add(new KnnVectorField("field", new float[] {j, j}));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        KnnVectorQuery query = new KnnVectorQuery("field", new float[] {2, 3}, 3);
        Explanation matched = searcher.explain(query, 2);
        assertTrue(matched.isMatch());
        assertEquals(1 / 2f, matched.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("within top 3", matched.getDescription());

        Explanation nomatch = searcher.explain(query, 4);
        assertFalse(nomatch.isMatch());
        assertEquals(0f, nomatch.getValue());
        assertEquals(0, matched.getDetails().length);
        assertEquals("not in top 3", nomatch.getDescription());
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
            doc.add(new KnnVectorField("field", new float[] {r, r}));
            w.addDocument(doc);
            ++r;
          }
          w.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        TopDocs results = searcher.search(new KnnVectorQuery("field", new float[] {0, 0}, 8), 10);
        assertEquals(8, results.scoreDocs.length);
        assertEquals(0, results.scoreDocs[0].doc);
        assertEquals(7, results.scoreDocs[7].doc);

        // test some results in the middle of the sequence - also tests docid tiebreaking
        results = searcher.search(new KnnVectorQuery("field", new float[] {10, 10}, 8), 10);
        assertEquals(8, results.scoreDocs.length);
        assertEquals(10, results.scoreDocs[0].doc);
        assertEquals(6, results.scoreDocs[7].doc);
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
          doc.add(new KnnVectorField("field", randomVector(dimension)));
        }
        w.addDocument(doc);
      }
      w.close();
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i < numIters; i++) {
          int k = random().nextInt(80) + 1;
          KnnVectorQuery query = new KnnVectorQuery("field", randomVector(dimension), k);
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

  public void testDeletes() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final int numDocs = atLeast(100);
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(new StringField("index", String.valueOf(i), Field.Store.YES));
        if (frequently()) {
          d.add(new KnnVectorField("vector", randomVector(dim)));
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
        KnnVectorQuery query = new KnnVectorQuery("vector", randomVector(dim), hits);
        TopDocs topDocs = searcher.search(query, numDocs);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          Document doc = reader.document(scoreDoc.doc, Set.of("index"));
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
        d.add(new KnnVectorField("vector", randomVector(dim)));
        w.addDocument(d);
      }
      w.commit();

      w.deleteDocuments(new MatchAllDocsQuery());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        KnnVectorQuery query = new KnnVectorQuery("vector", randomVector(dim), numDocs);
        TopDocs topDocs = searcher.search(query, numDocs);
        assertEquals(0, topDocs.scoreDocs.length);
      }
    }
  }

  private Directory getIndexStore(String field, float[]... contents) throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      doc.add(new KnnVectorField(field, contents[i]));
      doc.add(new StringField("id", "id" + i, Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.close();
    return indexStore;
  }

  private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
      throws IOException {
    ScoreDoc[] result = searcher.search(q, 1000).scoreDocs;
    assertEquals(expectedMatches, result.length);
  }
}
