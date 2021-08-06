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

import static org.apache.lucene.util.TestVectorUtil.randomVector;

import java.io.IOException;
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

  /** testEquals */
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

  /** Test that when vectors are abnormally distributed among segments, we still find the top K */
  public void testSkewedIndex() throws IOException {
    // We have to choose the numbers carefully here so that some segment has more than the expected
    // number of top K
    // documents, but no more than K documents in total (otherwise we might occasionally randomly
    // fail to find one).
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
          int k = random().nextInt(100) + 1;
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
