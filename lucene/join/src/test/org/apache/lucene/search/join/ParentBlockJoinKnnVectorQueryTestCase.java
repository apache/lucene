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

package org.apache.lucene.search.join;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

abstract class ParentBlockJoinKnnVectorQueryTestCase extends LuceneTestCase {

  static String encodeInts(int[] i) {
    return Arrays.toString(i);
  }

  static BitSetProducer parentFilter(IndexReader r) throws IOException {
    // Create a filter that defines "parent" documents in the index
    BitSetProducer parentsFilter =
        new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
    CheckJoinIndex.check(r, parentsFilter);
    return parentsFilter;
  }

  Document makeParent(int[] children) {
    Document parent = new Document();
    parent.add(newStringField("docType", "_parent", Field.Store.NO));
    parent.add(newStringField("id", encodeInts(children), Field.Store.YES));
    return parent;
  }

  abstract Query getParentJoinKnnQuery(
      String fieldName, float[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet);

  public void testEmptyIndex() throws IOException {
    try (Directory indexStore = getIndexStore("field");
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      Query kvq =
          getParentJoinKnnQuery(
              "field",
              new float[] {1, 2},
              null,
              2,
              new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent"))));
      assertMatches(searcher, kvq, 0);
      Query q = searcher.rewrite(kvq);
      assertTrue(q instanceof MatchNoDocsQuery);
    }
  }

  public void testIndexWithNoVectorsNorParents() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        // Add some documents without a vector
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          doc.add(new StringField("other", "value", Field.Store.NO));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        // Create parent filter directly, tests use "check" to verify parentIds exist. Production
        // may not
        // verify we handle it gracefully
        BitSetProducer parentFilter =
            new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        Query query = getParentJoinKnnQuery("field", new float[] {2, 2}, null, 3, parentFilter);
        TopDocs topDocs = searcher.search(query, 3);
        assertEquals(0, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs.length);

        // Test with match_all filter and large k to test exact search
        query =
            getParentJoinKnnQuery(
                "field", new float[] {2, 2}, new MatchAllDocsQuery(), 10, parentFilter);
        topDocs = searcher.search(query, 3);
        assertEquals(0, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs.length);
      }
    }
  }

  public void testIndexWithNoParents() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int i = 0; i < 3; ++i) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {2, 2}));
          doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
          w.addDocument(doc);
        }
        // Add some documents without a vector
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          doc.add(new StringField("other", "value", Field.Store.NO));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        // Create parent filter directly, tests use "check" to verify parentIds exist. Production
        // may not
        // verify we handle it gracefully
        BitSetProducer parentFilter =
            new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        Query query = getParentJoinKnnQuery("field", new float[] {2, 2}, null, 3, parentFilter);
        TopDocs topDocs = searcher.search(query, 3);
        assertEquals(0, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs.length);

        // Test with match_all filter and large k to test exact search
        query =
            getParentJoinKnnQuery(
                "field", new float[] {2, 2}, new MatchAllDocsQuery(), 10, parentFilter);
        topDocs = searcher.search(query, 3);
        assertEquals(0, topDocs.totalHits.value);
        assertEquals(0, topDocs.scoreDocs.length);
      }
    }
  }

  public void testFilterWithNoVectorMatches() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);
      Query filter = new TermQuery(new Term("other", "value"));
      BitSetProducer parentFilter = parentFilter(reader);
      Query kvq = getParentJoinKnnQuery("field", new float[] {1, 2}, filter, 2, parentFilter);
      TopDocs topDocs = searcher.search(kvq, 3);
      assertEquals(0, topDocs.totalHits.value);
    }
  }

  public void testScoringWithMultipleChildren() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        List<Document> toAdd = new ArrayList<>();
        for (int j = 1; j <= 5; j++) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j}));
          doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
          toAdd.add(doc);
        }
        toAdd.add(makeParent(new int[] {1, 2, 3, 4, 5}));
        w.addDocuments(toAdd);

        toAdd = new ArrayList<>();
        for (int j = 7; j <= 11; j++) {
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j}));
          doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
          toAdd.add(doc);
        }
        toAdd.add(makeParent(new int[] {6, 7, 8, 9, 10}));
        w.addDocuments(toAdd);
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
        Query query = getParentJoinKnnQuery("field", new float[] {2, 2}, null, 3, parentFilter);
        assertScorerResults(searcher, query, new float[] {1f, 1f / 51f}, new String[] {"2", "7"});

        query = getParentJoinKnnQuery("field", new float[] {6, 6}, null, 3, parentFilter);
        assertScorerResults(
            searcher, query, new float[] {1f / 3f, 1f / 3f}, new String[] {"5", "7"});
        query =
            getParentJoinKnnQuery(
                "field", new float[] {6, 6}, new MatchAllDocsQuery(), 20, parentFilter);
        assertScorerResults(
            searcher, query, new float[] {1f / 3f, 1f / 3f}, new String[] {"5", "7"});

        query =
            getParentJoinKnnQuery(
                "field", new float[] {6, 6}, new MatchAllDocsQuery(), 1, parentFilter);
        assertScorerResults(searcher, query, new float[] {1f / 3f}, new String[] {"5"});
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
            List<Document> toAdd = new ArrayList<>();
            Document doc = new Document();
            doc.add(getKnnVectorField("field", new float[] {r, r}));
            doc.add(newStringField("id", Integer.toString(r), Field.Store.YES));
            toAdd.add(doc);
            toAdd.add(makeParent(new int[] {r}));
            w.addDocuments(toAdd);
            ++r;
          }
          w.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        TopDocs results =
            searcher.search(
                getParentJoinKnnQuery(
                    "field", new float[] {0, 0}, null, 8, parentFilter(searcher.getIndexReader())),
                10);
        assertEquals(8, results.scoreDocs.length);
        assertIdMatches(reader, "0", results.scoreDocs[0].doc);
        assertIdMatches(reader, "7", results.scoreDocs[7].doc);

        // test some results in the middle of the sequence - also tests docid tiebreaking
        results =
            searcher.search(
                getParentJoinKnnQuery(
                    "field",
                    new float[] {10, 10},
                    null,
                    8,
                    parentFilter(searcher.getIndexReader())),
                10);
        assertEquals(8, results.scoreDocs.length);
        assertIdMatches(reader, "10", results.scoreDocs[0].doc);
        assertIdMatches(reader, "6", results.scoreDocs[7].doc);
      }
    }
  }

  Directory getIndexStore(String field, float[]... contents) throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for (int i = 0; i < contents.length; ++i) {
      List<Document> toAdd = new ArrayList<>();
      Document doc = new Document();
      doc.add(getKnnVectorField(field, contents[i]));
      doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
      toAdd.add(doc);
      toAdd.add(makeParent(new int[] {i}));
      writer.addDocuments(toAdd);
    }
    // Add some documents without a vector
    for (int i = 0; i < 5; i++) {
      List<Document> toAdd = new ArrayList<>();
      Document doc = new Document();
      doc.add(new StringField("other", "value", Field.Store.NO));
      toAdd.add(doc);
      toAdd.add(makeParent(new int[0]));
      writer.addDocuments(toAdd);
    }
    writer.close();
    return indexStore;
  }

  // @Override
  abstract Field getKnnVectorField(String name, float[] vector);

  abstract Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction vectorSimilarityFunction);

  private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
      throws IOException {
    ScoreDoc[] result = searcher.search(q, 1000).scoreDocs;
    assertEquals(expectedMatches, result.length);
  }

  void assertIdMatches(IndexReader reader, String expectedId, int docId) throws IOException {
    String actualId = reader.storedFields().document(docId).get("id");
    assertEquals(expectedId, actualId);
  }

  void assertScorerResults(IndexSearcher searcher, Query query, float[] scores, String[] ids)
      throws IOException {
    IndexReader reader = searcher.getIndexReader();
    Query rewritten = query.rewrite(searcher);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
    Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    // prior to advancing, score is undefined
    assertEquals(-1, scorer.docID());
    expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
    DocIdSetIterator it = scorer.iterator();
    for (int i = 0; i < scores.length; i++) {
      int docId = it.nextDoc();
      assertNotEquals(NO_MORE_DOCS, docId);
      assertEquals(scores[i], scorer.score(), 0.0001);
      assertIdMatches(reader, ids[i], docId);
    }
  }
}
