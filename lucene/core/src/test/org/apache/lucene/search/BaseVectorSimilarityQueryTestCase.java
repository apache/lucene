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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

@LuceneTestCase.SuppressCodecs("SimpleText")
abstract class BaseVectorSimilarityQueryTestCase<
        V, F extends Field, Q extends AbstractVectorSimilarityQuery>
    extends LuceneTestCase {
  String vectorField, idField;
  VectorSimilarityFunction function;
  int numDocs, dim;

  abstract V getRandomVector(int dim);

  abstract float compare(V vector1, V vector2);

  abstract boolean checkEquals(V vector1, V vector2);

  abstract F getVectorField(String name, V vector, VectorSimilarityFunction function);

  abstract Q getVectorQuery(
      String field, V vector, float traversalSimilarity, float resultSimilarity, Query filter);

  abstract Q getThrowingVectorQuery(
      String field, V vector, float traversalSimilarity, float resultSimilarity, Query filter);

  public void testEquals() {
    String field1 = "f1", field2 = "f2";

    V vector1 = getRandomVector(dim);
    V vector2;
    do {
      vector2 = getRandomVector(dim);
    } while (checkEquals(vector1, vector2));

    float traversalSimilarity1 = 0.3f, traversalSimilarity2 = 0.4f;
    float resultSimilarity1 = 0.4f, resultSimilarity2 = 0.5f;

    Query filter1 = new TermQuery(new Term("t1", "v1"));
    Query filter2 = new TermQuery(new Term("t2", "v2"));

    Query query = getVectorQuery(field1, vector1, traversalSimilarity1, resultSimilarity1, filter1);

    // Everything is equal
    assertEquals(
        query, getVectorQuery(field1, vector1, traversalSimilarity1, resultSimilarity1, filter1));

    // Null check
    assertNotEquals(query, null);

    // Different field
    assertNotEquals(
        query, getVectorQuery(field2, vector1, traversalSimilarity1, resultSimilarity1, filter1));

    // Different vector
    assertNotEquals(
        query, getVectorQuery(field1, vector2, traversalSimilarity1, resultSimilarity1, filter1));

    // Different traversalSimilarity
    assertNotEquals(
        query, getVectorQuery(field1, vector1, traversalSimilarity2, resultSimilarity1, filter1));

    // Different resultSimilarity
    assertNotEquals(
        query, getVectorQuery(field1, vector1, traversalSimilarity1, resultSimilarity2, filter1));

    // Different filter
    assertNotEquals(
        query, getVectorQuery(field1, vector1, traversalSimilarity1, resultSimilarity1, filter2));
  }

  public void testEmptyIndex() throws IOException {
    // Do not index any vectors
    numDocs = 0;

    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);

      // Check that no vectors are found
      assertEquals(0, searcher.count(query));
    }
  }

  public void testExtremes() throws IOException {
    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      // All vectors are above -Infinity
      Query query1 =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);

      // Check that all vectors are found
      assertEquals(numDocs, searcher.count(query1));

      // No vectors are above +Infinity
      Query query2 =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.POSITIVE_INFINITY,
              Float.POSITIVE_INFINITY,
              null);

      // Check that no vectors are found
      assertEquals(0, searcher.count(query2));
    }
  }

  public void testRandomFilter() throws IOException {
    // Filter a sub-range from 0 to numDocs
    int startIndex = random().nextInt(numDocs);
    int endIndex = random().nextInt(numDocs - startIndex) + startIndex;
    Query filter = IntField.newRangeQuery(idField, startIndex, endIndex);

    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              filter);

      ScoreDoc[] scoreDocs = searcher.search(query, numDocs).scoreDocs;
      for (ScoreDoc scoreDoc : scoreDocs) {
        int id = getId(searcher, scoreDoc.doc);

        // Check that returned document is in selected range
        assertTrue(id >= startIndex && id <= endIndex);
      }
      // Check that all filtered vectors are found
      assertEquals(endIndex - startIndex + 1, scoreDocs.length);
    }
  }

  public void testFilterWithNoMatches() throws IOException {
    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      // Non-existent field
      Query filter1 = new TermQuery(new Term("random_field", "random_value"));
      Query query1 =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              filter1);

      // Check that no vectors are found
      assertEquals(0, searcher.count(query1));

      // Field exists, but value of -1 is not indexed
      Query filter2 = IntField.newExactQuery(idField, -1);
      Query query2 =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              filter2);

      // Check that no vectors are found
      assertEquals(0, searcher.count(query2));
    }
  }

  public void testDimensionMismatch() throws IOException {
    // Different dimension
    int newDim = atLeast(dim + 1);

    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query =
          getVectorQuery(
              vectorField,
              getRandomVector(newDim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);

      // Check that an exception for differing dimensions is thrown
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> searcher.count(query));
      assertEquals(
          String.format(
              Locale.ROOT,
              "vector query dimension: %d differs from field dimension: %d",
              newDim,
              dim),
          e.getMessage());
    }
  }

  public void testNonVectorsField() throws IOException {
    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      // Non-existent field
      Query query1 =
          getVectorQuery(
              "random_field",
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);
      assertEquals(0, searcher.count(query1));

      // Indexed as int field
      Query query2 =
          getVectorQuery(
              idField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);
      assertEquals(0, searcher.count(query2));
    }
  }

  public void testSomeDeletes() throws IOException {
    // Delete a sub-range from 0 to numDocs
    int startIndex = random().nextInt(numDocs);
    int endIndex = random().nextInt(numDocs - startIndex) + startIndex;
    Query delete = IntField.newRangeQuery(idField, startIndex, endIndex);

    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexWriter w = new IndexWriter(indexStore, newIndexWriterConfig())) {

      w.deleteDocuments(delete);
      w.commit();

      try (IndexReader reader = DirectoryReader.open(indexStore)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            getVectorQuery(
                vectorField,
                getRandomVector(dim),
                Float.NEGATIVE_INFINITY,
                Float.NEGATIVE_INFINITY,
                null);

        ScoreDoc[] scoreDocs = searcher.search(query, numDocs).scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
          int id = getId(searcher, scoreDoc.doc);

          // Check that returned document is not deleted
          assertFalse(id >= startIndex && id <= endIndex);
        }
        // Check that all live docs are returned
        assertEquals(numDocs - endIndex + startIndex - 1, scoreDocs.length);
      }
    }
  }

  public void testAllDeletes() throws IOException {
    try (Directory dir = getIndexStore(getRandomVectors(numDocs, dim));
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      // Delete all documents
      w.deleteDocuments(new MatchAllDocsQuery());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            getVectorQuery(
                vectorField,
                getRandomVector(dim),
                Float.NEGATIVE_INFINITY,
                Float.NEGATIVE_INFINITY,
                null);

        // Check that no vectors are found
        assertEquals(0, searcher.count(query));
      }
    }
  }

  public void testBoostQuery() throws IOException {
    // Define the boost and allowed delta
    float boost = random().nextInt(5) + 5;
    float delta = 1e-3f;

    try (Directory indexStore = getIndexStore(getRandomVectors(numDocs, dim));
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query1 =
          getVectorQuery(
              vectorField,
              getRandomVector(dim),
              Float.NEGATIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              null);
      ScoreDoc[] scoreDocs1 = searcher.search(query1, numDocs).scoreDocs;

      Query query2 = new BoostQuery(query1, boost);
      ScoreDoc[] scoreDocs2 = searcher.search(query2, numDocs).scoreDocs;

      // Check that original scores and boosted scores are equal considering the delta to account
      // for floating point precision limitations. Don't take the exact result order into
      // consideration as for small original scores with tiny differences
      // the boosted scores can be become the same, which might affect the result order.
      assertEquals(scoreDocs1.length, scoreDocs2.length);
      for (int i = 0; i < scoreDocs1.length; i++) {
        int idx = i;
        Optional<ScoreDoc> boostedDoc =
            Arrays.stream(scoreDocs2).filter(d -> d.doc == scoreDocs1[idx].doc).findFirst();

        assertTrue(boostedDoc.isPresent());
        assertEquals(boost * scoreDocs1[i].score, boostedDoc.get().score, delta);
      }
    }
  }

  public void testVectorsAboveSimilarity() throws IOException {
    // Pick number of docs to accept
    int numAccepted = random().nextInt(numDocs / 2 - numDocs / 3) + numDocs / 3;
    float delta = 1e-3f;

    V[] vectors = getRandomVectors(numDocs, dim);
    V queryVector = getRandomVector(dim);

    // Find score above which we get (at least) numAccepted vectors
    float resultSimilarity = getSimilarity(vectors, queryVector, numAccepted);

    // Cache scores of vectors
    Map<Integer, Float> scores = new HashMap<>();
    for (int i = 0; i < numDocs; i++) {
      float score = compare(queryVector, vectors[i]);
      if (score >= resultSimilarity) {
        scores.put(i, score);
      }
    }

    try (Directory indexStore = getIndexStore(vectors);
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query =
          getVectorQuery(vectorField, queryVector, Float.NEGATIVE_INFINITY, resultSimilarity, null);

      ScoreDoc[] scoreDocs = searcher.search(query, numDocs).scoreDocs;
      for (ScoreDoc scoreDoc : scoreDocs) {
        int id = getId(searcher, scoreDoc.doc);

        // Check that the collected result is above accepted similarity
        assertTrue(scores.containsKey(id));

        // Check that the score is correct
        assertEquals(scores.get(id), scoreDoc.score, delta);
      }

      // Check that all results are collected
      assertEquals(scores.size(), scoreDocs.length);
    }
  }

  public void testFallbackToExact() throws IOException {
    // Restrictive filter, along with similarity to visit a large number of nodes
    int numFiltered = random().nextInt(numDocs / 5 - numDocs / 10) + numDocs / 10;
    int targetVisited = random().nextInt(numDocs - numFiltered * 2) + numFiltered * 2;

    V[] vectors = getRandomVectors(numDocs, dim);
    V queryVector = getRandomVector(dim);

    float resultSimilarity = getSimilarity(vectors, queryVector, targetVisited);
    Query filter = IntField.newSetQuery(idField, getFiltered(numFiltered));

    try (Directory indexStore = getIndexStore(vectors);
        IndexReader reader = DirectoryReader.open(indexStore)) {
      IndexSearcher searcher = newSearcher(reader);

      Query query =
          getThrowingVectorQuery(
              vectorField, queryVector, resultSimilarity, resultSimilarity, filter);

      // Falls back to exact search
      expectThrows(UnsupportedOperationException.class, () -> searcher.count(query));
    }
  }

  public void testApproximate() throws IOException {
    // Non-restrictive filter, along with similarity to visit a small number of nodes
    int numFiltered = numDocs - 1;
    int targetVisited = random().nextInt(numFiltered / 10 - 1) + 1;

    V[] vectors = getRandomVectors(numDocs, dim);
    V queryVector = getRandomVector(dim);

    float resultSimilarity = getSimilarity(vectors, queryVector, targetVisited);
    Query filter = IntField.newSetQuery(idField, getFiltered(numFiltered));

    try (Directory indexStore = getIndexStore(vectors);
        IndexWriter w = new IndexWriter(indexStore, newIndexWriterConfig())) {
      // Force merge because smaller segments have few filtered docs and often fall back to exact
      // search, making this test flaky
      w.forceMerge(1);
      w.commit();

      try (IndexReader reader = DirectoryReader.open(indexStore)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            getThrowingVectorQuery(
                vectorField, queryVector, resultSimilarity, resultSimilarity, filter);

        // Does not fall back to exact search
        assertTrue(searcher.count(query) <= numFiltered);
      }
    }
  }

  private float getSimilarity(V[] vectors, V queryVector, int targetVisited) {
    assertTrue(targetVisited >= 0 && targetVisited <= numDocs);
    if (targetVisited == 0) {
      return Float.POSITIVE_INFINITY;
    }

    float[] scores = new float[numDocs];
    for (int i = 0; i < numDocs; i++) {
      scores[i] = compare(queryVector, vectors[i]);
    }
    Arrays.sort(scores);

    return scores[numDocs - targetVisited];
  }

  private int[] getFiltered(int numFiltered) {
    Set<Integer> accepted = new HashSet<>();
    for (int i = 0; i < numFiltered; ) {
      int index = random().nextInt(numDocs);
      if (!accepted.contains(index)) {
        accepted.add(index);
        i++;
      }
    }
    return accepted.stream().mapToInt(Integer::intValue).toArray();
  }

  private int getId(IndexSearcher searcher, int doc) throws IOException {
    return Objects.requireNonNull(searcher.storedFields().document(doc).getField(idField))
        .numericValue()
        .intValue();
  }

  @SuppressWarnings("unchecked")
  V[] getRandomVectors(int numDocs, int dim) {
    return (V[]) IntStream.range(0, numDocs).mapToObj(i -> getRandomVector(dim)).toArray();
  }

  @SafeVarargs
  final Directory getIndexStore(V... vectors) throws IOException {
    Directory dir = newDirectory();
    try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < vectors.length; ++i) {
        Document doc = new Document();
        doc.add(getVectorField(vectorField, vectors[i], function));
        doc.add(new IntField(idField, i, Field.Store.YES));
        writer.addDocument(doc);
      }
    }
    return dir;
  }
}
