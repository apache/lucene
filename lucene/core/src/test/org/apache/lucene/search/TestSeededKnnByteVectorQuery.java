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

import static org.apache.lucene.search.TestKnnByteVectorQuery.floatToBytes;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.TestVectorUtil;

public class TestSeededKnnByteVectorQuery extends BaseKnnVectorQueryTestCase {

  private static final Query MATCH_NONE = new MatchNoDocsQuery();

  @Override
  AbstractKnnVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter) {
    return new SeededKnnByteVectorQuery(field, floatToBytes(query), k, queryFilter, MATCH_NONE);
  }

  @Override
  AbstractKnnVectorQuery getThrowingKnnVectorQuery(String field, float[] vec, int k, Query query) {
    return new ThrowingKnnVectorQuery(field, floatToBytes(vec), k, query, MATCH_NONE);
  }

  @Override
  float[] randomVector(int dim) {
    byte[] b = TestVectorUtil.randomVectorBytes(dim);
    float[] v = new float[b.length];
    int vi = 0;
    for (int i = 0; i < v.length; i++) {
      v[vi++] = b[i];
    }
    return v;
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnByteVectorField(name, floatToBytes(vector), similarityFunction);
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnByteVectorField(name, floatToBytes(vector), VectorSimilarityFunction.EUCLIDEAN);
  }

  /** Tests with random vectors and a random seed. Uses RandomIndexWriter. */
  public void testRandomWithSeed() throws IOException {
    int numDocs = 1000;
    int dimension = atLeast(5);
    int numIters = atLeast(10);
    int numDocsWithVector = 0;
    try (Directory d = newDirectoryForTest()) {
      // Always use the default kNN format to have predictable behavior around when it hits
      // visitedLimit. This is fine since the test targets AbstractKnnVectorQuery logic, not the kNN
      // format
      // implementation.
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
      RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) {
          // Randomly skip some vectors to test the mapping from docid to ordinals
          doc.add(getKnnVectorField("field", randomVector(dimension)));
          numDocsWithVector += 1;
        }
        doc.add(new NumericDocValuesField("tag", i));
        doc.add(new IntPoint("tag", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.close();

      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i < numIters; i++) {
          int k = random().nextInt(80) + 1;
          int n = random().nextInt(100) + 1;
          // we may get fewer results than requested if there are deletions, but this test doesn't
          // check that
          assert reader.hasDeletions() == false;

          // All documents as seeds
          Query seed1 = new MatchAllDocsQuery();
          Query filter = random().nextBoolean() ? null : new MatchAllDocsQuery();
          SeededKnnByteVectorQuery query =
              new SeededKnnByteVectorQuery(
                  "field", floatToBytes(randomVector(dimension)), k, filter, seed1);
          TopDocs results = searcher.search(query, n);
          int expected = Math.min(Math.min(n, k), numDocsWithVector);

          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          float last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }

          // Restrictive seed query -- 6 documents
          Query seed2 = IntPoint.newRangeQuery("tag", 1, 6);
          query =
              new SeededKnnByteVectorQuery(
                  "field", floatToBytes(randomVector(dimension)), k, null, seed2);
          results = searcher.search(query, n);
          expected = Math.min(Math.min(n, k), reader.numDocs());
          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }

          // No seed documents -- falls back on full approx search
          Query seed3 = new MatchNoDocsQuery();
          query =
              new SeededKnnByteVectorQuery(
                  "field", floatToBytes(randomVector(dimension)), k, null, seed3);
          results = searcher.search(query, n);
          expected = Math.min(Math.min(n, k), reader.numDocs());
          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }
        }
      }
    }
  }

  private static class ThrowingKnnVectorQuery extends SeededKnnByteVectorQuery {

    public ThrowingKnnVectorQuery(String field, byte[] target, int k, Query filter, Query seed) {
      super(field, target, k, filter, seed);
    }

    @Override
    protected TopDocs exactSearch(
        LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout) {
      throw new UnsupportedOperationException("exact search is not supported");
    }

    @Override
    public String toString(String field) {
      return null;
    }
  }
}
