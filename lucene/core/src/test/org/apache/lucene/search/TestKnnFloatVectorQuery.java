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

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestVectorUtil;
import org.apache.lucene.util.VectorUtil;

public class TestKnnFloatVectorQuery extends BaseKnnVectorQueryTestCase {
  @Override
  KnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter) {
    return new KnnFloatVectorQuery(field, query, k, queryFilter);
  }

  @Override
  AbstractKnnVectorQuery getThrowingKnnVectorQuery(String field, float[] vec, int k, Query query) {
    return new ThrowingKnnVectorQuery(field, vec, k, query);
  }

  @Override
  float[] randomVector(int dim) {
    return TestVectorUtil.randomVector(dim);
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnFloatVectorField(name, vector, similarityFunction);
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnFloatVectorField(name, vector);
  }

  public void testToString() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 10);
    assertEquals("KnnFloatVectorQuery:f1[0.0,...][10]", q1.toString("ignored"));
  }

  public void testGetTarget() {
    float[] queryVector = new float[] {0, 1};
    KnnFloatVectorQuery q1 = new KnnFloatVectorQuery("f1", queryVector, 10);

    assertArrayEquals(queryVector, q1.getTargetCopy(), 0);
    assertNotEquals(queryVector, q1.getTargetCopy());
  }

  public void testScoreNegativeDotProduct() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(getKnnVectorField("field", new float[] {-1, 0}, DOT_PRODUCT));
        w.addDocument(doc);
        doc = new Document();
        doc.add(getKnnVectorField("field", new float[] {1, 0}, DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {1, 0}, 2);
        Query rewritten = query.rewrite(reader);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.leaves().get(0));

        // scores are normalized to lie in [0, 1]
        DocIdSetIterator it = scorer.iterator();
        assertEquals(2, it.cost());
        assertEquals(0, it.nextDoc());
        assertEquals(0, scorer.score(), 0);
        assertEquals(1, it.advance(1));
        assertEquals(1, scorer.score(), 0);
      }
    }
  }

  public void testScoreDotProduct() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
        for (int j = 1; j <= 5; j++) {
          Document doc = new Document();
          doc.add(
              getKnnVectorField(
                  "field", VectorUtil.l2normalize(new float[] {j, j * j}), DOT_PRODUCT));
          w.addDocument(doc);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        AbstractKnnVectorQuery query =
            getKnnVectorQuery("field", VectorUtil.l2normalize(new float[] {2, 3}), 3);
        Query rewritten = query.rewrite(reader);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(reader.leaves().get(0));

        // prior to advancing, score is undefined
        assertEquals(-1, scorer.docID());
        expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);

        // test getMaxScore
        assertEquals(0, scorer.getMaxScore(-1), 0);
        /* maxAtZero = ((2,3) * (1, 1) = 5) / (||2, 3|| * ||1, 1|| = sqrt(26)), then
         * normalized by (1 + x) /2.
         */
        float maxAtZero =
            (float) ((1 + (2 * 1 + 3 * 1) / Math.sqrt((2 * 2 + 3 * 3) * (1 * 1 + 1 * 1))) / 2);
        assertEquals(maxAtZero, scorer.getMaxScore(0), 0.001);

        /* max at 2 is actually the score for doc 1 which is the highest (since doc 1 vector (2, 4)
         * is the closest to (2, 3)). This is ((2,3) * (2, 4) = 16) / (||2, 3|| * ||2, 4|| = sqrt(260)), then
         * normalized by (1 + x) /2
         */
        float expected =
            (float) ((1 + (2 * 2 + 3 * 4) / Math.sqrt((2 * 2 + 3 * 3) * (2 * 2 + 4 * 4))) / 2);
        assertEquals(expected, scorer.getMaxScore(2), 0);
        assertEquals(expected, scorer.getMaxScore(Integer.MAX_VALUE), 0);

        DocIdSetIterator it = scorer.iterator();
        assertEquals(3, it.cost());
        assertEquals(0, it.nextDoc());
        // doc 0 has (1, 1)
        assertEquals(maxAtZero, scorer.score(), 0.0001);
        assertEquals(1, it.advance(1));
        assertEquals(expected, scorer.score(), 0);
        assertEquals(2, it.nextDoc());
        // since topK was 3
        assertEquals(NO_MORE_DOCS, it.advance(4));
        expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
      }
    }
  }

  private static class ThrowingKnnVectorQuery extends KnnFloatVectorQuery {

    public ThrowingKnnVectorQuery(String field, float[] target, int k, Query filter) {
      super(field, target, k, filter);
    }

    @Override
    protected TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator) {
      throw new UnsupportedOperationException("exact search is not supported");
    }

    @Override
    public String toString(String field) {
      return null;
    }
  }
}
