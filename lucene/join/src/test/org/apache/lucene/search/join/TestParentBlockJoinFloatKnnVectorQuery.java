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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;

public class TestParentBlockJoinFloatKnnVectorQuery extends ParentBlockJoinKnnVectorQueryTestCase {

  @Override
  Query getParentJoinKnnQuery(
      String fieldName,
      float[] queryVector,
      Query childFilter,
      int k,
      BitSetProducer parentBitSet) {
    return new DiversifyingChildrenFloatKnnVectorQuery(
        fieldName, queryVector, childFilter, k, parentBitSet);
  }

  public void testVectorEncodingMismatch() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d, new IndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
        List<Document> toAdd = new ArrayList<>();
        Document doc = new Document();
        doc.add(getKnnVectorField("field", new float[] {1, 1}, COSINE));
        toAdd.add(doc);
        toAdd.add(makeParent(new int[] {1}));
        w.addDocuments(toAdd);
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        BitSetProducer parentFilter = parentFilter(reader);
        Query kvq =
            new DiversifyingChildrenByteKnnVectorQuery(
                "field", new byte[] {1, 2}, null, 2, parentFilter);
        assertThrows(IllegalStateException.class, () -> searcher.search(kvq, 3));
      }
    }
  }

  public void testScoreCosine() throws IOException {
    try (Directory d = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              d,
              new IndexWriterConfig()
                  .setCodec(TestUtil.getDefaultCodec())
                  .setMergePolicy(newMergePolicy(random(), false)))) {
        for (int j = 1; j <= 5; j++) {
          List<Document> toAdd = new ArrayList<>();
          Document doc = new Document();
          doc.add(getKnnVectorField("field", new float[] {j, j * j}, COSINE));
          doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
          toAdd.add(doc);
          toAdd.add(makeParent(new int[] {j}));
          w.addDocuments(toAdd);
        }
      }
      try (IndexReader reader = DirectoryReader.open(d)) {
        assertEquals(1, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
        DiversifyingChildrenFloatKnnVectorQuery query =
            new DiversifyingChildrenFloatKnnVectorQuery(
                "field", new float[] {2, 3}, null, 3, parentFilter);
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

        assertScorerResults(
            searcher, query, new float[] {score0, score1}, new String[] {"1", "2"}, 2);
      }
    }
  }

  public void testToString() {
    // test without filter
    Query query = getParentJoinKnnQuery("field", new float[] {0, 1}, null, 10, null);
    assertEquals(
        "DiversifyingChildrenFloatKnnVectorQuery:field[0.0,...][10]", query.toString("ignored"));

    // test with filter
    Query filter = new TermQuery(new Term("id", "text"));
    query = getParentJoinKnnQuery("field", new float[] {0.0f, 1.0f}, filter, 10, null);
    assertEquals(
        "DiversifyingChildrenFloatKnnVectorQuery:field[0.0,...][10][id:text]",
        query.toString("ignored"));
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnFloatVectorField(name, vector);
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
    return new KnnFloatVectorField(name, vector, vectorSimilarityFunction);
  }

  @Override
  float[] randomVector(int dim) {
    float[] v = new float[dim];
    Random random = random();
    for (int i = 0; i < dim; i++) {
      v[i] = random.nextFloat();
    }
    return v;
  }
}
