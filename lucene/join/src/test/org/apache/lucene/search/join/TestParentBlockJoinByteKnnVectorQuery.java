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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
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
import org.apache.lucene.util.BytesRef;

public class TestParentBlockJoinByteKnnVectorQuery extends ParentBlockJoinKnnVectorQueryTestCase {

  @Override
  Query getParentJoinKnnQuery(
      String fieldName,
      float[] queryVector,
      Query childFilter,
      int k,
      BitSetProducer parentBitSet) {
    return new DiversifyingChildrenByteKnnVectorQuery(
        fieldName, fromFloat(queryVector), childFilter, k, parentBitSet);
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnByteVectorField(name, fromFloat(vector));
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
    return new KnnByteVectorField(name, fromFloat(vector), vectorSimilarityFunction);
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
            new DiversifyingChildrenFloatKnnVectorQuery(
                "field", new float[] {1, 2}, null, 2, parentFilter);
        assertThrows(IllegalStateException.class, () -> searcher.search(kvq, 3));
      }
    }
  }

  public void testToString() {
    // test without filter
    Query query = getParentJoinKnnQuery("field", new float[] {0, 1}, null, 10, null);
    assertEquals(
        "DiversifyingChildrenByteKnnVectorQuery:field[0,...][10]", query.toString("ignored"));

    // test with filter
    Query filter = new TermQuery(new Term("id", "text"));
    query = getParentJoinKnnQuery("field", new float[] {0, 1}, filter, 10, null);
    assertEquals(
        "DiversifyingChildrenByteKnnVectorQuery:field[0,...][10][id:text]",
        query.toString("ignored"));
  }

  private static byte[] fromFloat(float[] queryVector) {
    byte[] query = new byte[queryVector.length];
    for (int i = 0; i < queryVector.length; i++) {
      assert queryVector[i] == (byte) queryVector[i];
      query[i] = (byte) queryVector[i];
    }
    return query;
  }

  @Override
  float[] randomVector(int dim) {
    BytesRef v = TestUtil.randomBinaryTerm(random(), dim);
    // clip at -127 to avoid overflow
    for (int i = v.offset; i < v.offset + v.length; i++) {
      if (v.bytes[i] == -128) {
        v.bytes[i] = -127;
      }
    }
    assert v.offset == 0;
    byte[] b = v.bytes;
    float[] v1 = new float[b.length];
    int vi = 0;
    for (int i = 0; i < v.length; i++) {
      v1[vi++] = b[i];
    }
    return v1;
  }
}
