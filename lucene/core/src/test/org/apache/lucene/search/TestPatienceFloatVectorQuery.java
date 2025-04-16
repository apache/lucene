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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestVectorUtil;

public class TestPatienceFloatVectorQuery extends BaseKnnVectorQueryTestCase {

  @Override
  PatienceKnnVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter) {
    return PatienceKnnVectorQuery.fromFloatQuery(
        new KnnFloatVectorQuery(field, query, k, queryFilter));
  }

  @Override
  AbstractKnnVectorQuery getThrowingKnnVectorQuery(String field, float[] vec, int k, Query query) {
    return PatienceKnnVectorQuery.fromFloatQuery(new ThrowingKnnVectorQuery(field, vec, k, query));
  }

  @Override
  AbstractKnnVectorQuery getCappedResultsThrowingKnnVectorQuery(
      String field, float[] vec, int k, Query query, int maxResults) {
    return new TestKnnFloatVectorQuery.CappedResultsThrowingKnnVectorQuery(
        field, vec, k, query, maxResults);
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

  public void testToString() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      AbstractKnnVectorQuery query = getKnnVectorQuery("field", new float[] {0.0f, 1.0f}, 10);
      assertEquals(
          "PatienceKnnVectorQuery{saturationThreshold=0.995, patience=7, delegate=KnnFloatVectorQuery:field[0.0,...][10]}",
          query.toString("ignored"));

      assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

      // test with filter
      Query filter = new TermQuery(new Term("id", "text"));
      query = getKnnVectorQuery("field", new float[] {0.0f, 1.0f}, 10, filter);
      assertEquals(
          "PatienceKnnVectorQuery{saturationThreshold=0.995, patience=7, delegate=KnnFloatVectorQuery:field[0.0,...][10][id:text]}",
          query.toString("ignored"));
    }
  }

  static class ThrowingKnnVectorQuery extends KnnFloatVectorQuery {

    public ThrowingKnnVectorQuery(String field, float[] target, int k, Query filter) {
      super(field, target, k, filter, new KnnSearchStrategy.Hnsw(0));
    }

    @Override
    protected TopDocs exactSearch(
        LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout) {
      throw new UnsupportedOperationException("exact search is not supported");
    }

    @Override
    public String toString(String field) {
      return "ThrowingKnnVectorQuery{" + field + "}";
    }
  }
}
