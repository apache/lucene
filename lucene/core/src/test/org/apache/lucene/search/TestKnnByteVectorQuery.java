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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.TestVectorUtil;

public class TestKnnByteVectorQuery extends BaseKnnVectorQueryTestCase {
  @Override
  AbstractKnnVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter) {
    return new KnnByteVectorQuery(field, floatToBytes(query), k, queryFilter);
  }

  @Override
  AbstractKnnVectorQuery getThrowingKnnVectorQuery(String field, float[] vec, int k, Query query) {
    return new ThrowingKnnVectorQuery(field, floatToBytes(vec), k, query);
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

  private static byte[] floatToBytes(float[] query) {
    byte[] bytes = new byte[query.length];
    for (int i = 0; i < query.length; i++) {
      assert query[i] <= Byte.MAX_VALUE && query[i] >= Byte.MIN_VALUE && (query[i] % 1) == 0
          : "float value cannot be converted to byte; provided: " + query[i];
      bytes[i] = (byte) query[i];
    }
    return bytes;
  }

  public void testToString() {
    AbstractKnnVectorQuery q1 = getKnnVectorQuery("f1", new float[] {0, 1}, 10);
    assertEquals("KnnByteVectorQuery:f1[0,...][10]", q1.toString("ignored"));
  }

  public void testGetTarget() {
    byte[] queryVectorBytes = floatToBytes(new float[] {0, 1});
    KnnByteVectorQuery q1 = new KnnByteVectorQuery("f1", queryVectorBytes, 10);
    assertArrayEquals(queryVectorBytes, q1.getTargetCopy());
    assertNotSame(queryVectorBytes, q1.getTargetCopy());
  }

  private static class ThrowingKnnVectorQuery extends KnnByteVectorQuery {

    public ThrowingKnnVectorQuery(String field, byte[] target, int k, Query filter) {
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
