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

import org.apache.lucene.codecs.ByteVectorProvider;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.TestVectorUtil;
import org.junit.Before;

public class TestByteVectorSimilarityQuery
    extends BaseVectorSimilarityQueryTestCase<
        byte[], KnnByteVectorField, ByteVectorSimilarityQuery> {

  @Before
  public void setup() {
    vectorField = getClass().getSimpleName() + ":VectorField";
    idField = getClass().getSimpleName() + ":IdField";
    function = VectorSimilarity.EuclideanDistanceSimilarity.INSTANCE;
    numDocs = atLeast(100);
    dim = atLeast(5);
  }

  @Override
  byte[] getRandomVector(int dim) {
    return TestVectorUtil.randomVectorBytes(dim);
  }

  @Override
  VectorSimilarity.VectorScorer compare(byte[] queryVector, byte[][] vectors) throws IOException {
    return function.getVectorScorer(fromByteArrays(vectors), queryVector);
  }

  @Override
  boolean checkEquals(byte[] vector1, byte[] vector2) {
    return Arrays.equals(vector1, vector2);
  }

  @Override
  KnnByteVectorField getVectorField(String name, byte[] vector, VectorSimilarity function) {
    return new KnnByteVectorField(name, vector, function);
  }

  @Override
  ByteVectorSimilarityQuery getVectorQuery(
      String field,
      byte[] vector,
      float traversalSimilarity,
      float resultSimilarity,
      Query filter) {
    return new ByteVectorSimilarityQuery(
        field, vector, traversalSimilarity, resultSimilarity, filter);
  }

  @Override
  ByteVectorSimilarityQuery getThrowingVectorQuery(
      String field,
      byte[] vector,
      float traversalSimilarity,
      float resultSimilarity,
      Query filter) {
    return new ByteVectorSimilarityQuery(
        field, vector, traversalSimilarity, resultSimilarity, filter) {
      @Override
      VectorScorer createVectorScorer(LeafReaderContext context) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static ByteVectorProvider fromByteArrays(byte[][] vectors) {
    return new ByteVectorProvider() {
      @Override
      public int dimension() {
        return vectors[0].length;
      }

      @Override
      public byte[] vectorValue(int targetOrd) throws IOException {
        return vectors[targetOrd];
      }

      @Override
      public ByteVectorProvider copy() throws IOException {
        return this;
      }
    };
  }
}
