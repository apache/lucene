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

import java.util.Arrays;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.junit.Before;

public class TestFloat16VectorSimilarityQuery
    extends BaseVectorSimilarityQueryTestCase<
        short[], KnnFloat16VectorField, Float16VectorSimilarityQuery> {

  @Before
  public void setup() {
    vectorField = getClass().getSimpleName() + ":VectorField";
    idField = getClass().getSimpleName() + ":IdField";
    function = VectorSimilarityFunction.EUCLIDEAN;
    numDocs = atLeast(100);
    dim = atLeast(5);
  }

  @Override
  short[] getRandomVector(int dim) {
    return BaseKnnVectorsFormatTestCase.randomNormalizedFloat16Vector(dim);
  }

  @Override
  float compare(short[] vector1, short[] vector2) {
    return function.compare(vector1, vector2);
  }

  @Override
  boolean checkEquals(short[] vector1, short[] vector2) {
    return Arrays.equals(vector1, vector2);
  }

  @Override
  KnnFloat16VectorField getVectorField(
      String name, short[] vector, VectorSimilarityFunction function) {
    return new KnnFloat16VectorField(name, vector, function);
  }

  @Override
  Float16VectorSimilarityQuery getVectorQuery(
      String field, short[] vector, float resultSimilarity, float decay, Query filter) {
    return new Float16VectorSimilarityQuery.Adaptive(
        field, vector, resultSimilarity, decay, filter);
  }

  @Override
  Float16VectorSimilarityQuery getVectorQuery(
      String field,
      short[] vector,
      float resultSimilarity,
      float decay,
      Query filter,
      KnnSearchStrategy searchStrategy) {
    return new Float16VectorSimilarityQuery.Adaptive(
        field, vector, resultSimilarity, decay, filter, searchStrategy);
  }

  @Override
  Float16VectorSimilarityQuery getThrowingVectorQuery(
      String field, short[] vector, float resultSimilarity, float decay, Query filter) {
    return new Float16VectorSimilarityQuery.Adaptive(
        field, vector, resultSimilarity, decay, filter) {
      @Override
      VectorScorer createVectorScorer(LeafReaderContext context) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
