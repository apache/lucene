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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;

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

  private static byte[] fromFloat(float[] queryVector) {
    byte[] query = new byte[queryVector.length];
    for (int i = 0; i < queryVector.length; i++) {
      assert queryVector[i] == (byte) queryVector[i];
      query[i] = (byte) queryVector[i];
    }
    return query;
  }
}
