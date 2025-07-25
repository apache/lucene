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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

public class ListFloatVectorValues extends FloatVectorValues {

  final List<float[]> vectors;
  final int dims;

  ListFloatVectorValues(List<float[]> vectors, int dims) {
    this.vectors = vectors;
    this.dims = dims;
    assert vectors.isEmpty() || vectors.getFirst().length == dims;
  }

  @Override
  public int size() {
    return vectors.size();
  }

  @Override
  public int dimension() {
    return dims;
  }

  @Override
  public float[] vectorValue(int targetOrd) {
    return vectors.get(targetOrd);
  }

  @Override
  public FloatVectorValues copy() {
    return this;
  }

  @Override
  public KnnVectorValues.DocIndexIterator iterator() {
    return createDenseIterator();
  }

  public RandomVectorScorerSupplier getScorer(VectorSimilarityFunction similarityFunction) {
    return new ListRandomVectorScorerSupplier(this, similarityFunction);
  }

  static class ListRandomVectorScorerSupplier implements RandomVectorScorerSupplier {

    final ListFloatVectorValues values;
    final List<float[]> vectors;
    final VectorSimilarityFunction similarityFunction;

    ListRandomVectorScorerSupplier(
        ListFloatVectorValues values, VectorSimilarityFunction similarityFunction) {
      this.values = values;
      this.vectors = values.vectors;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new ListUpdateableRandomVectorScorer(values, similarityFunction);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return this;
    }
  }

  static final class ListUpdateableRandomVectorScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {

    final List<float[]> vectors;
    final VectorSimilarityFunction similarityFunction;
    int queryOrd;

    ListUpdateableRandomVectorScorer(
        ListFloatVectorValues values, VectorSimilarityFunction similarityFunction) {
      super(values);
      this.vectors = values.vectors;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score(int node) {
      return similarityFunction.compare(vectors.get(queryOrd), vectors.get(node));
    }

    //    @Override
    //    public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
    //
    //    }
    //
    //    @Override
    //    public void bulkScore(float[] scores, int node1, int node2, int node3, int node4) {
    //      float[] query = vectors.get(queryOrd);
    //      float[] vec1 = vectors.get(node1);
    //      float[] vec2 = vectors.get(node2);
    //      float[] vec3 = vectors.get(node3);
    //      float[] vec4 = vectors.get(node4);
    //      similarityFunction.compareBulk(scores, query, vec1, vec2, vec3, vec4);
    //    }

    @Override
    public void setScoringOrdinal(int node) {
      queryOrd = node;
    }
  }
}
