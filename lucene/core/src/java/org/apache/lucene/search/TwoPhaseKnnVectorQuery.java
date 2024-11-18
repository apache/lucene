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
import java.util.Objects;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;

/**
 * A subclass of KnnFloatVectorQuery which does oversampling and full-precision reranking.
 *
 * @lucene.experimental
 */
public class TwoPhaseKnnVectorQuery extends KnnFloatVectorQuery {

  private final int originalK;
  private final double oversample;

  /**
   * Find the <code>k</code> nearest documents to the target vector according to the vectors in the
   * given field. <code>target</code> vector. It also over-samples by oversample parameter and does
   * full precision reranking if oversample > 0
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @param oversample the oversampling factor, a value of 0 means no oversampling
   * @param filter a filter applied before the vector search
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public TwoPhaseKnnVectorQuery(
      String field, float[] target, int k, double oversample, Query filter) {
    super(field, target, k + (int) Math.ceil(k * oversample), filter);
    if (oversample < 0) {
      throw new IllegalArgumentException("oversample must be non-negative, got " + oversample);
    }
    this.originalK = k;
    this.oversample = oversample;
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return TopDocs.merge(originalK, perLeafResults);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    TopDocs results =
        super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
    if (results.scoreDocs.length <= originalK) {
      // short-circuit: no re-ranking needed. we got what we need
      return results;
    }
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null) {
      return results;
    }
    FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(field);
    if (floatVectorValues == null) {
      return results;
    }

    for (int i = 0; i < results.scoreDocs.length; i++) {
      // get the raw vector value
      float[] vectorValue = floatVectorValues.vectorValue(results.scoreDocs[i].doc);

      // recompute the score
      results.scoreDocs[i].score = fi.getVectorSimilarityFunction().compare(vectorValue, target);
    }

    // Sort the ScoreDocs by the new scores in descending order
    Arrays.sort(results.scoreDocs, (a, b) -> Float.compare(b.score, a.score));

    // Select the top-k ScoreDocs after re-ranking
    ScoreDoc[] topKDocs = ArrayUtil.copyOfSubArray(results.scoreDocs, 0, originalK);

    assert topKDocs.length == originalK;

    return new TopDocs(results.totalHits, topKDocs);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Objects.hash(originalK, oversample);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (super.equals(o) == false) return false;
    TwoPhaseKnnVectorQuery that = (TwoPhaseKnnVectorQuery) o;
    return oversample == that.oversample && originalK == that.originalK;
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + ":"
        + this.field
        + "["
        + target[0]
        + ",...]["
        + originalK
        + "]["
        + oversample
        + "]";
  }
}
