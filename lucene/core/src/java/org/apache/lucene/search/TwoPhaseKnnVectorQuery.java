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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;

public class TwoPhaseKnnVectorQuery extends KnnFloatVectorQuery {

  private final int originalK;
  private final double oversample;

  public TwoPhaseKnnVectorQuery(
      String field, float[] target, int k, double oversample, Query filter) {
    super(field, target, k + (int) Math.round(k * oversample), filter);
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
  protected TopDocs getLeafResults(
      LeafReaderContext context,
      Weight filterWeight,
      TimeLimitingKnnCollectorManager knnCollectorManager)
      throws IOException {
    TopDocs results = super.getLeafResults(context, filterWeight, knnCollectorManager);
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
    ScoreDoc[] topKDocs = Arrays.copyOfRange(results.scoreDocs, 0, originalK);

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
