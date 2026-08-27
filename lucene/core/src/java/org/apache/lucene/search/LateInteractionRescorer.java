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

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Rescores top N results from a first pass query using a {@link LateInteractionFloatValuesSource}
 *
 * <p>Typically, you run a low-cost first pass query to collect results from across the index, then
 * use this rescorer to rerank top N hits using multi-vectors, usually from a late interaction
 * model. Multi-vectors should be indexed in the {@link
 * org.apache.lucene.document.LateInteractionField} provided to rescorer.
 *
 * @lucene.experimental
 */
public class LateInteractionRescorer extends DoubleValuesSourceRescorer {

  public LateInteractionRescorer(LateInteractionFloatValuesSource valuesSource) {
    super(valuesSource);
  }

  /** Creates a LateInteractionRescorer for provided query vector. */
  public static LateInteractionRescorer create(String fieldName, float[][] queryVector) {
    return create(fieldName, queryVector, VectorSimilarityFunction.COSINE);
  }

  /**
   * Creates a LateInteractionRescorer for provided query vector.
   *
   * <p>Top N results from a first pass query are rescored based on the similarity between {@code
   * queryVector} and the multi-vector indexed in {@code fieldName}. If document does not have a
   * value indexed in {@code fieldName}, a 0f score is assigned.
   *
   * @param fieldName the {@link org.apache.lucene.document.LateInteractionField} used for
   *     reranking.
   * @param queryVector query multi-vector to use for similarity comparison
   * @param vectorSimilarityFunction function used for vector similarity comparisons
   */
  public static LateInteractionRescorer create(
      String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource =
        new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new LateInteractionRescorer(valuesSource);
  }

  @Override
  protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
    return valuePresent ? (float) sourceValue : 0f;
  }

  /**
   * Creates a LateInteractionRescorer for provided query vector.
   *
   * <p>Top N results from a first pass query are rescored based on the similarity between {@code
   * queryVector} and the multi-vector indexed in {@code fieldName}. Falls back to score from the
   * first pass query if a document does not have a value indexed in {@code fieldName}.
   *
   * @param fieldName the {@link org.apache.lucene.document.LateInteractionField} used for
   *     reranking.
   * @param queryVector query multi-vector to use for similarity comparison
   * @param vectorSimilarityFunction function used for vector similarity comparisons.
   */
  public static LateInteractionRescorer withFallbackToFirstPassScore(
      String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource =
        new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new LateInteractionRescorer(valuesSource) {
      @Override
      protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
        return valuePresent ? (float) sourceValue : firstPassScore;
      }
    };
  }
}
