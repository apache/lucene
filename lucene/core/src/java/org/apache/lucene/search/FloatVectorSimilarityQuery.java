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
import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

/**
 * Search for all (approximate) float vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
public class FloatVectorSimilarityQuery extends AbstractVectorSimilarityQuery {
  private final float[] target;

  /**
   * Search for all (approximate) float vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the cost of
   * the filter, and then falls back to exact search if results are incomplete.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param filter a filter applied before the vector search.
   */
  public FloatVectorSimilarityQuery(
      String field,
      float[] target,
      float traversalSimilarity,
      float resultSimilarity,
      Query filter) {
    super(field, traversalSimilarity, resultSimilarity, filter);
    this.target = VectorUtil.checkFinite(Objects.requireNonNull(target, "target"));
  }

  /**
   * Search for all (approximate) float vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   */
  public FloatVectorSimilarityQuery(
      String field, float[] target, float traversalSimilarity, float resultSimilarity) {
    this(field, target, traversalSimilarity, resultSimilarity, null);
  }

  /**
   * Search for all (approximate) float vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the cost of
   * the filter, and then falls back to exact search if results are incomplete.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param resultSimilarity similarity score for result collection.
   * @param filter a filter applied before the vector search.
   */
  public FloatVectorSimilarityQuery(
      String field, float[] target, float resultSimilarity, Query filter) {
    this(field, target, resultSimilarity, resultSimilarity, filter);
  }

  /**
   * Search for all (approximate) float vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search.
   * @param resultSimilarity similarity score for result collection.
   */
  public FloatVectorSimilarityQuery(String field, float[] target, float resultSimilarity) {
    this(field, target, resultSimilarity, resultSimilarity, null);
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context) throws IOException {
    @SuppressWarnings("resource")
    FloatVectorValues vectorValues = context.reader().getFloatVectorValues(field);
    if (vectorValues == null) {
      return null;
    }
    return vectorValues.scorer(target);
  }

  @Override
  @SuppressWarnings("resource")
  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitLimit)
      throws IOException {
    KnnCollector collector =
        new VectorSimilarityCollector(traversalSimilarity, resultSimilarity, visitLimit);
    context.reader().searchNearestVectors(field, target, collector, acceptDocs);
    return collector.topDocs();
  }

  @Override
  public String toString(String field) {
    return String.format(
        Locale.ROOT,
        "%s[field=%s target=[%f...] traversalSimilarity=%f resultSimilarity=%f filter=%s]",
        getClass().getSimpleName(),
        field,
        target[0],
        traversalSimilarity,
        resultSimilarity,
        filter);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o)
        && super.equals(o)
        && Arrays.equals(target, ((FloatVectorSimilarityQuery) o).target);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(target);
    return result;
  }
}
