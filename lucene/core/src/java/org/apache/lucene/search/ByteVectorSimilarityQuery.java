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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.knn.KnnSearchStrategy.Hnsw;

/**
 * Search for all (approximate) byte vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
public abstract sealed class ByteVectorSimilarityQuery extends AbstractVectorSimilarityQuery {
  private final byte[] target;

  /** A {@link ByteVectorSimilarityQuery} with an adaptive threshold for graph traversal. */
  public static non-sealed class Adaptive extends ByteVectorSimilarityQuery {
    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * VectorSimilarityCollector}, with a caller-supplied {@link KnnSearchStrategy}. If a filter is
     * applied, it traverses as many nodes as the cost of the filter, and then falls back to exact
     * search if results are incomplete.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     * @param decay decay factor for graph traversal buffer.
     * @param filter a filter applied before the vector search.
     * @param searchStrategy the {@link KnnSearchStrategy} to use during graph search. If {@code
     *     null}, this query's own default is used: an {@link Hnsw} with {@code
     *     filteredSearchThreshold == 0}, which preserves this query's filter handling. Note this
     *     differs from {@link Hnsw#DEFAULT}, which uses a threshold of 60. The underlying format
     *     may not support all strategies and is free to ignore the requested strategy.
     */
    public Adaptive(
        String field,
        byte[] target,
        float resultSimilarity,
        float decay,
        Query filter,
        KnnSearchStrategy searchStrategy) {
      super(field, target, resultSimilarity, decay, filter, searchStrategy);
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * VectorSimilarityCollector}, with the default {@link KnnSearchStrategy}. If a filter is
     * applied, it traverses as many nodes as the cost of the filter, and then falls back to exact
     * search if results are incomplete.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     * @param decay decay factor for graph traversal buffer.
     * @param filter a filter applied before the vector search.
     */
    public Adaptive(
        String field, byte[] target, float resultSimilarity, float decay, Query filter) {
      this(field, target, resultSimilarity, decay, filter, DEFAULT_STRATEGY);
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * VectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the cost of
     * the filter, and then falls back to exact search if results are incomplete.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     * @param filter a filter applied before the vector search.
     */
    public Adaptive(String field, byte[] target, float resultSimilarity, Query filter) {
      this(field, target, resultSimilarity, DEFAULT_DECAY, filter);
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * VectorSimilarityCollector}.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     */
    public Adaptive(String field, byte[] target, float resultSimilarity) {
      this(field, target, resultSimilarity, null);
    }

    @Override
    public String toString(String field) {
      return String.format(
          Locale.ROOT,
          "ByteVectorSimilarityQuery.Adaptive[field=%s target=[%d...] resultSimilarity=%f decay=%f filter=%s]",
          field,
          super.target[0],
          resultSimilarity,
          decay,
          filter);
    }
  }

  /**
   * A {@link ByteVectorSimilarityQuery} with an explicit threshold for graph traversal.
   *
   * @deprecated Equivalent to the {@link ByteVectorSimilarityQuery} constructors in Lucene 10.4 and
   *     earlier, use {@link Adaptive} for a more performant version.
   */
  @Deprecated
  public static non-sealed class Explicit extends ByteVectorSimilarityQuery {
    private final float traversalSimilarity;

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * ExplicitVectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the
     * cost of the filter, and then falls back to exact search if results are incomplete.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param traversalSimilarity (lower) similarity score for graph traversal.
     * @param resultSimilarity (higher) similarity score for result collection.
     * @param filter a filter applied before the vector search.
     */
    public Explicit(
        String field,
        byte[] target,
        float traversalSimilarity,
        float resultSimilarity,
        Query filter) {
      super(field, target, resultSimilarity, 0f, filter, DEFAULT_STRATEGY);
      if (traversalSimilarity > resultSimilarity) {
        throw new IllegalArgumentException("traversalSimilarity should be <= resultSimilarity");
      }
      this.traversalSimilarity = traversalSimilarity;
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * ExplicitVectorSimilarityCollector}.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param traversalSimilarity (lower) similarity score for graph traversal.
     * @param resultSimilarity (higher) similarity score for result collection.
     */
    public Explicit(
        String field, byte[] target, float traversalSimilarity, float resultSimilarity) {
      this(field, target, traversalSimilarity, resultSimilarity, null);
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * ExplicitVectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the
     * cost of the filter, and then falls back to exact search if results are incomplete.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     * @param filter a filter applied before the vector search.
     */
    public Explicit(String field, byte[] target, float resultSimilarity, Query filter) {
      this(field, target, resultSimilarity, resultSimilarity, filter);
    }

    /**
     * Search for all (approximate) byte vectors above a similarity threshold using {@link
     * ExplicitVectorSimilarityCollector}.
     *
     * @param field a field that has been indexed as a {@link KnnByteVectorField}.
     * @param target the target of the search.
     * @param resultSimilarity similarity score for result collection.
     */
    public Explicit(String field, byte[] target, float resultSimilarity) {
      this(field, target, resultSimilarity, resultSimilarity, null);
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager() {
      return (visitedLimit, _, _) ->
          new ExplicitVectorSimilarityCollector(
              traversalSimilarity, resultSimilarity, visitedLimit);
    }

    @Override
    public String toString(String field) {
      return String.format(
          Locale.ROOT,
          "ByteVectorSimilarityQuery.Explicit[field=%s target=[%d...] traversalSimilarity=%f resultSimilarity=%f filter=%s]",
          field,
          super.target[0],
          traversalSimilarity,
          resultSimilarity,
          filter);
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o)
          && Float.compare(traversalSimilarity, ((Explicit) o).traversalSimilarity) == 0;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + Float.hashCode(traversalSimilarity);
      return result;
    }
  }

  private ByteVectorSimilarityQuery(
      String field,
      byte[] target,
      float resultSimilarity,
      float decay,
      Query filter,
      KnnSearchStrategy searchStrategy) {
    super(field, resultSimilarity, decay, filter, searchStrategy);
    this.target = Objects.requireNonNull(target, "target");
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context) throws IOException {
    ByteVectorValues vectorValues = context.reader().getByteVectorValues(field);
    if (vectorValues == null) {
      return null;
    }
    return vectorValues.scorer(target);
  }

  @Override
  @SuppressWarnings("resource")
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    KnnCollector collector = knnCollectorManager.newCollector(visitLimit, null, context);
    context.reader().searchNearestVectors(field, target, collector, acceptDocs);
    return collector.topDocs();
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o)
        && super.equals(o)
        && Arrays.equals(target, ((ByteVectorSimilarityQuery) o).target);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(target);
    return result;
  }
}
