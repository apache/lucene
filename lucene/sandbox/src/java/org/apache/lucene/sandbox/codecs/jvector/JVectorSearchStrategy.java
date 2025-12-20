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

package org.apache.lucene.sandbox.codecs.jvector;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/// Defines query-time parameters for searching a JVector index to be passed into
/// [`search()`][JVectorReader#search] via [`KnnCollector`][org.apache.lucene.search.KnnCollector].
public class JVectorSearchStrategy extends KnnSearchStrategy {
  static final float DEFAULT_QUERY_SIMILARITY_THRESHOLD = 0f;
  static final float DEFAULT_QUERY_RERANK_FLOOR = 0f;
  static final int DEFAULT_OVER_QUERY_FACTOR = 5;
  static final boolean DEFAULT_QUERY_USE_PRUNING = false;
  static final boolean DEFAULT_QUERY_USE_RERANKING = false;

  public static final JVectorSearchStrategy DEFAULT =
      new JVectorSearchStrategy(
          DEFAULT_QUERY_SIMILARITY_THRESHOLD,
          DEFAULT_QUERY_RERANK_FLOOR,
          DEFAULT_OVER_QUERY_FACTOR,
          DEFAULT_QUERY_USE_PRUNING,
          DEFAULT_QUERY_USE_RERANKING);

  final float threshold;
  final float rerankFloor;
  final int overQueryFactor;
  final boolean usePruning;
  final boolean useReranking;

  private JVectorSearchStrategy(
      float threshold,
      float rerankFloor,
      int overQueryFactor,
      boolean usePruning,
      boolean useReranking) {
    this.threshold = threshold;
    this.rerankFloor = rerankFloor;
    this.overQueryFactor = overQueryFactor;
    this.usePruning = usePruning;
    this.useReranking = useReranking;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%s[threshold=%f, rerankFloor=%f, overQueryFactor=%d, usePruning=%s]",
        getClass().getSimpleName(),
        threshold,
        rerankFloor,
        overQueryFactor,
        usePruning);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof JVectorSearchStrategy other) {
      return this.threshold == other.threshold
          && this.rerankFloor == other.rerankFloor
          && this.overQueryFactor == other.overQueryFactor
          && this.usePruning == other.usePruning
          && this.useReranking == other.useReranking;
    } else return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), threshold, rerankFloor, overQueryFactor, usePruning);
  }

  @Override
  public void nextVectorsBlock() {}

  public static Builder builder() {
    return new Builder();
  }

  /// Builder for defining a [JVectorSearchStrategy].
  public static class Builder {
    private float threshold = DEFAULT_QUERY_SIMILARITY_THRESHOLD;
    private float rerankFloor = DEFAULT_QUERY_RERANK_FLOOR;
    private int overQueryFactor = DEFAULT_OVER_QUERY_FACTOR;
    private boolean usePruning = DEFAULT_QUERY_USE_PRUNING;
    private boolean useReranking = DEFAULT_QUERY_USE_RERANKING;

    private Builder() {}

    public Builder withThreshold(float threshold) {
      this.threshold = threshold;
      return this;
    }

    public Builder withRerankFloor(float rerankFloor) {
      this.rerankFloor = rerankFloor;
      return this;
    }

    public Builder withOverQueryFactor(int overQueryFactor) {
      this.overQueryFactor = overQueryFactor;
      return this;
    }

    public Builder withUsePruning(boolean usePruning) {
      this.usePruning = usePruning;
      return this;
    }

    public Builder withUseReranking(boolean useReranking) {
      this.useReranking = useReranking;
      return this;
    }

    public JVectorSearchStrategy build() {
      return new JVectorSearchStrategy(
          threshold, rerankFloor, overQueryFactor, usePruning, useReranking);
    }
  }
}
