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

import java.util.Objects;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * Wrapper class for KnnCollector that provides passing of additional parameters specific for
 * JVector.
 */
public final class JVectorKnnCollector implements KnnCollector {
  private final KnnCollector delegate;
  private final float threshold;
  private final float rerankFloor;
  private final int overQueryFactor;
  private final boolean usePruning;

  /**
   * Constructs a new JVectorKnnCollector.
   *
   * @param delegate the underlying KnnCollector to delegate calls to
   * @param threshold the similarity threshold for JVector
   * @param rerankFloor the rerank floor value
   * @param overQueryFactor the over-query factor
   * @param usePruning whether to apply pruning
   */
  public JVectorKnnCollector(
      KnnCollector delegate,
      float threshold,
      float rerankFloor,
      int overQueryFactor,
      boolean usePruning) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.threshold = threshold;
    this.rerankFloor = rerankFloor;
    this.overQueryFactor = overQueryFactor;
    this.usePruning = usePruning;
  }

  public KnnCollector getDelegate() {
    return delegate;
  }

  public float getThreshold() {
    return threshold;
  }

  public float getRerankFloor() {
    return rerankFloor;
  }

  public int getOverQueryFactor() {
    return overQueryFactor;
  }

  public boolean isUsePruning() {
    return usePruning;
  }

  @Override
  public boolean earlyTerminated() {
    return delegate.earlyTerminated();
  }

  @Override
  public void incVisitedCount(int count) {
    delegate.incVisitedCount(count);
  }

  @Override
  public long visitedCount() {
    return delegate.visitedCount();
  }

  @Override
  public long visitLimit() {
    return delegate.visitLimit();
  }

  @Override
  public int k() {
    return delegate.k();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    return delegate.collect(docId, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return delegate.minCompetitiveSimilarity();
  }

  @Override
  public TopDocs topDocs() {
    return delegate.topDocs();
  }

  @Override
  public KnnSearchStrategy getSearchStrategy() {
    return null;
  }
}
