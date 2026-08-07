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

package org.apache.lucene.search.knn;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.QueryReadHint;

/**
 * KnnCollectorManager responsible for creating {@link KnnCollector} instances. Useful to create
 * {@link KnnCollector} instances that share global state across leaves, such a global queue of
 * results collected so far.
 */
public interface KnnCollectorManager {

  /**
   * Return a new {@link KnnCollector} instance.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param searchStrategy the optional search strategy configuration
   * @param context the leaf reader context
   */
  KnnCollector newCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
      throws IOException;

  /**
   * Return a new {@link KnnCollector} instance, generally with a specific k value, scaled per leaf
   * statistics
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param searchStrategy the optional search strategy configuration
   * @param context the leaf reader context
   * @param k the number of neighbors to collect, this is the expected number of results
   * @return a new KnnCollector instance
   * @throws IOException if there is an error creating the collector
   */
  default KnnCollector newOptimisticCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context, int k)
      throws IOException {
    return null;
  }

  default boolean isOptimistic() {
    return false;
  }

  /**
   * Wrap {@code delegate} so the collectors it produces advertise {@code readHints} via {@link
   * KnnCollector#readHints()}, delegating everything else. Returns {@code delegate} unchanged when
   * {@code readHints} is {@code null} or empty, so the no-hint path allocates nothing extra. This
   * is the single delegate-wrapping implementation shared by the KNN queries that surface
   * per-query/searcher read hints to codec readers.
   *
   * @lucene.experimental
   */
  static KnnCollectorManager withReadHints(
      KnnCollectorManager delegate, Set<QueryReadHint> readHints) {
    if (readHints == null || readHints.isEmpty()) {
      return delegate;
    }
    return new KnnCollectorManager() {
      @Override
      public KnnCollector newCollector(
          int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
          throws IOException {
        return KnnCollector.withReadHints(
            delegate.newCollector(visitedLimit, searchStrategy, context), readHints);
      }

      @Override
      public KnnCollector newOptimisticCollector(
          int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context, int k)
          throws IOException {
        return KnnCollector.withReadHints(
            delegate.newOptimisticCollector(visitedLimit, searchStrategy, context, k), readHints);
      }

      @Override
      public boolean isOptimistic() {
        return delegate.isOptimistic();
      }
    };
  }
}
