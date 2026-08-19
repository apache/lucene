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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.QueryReadHint;
import org.apache.lucene.search.TopKnnCollector;

/** TopKnnCollectorManager responsible for creating {@link TopKnnCollector} instances. */
public class TopKnnCollectorManager implements KnnCollectorManager {

  // the number of docs to collect
  private final int k;
  // Per-query read hints carried from the IndexSearcher, surfaced to codec readers via
  // KnnCollector#readHints(). Empty (the default) means no hint and no extra allocation.
  private final Set<QueryReadHint> readHints;

  public TopKnnCollectorManager(int k, IndexSearcher indexSearcher) {
    this(k, indexSearcher, Set.of());
  }

  /**
   * @param queryReadHints per-query read hints carried by the query itself (may be {@code null} or
   *     empty). When non-empty these take precedence over the searcher's {@link
   *     IndexSearcher#getReadHints()}, giving genuine per-query control even when a single {@link
   *     IndexSearcher} is shared across threads; {@code null}/empty inherits the searcher's hints.
   */
  public TopKnnCollectorManager(
      int k, IndexSearcher indexSearcher, Set<QueryReadHint> queryReadHints) {
    this.k = k;
    Set<QueryReadHint> searcherHints =
        indexSearcher == null ? Set.of() : indexSearcher.getReadHints();
    this.readHints = QueryReadHint.resolve(queryReadHints, searcherHints);
  }

  /**
   * Return a new {@link TopKnnCollector} instance.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param context the leaf reader context
   */
  @Override
  public KnnCollector newCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
      throws IOException {
    return KnnCollector.withReadHints(
        new TopKnnCollector(k, visitedLimit, searchStrategy), readHints);
  }

  @Override
  public KnnCollector newOptimisticCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context, int k) {
    return KnnCollector.withReadHints(
        new TopKnnCollector(k, visitedLimit, searchStrategy), readHints);
  }

  @Override
  public boolean isOptimistic() {
    return true;
  }
}
