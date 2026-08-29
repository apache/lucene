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

import java.util.Set;

/**
 * Marker interface for hints a query or collector can attach to influence how per-query reader
 * access is performed.
 *
 * <p>Hints describe the access pattern of the query itself (for example, whether a query expects to
 * issue many accesses or just a few). They are deliberately decoupled from any specific I/O
 * implementation detail: readers are free to ignore them, or to combine them with their own
 * knowledge of segment layout to decide on a strategy (prefetch depth, cache budget, etc.).
 *
 * <p>Hints never affect correctness; they only influence I/O strategy.
 *
 * <p>This is the query-time analogue of {@link org.apache.lucene.store.IOContext.FileOpenHint},
 * which describes how a file is opened. {@code QueryReadHint} instead describes how a query intends
 * to use already-open readers. It is carried from {@link IndexSearcher#getReadHints()} (or a
 * per-query override on a supporting query) down to {@link KnnCollector#readHints()}, which codec
 * readers such as the HNSW vectors reader consult, rather than from {@link
 * org.apache.lucene.store.Directory#openInput}.
 *
 * <p>Implementations MUST be immutable and safe to share across threads. Enums are the expected
 * shape; ad-hoc bag-like implementations are discouraged.
 *
 * @lucene.experimental
 */
public interface QueryReadHint {

  /**
   * Resolve the effective read hints for a search. A non-empty per-query hint set takes precedence
   * over (overrides) the searcher-wide hints; an empty or {@code null} per-query set inherits the
   * searcher's. This centralizes the one precedence rule shared by {@link
   * org.apache.lucene.search.knn.TopKnnCollectorManager} and the vector-similarity queries so it
   * cannot drift between call sites.
   *
   * @param queryHints hints carried by the query itself (may be {@code null} or empty)
   * @param searcherHints hints set on the {@link IndexSearcher} (may be {@code null} or empty)
   * @return the effective, never-{@code null} hint set
   * @lucene.experimental
   */
  static Set<QueryReadHint> resolve(
      Set<QueryReadHint> queryHints, Set<QueryReadHint> searcherHints) {
    if (queryHints == null || queryHints.isEmpty()) {
      return searcherHints == null ? Set.of() : searcherHints;
    }
    return queryHints;
  }
}
