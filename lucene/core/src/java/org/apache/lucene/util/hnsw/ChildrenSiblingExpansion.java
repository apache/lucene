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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.BitSet;

/**
 * Implemented by {@link org.apache.lucene.search.KnnCollector} instances that support dynamic
 * sibling expansion during HNSW graph search. When the graph searcher collects a child node
 * belonging to a newly-discovered parent, all siblings of that parent are immediately scored and
 * collected without requiring full graph traversal.
 *
 * <p>The graph searcher calls {@link #pendingSiblingOrdinals} <em>before</em> invoking {@link
 * org.apache.lucene.search.KnnCollector#collect} for the triggering node, so that the collector can
 * safely inspect its internal state before the parent is registered.
 *
 * @lucene.experimental
 */
public interface ChildrenSiblingExpansion {

  /**
   * Called by the HNSW graph searcher before {@link org.apache.lucene.search.KnnCollector#collect}
   * is invoked for {@code collectedOrdinal}. The implementation inspects the node's parent and
   * returns the ordinals of unvisited siblings that should be scored immediately.
   *
   * <p>The caller is responsible for marking the returned ordinals as visited in {@code
   * visitedOrds} and for scoring and collecting them.
   *
   * @param collectedOrdinal the vector ordinal about to be collected
   * @param visitedOrds the graph searcher's current visited bitset; returned siblings are not yet
   *     marked here — the caller does that
   * @return ordinals of sibling nodes to score next, or {@code null} if none
   */
  int[] pendingSiblingOrdinals(int collectedOrdinal, BitSet visitedOrds);
}
