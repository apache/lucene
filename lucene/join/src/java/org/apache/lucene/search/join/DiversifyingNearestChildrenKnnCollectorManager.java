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

package org.apache.lucene.search.join;

import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSet;

/**
 * DiversifyingNearestChildrenKnnCollectorManager responsible for creating {@link
 * DiversifyingNearestChildrenKnnCollector} instances.
 */
public class DiversifyingNearestChildrenKnnCollectorManager
    extends KnnCollectorManager<DiversifyingNearestChildrenKnnCollector> {

  // the number of docs to collect
  private final int k;

  /**
   * Constructor
   *
   * @param k - the number of top k vectors to collect
   */
  public DiversifyingNearestChildrenKnnCollectorManager(int k) {
    this.k = k;
  }

  /**
   * Return a new {@link DiversifyingNearestChildrenKnnCollector} instance.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param parentBitSet the parent bitset
   */
  @Override
  public DiversifyingNearestChildrenKnnCollector newCollector(
      int visitedLimit, BitSet parentBitSet) {
    return new DiversifyingNearestChildrenKnnCollector(k, visitedLimit, parentBitSet);
  }
}
