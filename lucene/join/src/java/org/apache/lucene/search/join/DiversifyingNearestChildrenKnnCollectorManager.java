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

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSet;

/**
 * DiversifyingNearestChildrenKnnCollectorManager responsible for creating {@link
 * DiversifyingNearestChildrenKnnCollector} instances.
 */
public class DiversifyingNearestChildrenKnnCollectorManager implements KnnCollectorManager {

  // the number of docs to collect
  private final int k;
  // filter identifying the parent documents.
  private final BitSetProducer parentsFilter;

  /**
   * Constructor
   *
   * @param k - the number of top k vectors to collect
   * @param parentsFilter Filter identifying the parent documents.
   */
  public DiversifyingNearestChildrenKnnCollectorManager(int k, BitSetProducer parentsFilter) {
    this.k = k;
    this.parentsFilter = parentsFilter;
  }

  /**
   * Return a new {@link DiversifyingNearestChildrenKnnCollector} instance.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param context the leaf reader context
   */
  @Override
  public DiversifyingNearestChildrenKnnCollector newCollector(
      int visitedLimit, LeafReaderContext context) throws IOException {
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return null;
    }
    return new DiversifyingNearestChildrenKnnCollector(k, visitedLimit, parentBitSet);
  }
}
