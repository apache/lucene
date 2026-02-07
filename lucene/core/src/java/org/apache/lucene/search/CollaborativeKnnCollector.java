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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * A {@link KnnCollector} that allows for collaborative search by sharing a global minimum
 * competitive similarity across multiple threads or nodes.
 *
 * <p>This collector wraps a {@link TopKnnCollector} and an {@link AtomicInteger} (storing float
 * bits). It ensures that the search can be pruned by scores found in other concurrent search
 * processes (e.g., other shards in a cluster).
 *
 * @lucene.experimental
 */
public class CollaborativeKnnCollector extends KnnCollector.Decorator {

  private final AtomicInteger globalMinSimBits;

  /**
   * Create a new CollaborativeKnnCollector
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param globalMinSimBits shared atomic float bits for global pruning
   */
  public CollaborativeKnnCollector(int k, int visitLimit, AtomicInteger globalMinSimBits) {
    this(new TopKnnCollector(k, visitLimit), globalMinSimBits);
  }

  /**
   * Create a new CollaborativeKnnCollector with a search strategy
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param searchStrategy search strategy to use
   * @param globalMinSimBits shared atomic float bits for global pruning
   */
  public CollaborativeKnnCollector(
      int k, int visitLimit, KnnSearchStrategy searchStrategy, AtomicInteger globalMinSimBits) {
    this(new TopKnnCollector(k, visitLimit, searchStrategy), globalMinSimBits);
  }

  private CollaborativeKnnCollector(KnnCollector delegate, AtomicInteger globalMinSimBits) {
    super(delegate);
    this.globalMinSimBits = globalMinSimBits;
  }

  @Override
  public float minCompetitiveSimilarity() {
    float localMin = super.minCompetitiveSimilarity();
    float globalMin = Float.intBitsToFloat(globalMinSimBits.get());
    return Math.max(localMin, globalMin);
  }

  /**
   * Update the global minimum similarity if the provided score is higher.
   *
   * @param score the new potential global minimum
   */
  public void updateGlobalMinSimilarity(float score) {
    int newBits = Float.floatToRawIntBits(score);
    while (true) {
      int currentBits = globalMinSimBits.get();
      if (score <= Float.intBitsToFloat(currentBits)) {
        break;
      }
      if (globalMinSimBits.compareAndSet(currentBits, newBits)) {
        break;
      }
    }
  }
}
