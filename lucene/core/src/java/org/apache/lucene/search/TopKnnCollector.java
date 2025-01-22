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

import org.apache.lucene.search.knn.HnswSearchStrategy;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * TopKnnCollector is a specific KnnCollector. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 *
 * @lucene.experimental
 */
public class TopKnnCollector extends AbstractKnnCollector implements HnswSearchStrategy {

  protected final NeighborQueue queue;
  private final float filterHeuristicThreshold;

  /**
   * Create a new TopKnnCollector.
   *
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   */
  public TopKnnCollector(int k, int visitLimit) {
    this(k, visitLimit, 0.0f);
  }

  /**
   * Create a new TopKnnCollector.
   *
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   * @param filterHeuristicThreshold the threshold of vectors passing a pre-filter determining if
   *     optimized filtered search should be executed. 1f means always execute the optimized
   *     filtered search, 0f means never execute it. All values in between are a trade-off between
   *     the two.
   */
  public TopKnnCollector(int k, int visitLimit, float filterHeuristicThreshold) {
    super(k, visitLimit);
    this.queue = new NeighborQueue(k, false);
    this.filterHeuristicThreshold = filterHeuristicThreshold;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    return queue.insertWithOverflow(docId, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return queue.size() >= k() ? queue.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public TopDocs topDocs() {
    assert queue.size() <= k() : "Tried to collect more results than the maximum number allowed";
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    for (int i = 1; i <= scoreDocs.length; i++) {
      scoreDocs[scoreDocs.length - i] = new ScoreDoc(queue.topNode(), queue.topScore());
      queue.pop();
    }
    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  @Override
  public int numCollected() {
    return queue.size();
  }

  @Override
  public String toString() {
    return "TopKnnCollector[k=" + k() + ", size=" + queue.size() + "]";
  }

  @Override
  public boolean shouldExecuteOptimizedFilteredSearch(float filterRatio) {
    return filterRatio < filterHeuristicThreshold;
  }
}
