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

import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * TopKnnCollector is a specific KnnCollector. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 *
 * @lucene.experimental
 */
public class TopKnnCollector extends AbstractKnnCollector {

  protected final NeighborQueue queue;
  private final int globalK;
  // Tracks only the best k() hits when collecting more than k() (globalK > k), so the search stops
  // exploring at the same point as a plain top-k() search; null when globalK == k()
  private final NeighborQueue strongQueue;

  /** Constructor, with default search strategy */
  public TopKnnCollector(int k, int visitLimit) {
    this(k, k, visitLimit, null);
  }

  /** Constructor, with default search strategy */
  public TopKnnCollector(int k, int globalK, int visitLimit) {
    this(k, globalK, visitLimit, null);
  }

  /**
   * @param k the number of neighbors to search for; this controls how far the search explores
   * @param globalK the maximum number of neighbors to collect, at least {@code k}. Hits beyond the
   *     best {@code k} are kept if they were visited anyway, but do not make the search explore
   *     further
   * @param visitLimit how many vector nodes the results are allowed to visit
   * @param searchStrategy the search strategy to use
   */
  public TopKnnCollector(int k, int globalK, int visitLimit, KnnSearchStrategy searchStrategy) {
    super(k, visitLimit, searchStrategy);
    if (globalK < k) {
      throw new IllegalArgumentException(
          "globalK must be >= k, got globalK=" + globalK + " k=" + k);
    }
    this.globalK = globalK;
    this.queue = new NeighborQueue(globalK, false);
    this.strongQueue = globalK > k ? new NeighborQueue(k, false) : null;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    if (strongQueue == null) {
      return queue.insertWithOverflow(docId, similarity);
    }
    queue.insertWithOverflow(docId, similarity);
    return strongQueue.insertWithOverflow(docId, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    NeighborQueue q = strongQueue == null ? queue : strongQueue;
    return q.size() >= k() ? q.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public float minCollectableSimilarity() {
    return queue.size() >= globalK ? queue.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public KnnResult topDocs() {
    assert queue.size() <= this.globalK
        : "Tried to collect more results than the maximum number allowed";
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    for (int i = 1; i <= scoreDocs.length; i++) {
      scoreDocs[scoreDocs.length - i] = new ScoreDoc(queue.topNode(), queue.topScore());
      queue.pop();
    }
    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new KnnResult(
        strategy,
        new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs),
        Math.min(k(), scoreDocs.length));
  }

  @Override
  public int numCollected() {
    return queue.size();
  }

  @Override
  public String toString() {
    return "TopKnnCollector[k=" + k() + ", size=" + queue.size() + "]";
  }
}
