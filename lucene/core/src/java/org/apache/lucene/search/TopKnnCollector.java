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
    this.globalK = Math.max(k, globalK);
    // Heap to store top perLeafK results
    this.strongQueue = new NeighborQueue(k, false);
    // Heap to store remaining results up to globalK
    this.queue = globalK > k ? new NeighborQueue(globalK - k, false) : null;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    if (queue == null || strongQueue.size() < k()) {
      return strongQueue.insertWithOverflow(docId, similarity);
    }

    // If new value is globally competitive, we may need to demote worst hit to the weaker queue
    float topScore = strongQueue.topScore();

    if (similarity >= topScore) {
      int topNode = strongQueue.topNode();
      if (strongQueue.insertWithOverflow(docId, similarity)) {
        queue.insertWithOverflow(topNode, topScore);
        return true;
      }
    }

    return queue.insertWithOverflow(docId, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return strongQueue.size() >= k() ? strongQueue.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public float minCollectableSimilarity() {
    if (queue == null) {
      return strongQueue.size() >= globalK ? strongQueue.topScore() : Float.NEGATIVE_INFINITY;
    }
    // The weak queue is only populated once the strong queue is full, so when it has filled
    // globalK - k() hits the collector is holding globalK hits in total
    return queue.size() >= globalK - k() ? queue.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public KnnResult knnResult() {
    assert strongQueue.size() <= this.globalK
        : "Tried to collect more strong results than the maximum number allowed";
    assert queue == null || queue.size() <= this.globalK
        : "Tried to collect more results than the maximum number allowed";
    ScoreDoc[] scoreDocs = new ScoreDoc[strongQueue.size() + (queue == null ? 0 : queue.size())];
    // An exhaustive search collects only hits it scored, and it scores everything it visited, so
    // everything it collected is a strong result; it cannot have collected more than globalK
    int strongResults = strategy == KnnResult.Method.EXACT ? scoreDocs.length : strongQueue.size();

    int i = scoreDocs.length - 1;
    // Drain the weak hits first: every weak hit is scored no higher than the weakest strong hit,
    // so popping the weak queue into the tail keeps the results ordered by score descending
    if (queue != null) {
      while (queue.size() > 0) {
        float score = queue.topScore();
        int node = queue.pop();
        scoreDocs[i--] = new ScoreDoc(node, score);
      }
    }
    while (strongQueue.size() > 0) {
      float score = strongQueue.topScore();
      int node = strongQueue.pop();
      scoreDocs[i--] = new ScoreDoc(node, score);
    }
    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;

    return new KnnResult(
        strategy, new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs), strongResults);
  }

  @Override
  public int numCollected() {
    return strongQueue.size() + (queue == null ? 0 : queue.size());
  }

  @Override
  public String toString() {
    return "TopKnnCollector[k=" + k() + ", globalK=" + globalK + ", size=" + numCollected() + "]";
  }
}
