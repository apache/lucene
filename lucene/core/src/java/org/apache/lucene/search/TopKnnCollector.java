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

import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.FloatHeap;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * TopKnnCollector is a specific KnnCollector. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 *
 * @lucene.experimental
 */
public final class TopKnnCollector extends AbstractKnnCollector {

  // greediness of globally non-competitive search: (0,1]
  private static final float DEFAULT_GREEDINESS = 0.9f;
  // the local queue of the results with the highest similarities collected so far in the current
  // segment
  private final NeighborQueue queue;
  // the global queue of the highest similarities collected so far across all segments
  private final BlockingFloatHeap globalSimilarityQueue;
  // the local queue of the highest similarities if we are not competitive globally
  // the size of this queue is defined by greediness
  private final FloatHeap nonCompetitiveQueue;
  private final float greediness;
  // the queue of the local similarities to periodically update with the global queue
  private final FloatHeap updatesQueue;
  // interval to synchronize the local and global queues, as a number of visited vectors
  private final int interval = 0xff; // 255
  private boolean kResultsCollected = false;
  private float cachedGlobalMinSim = Float.NEGATIVE_INFINITY;

  /**
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   */
  public TopKnnCollector(int k, int visitLimit, BlockingFloatHeap globalSimilarityQueue) {
    super(k, visitLimit);
    this.greediness = DEFAULT_GREEDINESS;
    this.queue = new NeighborQueue(k, false);
    this.globalSimilarityQueue = globalSimilarityQueue;

    if (globalSimilarityQueue == null) {
      this.nonCompetitiveQueue = null;
      this.updatesQueue = null;
    } else {
      this.nonCompetitiveQueue = new FloatHeap(Math.max(1, Math.round((1 - greediness) * k)));
      this.updatesQueue = new FloatHeap(k);
    }
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean localSimUpdated = queue.insertWithOverflow(docId, similarity);
    boolean firstKResultsCollected = (kResultsCollected == false && queue.size() == k());
    if (firstKResultsCollected) {
      kResultsCollected = true;
    }
    boolean globalSimUpdated = false;
    if (globalSimilarityQueue != null) {
      updatesQueue.offer(similarity);
      globalSimUpdated = nonCompetitiveQueue.offer(similarity);

      if (kResultsCollected) {
        // as we've collected k results, we can start do periodic updates with the global queue
        if (firstKResultsCollected || (visitedCount & interval) == 0) {
          cachedGlobalMinSim = globalSimilarityQueue.offer(updatesQueue.getHeap());
          updatesQueue.clear();
          globalSimUpdated = true;
        }
      }
    }
    return localSimUpdated || globalSimUpdated;
  }

  @Override
  public float minCompetitiveSimilarity() {
    if (kResultsCollected == false) {
      return Float.NEGATIVE_INFINITY;
    }
    float minSim = queue.topScore();
    if (globalSimilarityQueue == null) {
      return minSim;
    } else {
      return Math.max(minSim, Math.min(nonCompetitiveQueue.peek(), cachedGlobalMinSim));
    }
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
  public String toString() {
    return "TopKnnCollector[k=" + k() + ", size=" + queue.size() + "]";
  }
}
