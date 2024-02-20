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

import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.FloatHeap;

/**
 * MultiLeafKnnCollector is a specific KnnCollector that can exchange the top collected results
 * across segments through a shared global queue.
 *
 * @lucene.experimental
 */
public final class MultiLeafKnnCollector implements KnnCollector {

  // greediness of globally non-competitive search: (0,1]
  private static final float DEFAULT_GREEDINESS = 0.9f;
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
  private final AbstractKnnCollector subCollector;

  /**
   * Create a new MultiLeafKnnCollector.
   *
   * @param k the number of neighbors to collect
   * @param globalSimilarityQueue the global queue of the highest similarities collected so far
   *     across all segments
   * @param subCollector the local collector
   */
  public MultiLeafKnnCollector(
      int k, BlockingFloatHeap globalSimilarityQueue, AbstractKnnCollector subCollector) {
    this.greediness = DEFAULT_GREEDINESS;
    this.subCollector = subCollector;
    this.globalSimilarityQueue = globalSimilarityQueue;
    this.nonCompetitiveQueue = new FloatHeap(Math.max(1, Math.round((1 - greediness) * k)));
    this.updatesQueue = new FloatHeap(k);
  }

  @Override
  public boolean earlyTerminated() {
    return subCollector.earlyTerminated();
  }

  @Override
  public void incVisitedCount(int count) {
    subCollector.incVisitedCount(count);
  }

  @Override
  public long visitedCount() {
    return subCollector.visitedCount();
  }

  @Override
  public long visitLimit() {
    return subCollector.visitLimit();
  }

  @Override
  public int k() {
    return subCollector.k();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean localSimUpdated = subCollector.collect(docId, similarity);
    boolean firstKResultsCollected =
        (kResultsCollected == false && subCollector.numCollected() == k());
    if (firstKResultsCollected) {
      kResultsCollected = true;
    }
    updatesQueue.offer(similarity);
    boolean globalSimUpdated = nonCompetitiveQueue.offer(similarity);

    if (kResultsCollected) {
      // as we've collected k results, we can start do periodic updates with the global queue
      if (firstKResultsCollected || (subCollector.visitedCount() & interval) == 0) {
        cachedGlobalMinSim = globalSimilarityQueue.offer(updatesQueue.getHeap());
        updatesQueue.clear();
        globalSimUpdated = true;
      }
    }
    return localSimUpdated || globalSimUpdated;
  }

  @Override
  public float minCompetitiveSimilarity() {
    if (kResultsCollected == false) {
      return Float.NEGATIVE_INFINITY;
    }
    return Math.max(
        subCollector.minCompetitiveSimilarity(),
        Math.min(nonCompetitiveQueue.peek(), cachedGlobalMinSim));
  }

  @Override
  public TopDocs topDocs() {
    return subCollector.topDocs();
  }

  @Override
  public String toString() {
    return "MultiLeafKnnCollector[subCollector=" + subCollector + "]";
  }
}
