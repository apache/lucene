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
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.FloatHeap;

/**
 * MultiLeafKnnCollector is a specific KnnCollector that can exchange the top collected results
 * across segments through a shared global queue.
 *
 * @lucene.experimental
 */
public final class MultiLeafKnnCollector extends KnnCollector.Decorator {

  // greediness of globally non-competitive search: (0,1]
  private static final float DEFAULT_GREEDINESS = 0.9f;
  private static final int DEFAULT_INTERVAL = 0xff;
  // the global queue of the highest similarities collected so far across all segments
  private final BlockingFloatHeap globalSimilarityQueue;
  // the local queue of the highest similarities if we are not competitive globally
  // the size of this queue is defined by greediness
  private final FloatHeap nonCompetitiveQueue;
  // the queue of the local similarities to periodically update with the global queue
  private final FloatHeap updatesQueue;
  private final float[] updatesScratch;
  // interval to synchronize the local and global queues, as a number of visited vectors
  private final int interval;
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
    this(k, DEFAULT_GREEDINESS, DEFAULT_INTERVAL, globalSimilarityQueue, subCollector);
  }

  /**
   * Create a new MultiLeafKnnCollector.
   *
   * @param k the number of neighbors to collect
   * @param greediness the greediness of the global search
   * @param interval (by number of collected values) the interval to synchronize the local and
   *     global queues
   * @param globalSimilarityQueue the global queue of the highest similarities collected so far
   * @param subCollector the local collector
   */
  public MultiLeafKnnCollector(
      int k,
      float greediness,
      int interval,
      BlockingFloatHeap globalSimilarityQueue,
      AbstractKnnCollector subCollector) {
    super(subCollector);
    if (greediness < 0 || greediness > 1) {
      throw new IllegalArgumentException("greediness must be in [0,1]");
    }
    if (interval <= 0) {
      throw new IllegalArgumentException("interval must be positive");
    }
    this.interval = interval;
    this.subCollector = subCollector;
    this.globalSimilarityQueue = globalSimilarityQueue;
    this.nonCompetitiveQueue = new FloatHeap(Math.max(1, Math.round((1 - greediness) * k)));
    this.updatesQueue = new FloatHeap(k);
    this.updatesScratch = new float[k];
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
        // BlockingFloatHeap#offer requires input to be sorted in ascending order, so we can't
        // pass in the underlying updatesQueue array as-is since it is only partially ordered
        // (see GH#13462):
        int len = updatesQueue.size();
        if (len > 0) {
          for (int i = 0; i < len; i++) {
            updatesScratch[i] = updatesQueue.poll();
          }
          assert updatesQueue.size() == 0;
          cachedGlobalMinSim = globalSimilarityQueue.offer(updatesScratch, len);
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
    return Math.max(
        subCollector.minCompetitiveSimilarity(),
        Math.min(nonCompetitiveQueue.peek(), cachedGlobalMinSim));
  }

  @Override
  public String toString() {
    return "MultiLeafKnnCollector[subCollector=" + subCollector + "]";
  }
}
