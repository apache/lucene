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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.FloatHeap;

/**
 * MultiLeafTopKnnCollector is a specific KnnCollector that can exchange the top collected results
 * across segments through a shared global queue.
 *
 * @lucene.experimental
 */
public final class MultiLeafTopKnnCollector extends TopKnnCollector {

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

  /**
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   */
  public MultiLeafTopKnnCollector(int k, int visitLimit, BlockingFloatHeap globalSimilarityQueue) {
    super(k, visitLimit);
    this.greediness = DEFAULT_GREEDINESS;
    this.globalSimilarityQueue = globalSimilarityQueue;
    this.nonCompetitiveQueue = new FloatHeap(Math.max(1, Math.round((1 - greediness) * k)));
    this.updatesQueue = new FloatHeap(k);
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean localSimUpdated = queue.insertWithOverflow(docId, similarity);
    boolean firstKResultsCollected = (kResultsCollected == false && queue.size() == k());
    if (firstKResultsCollected) {
      kResultsCollected = true;
    }
    updatesQueue.offer(similarity);
    boolean globalSimUpdated = nonCompetitiveQueue.offer(similarity);

    if (kResultsCollected) {
      // as we've collected k results, we can start do periodic updates with the global queue
      if (firstKResultsCollected || (visitedCount & interval) == 0) {
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
    return Math.max(queue.topScore(), Math.min(nonCompetitiveQueue.peek(), cachedGlobalMinSim));
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
    return "MultiLeafTopKnnCollector[k=" + k() + ", size=" + queue.size() + "]";
  }
}
