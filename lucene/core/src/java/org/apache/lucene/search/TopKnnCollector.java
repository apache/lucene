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

import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * TopKnnCollector is a specific KnnCollector. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 *
 * @lucene.experimental
 */
public final class TopKnnCollector extends AbstractKnnCollector {
  private static final float DEFAULT_GREEDINESS = 0.9f;

  private final NeighborQueue queue;
  private final float greediness;
  private final NeighborQueue queueg;
  private final MaxScoreAccumulator globalMinSimAcc;
  private boolean kResultsCollected = false;
  private float cachedGlobalMinSim = Float.NEGATIVE_INFINITY;

  // greediness of globally non-competitive search: [0,1]

  /**
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   * @param globalMinSimAcc the global minimum competitive similarity tracked across all segments
   */
  public TopKnnCollector(int k, int visitLimit, MaxScoreAccumulator globalMinSimAcc) {
    super(k, visitLimit);
    this.greediness = DEFAULT_GREEDINESS;
    this.queue = new NeighborQueue(k, false);
    int queuegSize = Math.max(1, Math.round((1 - greediness) * k));
    this.queueg = new NeighborQueue(queuegSize, false);
    this.globalMinSimAcc = globalMinSimAcc;
  }

  public TopKnnCollector(
      int k, int visitLimit, MaxScoreAccumulator globalMinSimAcc, float greediness) {
    super(k, visitLimit);
    this.greediness = greediness;
    this.queue = new NeighborQueue(k, false);
    this.queueg = new NeighborQueue(Math.round((1 - greediness) * k), false);
    this.globalMinSimAcc = globalMinSimAcc;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean result = queue.insertWithOverflow(docId, similarity);
    queueg.insertWithOverflow(docId, similarity);

    boolean reachedKResults = (kResultsCollected == false && queue.size() == k());
    if (reachedKResults) {
      kResultsCollected = true;
    }
    if (globalMinSimAcc != null && kResultsCollected) {
      // as we've collected k results, we can start exchanging globally
      globalMinSimAcc.accumulate(queue.topNode(), queue.topScore());

      // periodically update the local copy of global similarity
      if (reachedKResults || (visitedCount & globalMinSimAcc.modInterval) == 0) {
        MaxScoreAccumulator.DocAndScore docAndScore = globalMinSimAcc.get();
        cachedGlobalMinSim = docAndScore.score;
      }
    }
    return result;
  }

  @Override
  public float minCompetitiveSimilarity() {
    float minSim = kResultsCollected ? queue.topScore() : Float.NEGATIVE_INFINITY;
    if (globalMinSimAcc == null) {
      return minSim;
    } else {
      float globalAccountedSim = Math.min(queueg.topScore(), cachedGlobalMinSim);
      return Math.max(minSim, globalAccountedSim);
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
