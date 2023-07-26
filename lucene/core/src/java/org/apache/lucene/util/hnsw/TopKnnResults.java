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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.KnnResults;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * TopKnnResults is a specific KnnResults. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 */
public class TopKnnResults extends KnnResults {

  protected final NeighborQueue queue;

  /**
   * @param k the number of neighbors to collect
   * @param visitLimit how many vector nodes the results are allowed to visit
   */
  public TopKnnResults(int k, int visitLimit) {
    super(k, visitLimit);
    this.queue = new NeighborQueue(k, false);
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
    while (queue.size() > k()) {
      queue.pop();
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    while (i < scoreDocs.length) {
      int node = queue.topNode();
      float score = queue.topScore();
      queue.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(node, score);
    }
    TotalHits.Relation relation =
        incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  @Override
  public String toString() {
    return "TopKnnResults[" + queue.size() + "]";
  }
}
