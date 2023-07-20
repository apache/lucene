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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * TopKnnResults is a specific KnnResults. A minHeap is used to keep track of the currently
 * collected vectors allowing for efficient updates as better vectors are collected.
 */
public class TopKnnResults extends KnnResults {

  /** A provider used to construct a new {@link TopKnnResults} */
  public static class Provider implements KnnResultsProvider {
    private final int k;
    private final int visitLimit;

    public Provider(int k, int visitLimit) {
      this.k = k;
      this.visitLimit = visitLimit;
    }

    @Override
    public int k() {
      return k;
    }

    @Override
    public int visitLimit() {
      return visitLimit;
    }

    @Override
    public KnnResults getKnnResults(IntToIntFunction vectorToOrd) {
      return new TopKnnResults(k, visitLimit, vectorToOrd);
    }
  }

  protected final int k;
  private final IntToIntFunction vectorOrdToDocId;
  protected final NeighborQueue queue;

  /**
   * @param k the number of neighbors to collect
   * @param vectorOrdToDocId translating vector ordinals to document ids, used when building TopDocs
   *     result
   */
  public TopKnnResults(int k, int visitLimit, IntToIntFunction vectorOrdToDocId) {
    super(visitLimit);
    this.k = k;
    this.vectorOrdToDocId = vectorOrdToDocId;
    this.queue = new NeighborQueue(k, false);
  }

  @Override
  public void doClear() {
    this.queue.clear();
  }

  @Override
  public void collect(int vectorId, float similarity) {
    queue.add(vectorId, similarity);
  }

  /**
   * If the collection is not full, adds a new node-and-score element.
   *
   * <p>If the collection is full, compares the score against the current top score, and replaces
   * the top element if newScore is better than the current top score.
   *
   * @param vectorId the neighbor node id
   * @param similarity the score of the neighbor, relative to some other node
   */
  @Override
  public boolean collectWithOverflow(int vectorId, float similarity) {
    return queue.insertWithOverflow(vectorId, similarity);
  }

  @Override
  public boolean isFull() {
    return queue.size() >= k;
  }

  @Override
  public float minSimilarity() {
    return queue.topScore();
  }

  @Override
  public TopDocs topDocs() {
    while (queue.size() > k) {
      queue.pop();
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    while (i < scoreDocs.length) {
      int node = queue.topNode();
      float score = queue.topScore();
      queue.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(vectorOrdToDocId.apply(node), score);
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
