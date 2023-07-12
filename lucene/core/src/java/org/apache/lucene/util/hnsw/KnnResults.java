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
 * KnnResults is a specific NeighborQueue, enforcing a minHeap is utilized for results.
 *
 * <p>This way as better results are found, the minimum result can be easily removed from the
 * collection
 */
public abstract class KnnResults {

  /** KnnResults when exiting search early and returning empty top docs */
  public static class EmptyKnnResults extends KnnResults {
    public EmptyKnnResults() {
      super(1, i -> i);
      markIncomplete();
    }

    @Override
    protected void doClear() {}

    @Override
    public TopDocs topDocs() {
      TotalHits th = new TotalHits(visitedCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
      return new TopDocs(th, new ScoreDoc[0]);
    }
  }

  protected final NeighborQueue queue;
  private final int k;
  private final IntToIntFunction ordToDoc;

  public KnnResults(int k, IntToIntFunction ordToDoc) {
    this.queue = new NeighborQueue(k, false);
    this.k = k;
    this.ordToDoc = ordToDoc;
  }

  public final void clear() {
    queue.clear();
    doClear();
  }

  public final boolean incomplete() {
    return queue.incomplete();
  }

  public final void markIncomplete() {
    this.queue.markIncomplete();
  }

  public final int visitedCount() {
    return queue.visitedCount();
  }

  public final void setVisitedCount(int count) {
    queue.setVisitedCount(count);
  }

  public final int size() {
    return queue.size();
  }

  public void collect(int docID, float similarity) {
    queue.add(docID, similarity);
  }

  public boolean collectWithOverflow(int docID, float similarity) {
    return queue.insertWithOverflow(docID, similarity);
  }

  public final boolean isFull() {
    return size() >= k;
  }

  public final float minSimilarity() {
    return queue.topScore();
  }

  public final int popNode() {
    return queue.pop();
  }

  /**
   * This will reduce the collected nodes to be `k` if there happens to be more than that many
   * inserted. Then the nodes stored will be returned. They won't necessarily be in a specific order
   *
   * @return the nearest K nodes, but not in a specific order
   */
  public final int[] popUntilNearestKNodes() {
    while (size() > k) {
      queue.pop();
    }
    return queue.nodes();
  }

  protected abstract void doClear();

  /**
   * This drains the collected nearest kNN results and returns them in a new {@link TopDocs}
   * collection, ordered by score descending
   *
   * @return The collected top documents
   */
  public TopDocs topDocs() {
    while (size() > k) {
      queue.pop();
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[size()];
    while (i < scoreDocs.length) {
      int node = queue.topNode();
      float score = queue.topScore();
      queue.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(ordToDoc.apply(node), score);
    }

    TotalHits.Relation relation =
        incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  @Override
  public String toString() {
    return "KnnResults[" + size() + "]";
  }
}
