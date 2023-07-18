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

import java.util.HashMap;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSet;

/** parent joining knn results, de-duplicates children nodes by their parent bit set */
public class ToParentJoinKnnResults extends KnnResults {

  /** provider class for creating a new {@link ToParentJoinKnnResults} */
  public static class Provider implements KnnResultsProvider {

    private final int k;
    private final BitSet parentBitSet;

    public Provider(int k, BitSet parentBitSet) {
      this.k = k;
      this.parentBitSet = parentBitSet;
    }

    @Override
    public int k() {
      return k;
    }

    @Override
    public KnnResults getKnnResults(IntToIntFunction vectorToOrd) {
      return new ToParentJoinKnnResults(k, parentBitSet, vectorToOrd);
    }
  }

  private final HashMap<Integer, IntCounter> docIdRefs;
  private final BitSet parentBitSet;
  private final int k;
  private final IntToIntFunction vectorToOrd;

  public ToParentJoinKnnResults(int k, BitSet parentBitSet, IntToIntFunction vectorToOrd) {
    super(k, vectorToOrd);
    this.docIdRefs = new HashMap<>(k < 2 ? k + 1 : (int) (k / 0.75 + 1.0));
    this.parentBitSet = parentBitSet;
    this.k = k;
    this.vectorToOrd = vectorToOrd;
  }

  @Override
  public int size() {
    return docIdRefs.size();
  }

  /**
   * Adds a new graph arc, extending the storage as needed. This variant is more expensive but it is
   * compatible with a multi-valued scenario.
   *
   * @param childNodeId the neighbor node id
   * @param nodeScore the score of the neighbor, relative to some other node
   */
  @Override
  public void collect(int childNodeId, float nodeScore) {
    childNodeId = vectorToOrd.apply(childNodeId);
    assert !parentBitSet.get(childNodeId);
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    docIdRefs.computeIfAbsent(nodeId, k -> new IntCounter()).inc();
    queue.add(nodeId, nodeScore);
  }

  /**
   * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
   * new node-and-score element. If the heap is full, compares the score against the current top
   * score, and replaces the top element if newScore is better than (greater than unless the heap is
   * reversed), the current top score.
   *
   * @param childNodeId the neighbor node id
   * @param nodeScore the score of the neighbor, relative to some other node
   */
  @Override
  public boolean collectWithOverflow(int childNodeId, float nodeScore) {
    // Parent and child nodes should be disjoint sets parent bit set should never have a child node
    // ID present
    childNodeId = vectorToOrd.apply(childNodeId);
    assert !parentBitSet.get(childNodeId);
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    if (isFull()) {
      final float topScore = queue.topScore();
      if (nodeScore > topScore || (topScore == nodeScore && childNodeId < queue.topNode())) {
        int refs = docIdRefs.computeIfAbsent(nodeId, k -> new IntCounter()).inc().count;
        // We added a new doc! pop the old ones to only have `k`
        while (refs == 1 && size() > k) {
          pop();
        }
        queue.add(nodeId, nodeScore);
        return true;
      }
      return false;
    } else {
      docIdRefs.computeIfAbsent(nodeId, k -> new IntCounter()).inc();
      queue.add(nodeId, nodeScore);
    }
    return true;
  }

  private void pop() {
    if (queue.size() > 0) {
      Integer node = queue.pop();
      popDocRef(node);
    }
  }

  private boolean popDocRef(Integer node) {
    return docIdRefs.compute(
            node,
            (n, c) -> {
              if (c == null) {
                return null;
              }
              if (c.dec().count <= 0) {
                return null;
              }
              return c;
            })
        == null;
  }

  @Override
  protected void doClear() {
    docIdRefs.clear();
  }

  @Override
  public String toString() {
    return "ToParentJoinKnnResults[" + size() + "]";
  }

  @Override
  public TopDocs topDocs() {
    while (size() > k) {
      pop();
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(size(), k)];
    while (i < scoreDocs.length) {
      int node = queue.topNode();
      float score = queue.topScore();
      queue.pop();
      if (popDocRef(node)) {
        scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(node, score);
      }
    }

    TotalHits.Relation relation =
        incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  private static class IntCounter {
    int count;

    IntCounter dec() {
      --this.count;
      return this;
    }

    IntCounter inc() {
      ++this.count;
      return this;
    }
  }
}
