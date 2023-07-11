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
import java.util.Map;
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

  private final Map<Integer, Integer> nodeIdToHeapIndex;
  private final BitSet parentBitSet;
  private final int k;
  private final IntToIntFunction vectorToOrd;

  public ToParentJoinKnnResults(int k, BitSet parentBitSet, IntToIntFunction vectorToOrd) {
    super(k);
    this.nodeIdToHeapIndex = new HashMap<>(k < 2 ? k + 1 : (int) (k / 0.75 + 1.0));
    this.parentBitSet = parentBitSet;
    this.k = k;
    this.vectorToOrd = vectorToOrd;
  }

  /**
   * Adds a new graph arc, extending the storage as needed. This variant is more expensive but it is
   * compatible with a multi-valued scenario.
   *
   * @param childNodeId the neighbor node id
   * @param nodeScore the score of the neighbor, relative to some other node
   */
  @Override
  public void add(int childNodeId, float nodeScore) {
    int newHeapIndex;
    childNodeId = vectorToOrd.apply(childNodeId);
    assert !parentBitSet.get(childNodeId);
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    Integer existingHeapIndex = nodeIdToHeapIndex.get(nodeId);
    if (existingHeapIndex == null) {
      newHeapIndex = this.push(nodeId, nodeScore);
      this.shiftDownIndexesCache(newHeapIndex, nodeId);
    } else {
      float originalScore = getScoreAt(existingHeapIndex);
      if (originalScore > nodeScore) {
        return;
      }
      newHeapIndex = updateElement(existingHeapIndex, nodeId, nodeScore);
      this.shiftUpIndexesCache(newHeapIndex, nodeId);
    }
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
  public boolean insertWithOverflow(int childNodeId, float nodeScore) {
    final boolean full = isHeapFull();
    int minNodeId = this.topNode();
    // Parent and child nodes should be disjoint sets parent bit set should never have a child node
    // ID present
    childNodeId = vectorToOrd.apply(childNodeId);
    assert !parentBitSet.get(childNodeId);
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    boolean nodeAdded = false;
    Integer heapIndex = nodeIdToHeapIndex.get(nodeId);
    if (heapIndex == null) {
      heapIndex = insertWithOverflowWithPos(nodeId, nodeScore);
      if (heapIndex != -1) {
        nodeAdded = true;
        if (full) {
          nodeIdToHeapIndex.remove(minNodeId);
          this.shiftUpIndexesCache(heapIndex, nodeId);
        } else {
          this.shiftDownIndexesCache(heapIndex, nodeId);
        }
      }
      return nodeAdded;
    } else {
      // We are not removing a node, so no overflow detected in this branch
      float originalScore = getScoreAt(heapIndex);
      if (originalScore > nodeScore) {
        return true;
      }
      heapIndex = updateElement(heapIndex, nodeId, nodeScore);
      this.shiftUpIndexesCache(heapIndex, nodeId);
      return true;
    }
  }

  @Override
  public int pop() {
    int popped = super.pop();
    nodeIdToHeapIndex.remove(popped);
    // Shift all node IDs above the popped index down by 1
    for (int i = 1; i < size(); i++) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    assert ensureValidCache();
    return popped;
  }

  boolean ensureValidCache() {
    nodeIdToHeapIndex.forEach(
        (nodeId, heapIndex) -> {
          assert nodeId == getNodeAt(heapIndex)
              : "["
                  + nodeId
                  + "] not at ["
                  + heapIndex
                  + "] but ["
                  + getNodeAt(heapIndex)
                  + "]"
                  + nodeIdToHeapIndex;
        });
    return true;
  }

  @Override
  public void popWhileFull() {
    boolean didPop = false;
    while (size() > k) {
      int nodeId = super.pop();
      nodeIdToHeapIndex.remove(nodeId);
      didPop = true;
    }
    // Shift all node IDs above the popped index down by 1
    if (didPop) {
      for (int i = 1; i < size(); i++) {
        int nodeIdToShift = getNodeAt(i);
        nodeIdToHeapIndex.put(nodeIdToShift, i);
      }
    }
    assert ensureValidCache();
  }

  @Override
  public boolean isFull() {
    return size() >= k;
  }

  @Override
  protected void doClear() {
    nodeIdToHeapIndex.clear();
  }

  @Override
  public String toString() {
    return "ToParentJoinNeighborQueueResults[" + size() + "]";
  }

  private void shiftUpIndexesCache(Integer heapIndex, int nodeId) {
    for (int i = heapIndex - 1; i > 0; i--) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    nodeIdToHeapIndex.put(nodeId, heapIndex);
    assert ensureValidCache();
  }

  private void shiftDownIndexesCache(Integer heapIndex, int nodeId) {
    for (int i = heapIndex + 1; i <= size(); i++) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    nodeIdToHeapIndex.put(nodeId, heapIndex);
    assert ensureValidCache();
  }

  @Override
  public TopDocs topDocs() {
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(size(), k)];
    while (size() > 0) {
      int node = topNode();
      float score = topScore();
      pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(node, score);
    }

    TotalHits.Relation relation =
        incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }
}
