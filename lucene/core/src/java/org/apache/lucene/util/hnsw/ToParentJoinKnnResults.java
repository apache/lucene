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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;

/** parent joining knn results, vectorIds are deduplicated according to the parent bit set. */
public class ToParentJoinKnnResults extends KnnResults {

  /** provider class for creating a new {@link ToParentJoinKnnResults} */
  public static class Provider implements KnnResultsProvider {

    private final int k, visitLimit;
    private final BitSet parentBitSet;

    public Provider(int k, int visitLimit, BitSet parentBitSet) {
      this.k = k;
      this.parentBitSet = parentBitSet;
      this.visitLimit = visitLimit;
    }

    @Override
    public int visitLimit() {
      return visitLimit;
    }

    @Override
    public int k() {
      return k;
    }

    @Override
    public KnnResults getKnnResults(IntToIntFunction vectorToOrd) {
      return new ToParentJoinKnnResults(k, visitLimit, parentBitSet, vectorToOrd);
    }
  }

  private final BitSet parentBitSet;
  private final int k;
  private final IntToIntFunction vectorToOrd;
  private final NodeIdCachingHeap heap;

  public ToParentJoinKnnResults(
      int k, int visitLimit, BitSet parentBitSet, IntToIntFunction vectorToOrd) {
    super(visitLimit);
    this.parentBitSet = parentBitSet;
    this.k = k;
    this.vectorToOrd = vectorToOrd;
    this.heap = new NodeIdCachingHeap(k);
  }

  /**
   * Adds a new graph arc, extending the storage as needed.
   *
   * <p>If the provided childNodeId's parent has been previously collected and the nodeScore is less
   * than the previously stored score, this node will not be added to the collection.
   *
   * @param childNodeId the neighbor node id
   * @param nodeScore the score of the neighbor, relative to some other node
   */
  @Override
  public void collect(int childNodeId, float nodeScore) {
    childNodeId = vectorToOrd.apply(childNodeId);
    assert !parentBitSet.get(childNodeId);
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    heap.push(nodeId, nodeScore);
  }

  /**
   * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
   * new node-and-score element. If the heap is full, compares the score against the current top
   * score, and replaces the top element if newScore is better than (greater than unless the heap is
   * reversed), the current top score.
   *
   * <p>If childNodeId's parent node has previously been collected and the provided nodeScore is
   * less than the stored score it will not be collected.
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
    return heap.insertWithOverflow(nodeId, nodeScore);
  }

  @Override
  public boolean isFull() {
    return heap.size >= k;
  }

  @Override
  public float minSimilarity() {
    return heap.topScore();
  }

  @Override
  public void doClear() {
    heap.clear();
  }

  @Override
  public String toString() {
    return "ToParentJoinKnnResults[" + heap.size + "]";
  }

  @Override
  public TopDocs topDocs() {
    while (heap.size() > k) {
      heap.popToDrain();
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[heap.size()];
    while (i < scoreDocs.length) {
      int node = heap.topNode();
      float score = heap.topScore();
      heap.popToDrain();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(node, score);
    }

    TotalHits.Relation relation =
        incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  /**
   * This is a minimum binary heap, inspired by {@link org.apache.lucene.util.LongHeap}. But instead
   * of encoding and using `long` values. Node ids and scores are kept separate. Additionally, this
   * prevents duplicate nodes from being added.
   *
   * <p>So, for every node added, we will update its score if the newly provided score is better.
   * Every time we update a node's stored score, we ensure the heap's order.
   */
  private static class NodeIdCachingHeap {
    private final int maxSize;
    private int[] heapNodes;
    private float[] heapScores;
    private int size = 0;

    // Used to keep track of nodeId -> positionInHeap. This way when new scores are added for a
    // node, the heap can be
    // updated efficiently.
    private final Map<Integer, Integer> nodeIdHeapIndex;
    private boolean closed = false;

    public NodeIdCachingHeap(int maxSize) {
      final int heapSize;
      if (maxSize < 1 || maxSize >= ArrayUtil.MAX_ARRAY_LENGTH) {
        // Throw exception to prevent confusing OOME:
        throw new IllegalArgumentException(
            "maxSize must be > 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH - 1) + "; got: " + maxSize);
      }
      // NOTE: we add +1 because all access to heap is 1-based not 0-based.  heap[0] is unused.
      heapSize = maxSize + 1;
      this.maxSize = maxSize;
      this.nodeIdHeapIndex =
          new HashMap<>(maxSize < 2 ? maxSize + 1 : (int) (maxSize / 0.75 + 1.0));
      this.heapNodes = new int[heapSize];
      this.heapScores = new float[heapSize];
    }

    public final int topNode() {
      return heapNodes[1];
    }

    public final float topScore() {
      return heapScores[1];
    }

    public final void push(int nodeId, float score) {
      if (closed) {
        throw new IllegalStateException("must call clear() before adding new elements to heap");
      }
      Integer previouslyStoredHeapIndex = nodeIdHeapIndex.get(nodeId);
      if (previouslyStoredHeapIndex != null) {
        if (score > heapScores[previouslyStoredHeapIndex]) {
          updateElement(previouslyStoredHeapIndex, nodeId, score);
        }
        return;
      }
      pushIn(nodeId, score);
    }

    private void pushIn(int nodeId, float score) {
      size++;
      if (size == heapNodes.length) {
        heapNodes = ArrayUtil.grow(heapNodes, (size * 3 + 1) / 2);
        heapScores = ArrayUtil.grow(heapScores, (size * 3 + 1) / 2);
      }
      heapNodes[size] = nodeId;
      heapScores[size] = score;
      upHeap(size);
    }

    private void updateElement(int heapIndex, int nodeId, float score) {
      int oldValue = heapNodes[heapIndex];
      assert oldValue == nodeId
          : "attempted to update heap element value but with a different node id";
      float oldScore = heapScores[heapIndex];
      heapNodes[heapIndex] = nodeId;
      heapScores[heapIndex] = score;
      // Since we are a min heap, if the new value is less, we need to make sure to bubble it up
      if (score < oldScore) {
        upHeap(heapIndex);
      } else {
        downHeap(heapIndex);
      }
    }

    /**
     * Adds a value to an heap in log(size) time. If the number of values would exceed the heap's
     * maxSize, the least value is discarded.
     *
     * <p>If `node` already exists in the heap, this will return true if the stored score is updated
     * OR the heap is not currently at the maxSize.
     *
     * @return whether the value was added or updated
     */
    public boolean insertWithOverflow(int node, float score) {
      if (closed) {
        throw new IllegalStateException("must call clear() before adding new elements to heap");
      }
      Integer previousNodeIndex = nodeIdHeapIndex.get(node);
      if (previousNodeIndex != null) {
        if (heapScores[previousNodeIndex] < score) {
          updateElement(previousNodeIndex, node, score);
          return true;
        }
        return false;
      }
      if (size >= maxSize) {
        if (score < heapScores[1] || (score == heapScores[1] && node > heapNodes[1])) {
          return false;
        }
        updateTop(node, score);
        return true;
      }
      pushIn(node, score);
      return true;
    }

    private void popToDrain() {
      closed = true;
      if (size > 0) {
        heapNodes[1] = heapNodes[size]; // move last to first
        heapScores[1] = heapScores[size]; // move last to first
        size--;
        downHeapWithoutCacheUpdate(1); // adjust heap
      } else {
        throw new IllegalStateException("The heap is empty");
      }
    }

    private void updateTop(int nodeId, float score) {
      nodeIdHeapIndex.remove(heapNodes[1]);
      heapNodes[1] = nodeId;
      heapScores[1] = score;
      downHeap(1);
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final int size() {
      return size;
    }

    /** Removes all entries from the PriorityQueue. */
    public final void clear() {
      nodeIdHeapIndex.clear();
      size = 0;
      closed = false;
    }

    private boolean lessThan(int nodel, float scorel, int noder, float scorer) {
      if (scorel < scorer) {
        return true;
      }
      return scorel == scorer && nodel > noder;
    }

    private void upHeap(int origPos) {
      int i = origPos;
      int bottomNode = heapNodes[i];
      float bottomScore = heapScores[i];
      int j = i >>> 1;
      while (j > 0 && lessThan(bottomNode, bottomScore, heapNodes[j], heapScores[j])) {
        heapNodes[i] = heapNodes[j];
        heapScores[i] = heapScores[j];
        nodeIdHeapIndex.put(heapNodes[i], i);
        i = j;
        j = j >>> 1;
      }
      nodeIdHeapIndex.put(bottomNode, i);
      heapNodes[i] = bottomNode;
      heapScores[i] = bottomScore;
    }

    private int downHeap(int i) {
      int node = heapNodes[i];
      float score = heapScores[i];
      int j = i << 1; // find smaller child
      int k = j + 1;
      if (k <= size && lessThan(heapNodes[k], heapScores[k], heapNodes[j], heapScores[j])) {
        j = k;
      }
      while (j <= size && lessThan(heapNodes[j], heapScores[j], node, score)) {
        heapNodes[i] = heapNodes[j];
        heapScores[i] = heapScores[j];
        nodeIdHeapIndex.put(heapNodes[i], i);
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && lessThan(heapNodes[k], heapScores[k], heapNodes[j], heapScores[j])) {
          j = k;
        }
      }
      nodeIdHeapIndex.put(node, i);
      heapNodes[i] = node; // install saved value
      heapScores[i] = score; // install saved value
      return i;
    }

    private int downHeapWithoutCacheUpdate(int i) {
      int node = heapNodes[i];
      float score = heapScores[i];
      int j = i << 1; // find smaller child
      int k = j + 1;
      if (k <= size && lessThan(heapNodes[k], heapScores[k], heapNodes[j], heapScores[j])) {
        j = k;
      }
      while (j <= size && lessThan(heapNodes[j], heapScores[j], node, score)) {
        heapNodes[i] = heapNodes[j];
        heapScores[i] = heapScores[j];
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && lessThan(heapNodes[k], heapScores[k], heapNodes[j], heapScores[j])) {
          j = k;
        }
      }
      heapNodes[i] = node; // install saved value
      heapScores[i] = score; // install saved value
      return i;
    }
  }
}
