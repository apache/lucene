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

package org.apache.lucene.search.join;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;

/** parent joining knn collector, vector docIds are deduplicated according to the parent bit set. */
class ToParentJoinKnnCollector extends AbstractKnnCollector {

  private final BitSet parentBitSet;
  private final NodeIdCachingHeap heap;

  /**
   * Create a new object for joining nearest child kNN documents with a parent bitset
   *
   * @param k The number of joined parent documents to collect
   * @param visitLimit how many child vectors can be visited
   * @param parentBitSet The leaf parent bitset
   */
  public ToParentJoinKnnCollector(int k, int visitLimit, BitSet parentBitSet) {
    super(k, visitLimit);
    this.parentBitSet = parentBitSet;
    this.heap = new NodeIdCachingHeap(k);
  }

  /**
   * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
   * new node-and-score element. If the heap is full, compares the score against the current top
   * score, and replaces the top element if newScore is better than (greater than unless the heap is
   * reversed), the current top score.
   *
   * <p>If docId's parent node has previously been collected and the provided nodeScore is less than
   * the stored score it will not be collected.
   *
   * @param docId the neighbor docId
   * @param nodeScore the score of the neighbor, relative to some other node
   */
  @Override
  public boolean collect(int docId, float nodeScore) {
    assert !parentBitSet.get(docId);
    int nodeId = parentBitSet.nextSetBit(docId);
    return heap.insertWithOverflow(nodeId, nodeScore);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return heap.size >= k() ? heap.topScore() : Float.NEGATIVE_INFINITY;
  }

  @Override
  public String toString() {
    return "ToParentJoinKnnCollector[k=" + k() + ", size=" + heap.size() + "]";
  }

  @Override
  public TopDocs topDocs() {
    assert heap.size() <= k() : "Tried to collect more results than the maximum number allowed";
    while (heap.size() > k()) {
      heap.popToDrain();
    }
    ScoreDoc[] scoreDocs = new ScoreDoc[heap.size()];
    for (int i = 1; i <= scoreDocs.length; i++) {
      scoreDocs[scoreDocs.length - i] = new ScoreDoc(heap.topNode(), heap.topScore());
      heap.popToDrain();
    }

    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
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
        throw new IllegalStateException();
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
