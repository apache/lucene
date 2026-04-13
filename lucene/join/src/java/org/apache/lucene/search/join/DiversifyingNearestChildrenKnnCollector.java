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

import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;

/**
 * This collects the nearest children vectors. Diversifying the results over the provided parent
 * filter. This means the nearest children vectors are returned, but only one per parent
 */
class DiversifyingNearestChildrenKnnCollector extends AbstractKnnCollector {

  private final BitSet parentBitSet;
  private final NodeIdCachingHeap heap;

  /**
   * Create a new object for joining nearest child kNN documents with a parent bitset
   *
   * @param k The number of joined parent documents to collect
   * @param visitLimit how many child vectors can be visited
   * @param parentBitSet The leaf parent bitset
   */
  public DiversifyingNearestChildrenKnnCollector(int k, int visitLimit, BitSet parentBitSet) {
    this(k, visitLimit, null, parentBitSet);
  }

  /**
   * Create a new object for joining nearest child kNN documents with a parent bitset
   *
   * @param k The number of joined parent documents to collect
   * @param visitLimit how many child vectors can be visited
   * @param searchStrategy The search strategy to use
   * @param parentBitSet The leaf parent bitset
   */
  public DiversifyingNearestChildrenKnnCollector(
      int k, int visitLimit, KnnSearchStrategy searchStrategy, BitSet parentBitSet) {
    super(k, visitLimit, searchStrategy);
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
    int parentNode = parentBitSet.nextSetBit(docId);
    return heap.insertWithOverflow(docId, parentNode, nodeScore);
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

  @Override
  public int numCollected() {
    return heap.size();
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

    // Parallel primitive arrays; index 0 is unused (heap is 1-based).
    private int[] childNodes;
    private int[] parentNodes;
    private float[] scores;
    private int size = 0;

    // Used to keep track of nodeId -> positionInHeap. This way when new scores are added for a
    // node, the heap can be
    // updated efficiently.
    private final IntIntHashMap nodeIdHeapIndex;
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
      this.childNodes = new int[heapSize];
      this.parentNodes = new int[heapSize];
      this.scores = new float[heapSize];
      this.nodeIdHeapIndex = new IntIntHashMap(maxSize);
    }

    public final int topNode() {
      return childNodes[1];
    }

    public final float topScore() {
      return scores[1];
    }

    private void growArrays() {
      childNodes = ArrayUtil.grow(childNodes);
      parentNodes = ArrayUtil.grow(parentNodes);
      scores = ArrayUtil.grow(scores);
    }

    private void pushIn(int nodeId, int parentId, float score) {
      size++;
      if (size == childNodes.length) {
        growArrays();
      }
      childNodes[size] = nodeId;
      parentNodes[size] = parentId;
      scores[size] = score;
      upHeap(size);
    }

    private void updateElement(int heapIndex, int nodeId, int parentId, float score) {
      assert parentNodes[heapIndex] == parentId
          : "attempted to update heap element value but with a different parent id";
      float oldScore = scores[heapIndex];
      childNodes[heapIndex] = nodeId;
      scores[heapIndex] = score;
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
    public boolean insertWithOverflow(int node, int parentNode, float score) {
      if (closed) {
        throw new IllegalStateException();
      }
      // Fast reject: heap is full and the new score cannot beat the current minimum.
      // All stored scores are >= scores[1] (min-heap property), so no existing-parent
      // update is possible either. Skip the hash map lookup entirely.
      if (size >= maxSize && (score < scores[1] || (score == scores[1] && node > childNodes[1]))) {
        return false;
      }
      int cursor = nodeIdHeapIndex.indexOf(parentNode);
      if (cursor >= 0) {
        int existingIndex = nodeIdHeapIndex.indexGet(cursor);
        if (scores[existingIndex] < score) {
          updateElement(existingIndex, node, parentNode, score);
          return true;
        }
        return false;
      }
      if (size >= maxSize) {
        updateTop(node, parentNode, score);
        return true;
      }
      pushIn(node, parentNode, score);
      return true;
    }

    private void popToDrain() {
      closed = true;
      if (size > 0) {
        childNodes[1] = childNodes[size]; // move last to first
        parentNodes[1] = parentNodes[size];
        scores[1] = scores[size];
        size--;
        downHeapWithoutIndexUpdate(1); // adjust heap
      } else {
        throw new IllegalStateException("The heap is empty");
      }
    }

    private void updateTop(int nodeId, int parentId, float score) {
      nodeIdHeapIndex.remove(parentNodes[1]);
      childNodes[1] = nodeId;
      parentNodes[1] = parentId;
      scores[1] = score;
      downHeap(1);
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final int size() {
      return size;
    }

    /**
     * Returns true if the element at heap position {@code k} should be evicted before the element
     * at position {@code j} (i.e. a is "less than" b in the min-heap ordering). Lower score loses
     * first; ties are broken by higher child id losing first.
     */
    private boolean isLessThan(float scoreK, int childIdK, float scoreJ, int childIdJ) {
      if (scoreK != scoreJ) return scoreK < scoreJ;
      return childIdK > childIdJ;
    }

    private void upHeap(int origPos) {
      int i = origPos;
      int savedChild = childNodes[i];
      int savedParent = parentNodes[i];
      float savedScore = scores[i];
      int j = i >>> 1;
      while (j > 0 && isLessThan(savedScore, savedChild, scores[j], childNodes[j])) {
        childNodes[i] = childNodes[j];
        parentNodes[i] = parentNodes[j];
        scores[i] = scores[j];
        nodeIdHeapIndex.put(parentNodes[i], i);
        i = j;
        j = j >>> 1;
      }
      nodeIdHeapIndex.put(savedParent, i);
      childNodes[i] = savedChild;
      parentNodes[i] = savedParent;
      scores[i] = savedScore;
    }

    private int downHeap(int i) {
      int savedChild = childNodes[i];
      int savedParent = parentNodes[i];
      float savedScore = scores[i];
      int j = i << 1; // left child
      int k = j + 1;
      if (k <= size && isLessThan(scores[k], childNodes[k], scores[j], childNodes[j])) {
        j = k;
      }
      while (j <= size && isLessThan(scores[j], childNodes[j], savedScore, savedChild)) {
        childNodes[i] = childNodes[j];
        parentNodes[i] = parentNodes[j];
        scores[i] = scores[j];
        nodeIdHeapIndex.put(parentNodes[i], i);
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && isLessThan(scores[k], childNodes[k], scores[j], childNodes[j])) {
          j = k;
        }
      }
      nodeIdHeapIndex.put(savedParent, i);
      childNodes[i] = savedChild;
      parentNodes[i] = savedParent;
      scores[i] = savedScore;
      return i;
    }

    // Used only during popToDrain: the index map is never read again after closed=true,
    // so we skip the map updates for speed.
    private void downHeapWithoutIndexUpdate(int i) {
      int savedChild = childNodes[i];
      int savedParent = parentNodes[i];
      float savedScore = scores[i];
      int j = i << 1; // left child
      int k = j + 1;
      if (k <= size && isLessThan(scores[k], childNodes[k], scores[j], childNodes[j])) {
        j = k;
      }
      while (j <= size && isLessThan(scores[j], childNodes[j], savedScore, savedChild)) {
        childNodes[i] = childNodes[j];
        parentNodes[i] = parentNodes[j];
        scores[i] = scores[j];
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && isLessThan(scores[k], childNodes[k], scores[j], childNodes[j])) {
          j = k;
        }
      }
      childNodes[i] = savedChild;
      parentNodes[i] = savedParent;
      scores[i] = savedScore;
    }
  }
}
