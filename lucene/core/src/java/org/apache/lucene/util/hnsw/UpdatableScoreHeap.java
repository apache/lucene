package org.apache.lucene.util.hnsw;

import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.ArrayUtil;

/**
 * This is a binary min-heap, inspired by the in
 * `org.apache.lucene.search.join.DiversifyingNearestChildrenKnnCollector#NodeIdCachingHeap`, where
 * the top element is the document with the lowest score.
 *
 * <p>It maintains a map of docId to heap index, and updates the score of added document if it is
 * already present in the heap. Each time a doc's score is updated, we ensure heap order.
 */
public class UpdatableScoreHeap {
  // TODO: Had to (almost) duplicate this class to avoid dependency on join module.
  //  Can we refactor and avoid duplication with DiversifyingNearestChildrenKnnCollector?
  private final int maxSize;
  private DocScore[] heapNodes;
  private int size = 0;

  // Used to keep track of docId -> positionInHeap. This way when new scores are added for a
  // node, the heap can be updated efficiently.
  private final IntIntHashMap docToHeapIndex;
  private boolean closed = false;

  public UpdatableScoreHeap(int maxSize) {
    final int heapSize;
    if (maxSize < 1 || maxSize >= ArrayUtil.MAX_ARRAY_LENGTH) {
      // Throw exception to prevent confusing OOME:
      throw new IllegalArgumentException(
          "maxSize must be > 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH - 1) + "; got: " + maxSize);
    }
    // NOTE: we add +1 because all access to heap is 1-based not 0-based.  heap[0] is unused.
    heapSize = maxSize + 1;
    this.maxSize = maxSize;
    this.docToHeapIndex = new IntIntHashMap(maxSize);
    this.heapNodes = new DocScore[heapSize];
  }

  public final int topNode() {
    return heapNodes[1].doc();
  }

  public final float topScore() {
    return heapNodes[1].score();
  }

  private void pushIn(int doc, float score) {
    size++;
    if (size == heapNodes.length) {
      heapNodes = ArrayUtil.grow(heapNodes, (size * 3 + 1) / 2);
    }
    heapNodes[size] = new DocScore(doc, score);
    upHeap(size);
  }

  private void updateElement(int heapIndex, int doc, float score) {
    DocScore oldValue = heapNodes[heapIndex];
    assert oldValue.doc == doc
        : "attempted to update heap element value but with a different node id";
    float oldScore = heapNodes[heapIndex].score;
    heapNodes[heapIndex] = new DocScore(doc, score);
    // Since we are a min heap, if the new value is less, we need to make sure to bubble it up
    if (score < oldScore) {
      upHeap(heapIndex);
    } else {
      downHeap(heapIndex);
    }
  }

  /**
   * Adds a value to heap in log(size) time. If the number of values would exceed the heap's
   * maxSize, the least value is discarded.
   *
   * <p>If `doc` already exists in the heap, this will return true if the stored score is updated
   *
   * @return True if the value was added or updated, false otherwise
   */
  public boolean insertWithOverflow(int doc, float score) {
    if (closed) {
      throw new IllegalStateException();
    }
    // TODO: can we use IntIntHashMap#getOrDefault instead?
    int index = docToHeapIndex.indexOf(doc);
    if (index >= 0) {
      int previousNodeIndex = docToHeapIndex.indexGet(index);
      if (heapNodes[previousNodeIndex].score < score) {
        updateElement(previousNodeIndex, doc, score);
        return true;
      }
      return false;
    }
    if (size >= maxSize) {
      if (score < heapNodes[1].score || (score == heapNodes[1].score && doc > heapNodes[1].doc)) {
        return false;
      }
      updateTop(doc, score);
      return true;
    }
    pushIn(doc, score);
    return true;
  }

  /** Removes top element from the heap */
  public final void pop() {
    closed = true;
    if (size > 0) {
      heapNodes[1] = heapNodes[size]; // move last to first
      size--;
      downHeap(1);
      //      downHeapWithoutCacheUpdate(1); // adjust heap
    } else {
      throw new IllegalStateException("The heap is empty");
    }
  }

  private void updateTop(int doc, float score) {
    docToHeapIndex.remove(heapNodes[1].doc);
    heapNodes[1] = new DocScore(doc, score);
    downHeap(1);
  }

  /** Returns the number of elements currently stored in the PriorityQueue. */
  public final int size() {
    return size;
  }

  private void upHeap(int origPos) {
    int i = origPos;
    DocScore bottomNode = heapNodes[i];
    int j = i >>> 1;
    while (j > 0 && bottomNode.compareTo(heapNodes[j]) < 0) {
      heapNodes[i] = heapNodes[j];
      docToHeapIndex.put(heapNodes[i].doc, i);
      i = j;
      j = j >>> 1;
    }
    docToHeapIndex.put(bottomNode.doc, i);
    heapNodes[i] = bottomNode;
  }

  private int downHeap(int i) {
    DocScore node = heapNodes[i];
    int j = i << 1; // find smaller child
    int k = j + 1;
    if (k <= size && heapNodes[k].compareTo(heapNodes[j]) < 0) {
      j = k;
    }
    while (j <= size && heapNodes[j].compareTo(node) < 0) {
      heapNodes[i] = heapNodes[j];
      docToHeapIndex.put(heapNodes[i].doc, i);
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && heapNodes[k].compareTo(heapNodes[j]) < 0) {
        j = k;
      }
    }
    docToHeapIndex.put(node.doc, i);
    heapNodes[i] = node; // install saved value
    return i;
  }

  //  private int downHeapWithoutCacheUpdate(int i) {
  //    DocScore node = heapNodes[i];
  //    int j = i << 1; // find smaller child
  //    int k = j + 1;
  //    if (k <= size && heapNodes[k].compareTo(heapNodes[j]) < 0) {
  //      j = k;
  //    }
  //    while (j <= size && heapNodes[j].compareTo(node) < 0) {
  //      heapNodes[i] = heapNodes[j];
  //      i = j;
  //      j = i << 1;
  //      k = j + 1;
  //      if (k <= size && heapNodes[k].compareTo(heapNodes[j]) < 0) {
  //        j = k;
  //      }
  //    }
  //    heapNodes[i] = node; // install saved value
  //    return i;
  //  }

  /** Keeps track of docId and score. */
  private record DocScore(int doc, float score) implements Comparable<DocScore> {

    @Override
    public int compareTo(DocScore o) {
      int fc = Float.compare(score, o.score);
      if (fc == 0) {
        // lower numbers are the tiebreakers, lower ids are preferred.
        return Integer.compare(o.doc, doc);
      }
      return fc;
    }
  }
}
