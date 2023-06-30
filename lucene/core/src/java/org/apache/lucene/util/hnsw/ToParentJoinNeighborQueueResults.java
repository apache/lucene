package org.apache.lucene.util.hnsw;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.BitSet;

public class ToParentJoinNeighborQueueResults extends NeighborQueueResults {
  private final Map<Integer, Integer> nodeIdToHeapIndex;
  private final BitSet parentBitSet;

  public ToParentJoinNeighborQueueResults(int initialSize, BitSet parentBitSet) {
    super(initialSize);
    this.nodeIdToHeapIndex = new HashMap<>(initialSize < 2 ? initialSize + 1 : (int) (initialSize / 0.75 + 1.0));
    this.parentBitSet = parentBitSet;
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
    Integer newHeapIndex = null;
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    Integer existingHeapIndex = nodeIdToHeapIndex.get(nodeId);
    if (existingHeapIndex == null) {
      newHeapIndex = heap.push(encode(nodeId, nodeScore));
      this.shiftDownIndexesCache(newHeapIndex, nodeId);
    } else {
      float originalScore = decodeScore(heap.get(existingHeapIndex));
      if (originalScore > nodeScore) {
        return;
      }
      newHeapIndex = heap.updateElement(existingHeapIndex, encode(nodeId, nodeScore));
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
    final boolean full = size() == heap.maxSize();
    int minNodeId = this.topNode();
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    boolean nodeAdded = false;
    Integer heapIndex = nodeIdToHeapIndex.get(nodeId);
    if (heapIndex == null) {
      heapIndex = heap.insertWithOverflow(encode(nodeId, nodeScore));
      if (heapIndex != -1) {
        nodeAdded = true;
        if (full) {
          this.shiftUpIndexesCache(heapIndex, nodeId);
          nodeIdToHeapIndex.remove(minNodeId);
        } else {
          this.shiftDownIndexesCache(heapIndex, nodeId);
        }
      }
      return nodeAdded;
    } else {
      // We are not removing a node, so no overflow detected in this branch
      float originalScore = decodeScore(heap.get(heapIndex));
      if (originalScore > nodeScore) {
        return true;
      }
      heapIndex = heap.updateElement(heapIndex, encode(nodeId, nodeScore));
      this.shiftUpIndexesCache(heapIndex, nodeId);
      return true;
    }
  }

  private void shiftUpIndexesCache(Integer heapIndex, int nodeId) {
    for (int i = heapIndex - 1; i > 0; i--) {
      int nodeIdToShift = decodeNodeId(heap.get(i));
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    nodeIdToHeapIndex.put(nodeId, heapIndex);
  }

  private void shiftDownIndexesCache(Integer heapIndex, int nodeId) {
    for (int i = heapIndex + 1; i <= heap.size(); i++) {
      int nodeIdToShift = decodeNodeId(heap.get(i));
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    nodeIdToHeapIndex.put(nodeId, heapIndex);
  }

  @Override
  public int pop() {
    long popped = heap.pop();
    int nodeId = decodeNodeId(popped);
    nodeIdToHeapIndex.remove(nodeId);
    // Shift all node IDs above the popped index down by 1
    for (int i = 1; i < heap.size(); i++) {
      int nodeIdToShift = decodeNodeId(heap.get(i));
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    return nodeId;
  }

  void ensureValidCache() {
    nodeIdToHeapIndex.forEach(
        (nodeId, heapIndex) -> {
          assert nodeId == decodeNodeId(heap.get(heapIndex))
              : "["
                  + nodeId
                  + "] not at ["
                  + heapIndex
                  + "] but ["
                  + decodeNodeId(heap.get(heapIndex))
                  + "]"
                  + nodeIdToHeapIndex;
        });
  }

  @Override
  public void clear() {
    super.clear();
    nodeIdToHeapIndex.clear();
  }

  @Override
  public String toString() {
    return "ToParentJoinNeighborQueueResults[" + heap.size() + "]";
  }
}
