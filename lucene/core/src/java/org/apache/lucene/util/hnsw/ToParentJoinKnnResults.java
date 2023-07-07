package org.apache.lucene.util.hnsw;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.BitSet;

public class ToParentJoinKnnResults extends KnnResults {

  public record Provider(int k, BitSet parentBitSet) implements KnnResultsProvider {
    @Override
    public KnnResults getKnnResults() {
      return new ToParentJoinKnnResults(k, parentBitSet);
    }
  }

  private final Map<Integer, Integer> nodeIdToHeapIndex;
  private final BitSet parentBitSet;
  private final int k;

  public ToParentJoinKnnResults(int k, BitSet parentBitSet) {
    super(k);
    this.nodeIdToHeapIndex = new HashMap<>(k < 2 ? k + 1 : (int) (k / 0.75 + 1.0));
    this.parentBitSet = parentBitSet;
    this.k = k;
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
    int nodeId = parentBitSet.nextSetBit(childNodeId);
    boolean nodeAdded = false;
    Integer heapIndex = nodeIdToHeapIndex.get(nodeId);
    if (heapIndex == null) {
      heapIndex = insertWithOverflowWithPos(nodeId, nodeScore);
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
    long popped = super.pop();
    int nodeId = decodeNodeId(popped);
    nodeIdToHeapIndex.remove(nodeId);
    // Shift all node IDs above the popped index down by 1
    for (int i = 1; i < size(); i++) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    return nodeId;
  }

  void ensureValidCache() {
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
  }

  @Override
  public void popWhileFull() {
    while (size() > k) {
      long popped = super.pop();
      int nodeId = decodeNodeId(popped);
      nodeIdToHeapIndex.remove(nodeId);
    }
    // Shift all node IDs above the popped index down by 1
    for (int i = 1; i < size(); i++) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
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
  }

  private void shiftDownIndexesCache(Integer heapIndex, int nodeId) {
    for (int i = heapIndex + 1; i <= size(); i++) {
      int nodeIdToShift = getNodeAt(i);
      nodeIdToHeapIndex.put(nodeIdToShift, i);
    }
    nodeIdToHeapIndex.put(nodeId, heapIndex);
  }
}
