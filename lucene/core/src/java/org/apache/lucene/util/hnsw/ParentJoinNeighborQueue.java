package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.BitSet;

import java.util.HashMap;
import java.util.Map;

public class ParentJoinNeighborQueue extends NeighborQueue {
    static int capacity(int expectedSize) {
        assert expectedSize >= 0;
        return expectedSize < 2 ? expectedSize + 1 : (int) (expectedSize / 0.75 + 1.0);
    }
    private final Map<Integer, Integer> nodeIdToHeapIndex;
    private final BitSet parentBitSet;
    public ParentJoinNeighborQueue(int initialSize, BitSet parentBitSet) {
        super(initialSize, true);
        this.nodeIdToHeapIndex = new HashMap<>(capacity(initialSize));
        this.parentBitSet = parentBitSet;
    }

    /**
     * Adds a new graph arc, extending the storage as needed. This variant is more expensive but it is
     * compatible with a multi-valued scenario.
     *
     * @param nodeId the neighbor node id
     * @param nodeScore the score of the neighbor, relative to some other node
     */
    public void add(int nodeId, float nodeScore) {
        boolean nodeAdded = false;
        Integer newHeapIndex = null;
        Integer existingHeapIndex = nodeIdToHeapIndex.get(nodeId);
        if (existingHeapIndex == null) {
            nodeAdded = true;
            newHeapIndex = heap.push(encode(nodeId, nodeScore));
        } else {
            float originalScore = decodeScore(heap.get(existingHeapIndex));
            if (originalScore > nodeScore) {
                return;
            }
            newHeapIndex = heap.updateElement(existingHeapIndex, encode(nodeId, nodeScore));
        }
        this.updateHeapIndexesCache(existingHeapIndex == null || newHeapIndex > existingHeapIndex, newHeapIndex, nodeId);
    }

    /**
     * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
     * new node-and-score element. If the heap is full, compares the score against the current top
     * score, and replaces the top element if newScore is better than (greater than unless the heap is
     * reversed), the current top score.
     *
     * @param nodeId the neighbor node id
     * @param nodeScore the score of the neighbor, relative to some other node
     */
    public boolean insertWithOverflow(int nodeId, float nodeScore) {
        final boolean full = size() == heap.maxSize();
        int minNodeId = this.topNode();
        boolean nodeAdded = false;
        Integer heapIndex = nodeIdToHeapIndex.get(nodeId);
        if (heapIndex == null) {
            heapIndex = heap.insertWithOverflow(encode(nodeId, nodeScore));
            if (heapIndex != -1) {
                nodeAdded = true;
                this.updateHeapIndexesCache(full, true, heapIndex, nodeId);
            }
        } else {
            float originalScore = decodeScore(heap.get(heapIndex));
            float updatedScore = Math.max(originalScore, nodeScore);
            heapIndex = heap.updateElement(heapIndex, encode(nodeId, updatedScore));
            this.updateHeapIndexesCache(full, false, heapIndex, nodeId);
        }
        if (nodeAdded && full) {
            nodeIdToHeapIndex.remove(minNodeId);
        }
        return nodeAdded;
    }

    /**
     * This can be optimised if heap indexes have not changed, no update would be necessary
     *
     * @param shiftUp shift up or down from the heapIndex
     * @param heapIndex
     * @param nodeId
     */
    private void updateHeapIndexesCache(boolean shiftUp, Integer heapIndex, int nodeId) {
        if (shiftUp) {
            for (int i = heapIndex - 1; i > 0; i--) {
                int nodeIdToShift = decodeNodeId(heap.get(i));
                nodeIdToHeapIndex.put(nodeIdToShift, i);
            }
        } else {
            for (int i = heapIndex + 1; i <= heap.size(); i++) {
                int nodeIdToShift = decodeNodeId(heap.get(i));
                nodeIdToHeapIndex.put(nodeIdToShift, i);
            }
        }
        nodeIdToHeapIndex.put(nodeId, heapIndex);
    }

    @Override
    public int pop() {
        long popped = heap.pop();
        int nodeId = decodeNodeId(popped);
        int heapIndex = nodeIdToHeapIndex.remove(nodeId);

        // Shift all node IDs above the popped index down by 1
        for (int i = heapIndex; i <= heap.size(); i++) {
            int nodeIdToShift = decodeNodeId(heap.get(i));
            nodeIdToHeapIndex.put(nodeIdToShift, i - 1);
        }
        return nodeId;
    }

    void ensureValid() {
        nodeIdToHeapIndex.forEach((nodeId, heapIndex) -> {
            assert nodeId == decodeNodeId(heap.get(heapIndex)) : "[" + nodeId + "] not at [" + heapIndex +"] but [" + decodeNodeId(heap.get(heapIndex)) + "]" + nodeIdToHeapIndex;
        });
    }

    public void clear() {
        super.clear();
        nodeIdToHeapIndex.clear();
    }

    @Override
    public String toString() {
        return "DuplicateNeighbors[" + heap.size() + "]";
    }
}
