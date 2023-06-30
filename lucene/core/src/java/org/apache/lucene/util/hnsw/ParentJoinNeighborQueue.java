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
        this.shiftDownIndexesCache(newHeapIndex, nodeId);
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
            this.shiftDownIndexesCache(heapIndex, nodeId);
            return true;
        }
    }

    /**
     * This can be optimised if heap indexes have not changed, no update would be necessary
     *
     * @param heapIndex
     * @param nodeId
     */
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
