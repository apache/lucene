package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.BitSet;

public class ParentJoinNeighborQueue extends NeighborQueue {
    private final BitSet parentBitSet;
    public ParentJoinNeighborQueue(int initialSize, BitSet parentBitSet) {
        super(initialSize, true);
        this.parentBitSet = parentBitSet;
    }

    /**
     * Adds a new graph arc, extending the storage as needed.
     *
     * @param newNode the neighbor node id
     * @param newScore the score of the neighbor, relative to some other node
     */
    public void add(int newNode, float newScore) {
        int parentId = this.parentBitSet.nextSetBit(newNode);
        heap.push(encode(newNode, newScore));
    }

    /**
     * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
     * new node-and-score element. If the heap is full, compares the score against the current top
     * score, and replaces the top element if newScore is better than (greater than unless the heap is
     * reversed), the current top score.
     *
     * @param newNode the neighbor node id
     * @param newScore the score of the neighbor, relative to some other node
     */
    public boolean insertWithOverflow(int newNode, float newScore) {
        return heap.insertWithOverflow(encode(newNode, newScore));
    }
}
