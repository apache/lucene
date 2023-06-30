package org.apache.lucene.util.hnsw;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

import java.util.Arrays;

public class TestParentJoinNeighborQueue extends LuceneTestCase {

    public void testInsertions() {
        int[] nodes = new int[] {5, 1, 5, 6, 6, 8, 1};
        float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
        ParentJoinNeighborQueue parentJoinNeighborQueue = new ParentJoinNeighborQueue(
                7,
                null
        );
        for (int i = 0; i < nodes.length; i++) {
            parentJoinNeighborQueue.add(nodes[i], scores[i]);
            parentJoinNeighborQueue.ensureValid();
        }
        int[] sortedNodes = new int[parentJoinNeighborQueue.size()];
        float[] sortedScores = new float[parentJoinNeighborQueue.size()];
        int size = parentJoinNeighborQueue.size();
        for (int i = 0; i < size; i++) {
            sortedNodes[i] = parentJoinNeighborQueue.topNode();
            sortedScores[i] = parentJoinNeighborQueue.topScore();
            parentJoinNeighborQueue.pop();
            parentJoinNeighborQueue.ensureValid();
        }
        assertArrayEquals(new int[]{1, 6, 8, 5}, sortedNodes);
        assertArrayEquals(new float[]{4f, 2f, 1.2f, 1f}, sortedScores, 0f);
    }

    public void testInsertionWithOverflow() {
        int[] nodes = new int[] {5, 1, 5, 6, 6, 8, 1, 9, 10};
        float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 3f, 4f, 1f, 1f};
        ParentJoinNeighborQueue parentJoinNeighborQueue = new ParentJoinNeighborQueue(
                5,
                null
        );
        for (int i = 0; i < 5; i++) {
            assertTrue(parentJoinNeighborQueue.insertWithOverflow(nodes[i], scores[i]));
            parentJoinNeighborQueue.ensureValid();
        }
        for (int i = 5; i < nodes.length; i++) {
            assertTrue(parentJoinNeighborQueue.insertWithOverflow(nodes[i], scores[i]));
            parentJoinNeighborQueue.ensureValid();
        }
        long v1 = parentJoinNeighborQueue.encode(11, 0.5f);
        long v2 = parentJoinNeighborQueue.encode(1, 4f);
        assertTrue(parentJoinNeighborQueue.decodeNodeId(v1) == 11);
        assertTrue(parentJoinNeighborQueue.decodeNodeId(v2) == 1);
        assertTrue(v2 < v1);
        int topNode = parentJoinNeighborQueue.topNode();
        float topScore = parentJoinNeighborQueue.topScore();
        assertTrue(topNode == 1);
        assertTrue(topScore == 4f);
        assertFalse(parentJoinNeighborQueue.insertWithOverflow(11, 0.5f));
        int[] sortedNodes = new int[5];
        float[] sortedScores = new float[5];
        int size = parentJoinNeighborQueue.size();
        for (int i = 0; i < size; i++) {
            sortedNodes[i] = parentJoinNeighborQueue.topNode();
            sortedScores[i] = parentJoinNeighborQueue.topScore();
            parentJoinNeighborQueue.pop();
            parentJoinNeighborQueue.ensureValid();
        }
        assertArrayEquals(new int[]{1, 8, 6, 5, 9, 10}, sortedNodes);
        assertArrayEquals(new float[]{4f, 3f, 2f, 1f, 1f, 1f}, sortedScores, 0f);
    }

}
