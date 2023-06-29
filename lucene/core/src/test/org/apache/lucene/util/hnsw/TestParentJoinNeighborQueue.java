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

}
