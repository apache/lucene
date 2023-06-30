package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.Arrays;

public class TestToParentJoinNeighborQueueResults extends LuceneTestCase {

  public void testNeighborsProduct() throws IOException {
    // make sure we have the sign correct
    BitSet bitSet = BitSet.of(DocIdSetIterator.all(3), 3);
    ToParentJoinNeighborQueueResults nn = new ToParentJoinNeighborQueueResults(2, bitSet);
    assertTrue(nn.insertWithOverflow(2, 0.5f));
    assertTrue(nn.insertWithOverflow(1, 0.2f));
    assertTrue(nn.insertWithOverflow(3, 1f));
    assertEquals(0.5f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
    nn.pop();
  }

  public void testInsertions() {
    int[] nodes = new int[] {5, 1, 5, 6, 6, 8, 1};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
    
    ToParentJoinNeighborQueueResults ToParentJoinNeighborQueueResults = new ToParentJoinNeighborQueueResults(7, null);
    for (int i = 0; i < nodes.length; i++) {
      ToParentJoinNeighborQueueResults.add(nodes[i], scores[i]);
      ToParentJoinNeighborQueueResults.ensureValidCache();
    }
    int[] sortedNodes = new int[ToParentJoinNeighborQueueResults.size()];
    float[] sortedScores = new float[ToParentJoinNeighborQueueResults.size()];
    int size = ToParentJoinNeighborQueueResults.size();
    for (int i = 0; i < size; i++) {
      sortedNodes[i] = ToParentJoinNeighborQueueResults.topNode();
      sortedScores[i] = ToParentJoinNeighborQueueResults.topScore();
      ToParentJoinNeighborQueueResults.pop();
      ToParentJoinNeighborQueueResults.ensureValidCache();
    }
    assertArrayEquals(new int[] {5, 8, 6, 1}, sortedNodes);
    assertArrayEquals(new float[] {1f, 1.2f, 2f, 4f}, sortedScores, 0f);
  }

  public void testInsertionWithOverflow() {
    int[] nodes = new int[] {5, 1, 5, 6, 6, 8, 1, 9, 10};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 3f, 4f, 1f, 1f};
    ToParentJoinNeighborQueueResults ToParentJoinNeighborQueueResults = new ToParentJoinNeighborQueueResults(5, null);
    for (int i = 0; i < 5; i++) {
      assertTrue(ToParentJoinNeighborQueueResults.insertWithOverflow(nodes[i], scores[i]));
      ToParentJoinNeighborQueueResults.ensureValidCache();
    }
    for (int i = 5; i < nodes.length - 1; i++) {
      assertTrue(ToParentJoinNeighborQueueResults.insertWithOverflow(nodes[i], scores[i]));
      ToParentJoinNeighborQueueResults.ensureValidCache();
    }
    assertFalse(
        ToParentJoinNeighborQueueResults.insertWithOverflow(
            nodes[nodes.length - 1], scores[nodes.length - 1]));
    int[] sortedNodes = new int[5];
    float[] sortedScores = new float[5];
    int size = ToParentJoinNeighborQueueResults.size();
    for (int i = 0; i < size; i++) {
      sortedNodes[i] = ToParentJoinNeighborQueueResults.topNode();
      sortedScores[i] = ToParentJoinNeighborQueueResults.topScore();
      ToParentJoinNeighborQueueResults.pop();
      ToParentJoinNeighborQueueResults.ensureValidCache();
    }
    assertArrayEquals(new int[] {9, 5, 6, 8, 1}, sortedNodes);
    assertArrayEquals(new float[] {1f, 1f, 2f, 3f, 4f}, sortedScores, 0f);
  }

  static class IntArrayDocIdSetIterator extends DocIdSetIterator {

    private final int[] docs;
    private final int length;
    private int i = 0;
    private int doc = -1;

    IntArrayDocIdSetIterator(int[] docs, int length) {
      this.docs = docs;
      this.length = length;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return doc = docs[i++];
    }

    @Override
    public int advance(int target) throws IOException {
      int bound = 1;
      // given that we use this for small arrays only, this is very unlikely to overflow
      while (i + bound < length && docs[i + bound] < target) {
        bound *= 2;
      }
      i = Arrays.binarySearch(docs, i + bound / 2, Math.min(i + bound + 1, length), target);
      if (i < 0) {
        i = -1 - i;
      }
      return doc = docs[i++];
    }

    @Override
    public long cost() {
      return length;
    }
  }
}
