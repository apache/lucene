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

package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;

public class TestToParentJoinNeighborQueueResults extends LuceneTestCase {

  public void testNeighborsProduct() throws IOException {
    // make sure we have the sign correct
    BitSet parentBitSet = BitSet.of(new IntArrayDocIdSetIterator(new int[] {1, 3, 5}, 3), 6);
    ToParentJoinKnnResults nn = new ToParentJoinKnnResults(2, parentBitSet, i -> i);
    assertTrue(nn.insertWithOverflow(2, 0.5f));
    assertTrue(nn.insertWithOverflow(0, 0.2f));
    assertTrue(nn.insertWithOverflow(4, 1f));
    assertEquals(0.5f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
    nn.pop();
  }

  public void testInsertions() throws IOException {
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
    BitSet parentBitSet = BitSet.of(new IntArrayDocIdSetIterator(new int[] {3, 6, 9, 12}, 4), 13);
    ToParentJoinKnnResults ToParentJoinNeighborQueueResults =
        new ToParentJoinKnnResults(7, parentBitSet, i -> i);
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
    assertArrayEquals(new int[] {6, 12, 9, 3}, sortedNodes);
    assertArrayEquals(new float[] {1f, 1.2f, 2f, 4f}, sortedScores, 0f);
  }

  public void testInsertionWithOverflow() throws IOException {
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2, 12, 14};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 3f, 4f, 1f, 1f};
    BitSet parentBitSet =
        BitSet.of(new IntArrayDocIdSetIterator(new int[] {3, 6, 9, 11, 13, 15}, 6), 16);
    ToParentJoinKnnResults ToParentJoinNeighborQueueResults =
        new ToParentJoinKnnResults(5, parentBitSet, i -> i);
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
    assertArrayEquals(new int[] {13, 6, 9, 11, 3}, sortedNodes);
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
      if (i >= length) {
        return NO_MORE_DOCS;
      }
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
