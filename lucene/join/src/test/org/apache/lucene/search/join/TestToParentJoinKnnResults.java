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

package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;

public class TestToParentJoinKnnResults extends LuceneTestCase {

  public void testNeighborsProduct() throws IOException {
    // make sure we have the sign correct
    BitSet parentBitSet = BitSet.of(new IntArrayDocIdSetIterator(new int[] {1, 3, 5}, 3), 6);
    DiversifyingNearestChildrenKnnCollector nn =
        new DiversifyingNearestChildrenKnnCollector(2, Integer.MAX_VALUE, parentBitSet);
    assertTrue(nn.collect(2, 0.5f));
    assertTrue(nn.collect(0, 0.2f));
    assertTrue(nn.collect(4, 1f));
    assertEquals(0.5f, nn.minCompetitiveSimilarity(), 0);
    TopDocs topDocs = nn.topDocs();
    assertEquals(topDocs.scoreDocs[0].score, 1f, 0);
    assertEquals(topDocs.scoreDocs[1].score, 0.5f, 0);
  }

  public void testInsertions() throws IOException {
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 1.2f, 4f};
    BitSet parentBitSet = BitSet.of(new IntArrayDocIdSetIterator(new int[] {3, 6, 9, 12}, 4), 13);
    DiversifyingNearestChildrenKnnCollector results =
        new DiversifyingNearestChildrenKnnCollector(7, Integer.MAX_VALUE, parentBitSet);
    for (int i = 0; i < nodes.length; i++) {
      results.collect(nodes[i], scores[i]);
    }
    TopDocs topDocs = results.topDocs();
    int[] sortedNodes = new int[topDocs.scoreDocs.length];
    float[] sortedScores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {2, 7, 10, 4}, sortedNodes);
    assertArrayEquals(new float[] {4f, 2f, 1.2f, 1f}, sortedScores, 0f);
  }

  public void testInsertionWithOverflow() throws IOException {
    int[] nodes = new int[] {4, 1, 5, 7, 8, 10, 2, 12, 14};
    float[] scores = new float[] {1f, 0.5f, 0.6f, 2f, 2f, 3f, 4f, 1f, 0.2f};
    BitSet parentBitSet =
        BitSet.of(new IntArrayDocIdSetIterator(new int[] {3, 6, 9, 11, 13, 15}, 6), 16);
    DiversifyingNearestChildrenKnnCollector results =
        new DiversifyingNearestChildrenKnnCollector(5, Integer.MAX_VALUE, parentBitSet);
    for (int i = 0; i < nodes.length - 1; i++) {
      results.collect(nodes[i], scores[i]);
    }
    assertFalse(results.collect(nodes[nodes.length - 1], scores[nodes.length - 1]));
    int[] sortedNodes = new int[5];
    float[] sortedScores = new float[5];
    TopDocs topDocs = results.topDocs();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      sortedNodes[i] = topDocs.scoreDocs[i].doc;
      sortedScores[i] = topDocs.scoreDocs[i].score;
    }
    assertArrayEquals(new int[] {2, 10, 7, 4, 12}, sortedNodes);
    assertArrayEquals(new float[] {4f, 3f, 2f, 1f, 1f}, sortedScores, 0f);
  }

  public void testRandomInsertionsWithOverflow() throws IOException {
    int[] parents = new int[100];
    List<Integer> children = new ArrayList<>();
    List<Float> childrenScores = new ArrayList<>();
    int previousParent = -1;
    int nextParent = random().nextInt(50) + 2;
    for (int i = 0; i < 100; i++) {
      for (int j = previousParent + 1; j < nextParent; j++) {
        children.add(j);
        childrenScores.add(random().nextFloat());
      }
      parents[i] = nextParent;
      previousParent = nextParent;
      nextParent = random().nextInt(50) + 2 + previousParent;
    }
    Collections.shuffle(children, random());
    BitSet parentBitSet =
        BitSet.of(new IntArrayDocIdSetIterator(parents, parents.length), nextParent + 1);
    DiversifyingNearestChildrenKnnCollector results =
        new DiversifyingNearestChildrenKnnCollector(20, Integer.MAX_VALUE, parentBitSet);
    for (int i = 0; i < children.size(); i++) {
      results.collect(children.get(i), childrenScores.get(i));
    }
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
