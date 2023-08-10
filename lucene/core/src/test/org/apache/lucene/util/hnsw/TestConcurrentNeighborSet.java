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
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.ConcurrentNeighborSet.ConcurrentNeighborArray;

public class TestConcurrentNeighborSet extends LuceneTestCase {
  private static final NeighborSimilarity simpleScore =
      new NeighborSimilarity() {
        @Override
        public float score(int a, int b) {
          return VectorSimilarityFunction.EUCLIDEAN.compare(new float[] {a}, new float[] {b});
        }

        @Override
        public ScoreFunction scoreProvider(int a) {
          return b -> score(a, b);
        }
      };

  private static float baseScore(int neighbor) throws IOException {
    return simpleScore.score(0, neighbor);
  }

  public void testInsertAndSize() throws IOException {
    ConcurrentNeighborSet neighbors = new ConcurrentNeighborSet(0, 2, simpleScore);
    neighbors.insert(1, baseScore(1));
    neighbors.insert(2, baseScore(2));
    assertEquals(2, neighbors.size());

    neighbors.insert(3, baseScore(3));
    assertEquals(2, neighbors.size());
  }

  public void testRemoveLeastDiverseFromEnd() throws IOException {
    ConcurrentNeighborSet neighbors = new ConcurrentNeighborSet(0, 3, simpleScore);
    neighbors.insert(1, baseScore(1));
    neighbors.insert(2, baseScore(2));
    neighbors.insert(3, baseScore(3));
    assertEquals(3, neighbors.size());

    neighbors.insert(4, baseScore(4));
    assertEquals(3, neighbors.size());

    List<Integer> expectedValues = Arrays.asList(1, 2, 3);
    Iterator<Integer> iterator = neighbors.nodeIterator();
    for (Integer expectedValue : expectedValues) {
      assertTrue(iterator.hasNext());
      assertEquals(expectedValue, iterator.next());
    }
    assertFalse(iterator.hasNext());
  }

  public void testInsertDiverse() throws IOException {
    var similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    var vectors = new HnswGraphTestCase.CircularFloatVectorValues(10);
    var vectorsCopy = vectors.copy();
    var candidates = new NeighborArray(10, false);
    NeighborSimilarity scoreBetween =
        new NeighborSimilarity() {
          @Override
          public float score(int a, int b) {
            return similarityFunction.compare(vectors.vectorValue(a), vectorsCopy.vectorValue(b));
          }

          @Override
          public ScoreFunction scoreProvider(int a) {
            return b -> score(a, b);
          }
        };
    IntStream.range(0, 10)
        .filter(i -> i != 7)
        .forEach(
            i -> {
              candidates.insertSorted(i, scoreBetween.score(7, i));
            });
    assert candidates.size() == 9;

    var neighbors = new ConcurrentNeighborSet(0, 3, scoreBetween);
    neighbors.insertDiverse(candidates);
    assertEquals(2, neighbors.size());
    assert neighbors.contains(8);
    assert neighbors.contains(6);
  }

  public void testNoDuplicatesDescOrder() {
    ConcurrentNeighborArray cna = new ConcurrentNeighborArray(5, true);
    cna.insertSorted(1, 10.0f);
    cna.insertSorted(2, 9.0f);
    cna.insertSorted(3, 8.0f);
    cna.insertSorted(1, 10.0f); // This is a duplicate and should be ignored
    cna.insertSorted(3, 8.0f); // This is also a duplicate
    assertArrayEquals(new int[] {1, 2, 3}, Arrays.copyOf(cna.node(), cna.size()));
    assertArrayEquals(new float[] {10.0f, 9.0f, 8.0f}, Arrays.copyOf(cna.score, cna.size()), 0.01f);
  }

  public void testNoDuplicatesAscOrder() {
    ConcurrentNeighborArray cna = new ConcurrentNeighborArray(5, false);
    cna.insertSorted(1, 8.0f);
    cna.insertSorted(2, 9.0f);
    cna.insertSorted(3, 10.0f);
    cna.insertSorted(1, 8.0f); // This is a duplicate and should be ignored
    cna.insertSorted(3, 10.0f); // This is also a duplicate
    assertArrayEquals(new int[] {1, 2, 3}, Arrays.copyOf(cna.node(), cna.size()));
    assertArrayEquals(new float[] {8.0f, 9.0f, 10.0f}, Arrays.copyOf(cna.score, cna.size()), 0.01f);
  }

  public void testNoDuplicatesSameScores() {
    ConcurrentNeighborArray cna = new ConcurrentNeighborArray(5, true);
    cna.insertSorted(1, 10.0f);
    cna.insertSorted(2, 10.0f);
    cna.insertSorted(3, 10.0f);
    cna.insertSorted(1, 10.0f); // This is a duplicate and should be ignored
    cna.insertSorted(3, 10.0f); // This is also a duplicate
    assertArrayEquals(new int[] {1, 2, 3}, Arrays.copyOf(cna.node(), cna.size()));
    assertArrayEquals(
        new float[] {10.0f, 10.0f, 10.0f}, Arrays.copyOf(cna.score, cna.size()), 0.01f);
  }
}
