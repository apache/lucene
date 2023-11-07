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
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestNeighborArray extends LuceneTestCase {

  public void testScoresDescOrder() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addInOrder(0, 1);
    neighbors.addInOrder(1, 0.8f);

    AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.addInOrder(2, 0.9f));
    assert ex.getMessage().startsWith("Nodes are added in the incorrect order!") : ex.getMessage();

    neighbors.insertSorted(3, 0.9f);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 3, 1}, neighbors);

    neighbors.insertSorted(4, 1f);
    assertScoresEqual(new float[] {1, 1, 0.9f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 4, 3, 1}, neighbors);

    neighbors.insertSorted(5, 1.1f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 3, 1}, neighbors);

    neighbors.insertSorted(6, 0.8f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 3, 1, 6}, neighbors);

    neighbors.insertSorted(7, 0.8f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(2);
    assertScoresEqual(new float[] {1.1f, 1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(0);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(4);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 3, 1, 6}, neighbors);

    neighbors.removeLast();
    assertScoresEqual(new float[] {1, 0.9f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 3, 1}, neighbors);

    neighbors.insertSorted(8, 0.9f);
    assertScoresEqual(new float[] {1, 0.9f, 0.9f, 0.8f}, neighbors);
    assertNodesEqual(new int[] {0, 3, 8, 1}, neighbors);
  }

  public void testScoresAscOrder() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, false);
    neighbors.addInOrder(0, 0.1f);
    neighbors.addInOrder(1, 0.3f);

    AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.addInOrder(2, 0.15f));
    assert ex.getMessage().startsWith("Nodes are added in the incorrect order!") : ex.getMessage();

    neighbors.insertSorted(3, 0.3f);
    assertScoresEqual(new float[] {0.1f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {0, 1, 3}, neighbors);

    neighbors.insertSorted(4, 0.2f);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {0, 4, 1, 3}, neighbors);

    neighbors.insertSorted(5, 0.05f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 1, 3}, neighbors);

    neighbors.insertSorted(6, 0.2f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 6, 1, 3}, neighbors);

    neighbors.insertSorted(7, 0.2f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 4, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(2);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {5, 0, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(0);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {0, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(4);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f, 0.3f}, neighbors);
    assertNodesEqual(new int[] {0, 6, 7, 1}, neighbors);

    neighbors.removeLast();
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f}, neighbors);
    assertNodesEqual(new int[] {0, 6, 7}, neighbors);

    neighbors.insertSorted(8, 0.01f);
    assertScoresEqual(new float[] {0.01f, 0.1f, 0.2f, 0.2f}, neighbors);
    assertNodesEqual(new int[] {8, 0, 6, 7}, neighbors);
  }

  public void testSortAsc() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, false);
    neighbors.addOutOfOrder(1, 2);
    // we disallow calling addInOrder after addOutOfOrder even if they're actual in order
    expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    neighbors.addOutOfOrder(2, 3);
    neighbors.addOutOfOrder(5, 6);
    neighbors.addOutOfOrder(3, 4);
    neighbors.addOutOfOrder(7, 8);
    neighbors.addOutOfOrder(6, 7);
    neighbors.addOutOfOrder(4, 5);
    int[] unchecked = neighbors.sort(null);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {1, 2, 3, 4, 5, 6, 7}, neighbors);
    assertScoresEqual(new float[] {2, 3, 4, 5, 6, 7, 8}, neighbors);

    NeighborArray neighbors2 = new NeighborArray(10, false);
    neighbors2.addInOrder(0, 1);
    neighbors2.addInOrder(1, 2);
    neighbors2.addInOrder(4, 5);
    neighbors2.addOutOfOrder(2, 3);
    neighbors2.addOutOfOrder(6, 7);
    neighbors2.addOutOfOrder(5, 6);
    neighbors2.addOutOfOrder(3, 4);
    unchecked = neighbors2.sort(null);
    assertArrayEquals(new int[] {2, 3, 5, 6}, unchecked);
    assertNodesEqual(new int[] {0, 1, 2, 3, 4, 5, 6}, neighbors2);
    assertScoresEqual(new float[] {1, 2, 3, 4, 5, 6, 7}, neighbors2);
  }

  public void testSortDesc() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addOutOfOrder(1, 7);
    // we disallow calling addInOrder after addOutOfOrder even if they're actual in order
    expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    neighbors.addOutOfOrder(2, 6);
    neighbors.addOutOfOrder(5, 3);
    neighbors.addOutOfOrder(3, 5);
    neighbors.addOutOfOrder(7, 1);
    neighbors.addOutOfOrder(6, 2);
    neighbors.addOutOfOrder(4, 4);
    int[] unchecked = neighbors.sort(null);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {1, 2, 3, 4, 5, 6, 7}, neighbors);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors);

    NeighborArray neighbors2 = new NeighborArray(10, true);
    neighbors2.addInOrder(1, 7);
    neighbors2.addInOrder(2, 6);
    neighbors2.addInOrder(5, 3);
    neighbors2.addOutOfOrder(3, 5);
    neighbors2.addOutOfOrder(7, 1);
    neighbors2.addOutOfOrder(6, 2);
    neighbors2.addOutOfOrder(4, 4);
    unchecked = neighbors2.sort(null);
    assertArrayEquals(new int[] {2, 3, 5, 6}, unchecked);
    assertNodesEqual(new int[] {1, 2, 3, 4, 5, 6, 7}, neighbors2);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors2);
  }

  public void testAddwithScoringFunction() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addOutOfOrder(1, Float.NaN);
    expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    neighbors.addOutOfOrder(2, Float.NaN);
    neighbors.addOutOfOrder(5, Float.NaN);
    neighbors.addOutOfOrder(3, Float.NaN);
    neighbors.addOutOfOrder(7, Float.NaN);
    neighbors.addOutOfOrder(6, Float.NaN);
    neighbors.addOutOfOrder(4, Float.NaN);
    int[] unchecked = neighbors.sort(nodeId -> 7 - nodeId + 1);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {1, 2, 3, 4, 5, 6, 7}, neighbors);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors);
  }

  public void testAddwithScoringFunctionLargeOrd() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addOutOfOrder(11, Float.NaN);
    expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    neighbors.addOutOfOrder(12, Float.NaN);
    neighbors.addOutOfOrder(15, Float.NaN);
    neighbors.addOutOfOrder(13, Float.NaN);
    neighbors.addOutOfOrder(17, Float.NaN);
    neighbors.addOutOfOrder(16, Float.NaN);
    neighbors.addOutOfOrder(14, Float.NaN);
    int[] unchecked = neighbors.sort(nodeId -> 7 - nodeId + 11);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {11, 12, 13, 14, 15, 16, 17}, neighbors);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors);
  }

  private void assertScoresEqual(float[] scores, NeighborArray neighbors) {
    for (int i = 0; i < scores.length; i++) {
      assertEquals(scores[i], neighbors.score[i], 0.01f);
    }
  }

  private void assertNodesEqual(int[] nodes, NeighborArray neighbors) {
    for (int i = 0; i < nodes.length; i++) {
      assertEquals(nodes[i], neighbors.node[i]);
    }
  }
}
