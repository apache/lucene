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
import org.apache.lucene.util.Bits;

public class TestNeighborArray extends LuceneTestCase {

  public void testScoresDescOrder() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addInOrder(0, 1);
    neighbors.addInOrder(1, 0.8f);

    if (TEST_ASSERTS_ENABLED) {
      AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.addInOrder(2, 0.9f));
      assert ex.getMessage().startsWith("Nodes are added in the incorrect order!")
          : ex.getMessage();
    }

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

    if (TEST_ASSERTS_ENABLED) {
      AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.addInOrder(2, 0.15f));
      assert ex.getMessage().startsWith("Nodes are added in the incorrect order!")
          : ex.getMessage();
    }

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
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    }
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
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    }
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
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    }
    neighbors.addOutOfOrder(2, Float.NaN);
    neighbors.addOutOfOrder(5, Float.NaN);
    neighbors.addOutOfOrder(3, Float.NaN);
    neighbors.addOutOfOrder(7, Float.NaN);
    neighbors.addOutOfOrder(6, Float.NaN);
    neighbors.addOutOfOrder(4, Float.NaN);
    int[] unchecked = neighbors.sort((TestRandomVectorScorer) nodeId -> 7 - nodeId + 1);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {1, 2, 3, 4, 5, 6, 7}, neighbors);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors);
  }

  public void testAddwithScoringFunctionLargeOrd() throws IOException {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.addOutOfOrder(11, Float.NaN);
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> neighbors.addInOrder(1, 2));
    }
    neighbors.addOutOfOrder(12, Float.NaN);
    neighbors.addOutOfOrder(15, Float.NaN);
    neighbors.addOutOfOrder(13, Float.NaN);
    neighbors.addOutOfOrder(17, Float.NaN);
    neighbors.addOutOfOrder(16, Float.NaN);
    neighbors.addOutOfOrder(14, Float.NaN);
    int[] unchecked = neighbors.sort((TestRandomVectorScorer) nodeId -> 7 - nodeId + 11);
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6}, unchecked);
    assertNodesEqual(new int[] {11, 12, 13, 14, 15, 16, 17}, neighbors);
    assertScoresEqual(new float[] {7, 6, 5, 4, 3, 2, 1}, neighbors);
  }

  public void testMaxSizeAddInOrder() throws IOException {
    NeighborArray neighbors = new NeighborArray(3, true);
    neighbors.addInOrder(0, 1.0f);
    neighbors.addInOrder(1, 0.8f);
    neighbors.addInOrder(2, 0.6f);

    // Should throw when trying to add beyond max size
    expectThrows(IllegalStateException.class, () -> neighbors.addInOrder(3, 0.4f));

    assertScoresEqual(new float[] {1.0f, 0.8f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {0, 1, 2}, neighbors);
  }

  public void testMaxSizeAddOutOfOrder() throws IOException {
    NeighborArray neighbors = new NeighborArray(3, true);
    neighbors.addOutOfOrder(0, 0.8f);
    neighbors.addOutOfOrder(1, 1.0f);
    neighbors.addOutOfOrder(2, 0.6f);

    // Should throw when trying to add beyond max size
    expectThrows(IllegalStateException.class, () -> neighbors.addOutOfOrder(3, 0.4f));

    // Sort to verify contents
    neighbors.sort(null);
    assertScoresEqual(new float[] {1.0f, 0.8f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {1, 0, 2}, neighbors);
  }

  public void testMaxSizeInsertSorted() throws IOException {
    NeighborArray neighbors = new NeighborArray(3, true);
    neighbors.insertSorted(0, 1.0f);
    neighbors.insertSorted(1, 0.8f);
    neighbors.insertSorted(2, 0.6f);

    // Should throw when trying to insert beyond max size
    expectThrows(IllegalStateException.class, () -> neighbors.insertSorted(3, 0.4f));

    assertScoresEqual(new float[] {1.0f, 0.8f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {0, 1, 2}, neighbors);
  }

  public void testMaxSizeMixedOperations() throws IOException {
    NeighborArray neighbors = new NeighborArray(3, true);

    // Add first node
    neighbors.addInOrder(0, 1.0f);

    // Add second node out of order
    neighbors.addOutOfOrder(1, 0.8f);

    // Try to add third node in order - should fail because we used addOutOfOrder
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> neighbors.addInOrder(2, 0.6f));
    }

    // Add third node out of order
    neighbors.addOutOfOrder(2, 0.6f);

    // Try to add fourth node - should fail due to max size
    expectThrows(IllegalStateException.class, () -> neighbors.addOutOfOrder(3, 0.4f));

    // Sort to verify contents
    neighbors.sort(null);
    assertScoresEqual(new float[] {1.0f, 0.8f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {0, 1, 2}, neighbors);
  }

  public void testBoundaryValues() throws IOException {
    // Test with empty array
    NeighborArray neighbors = new NeighborArray(3, true);
    assertEquals(0, neighbors.size());

    // Test with single element
    neighbors.addInOrder(0, Float.MAX_VALUE);
    assertScoresEqual(new float[] {Float.MAX_VALUE}, neighbors);
    assertNodesEqual(new int[] {0}, neighbors);

    // Test with NaN values
    neighbors.clear();
    neighbors.addOutOfOrder(0, Float.NaN);
    neighbors.addOutOfOrder(1, Float.NaN);
    assertScoresEqual(new float[] {Float.NaN, Float.NaN}, neighbors);
    assertNodesEqual(new int[] {0, 1}, neighbors);
  }

  public void testSortOrder() throws IOException {
    // Test ascending order
    NeighborArray ascNeighbors = new NeighborArray(5, false);
    ascNeighbors.addInOrder(0, 0.1f);
    ascNeighbors.addInOrder(1, 0.2f);
    ascNeighbors.addInOrder(2, 0.3f);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.3f}, ascNeighbors);

    // Test descending order
    NeighborArray descNeighbors = new NeighborArray(5, true);
    descNeighbors.addInOrder(0, 0.3f);
    descNeighbors.addInOrder(1, 0.2f);
    descNeighbors.addInOrder(2, 0.1f);
    assertScoresEqual(new float[] {0.3f, 0.2f, 0.1f}, descNeighbors);

    // Test equal scores
    NeighborArray equalNeighbors = new NeighborArray(5, true);
    equalNeighbors.addInOrder(0, 0.5f);
    equalNeighbors.addInOrder(1, 0.5f);
    equalNeighbors.addInOrder(2, 0.5f);
    assertScoresEqual(new float[] {0.5f, 0.5f, 0.5f}, equalNeighbors);
  }

  public void testRemoveOperations() throws IOException {
    NeighborArray neighbors = new NeighborArray(5, true);
    neighbors.addInOrder(0, 1.0f);
    neighbors.addInOrder(1, 0.8f);
    neighbors.addInOrder(2, 0.6f);
    neighbors.addInOrder(3, 0.4f);

    // Test remove last
    neighbors.removeLast();
    assertScoresEqual(new float[] {1.0f, 0.8f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {0, 1, 2}, neighbors);

    // Test remove middle
    neighbors.removeIndex(1);
    assertScoresEqual(new float[] {1.0f, 0.6f}, neighbors);
    assertNodesEqual(new int[] {0, 2}, neighbors);

    // Test remove first
    neighbors.removeIndex(0);
    assertScoresEqual(new float[] {0.6f}, neighbors);
    assertNodesEqual(new int[] {2}, neighbors);

    // Test remove last remaining
    neighbors.removeLast();
    assertEquals(0, neighbors.size());
  }

  public void testClearOperation() throws IOException {
    NeighborArray neighbors = new NeighborArray(5, true);
    neighbors.addInOrder(0, 1.0f);
    neighbors.addInOrder(1, 0.8f);
    neighbors.addInOrder(2, 0.6f);

    // Test clear
    neighbors.clear();
    assertEquals(0, neighbors.size());

    // Test adding after clear
    neighbors.addInOrder(3, 1.0f);
    assertScoresEqual(new float[] {1.0f}, neighbors);
    assertNodesEqual(new int[] {3}, neighbors);
  }

  public void testComplexOperations() throws IOException {
    NeighborArray neighbors = new NeighborArray(5, true);

    // Add some nodes in order
    neighbors.addInOrder(0, 1.0f);
    neighbors.addInOrder(1, 0.8f);

    // Add some nodes out of order
    neighbors.addOutOfOrder(2, 0.9f);
    neighbors.addOutOfOrder(3, 0.7f);

    // Sort and verify
    neighbors.sort(null);
    assertScoresEqual(new float[] {1.0f, 0.9f, 0.8f, 0.7f}, neighbors);
    assertNodesEqual(new int[] {0, 2, 1, 3}, neighbors);

    // Remove some nodes
    neighbors.removeIndex(1);
    neighbors.removeLast();

    // Add more nodes
    neighbors.insertSorted(4, 0.85f);
    neighbors.insertSorted(5, 0.75f);

    // Final verification
    assertScoresEqual(new float[] {1.0f, 0.85f, 0.8f, 0.75f}, neighbors);
    assertNodesEqual(new int[] {0, 4, 1, 5}, neighbors);
  }

  public void testScorerOperations() throws IOException {
    NeighborArray neighbors = new NeighborArray(5, true);
    TestRandomVectorScorer scorer =
        nodeId -> {
          switch (nodeId) {
            case 0:
              return 1.0f;
            case 1:
              return 0.8f;
            case 2:
              return 0.9f;
            case 3:
              return 0.7f;
            default:
              return 0.0f;
          }
        };

    // Add nodes with NaN scores
    neighbors.addOutOfOrder(0, Float.NaN);
    neighbors.addOutOfOrder(1, Float.NaN);
    neighbors.addOutOfOrder(2, Float.NaN);
    neighbors.addOutOfOrder(3, Float.NaN);

    // Sort using scorer
    neighbors.sort(scorer);
    assertScoresEqual(new float[] {1.0f, 0.9f, 0.8f, 0.7f}, neighbors);
    assertNodesEqual(new int[] {0, 2, 1, 3}, neighbors);
  }

  private void assertScoresEqual(float[] scores, NeighborArray neighbors) {
    for (int i = 0; i < scores.length; i++) {
      assertEquals(scores[i], neighbors.getScores(i), 0.01f);
    }
  }

  private void assertNodesEqual(int[] nodes, NeighborArray neighbors) {
    for (int i = 0; i < nodes.length; i++) {
      assertEquals(nodes[i], neighbors.nodes()[i]);
    }
  }

  interface TestRandomVectorScorer extends RandomVectorScorer {
    @Override
    default int maxOrd() {
      throw new UnsupportedOperationException();
    }

    @Override
    default int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    default Bits getAcceptOrds(Bits acceptDocs) {
      throw new UnsupportedOperationException();
    }
  }
}
