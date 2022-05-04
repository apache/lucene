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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestNeighborArray extends LuceneTestCase {

  public void testScoresDescOrder() {
    NeighborArray neighbors = new NeighborArray(10, true);
    neighbors.add(0, 1);
    neighbors.add(1, 0.8f);

    AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.add(2, 0.9f));
    assertEquals("Nodes are added in the incorrect order!", ex.getMessage());

    neighbors.insertSorted(3, 0.9f);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 3, 1}, neighbors);

    neighbors.insertSorted(4, 1f);
    assertScoresEqual(new float[] {1, 1, 0.9f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 4, 3, 1}, neighbors);

    neighbors.insertSorted(5, 1.1f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 3, 1}, neighbors);

    neighbors.insertSorted(6, 0.8f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 3, 1, 6}, neighbors);

    neighbors.insertSorted(7, 0.8f);
    assertScoresEqual(new float[] {1.1f, 1, 1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(2);
    assertScoresEqual(new float[] {1.1f, 1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(0);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f, 0.8f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 3, 1, 6, 7}, neighbors);

    neighbors.removeIndex(4);
    assertScoresEqual(new float[] {1, 0.9f, 0.8f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 3, 1, 6}, neighbors);

    neighbors.removeLast();
    assertScoresEqual(new float[] {1, 0.9f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 3, 1}, neighbors);

    neighbors.insertSorted(8, 0.9f);
    assertScoresEqual(new float[] {1, 0.9f, 0.9f, 0.8f}, neighbors);
    asserNodesEqual(new int[] {0, 3, 8, 1}, neighbors);
  }

  public void testScoresAscOrder() {
    NeighborArray neighbors = new NeighborArray(10, false);
    neighbors.add(0, 0.1f);
    neighbors.add(1, 0.3f);

    AssertionError ex = expectThrows(AssertionError.class, () -> neighbors.add(2, 0.15f));
    assertEquals("Nodes are added in the incorrect order!", ex.getMessage());

    neighbors.insertSorted(3, 0.3f);
    assertScoresEqual(new float[] {0.1f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {0, 1, 3}, neighbors);

    neighbors.insertSorted(4, 0.2f);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {0, 4, 1, 3}, neighbors);

    neighbors.insertSorted(5, 0.05f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 1, 3}, neighbors);

    neighbors.insertSorted(6, 0.2f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 6, 1, 3}, neighbors);

    neighbors.insertSorted(7, 0.2f);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 4, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(2);
    assertScoresEqual(new float[] {0.05f, 0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {5, 0, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(0);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f, 0.3f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {0, 6, 7, 1, 3}, neighbors);

    neighbors.removeIndex(4);
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f, 0.3f}, neighbors);
    asserNodesEqual(new int[] {0, 6, 7, 1}, neighbors);

    neighbors.removeLast();
    assertScoresEqual(new float[] {0.1f, 0.2f, 0.2f}, neighbors);
    asserNodesEqual(new int[] {0, 6, 7}, neighbors);

    neighbors.insertSorted(8, 0.01f);
    assertScoresEqual(new float[] {0.01f, 0.1f, 0.2f, 0.2f}, neighbors);
    asserNodesEqual(new int[] {8, 0, 6, 7}, neighbors);
  }

  private void assertScoresEqual(float[] scores, NeighborArray neighbors) {
    for (int i = 0; i < scores.length; i++) {
      assertEquals(scores[i], neighbors.score[i], 0.01f);
    }
  }

  private void asserNodesEqual(int[] nodes, NeighborArray neighbors) {
    for (int i = 0; i < nodes.length; i++) {
      assertEquals(nodes[i], neighbors.node[i]);
    }
  }
}
