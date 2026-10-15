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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Test OffHeapHnswGraph's behavior specifically, for more complex test, see {@link
 * HnswGraphTestCase}
 */
public class TestOffHeapHnswGraph extends LuceneTestCase {

  public void testRequiresSize() {
    expectThrows(IllegalArgumentException.class, () -> BuildableHnswGraph.create(10, -1, true));
  }

  /* assert exception will be thrown when we add out of bound node to a fixed size graph */
  public void testNoGrowth() {
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(10, 100)) {
      expectThrows(IllegalStateException.class, () -> graph.addNode(1, 100));
    }
  }

  /* assert exception will be thrown when we call getNodeOnLevel for an incomplete graph */
  public void testIncompleteGraphThrow() {
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(10, 6)) {
      graph.addNode(1, 0);
      graph.addNode(0, 0);
      graph.addNode(0, 5);
      expectThrows(IllegalStateException.class, () -> graph.getNodesOnLevel(0));
    }
  }

  public void testGraphBuildOutOfOrder() {
    int maxLevel = 5;
    int numNodes = atLeast(100);
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(10, numNodes)) {
      List<List<Integer>> levelToNodes = new ArrayList<>();
      for (int i = 0; i < maxLevel; i++) {
        levelToNodes.add(new ArrayList<>());
      }
      int[] insertions = new int[numNodes];
      for (int i = 0; i < numNodes; i++) {
        insertions[i] = i;
      }
      // shuffle the insertion order
      for (int i = 0; i < numNodes; i++) {
        int pos1 = random().nextInt(numNodes);
        int pos2 = random().nextInt(numNodes);
        int tmp = insertions[pos1];
        insertions[pos1] = insertions[pos2];
        insertions[pos2] = tmp;
      }

      for (int i : insertions) {
        int level = random().nextInt(maxLevel);
        for (int l = level; l >= 0; l--) {
          graph.addNode(l, i);
          graph.trySetNewEntryNode(i, l);
          if (l > graph.numLevels() - 1) {
            graph.tryPromoteNewEntryNode(i, l, graph.numLevels() - 1);
          }
          levelToNodes.get(l).add(i);
        }
      }

      for (int i = 0; i < maxLevel; i++) {
        levelToNodes.get(i).sort(Integer::compare);
      }
      for (int l = 0; l < graph.numLevels(); l++) {
        HnswGraph.NodesIterator nodesIterator = graph.getNodesOnLevel(l);
        assertEquals(levelToNodes.get(l).size(), nodesIterator.size());
        int idx = 0;
        while (nodesIterator.hasNext()) {
          assertEquals(levelToNodes.get(l).get(idx++), nodesIterator.next());
        }
      }
    }
  }

  /* neighbors written through beginUpdate/endUpdate are visible to all read paths */
  public void testUpdateAndRead() {
    int M = 4;
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(M, 10)) {
      graph.addNode(1, 3);
      graph.addNode(0, 3);
      NeighborArray scratch0 = graph.newUpdateScratch(0);
      NeighborArray scratch1 = graph.newUpdateScratch(1);
      assertEquals(2 * M + 1, scratch0.maxSize());
      assertEquals(M + 1, scratch1.maxSize());

      NeighborArray nbrs = graph.beginUpdate(0, 3, scratch0);
      assertEquals(0, nbrs.size());
      nbrs.addInOrder(7, 0.9f);
      nbrs.addInOrder(1, 0.5f);
      graph.endUpdate(0, 3, nbrs);

      nbrs = graph.beginUpdate(1, 3, scratch1);
      nbrs.addOutOfOrder(9, Float.NaN);
      graph.endUpdate(1, 3, nbrs);

      int[] buffer = new int[2 * M + 1];
      assertEquals(2, graph.copyNeighbors(0, 3, buffer));
      assertEquals(7, buffer[0]);
      assertEquals(1, buffer[1]);
      assertEquals(1, graph.copyNeighbors(1, 3, buffer));
      assertEquals(9, buffer[0]);

      graph.seek(0, 3);
      assertEquals(2, graph.neighborCount());
      assertEquals(7, graph.nextNeighbor());
      assertEquals(1, graph.nextNeighbor());
      assertEquals(NO_MORE_DOCS, graph.nextNeighbor());

      // scores and sort state survive the round trip
      nbrs = graph.beginUpdate(0, 3, scratch0);
      assertEquals(0.9f, nbrs.getScores(0), 0f);
      assertEquals(0.5f, nbrs.getScores(1), 0f);
      nbrs.addInOrder(2, 0.1f);
      graph.endUpdate(0, 3, nbrs);
      assertEquals(3, graph.copyNeighbors(0, 3, buffer));

      // a node that is added again starts out with an empty neighbor list
      graph.addNode(0, 3);
      assertEquals(0, graph.copyNeighbors(0, 3, buffer));
    }
  }

  /* promoting a node to a new top level keeps its neighbors on the existing levels */
  public void testPromotionKeepsNeighbors() {
    int M = 4;
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(M, 10)) {
      graph.addNode(2, 5);
      graph.addNode(1, 5);
      graph.addNode(0, 5);
      for (int level = 0; level <= 2; level++) {
        NeighborArray nbrs = graph.beginUpdate(level, 5, graph.newUpdateScratch(level));
        nbrs.addOutOfOrder(level + 1, Float.NaN);
        graph.endUpdate(level, 5, nbrs);
      }
      assertFalse(graph.nodeExistAtLevel(3, 5));
      graph.addNode(3, 5);
      assertTrue(graph.nodeExistAtLevel(3, 5));

      int[] buffer = new int[2 * M + 1];
      for (int level = 0; level <= 2; level++) {
        assertEquals(1, graph.copyNeighbors(level, 5, buffer));
        assertEquals(level + 1, buffer[0]);
      }
      assertEquals(0, graph.copyNeighbors(3, 5, buffer));
    }
  }

  public void testMemoryAccounting() {
    try (OffHeapHnswGraph graph = new OffHeapHnswGraph(16, 1000)) {
      long level0 = NeighborArray.slotBytes(33) * 1000;
      assertEquals(level0, graph.offHeapBytesUsed());
      graph.addNode(1, 0);
      assertTrue(graph.offHeapBytesUsed() > level0);
      assertTrue(graph.ramBytesUsed() > graph.offHeapBytesUsed());
    }
  }

  public void testCloseIsIdempotent() {
    OffHeapHnswGraph graph = new OffHeapHnswGraph(10, 10);
    graph.addNode(0, 0);
    graph.close();
    graph.close();
    expectThrows(IllegalStateException.class, () -> graph.seek(0, 0));
  }
}
