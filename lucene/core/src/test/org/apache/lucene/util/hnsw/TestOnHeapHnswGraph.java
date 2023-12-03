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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Test OnHeapHnswGraph's behavior specifically, for more complex test, see {@link
 * HnswGraphTestCase}
 */
public class TestOnHeapHnswGraph extends LuceneTestCase {

  /* assert exception will be thrown when we add out of bound node to a fixed size graph */
  public void testNoGrowth() {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(10, 100);
    expectThrows(IllegalStateException.class, () -> graph.addNode(1, 100));
  }

  /* AssertionError will be thrown if we add a node not from top most level,
  (likely NPE will be thrown in prod) */
  public void testAddLevelOutOfOrder() {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(10, -1);
    graph.addNode(0, 0);
    expectThrows(AssertionError.class, () -> graph.addNode(1, 0));
  }

  /* assert exception will be thrown when we call getNodeOnLevel for an incomplete graph */
  public void testIncompleteGraphThrow() {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(10, -1);
    graph.addNode(1, 0);
    graph.addNode(0, 0);
    assertEquals(1, graph.getNodesOnLevel(1).size());
    graph.addNode(0, 5);
    expectThrows(IllegalStateException.class, () -> graph.getNodesOnLevel(0));
  }

  public void testGraphGrowth() {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(10, -1);
    List<List<Integer>> levelToNodes = new ArrayList<>();
    int maxLevel = 5;
    for (int i = 0; i < maxLevel; i++) {
      levelToNodes.add(new ArrayList<>());
    }
    for (int i = 0; i < 101; i++) {
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
    assertGraphEquals(graph, levelToNodes);
  }

  public void testGraphBuildOutOfOrder() {
    OnHeapHnswGraph graph = new OnHeapHnswGraph(10, -1);
    List<List<Integer>> levelToNodes = new ArrayList<>();
    int maxLevel = 5;
    int numNodes = 100;
    for (int i = 0; i < maxLevel; i++) {
      levelToNodes.add(new ArrayList<>());
    }
    int[] insertions = new int[numNodes];
    for (int i = 0; i < numNodes; i++) {
      insertions[i] = i;
    }
    // shuffle the insertion order
    for (int i = 0; i < 40; i++) {
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

    assertGraphEquals(graph, levelToNodes);
  }

  private static void assertGraphEquals(OnHeapHnswGraph graph, List<List<Integer>> levelToNodes) {
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
