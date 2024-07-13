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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestHnswUtil extends LuceneTestCase {

  public void testTreeWithCycle() throws Exception {
    // test a graph that is a tree - this is rooted from its root node, not rooted
    // from any other node, and not strongly connected
    int[][][] nodes = {
      {
        {1, 2}, // node 0
        {3, 4}, // node 1
        {5, 6}, // node 2
        {}, {}, {}, {0}
      }
    };
    HnswGraph graph = new MockGraph(nodes);
    assertTrue(HnswUtil.isRooted(graph));
    assertFalse(HnswUtil.isStronglyConnected(graph));
    assertEquals(List.of(3, 1, 1, 1, 1), HnswUtil.componentSizes(graph));
  }

  public void testBackLinking() throws Exception {
    // test a graph that is a tree - this is rooted from its root node, not rooted
    // from any other node, and not strongly connected
    int[][][] nodes = {
      {
        {1, 2}, // node 0
        {3, 4}, // node 1
        {0}, // node 2
        {1}, {1}, {1}, {1}
      }
    };
    HnswGraph graph = new MockGraph(nodes);
    assertFalse(HnswUtil.isRooted(graph));
    assertFalse(HnswUtil.isStronglyConnected(graph));
    // [ {0, 2}, {1, 3, 4}, {5}, {6}
    assertEquals(List.of(2, 3, 1, 1), HnswUtil.componentSizes(graph));
  }

  public void testChain() throws Exception {
    // test a graph that is a chain - this is rooted from every node, thus strongly connected
    int[][][] nodes = {{{1}, {2}, {3}, {0}}};
    HnswGraph graph = new MockGraph(nodes);
    assertTrue(HnswUtil.isRooted(graph));
    assertTrue(HnswUtil.isStronglyConnected(graph));
    assertEquals(List.of(4), HnswUtil.componentSizes(graph));
  }

  public void testTwoChains() throws Exception {
    // test a graph that is two chains
    int[][][] nodes = {{{2}, {3}, {0}, {1}}};
    HnswGraph graph = new MockGraph(nodes);
    assertFalse(HnswUtil.isRooted(graph));
    assertFalse(HnswUtil.isStronglyConnected(graph));
    assertEquals(List.of(2, 2), HnswUtil.componentSizes(graph));
  }

  public void testLevels() throws Exception {
    // test a graph that has three levels
    int[][][] nodes = {
      {{1, 2}, {3}, {0}, {0}},
      {{2}, {0}},
      {{}}
    };
    HnswGraph graph = new MockGraph(nodes);
    assertTrue(HnswUtil.isRooted(graph));
    assertTrue(HnswUtil.isStronglyConnected(graph));
    assertEquals(List.of(4), HnswUtil.componentSizes(graph));
  }

  public void testRandom() throws Exception {
    for (int i = 0; i < atLeast(10); i++) {
      // test the strongly-connected test on a random directed graph comparing against a brute force
      // algorithm
      int numNodes = random().nextInt(1, 10);
      int[][][] nodes = new int[1][numNodes][];
      for (int node = 0; node < numNodes; node++) {
        int numNbrs = random().nextInt((numNodes + 3) / 4);
        nodes[0][node] = new int[numNbrs];
        for (int nbr = 0; nbr < numNbrs; nbr++) {
          int randomNbr = random().nextInt(numNodes);
          // allow self-linking; this doesn't arise in HNSW but it's valid more generally
          nodes[0][node][nbr] = randomNbr;
        }
      }
      assertEquals(
          isStronglyConnected(nodes[0]), HnswUtil.isStronglyConnected(new MockGraph(nodes)));
    }
  }

  public void testRandomUndirected() throws Exception {
    // test the strongly-connected test on a random undirected graph comparing against a brute force
    // algorithm
    int numNodes = random().nextInt(100);
    List<List<Integer>> nodesList = new ArrayList<>();
    for (int node = 0; node < numNodes; node++) {
      nodesList.add(new ArrayList<>());
    }
    for (int node = 0; node < numNodes; node++) {
      int numNbrs = random().nextInt(numNodes / 10);
      for (int nbr = 0; nbr < numNbrs; nbr++) {
        int randomNbr = random().nextInt(numNodes);
        // we may get duplicate nbrs; it's OK
        nodesList.get(node).add(randomNbr);
        nodesList.get(randomNbr).add(node);
      }
    }
    int[][][] nodesArray = new int[1][numNodes][];
    for (int node = 0; node < numNodes; node++) {
      List<Integer> nbrs = nodesList.get(node);
      nodesArray[0][node] = new int[nbrs.size()];
      for (int i = 0; i < nbrs.size(); i++) {
        nodesArray[0][node][i] = nbrs.get(i);
      }
    }
    assertEquals(
        isStronglyConnected(nodesArray[0]),
        HnswUtil.isStronglyConnected(new MockGraph(nodesArray)));
  }

  private boolean isStronglyConnected(int[][] nodes) {
    // check that the graph is rooted in every node
    for (int entryPoint = 0; entryPoint < nodes.length; entryPoint++) {
      FixedBitSet connected = new FixedBitSet(nodes.length);
      ArrayDeque<Integer> stack = new ArrayDeque<>();
      stack.push(entryPoint);
      int count = 0;
      while (!stack.isEmpty()) {
        int node = stack.pop();
        if (connected.get(node)) {
          continue;
        }
        connected.set(node);
        count++;
        for (int nbr : nodes[node]) {
          stack.push(nbr);
        }
      }
      if (count != nodes.length) {
        return false;
      }
    }
    return true;
  }

  /** Empty graph value */
  static class MockGraph extends HnswGraph {

    private final int[][][] nodes;

    private int currentLevel;
    private int currentNode;
    private int currentNeighbor;

    MockGraph(int[][][] nodes) {
      this.nodes = nodes;
    }

    @Override
    public int nextNeighbor() {
      if (currentNeighbor >= nodes[currentLevel][currentNode].length) {
        return NO_MORE_DOCS;
      } else {
        return nodes[currentLevel][currentNode][currentNeighbor++];
      }
    }

    @Override
    public void seek(int level, int target) {
      assert level >= 0 && level < nodes.length;
      currentLevel = level;
      currentNode = target * nodes[level].length / nodes[0].length;
      assert currentNode >= 0 && currentNode < nodes[level].length
          : "target out of range: " + target;
      currentNeighbor = 0;
    }

    @Override
    public int size() {
      return nodes[0].length;
    }

    @Override
    public int numLevels() {
      return nodes.length;
    }

    @Override
    public int entryNode() {
      return 0;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      int mul = nodes[0].length / nodes[level].length;
      assert mul > 0;
      return new NodesIterator(nodes[level].length) {
        int cur = 0;

        @Override
        public boolean hasNext() {
          return cur < nodes[level].length;
        }

        @Override
        public int nextInt() {
          // level 0 has nodes 0 .. len[0]
          // level 1 has nodes 0 .. len[1] * len[0] / len[1]
          // ...
          return cur++ * mul;
        }

        @Override
        public int consume(int[] dest) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
  ;
}
