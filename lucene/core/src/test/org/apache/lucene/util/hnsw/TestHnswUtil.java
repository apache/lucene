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
import java.util.Arrays;
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
    assertEquals(List.of(7), HnswUtil.componentSizes(graph));
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
    // [ {0, 1, 2, 3, 4}, {5}, {6}
    assertEquals(List.of(5, 1, 1), HnswUtil.componentSizes(graph));
  }

  public void testChain() throws Exception {
    // test a graph that is a chain - this is rooted from every node, thus strongly connected
    int[][][] nodes = {{{1}, {2}, {3}, {0}}};
    HnswGraph graph = new MockGraph(nodes);
    assertTrue(HnswUtil.isRooted(graph));
    assertEquals(List.of(4), HnswUtil.componentSizes(graph));
  }

  public void testTwoChains() throws Exception {
    // test a graph that is two chains
    int[][][] nodes = {{{2}, {3}, {0}, {1}}};
    HnswGraph graph = new MockGraph(nodes);
    assertFalse(HnswUtil.isRooted(graph));
    assertEquals(List.of(2, 2), HnswUtil.componentSizes(graph));
  }

  public void testLevels() throws Exception {
    // test a graph that has three levels
    int[][][] nodes = {
      {{1, 2}, {3}, {0}, {0}},
      {{2}, null, {0}, null},
      {{}, null, null, null}
    };
    HnswGraph graph = new MockGraph(nodes);
    // System.out.println(graph.toString());
    assertTrue(HnswUtil.isRooted(graph));
    assertEquals(List.of(4), HnswUtil.componentSizes(graph));
  }

  public void testLevelsNotRooted() throws Exception {
    // test a graph that has two levels with an orphaned node
    int[][][] nodes = {
      {{1}, {0}, {0}},
      {{}, null, null}
    };
    HnswGraph graph = new MockGraph(nodes);
    assertFalse(HnswUtil.isRooted(graph));
    assertEquals(List.of(2, 1), HnswUtil.componentSizes(graph));
  }

  public void testRandom() throws Exception {
    for (int i = 0; i < atLeast(10); i++) {
      // test on a random directed graph comparing against a brute force algorithm
      int numNodes = random().nextInt(1, 100);
      int numLevels = (int) Math.ceil(Math.log(numNodes));
      int[][][] nodes = new int[numLevels][][];
      for (int level = numLevels - 1; level >= 0; level--) {
        nodes[level] = new int[numNodes][];
        for (int node = 0; node < numNodes; node++) {
          if (level > 0) {
            if ((level == numLevels - 1 && node > 0)
                || (level < numLevels - 1 && nodes[level + 1][node] == null)) {
              if (random().nextFloat() > Math.pow(Math.E, -level)) {
                // skip some nodes, more on higher levels while ensuring every node present on a
                // given level is present on all lower levels. Also ensure node 0 is always present.
                continue;
              }
            }
          }
          int numNbrs = random().nextInt((numNodes + 7) / 8);
          if (level == 0) {
            numNbrs *= 2;
          }
          nodes[level][node] = new int[numNbrs];
          for (int nbr = 0; nbr < numNbrs; nbr++) {
            while (true) {
              int randomNbr = random().nextInt(numNodes);
              if (nodes[level][randomNbr] != null) {
                // allow self-linking; this doesn't arise in HNSW but it's valid more generally
                nodes[level][node][nbr] = randomNbr;
                break;
              }
              // nbr not on this level, try again
            }
          }
        }
      }
      MockGraph graph = new MockGraph(nodes);
      assertEquals(isRooted(nodes), HnswUtil.isRooted(graph));
    }
  }

  private boolean isRooted(int[][][] nodes) {
    for (int level = nodes.length - 1; level >= 0; level--) {
      if (isRooted(nodes, level) == false) {
        return false;
      }
    }
    return true;
  }

  private boolean isRooted(int[][][] nodes, int level) {
    // check that the graph is rooted in the union of the entry nodes' trees
    // System.out.println("isRooted level=" + level);
    int[][] entryPoints;
    if (level == nodes.length - 1) {
      // entry into the top level is from a single entry point, fixed at 0
      entryPoints = new int[][] {nodes[level][0]};
    } else {
      entryPoints = nodes[level + 1];
    }
    FixedBitSet connected = new FixedBitSet(nodes[level].length);
    int count = 0;
    for (int entryPoint = 0; entryPoint < entryPoints.length; entryPoint++) {
      if (entryPoints[entryPoint] == null) {
        // use nodes present on next higher level (or this level if top level) as entry points
        continue;
      }
      // System.out.println("  isRooted level=" + level + " entryPoint=" + entryPoint);
      ArrayDeque<Integer> stack = new ArrayDeque<>();
      stack.push(entryPoint);
      while (!stack.isEmpty()) {
        int node = stack.pop();
        if (connected.get(node)) {
          continue;
        }
        // System.out.println("    connected node=" + node);
        connected.set(node);
        count++;
        for (int nbr : nodes[level][node]) {
          stack.push(nbr);
        }
      }
    }
    return count == levelSize(nodes[level]);
  }

  static int levelSize(int[][] nodes) {
    int count = 0;
    for (int[] node : nodes) {
      if (node != null) {
        ++count;
      }
    }
    return count;
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
      assert target >= 0 && target < nodes[level].length
          : "target out of range: "
              + target
              + " for level "
              + level
              + "; should be less than "
              + nodes[level].length;
      assert nodes[level][target] != null : "target " + target + " not on level " + level;
      currentLevel = level;
      currentNode = target;
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
    public String toString() {
      StringBuilder buf = new StringBuilder();
      for (int level = nodes.length - 1; level >= 0; level--) {
        buf.append("\nLEVEL ").append(level).append("\n");
        for (int node = 0; node < nodes[level].length; node++) {
          if (nodes[level][node] != null) {
            buf.append("  ")
                .append(node)
                .append(':')
                .append(Arrays.toString(nodes[level][node]))
                .append("\n");
          }
        }
      }
      return buf.toString();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {

      int count = 0;
      for (int i = 0; i < nodes[level].length; i++) {
        if (nodes[level][i] != null) {
          count++;
        }
      }
      final int finalCount = count;

      return new NodesIterator(finalCount) {
        int cur = -1;
        int curCount = 0;

        @Override
        public boolean hasNext() {
          return curCount < finalCount;
        }

        @Override
        public int nextInt() {
          while (curCount < finalCount) {
            if (nodes[level][++cur] != null) {
              curCount++;
              return cur;
            }
          }
          throw new IllegalStateException("exhausted");
        }

        @Override
        public int consume(int[] dest) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
