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

import static org.apache.lucene.index.TestKnnGraph.assertMaxConn;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

public class TestHNSWGraph2 extends LuceneTestCase {

  // Tests that graph is consistent.
  public void testGraphConsistent() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    MockVectorValues values = new MockVectorValues(createRandomVectors(nDoc, dim, random()));
    int beamWidth = random().nextInt(10) + 5;
    int maxConn = random().nextInt(10) + 5;
    double ml = 1 / Math.log(1.0 * maxConn);
    long seed = random().nextLong();
    VectorSimilarityFunction similarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length - 1) + 1];
    HnswGraphBuilder builder =
        new HnswGraphBuilder(values, similarityFunction, maxConn, beamWidth, ml, seed);
    HnswGraph hnsw = builder.build(values);
    assertConsistentGraph(hnsw, maxConn);
  }

  /**
   * For each level of the graph, test that
   *
   * <p>1. There are no orphan nodes without any friends
   *
   * <p>2. If orphans are found, than the level must contain only 0 or a single node
   *
   * <p>3. If the number of nodes on the level doesn't exceed maxConn, assert that the graph is
   * fully connected, i.e. any node is reachable from any other node.
   *
   * <p>4. If the number of nodes on the level exceeds maxConn, assert that maxConn is respected.
   *
   * <p>copy from TestKnnGraph::assertConsistentGraph with parts relevant only to in-memory graphs
   * TODO: remove when hierarchical graph is implemented on disk
   */
  private static void assertConsistentGraph(HnswGraph hnsw, int maxConn) {
    for (int level = hnsw.maxLevel(); level >= 0; level--) {
      hnsw.seekLevel(level);

      int[][] graph = new int[hnsw.size()][];
      int nodesCount = 0;
      boolean foundOrphan = false;

      for (int node = hnsw.nextNodeOnLevel();
          node != DocIdSetIterator.NO_MORE_DOCS;
          node = hnsw.nextNodeOnLevel()) {
        hnsw.seek(node);
        int arc;
        List<Integer> friends = new ArrayList<>();
        while ((arc = hnsw.nextNeighbor()) != NO_MORE_DOCS) {
          friends.add(arc);
        }
        if (friends.size() == 0) {
          foundOrphan = true;
        } else {
          int[] friendsCopy = new int[friends.size()];
          for (int f = 0; f < friends.size(); f++) {
            friendsCopy[f] = friends.get(f);
          }
          graph[node] = friendsCopy;
        }
        nodesCount++;
      }
      // System.out.println("Level[" + level + "] has [" + nodesCount + "] nodes.");

      assertFalse("No nodes on level [" + level + "]", nodesCount == 0);
      if (nodesCount == 1) {
        assertTrue(
            "Graph with 1 node has unexpected neighbors on level [" + level + "]", foundOrphan);
      } else {
        assertFalse("Graph has orphan nodes with no friends on level [" + level + "]", foundOrphan);
        if (maxConn > nodesCount) {
          // assert that the graph is fully connected,
          // i.e. any node can be reached from any other node
          assertConnected(graph);
        } else {
          // assert that max-connections was respected
          assertMaxConn(graph, maxConn);
        }
      }
    }
  }

  /** Assert that every node is reachable from some other node */
  private static void assertConnected(int[][] graph) {
    List<Integer> nodes = new ArrayList<>();
    Set<Integer> visited = new HashSet<>();
    List<Integer> queue = new LinkedList<>();
    for (int i = 0; i < graph.length; i++) {
      if (graph[i] != null) {
        nodes.add(i);
      }
    }

    // start from any node
    int startIdx = random().nextInt(nodes.size());
    queue.add(nodes.get(startIdx));
    while (queue.isEmpty() == false) {
      int i = queue.remove(0);
      assertNotNull("expected neighbors of " + i, graph[i]);
      visited.add(i);
      for (int j : graph[i]) {
        if (visited.contains(j) == false) {
          queue.add(j);
        }
      }
    }
    // assert that every node is reachable from some other node as it was visited
    for (int node : nodes) {
      assertTrue(
          "Attempted to walk entire graph but never visited node [" + node + "]",
          visited.contains(node));
    }
  }

  private static float[][] createRandomVectors(int size, int dim, Random random) {
    float[][] vectors = new float[size][];
    for (int offset = 0; offset < size; offset++) {
      float[] vec = new float[dim];
      for (int i = 0; i < dim; i++) {
        vec[i] = random.nextFloat();
      }
      VectorUtil.l2normalize(vec);
      vectors[offset] = vec;
    }
    return vectors;
  }
}
