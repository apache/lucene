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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** validates that all nodes can reach every other node on the same level */
public class HnswGraphValidator {
  private final HnswGraph hnsw;

  public HnswGraphValidator(HnswGraph hnsw) {
    this.hnsw =
        hnsw instanceof ConcurrentOnHeapHnswGraph
            ? ((ConcurrentOnHeapHnswGraph) hnsw).getView()
            : hnsw;
  }

  public void validateReachability() throws IOException {
    for (int level = 0; level < hnsw.numLevels(); level++) {
      validateReachability(level);
    }
  }

  private void validateReachability(int level) throws IOException {
    Set<Integer> nodes = getAllNodes(level);
    for (Integer node : nodes) {
      validateNodeCanReachOthers(node, level, new HashSet<>(nodes));
    }
  }

  private Set<Integer> getAllNodes(int level) throws IOException {
    HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level);
    Set<Integer> nodes = new HashSet<>();
    while (nodesIterator.hasNext()) {
      nodes.add(nodesIterator.nextInt());
    }
    return nodes;
  }

  private void validateNodeCanReachOthers(Integer startNode, int level, Set<Integer> remaining)
      throws IOException {
    dfs(startNode, level, remaining);
    assert remaining.isEmpty()
        : "Node "
            + startNode
            + " cannot reach "
            + remaining
            + " on level "
            + level
            + " in "
            + ConcurrentHnswGraphTestCase.prettyPrint(hnsw);
  }

  private void dfs(Integer node, int level, Set<Integer> remaining) throws IOException {
    remaining.remove(node);
    List<Integer> neighbors = getAllNeighbors(node, level);
    for (int neighbor : neighbors) {
      if (remaining.contains(neighbor)) {
        dfs(neighbor, level, remaining);
      }
    }
  }

  // materialize getAllNeighbors before we seek anywhere else
  private List<Integer> getAllNeighbors(int node, int level) throws IOException {
    List<Integer> neighbors = new ArrayList<>();
    hnsw.seek(level, node);
    for (int neighbor = hnsw.nextNeighbor();
        neighbor != NO_MORE_DOCS;
        neighbor = hnsw.nextNeighbor()) {
      neighbors.add(neighbor);
    }
    return neighbors;
  }
}
