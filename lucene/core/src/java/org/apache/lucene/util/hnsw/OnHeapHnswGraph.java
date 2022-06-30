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
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.ArrayUtil;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class OnHeapHnswGraph extends HnswGraph {

  private int numLevels; // the current number of levels in the graph
  private int entryNode; // the current graph entry node on the top level

  // Nodes by level expressed as the level 0's nodes' ordinals.
  // As level 0 contains all nodes, nodesByLevel.get(0) is null.
  private final List<int[]> nodesByLevel;

  // graph is a list of graph levels.
  // Each level is represented as List<NeighborArray> – nodes' connections on this level.
  // Each entry in the list has the top maxConn/maxConn0 neighbors of a node. The nodes correspond
  // to vectors
  // added to HnswBuilder, and the node values are the ordinals of those vectors.
  // Thus, on all levels, neighbors expressed as the level 0's nodes' ordinals.
  private final List<List<NeighborArray>> graph;
  private final int nsize;
  private final int nsize0;

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  OnHeapHnswGraph(int M, int levelOfFirstNode) {
    this.numLevels = levelOfFirstNode + 1;
    this.graph = new ArrayList<>(numLevels);
    this.entryNode = 0;
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.nsize = M + 1;
    this.nsize0 = (M * 2 + 1);
    for (int l = 0; l < numLevels; l++) {
      graph.add(new ArrayList<>());
      graph.get(l).add(new NeighborArray(l == 0 ? nsize0 : nsize, true));
    }

    this.nodesByLevel = new ArrayList<>(numLevels);
    nodesByLevel.add(null); // we don't need this for 0th level, as it contains all nodes
    for (int l = 1; l < numLevels; l++) {
      nodesByLevel.add(new int[] {0});
    }
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public NeighborArray getNeighbors(int level, int node) {
    if (level == 0) {
      return graph.get(level).get(node);
    }
    int nodeIndex = Arrays.binarySearch(nodesByLevel.get(level), 0, graph.get(level).size(), node);
    assert nodeIndex >= 0;
    return graph.get(level).get(nodeIndex);
  }

  @Override
  public int size() {
    return graph.get(0).size(); // all nodes are located on the 0th level
  }

  /**
   * Add node on the given level
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (level > 0) {
      // if the new node introduces a new level, add more levels to the graph,
      // and make this node the graph's new entry point
      if (level >= numLevels) {
        for (int i = numLevels; i <= level; i++) {
          graph.add(new ArrayList<>());
          nodesByLevel.add(new int[] {node});
        }
        numLevels = level + 1;
        entryNode = node;
      } else {
        // Add this node id to this level's nodes
        int[] nodes = nodesByLevel.get(level);
        int idx = graph.get(level).size();
        if (idx < nodes.length) {
          nodes[idx] = node;
        } else {
          nodes = ArrayUtil.grow(nodes);
          nodes[idx] = node;
          nodesByLevel.set(level, nodes);
        }
      }
    }
    graph.get(level).add(new NeighborArray(level == 0 ? nsize0 : nsize, true));
  }

  @Override
  public void seek(int level, int targetNode) {
    cur = getNeighbors(level, targetNode);
    upto = -1;
  }

  @Override
  public int nextNeighbor() {
    if (++upto < cur.size()) {
      return cur.node[upto];
    }
    return NO_MORE_DOCS;
  }

  /**
   * Returns the current number of levels in the graph
   *
   * @return the current number of levels in the graph
   */
  @Override
  public int numLevels() {
    return numLevels;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public int entryNode() {
    return entryNode;
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) {
    if (level == 0) {
      return new NodesIterator(size());
    } else {
      return new NodesIterator(nodesByLevel.get(level), graph.get(level).size());
    }
  }
}
