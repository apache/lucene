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

package org.apache.lucene.backward_codecs.lucene91;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class Lucene91OnHeapHnswGraph extends HnswGraph {

  private final int maxConn;
  private int numLevels; // the current number of levels in the graph
  private int entryNode; // the current graph entry node on the top level

  // Nodes by level expressed as the level 0's nodes' ordinals.
  // As level 0 contains all nodes, nodesByLevel.get(0) is null.
  private final List<int[]> nodesByLevel;

  // graph is a list of graph levels.
  // Each level is represented as List<NeighborArray> – nodes' connections on this level.
  // Each entry in the list has the top maxConn neighbors of a node. The nodes correspond to vectors
  // added to HnswBuilder, and the node values are the ordinals of those vectors.
  // Thus, on all levels, neighbors expressed as the level 0's nodes' ordinals.
  private final List<List<Lucene91NeighborArray>> graph;

  // KnnGraphValues iterator members
  private int upto;
  private Lucene91NeighborArray cur;

  Lucene91OnHeapHnswGraph(int maxConn, int levelOfFirstNode) {
    this.maxConn = maxConn;
    this.numLevels = levelOfFirstNode + 1;
    this.graph = new ArrayList<>(numLevels);
    this.entryNode = 0;
    for (int i = 0; i < numLevels; i++) {
      graph.add(new ArrayList<>());
      // Typically with diversity criteria we see nodes not fully occupied;
      // average fanout seems to be about 1/2 maxConn.
      // There is some indexing time penalty for under-allocating, but saves RAM
      graph.get(i).add(new Lucene91NeighborArray(Math.max(32, maxConn / 4)));
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
  public Lucene91NeighborArray getNeighbors(int level, int node) {
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

    graph.get(level).add(new Lucene91NeighborArray(maxConn + 1));
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
      return new ArrayNodesIterator(size());
    } else {
      return new ArrayNodesIterator(nodesByLevel.get(level), graph.get(level).size());
    }
  }
}
