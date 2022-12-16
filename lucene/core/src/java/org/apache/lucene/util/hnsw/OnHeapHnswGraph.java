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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class OnHeapHnswGraph extends HnswGraph implements Accountable {

  private int numLevels; // the current number of levels in the graph
  private int entryNode; // the current graph entry node on the top level. -1 if not set

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
  // Keep track of the last added nodes in the graph for insertion optimization
  private final List<Integer> lastAddedPosInLayer;

  OnHeapHnswGraph(int M) {
    this.numLevels = 1; // Implicitly start the graph with a single level
    this.graph = new ArrayList<>(Collections.singleton(new ArrayList<>()));
    this.entryNode = -1; // Entry node should be negative until a node is added
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.nsize = M + 1;
    this.nsize0 = (M * 2 + 1);

    this.nodesByLevel = new ArrayList<>(numLevels);
    nodesByLevel.add(null); // we don't need this for 0th level, as it contains all nodes

    this.lastAddedPosInLayer = new ArrayList<>(numLevels);
    lastAddedPosInLayer.add(null);
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
   * Add node on the given level. Nodes can be inserted out of order, but it requires that the nodes
   * preceded by the node inserted out of order are eventually added.
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (entryNode == -1) {
      entryNode = node;
    }

    if (level > 0) {
      // if the new node introduces a new level, add more levels to the graph,
      // and make this node the graph's new entry point
      if (level >= numLevels) {
        for (int i = numLevels; i < level; i++) {
          graph.add(new ArrayList<>());
          nodesByLevel.add(new int[] {});
          lastAddedPosInLayer.add(-1);
        }
        nodesByLevel.add(new int[] {node});
        numLevels = level + 1;
        entryNode = node;
        lastAddedPosInLayer.add(0);
        graph.add(new ArrayList<>(Collections.singleton(new NeighborArray(nsize, true))));
      } else {
        // Add this node id to this level's nodes
        int[] nodes = nodesByLevel.get(level);
        int idx = graph.get(level).size();
        if (idx >= nodes.length) {
          nodes = ArrayUtil.grow(nodes);
          nodesByLevel.set(level, nodes);
        }

        // Find what position in the nodes array to insert the new node to ensure it stays sorted.
        // In the worst case, we need to perform a binary search to find the position to insert the
        // node.
        // However, we can avoid binary search in 2 common cases:
        // (1) When the node belongs at the end of the array
        // (2) When the node belongs after the position of the last inserted node
        int position;
        int lastPositionInsertedInLevel = lastAddedPosInLayer.get(level);

        if (lastPositionInsertedInLevel == -1) {
          position = 0;
        } else if (lastPositionInsertedInLevel == idx - 1 && node > nodes[idx - 1]) {
          position = idx;
        } else if (lastPositionInsertedInLevel < idx - 1
            && node > nodes[lastPositionInsertedInLevel]
            && node < nodes[lastPositionInsertedInLevel + 1]) {
          position = lastPositionInsertedInLevel + 1;
        } else {
          position = Arrays.binarySearch(nodes, 0, idx, node);
          assert position < 0;
          position = -1 * position - 1;
        }

        if (position == idx) {
          graph.get(level).add(new NeighborArray(nsize, true));
        } else {
          System.arraycopy(nodes, position, nodes, position + 1, (idx - position));
          graph.get(level).add(position, new NeighborArray(nsize, true));
        }
        nodes[position] = node;
        lastAddedPosInLayer.set(level, position);
      }
    } else {
      // Add nodes all the way up to and including "node" in the new graph on level 0. This will
      // cause the size of the
      // graph to differ from the number of nodes added to the graph. The size of the graph and the
      // number of nodes
      // added will only be in sync once all nodes from 0...last_node are added into the graph.
      List<NeighborArray> level0Neighbors = graph.get(level);
      while (node >= level0Neighbors.size()) {
        level0Neighbors.add(new NeighborArray(nsize0, true));
      }
    }
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

  @Override
  public long ramBytesUsed() {
    long neighborArrayBytes0 =
        nsize0 * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER * 2
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            + Integer.BYTES * 2;
    long neighborArrayBytes =
        nsize * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER * 2
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            + Integer.BYTES * 2;
    long total = 0;
    for (int l = 0; l < numLevels; l++) {
      int numNodesOnLevel = graph.get(l).size();
      if (l == 0) {
        total +=
            numNodesOnLevel * neighborArrayBytes0
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // for graph;
      } else {
        total +=
            nodesByLevel.get(l).length * Integer.BYTES
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // for nodesByLevel
        total +=
            numNodesOnLevel * neighborArrayBytes
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // for graph;
      }
    }
    return total;
  }
}
