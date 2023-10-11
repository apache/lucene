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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class OnHeapHnswGraph extends HnswGraph implements Accountable {

  private static final int INIT_SIZE = 128;

  private int numLevels; // the current number of levels in the graph
  private int entryNode; // the current graph entry node on the top level. -1 if not set

  // Level 0 is represented as List<NeighborArray> – nodes' connections on level 0.
  // Each entry in the list has the top maxConn/maxConn0 neighbors of a node. The nodes correspond
  // to vectors
  // added to HnswBuilder, and the node values are the ordinals of those vectors.
  // Thus, on all levels, neighbors expressed as the level 0's nodes' ordinals.
  private NeighborArray[][] graph;
  private List<Integer>[] levelToNodes;
  private int lastFreezeSize;
  private int size; // graph size, which is number of nodes in level 0
  private int nonZeroLevelSize; // total number of NeighborArrays created that is not on level 0
  private final int nsize; // neighbour array size at non-zero level
  private final int nsize0; // neighbour array size at zero level

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  OnHeapHnswGraph(int M, int numNodes) {
    this.numLevels = 1; // Implicitly start the graph with a single level
    this.entryNode = -1; // Entry node should be negative until a node is added
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.nsize = M + 1;
    this.nsize0 = (M * 2 + 1);
    if (numNodes == -1) {
      numNodes = INIT_SIZE;
    }
    this.graph = new NeighborArray[numNodes][];
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public NeighborArray getNeighbors(int level, int node) {
    assert graph.length > node && graph[node] != null && graph[node][level] != null;
    return graph[node][level];
  }

  @Override
  public int size() {
    return size;
  }

  /**
   * Add node on the given level. Nodes can be inserted out of order, but it requires that the nodes
   * preceded by the node inserted out of order are eventually added.
   *
   * <p>NOTE: You must add a node from the node's top level
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (entryNode == -1) {
      entryNode = node;
    }

    if (node >= graph.length) {
      graph = ArrayUtil.grow(graph, node + 1);
    }

    if (level >= numLevels) {
      numLevels = level + 1;
      entryNode = node;
    }

    assert graph[node] == null || graph[node].length > level;
    if (graph[node] == null) {
      graph[node] =
          new NeighborArray[level + 1]; // assumption: we always call this function from top level
      size++;
    }
    if (level == 0) {
      graph[node][level] = new NeighborArray(nsize0, true);
    } else {
      graph[node][level] = new NeighborArray(nsize, true);
      nonZeroLevelSize++;
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
      return new ArrayNodesIterator(size());
    } else {
      generateLevelToNodes();
      return new CollectionNodesIterator(levelToNodes[level]);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void generateLevelToNodes() {
    if (lastFreezeSize == size) {
      return;
    }

    levelToNodes = new List[numLevels];
    for (int i = 1; i < numLevels; i++) {
      levelToNodes[i] = new ArrayList<>();
    }
    int nonNullNode = 0;
    for (int node = 0; node < graph.length; node++) {
      // when we init from another graph, we could have holes where some slot is null
      if (graph[node] == null) {
        continue;
      }
      nonNullNode++;
      for (int i = 1; i < graph[node].length; i++) {
        levelToNodes[i].add(node);
      }
      if (nonNullNode == size) {
        break;
      }
    }
    lastFreezeSize = size;
  }

  @Override
  public long ramBytesUsed() {
    long neighborArrayBytes0 =
        (long) nsize0 * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2
            + Integer.BYTES * 3;
    long neighborArrayBytes =
        (long) nsize * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2
            + Integer.BYTES * 3;
    long total = 0;
    total +=
        size * (neighborArrayBytes0 + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER; // for graph and level 0;
    total += nonZeroLevelSize * neighborArrayBytes; // for non-zero level
    total += 8 * Integer.BYTES; // all int fields
    total += RamUsageEstimator.NUM_BYTES_OBJECT_REF; // field: cur
    total += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER; // field: levelToNodes
    if (levelToNodes != null) {
      total +=
          (long) (numLevels - 1) * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // no cost for level 0
      total +=
          (long) nonZeroLevelSize
              * (RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                  + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                  + Integer.BYTES);
    }
    return total;
  }

  @Override
  public String toString() {
    return "OnHeapHnswGraph(size="
        + size()
        + ", numLevels="
        + numLevels
        + ", entryNode="
        + entryNode
        + ")";
  }
}
