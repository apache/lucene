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

  // the internal graph representation where the first dimension is node id and second dimension is
  // level
  // e.g. graph[1][2] is all the neighbours of node 1 at level 2
  private NeighborArray[][] graph;
  // essentially another 2d map which the first dimension is level and second dimension is node id,
  // this is only
  // generated on demand when there's someone calling getNodeOnLevel on a non-zero level
  private List<Integer>[] levelToNodes;
  private int
      lastFreezeSize; // remember the size we are at last time to freeze the graph and generate
  // levelToNodes
  private int size; // graph size, which is number of nodes in level 0
  private int
      nonZeroLevelSize; // total number of NeighborArrays created that is not on level 0, for now it
  // is only used to account memory usage
  private int maxNodeId;
  private final int nsize; // neighbour array size at non-zero level
  private final int nsize0; // neighbour array size at zero level
  private final boolean
      noGrowth; // if an initial size is passed in, we don't expect the graph to grow itself

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  /**
   * ctor
   *
   * @param numNodes number of nodes that will be added to this graph, passing in -1 means unbounded
   *     while passing in a non-negative value will lock the whole graph and disable the graph from
   *     growing itself (you cannot add a node with has id >= numNodes)
   */
  OnHeapHnswGraph(int M, int numNodes) {
    this.numLevels = 1; // Implicitly start the graph with a single level
    this.entryNode = -1; // Entry node should be negative until a node is added
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.maxNodeId = -1;
    this.nsize = M + 1;
    this.nsize0 = (M * 2 + 1);
    noGrowth = numNodes != -1;
    if (noGrowth == false) {
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
    assert graph[node][level] != null;
    return graph[node][level];
  }

  @Override
  public int size() {
    return size;
  }

  /**
   * When we initialize from another graph, the max node id is different from {@link #size()},
   * because we will add nodes out of order, such that we need two method for each
   *
   * @return max node id (inclusive)
   */
  @Override
  public int maxNodeId() {
    return maxNodeId;
  }

  /**
   * Add node on the given level. Nodes can be inserted out of order, but it requires that the nodes
   * preceded by the node inserted out of order are eventually added.
   *
   * <p>NOTE: You must add a node starting from the node's top level
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (entryNode == -1) {
      entryNode = node;
    }

    if (node >= graph.length) {
      if (noGrowth) {
        throw new IllegalStateException(
            "The graph does not expect to grow when an initial size is given");
      }
      graph = ArrayUtil.grow(graph, node + 1);
    }

    if (level >= numLevels) {
      numLevels = level + 1;
      entryNode = node;
    }

    assert graph[node] == null || graph[node].length > level
        : "node must be inserted from the top level";
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
    maxNodeId = Math.max(maxNodeId, node);
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

  /**
   * WARN: calling this method will essentially iterate through all nodes at level 0 (even if you're
   * not getting node at level 0), we have built some caching mechanism such that if graph is not
   * changed only the first non-zero level call will pay the cost. So it is highly NOT recommended
   * to call this method while the graph is still building.
   *
   * <p>NOTE: calling this method while the graph is still building is prohibited
   */
  @Override
  public NodesIterator getNodesOnLevel(int level) {
    if (size() != maxNodeId() + 1) {
      throw new IllegalStateException(
          "graph build not complete, size=" + size() + " maxNodeId=" + maxNodeId());
    }
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
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2L
            + Integer.BYTES * 3;
    long neighborArrayBytes =
        (long) nsize * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2L
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
