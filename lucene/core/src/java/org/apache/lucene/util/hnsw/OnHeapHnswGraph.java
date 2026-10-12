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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class OnHeapHnswGraph extends BuildableHnswGraph {

  // shallow estimate of the statically used on-heap memory.
  private static final long RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(OnHeapHnswGraph.class);

  private static final int INIT_SIZE = 128;

  // the internal graph representation where the first dimension is node id and second dimension is
  // level
  // e.g. graph[1][2] is all the neighbours of node 1 at level 2
  private NeighborArray[][] graph;
  // essentially another 2d map in which the first dimension is level and second dimension is node
  // id, this is only generated on demand when there's someone calling getNodeOnLevel on a
  // non-zero level
  private IntArrayList[] levelToNodes;
  private int
      lastFreezeSize; // remember the size we are at last time to freeze the graph and generate
  // levelToNodes
  private final AtomicInteger size =
      new AtomicInteger(0); // graph size, which is number of nodes in level 0
  private final AtomicInteger maxNodeId = new AtomicInteger(-1);
  private final int nsize; // neighbour array size at non-zero level
  private final int nsize0; // neighbour array size at zero level
  private final boolean
      noGrowth; // if an initial size is passed in, we don't expect the graph to grow itself

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  private final LongAdder graphRamBytesUsed = new LongAdder();

  /**
   * ctor
   *
   * @param numNodes number of nodes that will be added to this graph, passing in -1 means unbounded
   *     while passing in a non-negative value will lock the whole graph and disable the graph from
   *     growing itself (you cannot add a node with id >= numNodes)
   */
  OnHeapHnswGraph(int M, int numNodes) {
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.nsize = M + 1;
    this.nsize0 = (M * 2 + 1);
    noGrowth = numNodes != -1;
    if (noGrowth == false) {
      numNodes = INIT_SIZE;
    }
    this.graph = new NeighborArray[numNodes][];
    this.graphRamBytesUsed.add(RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(graph));
  }

  /**
   * Returns the {@link NeighborArray} connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public NeighborArray getNeighbors(int level, int node) {
    assert node < graph.length;
    assert level < graph[node].length
        : "level="
            + level
            + ", node "
            + node
            + " has only "
            + graph[node].length
            + " levels for graph "
            + this;
    assert graph[node][level] != null : "node=" + node + ", level=" + level;
    return graph[node][level];
  }

  @Override
  public int size() {
    return size.get();
  }

  /**
   * When we initialize from another graph, the max node id is different from {@link #size()},
   * because we will add nodes out of order, such that we need two method for each
   *
   * @return max node id (inclusive)
   */
  @Override
  public int maxNodeId() {
    if (noGrowth) {
      // we know the eventual graph size and the graph can possibly
      // being concurrently modified
      return graph.length - 1;
    } else {
      // The graph cannot be concurrently modified (and searched) if
      // we don't know the size beforehand, so it's safe to return the
      // actual maxNodeId
      return maxNodeId.get();
    }
  }

  @Override
  public int numNeighbors(int level, int node) {
    return getNeighbors(level, node).size();
  }

  @Override
  public int copyNeighbors(int level, int node, int[] dst) {
    NeighborArray neighbors = getNeighbors(level, node);
    int size = neighbors.size();
    System.arraycopy(neighbors.nodes(), 0, dst, 0, size);
    return size;
  }

  @Override
  NeighborArray newUpdateScratch(int level) {
    return null;
  }

  @Override
  NeighborArray beginUpdate(int level, int node, NeighborArray scratch) {
    return getNeighbors(level, node);
  }

  @Override
  void endUpdate(int level, int node, NeighborArray neighbors) {
    // mutated in place
  }

  @Override
  public void addNode(int level, int node) {

    if (node >= graph.length) {
      if (noGrowth) {
        throw new IllegalStateException(
            "The graph does not expect to grow when an initial size is given");
      }
      graph = ArrayUtil.grow(graph, node + 1);
    }

    assert graph[node] == null || graph[node].length >= level
        : "node must be inserted from the top level: ";
    if (graph[node] == null) {
      graph[node] = new NeighborArray[level + 1];
      size.incrementAndGet();
    } else if (graph[node].length <= level) {
      graph[node] = ArrayUtil.growExact(graph[node], level + 1);
    }

    graph[node][level] =
        new NeighborArray(
            level == 0 ? nsize0 : nsize,
            true,
            l -> {
              assert l > 0;
              graphRamBytesUsed.add(l);
            });
    maxNodeId.accumulateAndGet(node, Math::max);
  }

  @Override
  public void seek(int level, int targetNode) {
    cur = getNeighbors(level, targetNode);
    upto = -1;
  }

  @Override
  public int neighborCount() {
    return cur.size();
  }

  @Override
  public int nextNeighbor() {
    if (++upto < cur.size()) {
      return cur.nodes()[upto];
    }
    return NO_MORE_DOCS;
  }

  @Override
  public int maxConn() {
    return nsize - 1;
  }

  @Override
  public boolean nodeExistAtLevel(int level, int node) {
    return graph[node] != null && graph[node].length > level;
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
      return new DenseNodesIterator(size());
    } else {
      generateLevelToNodes();
      return new CollectionNodesIterator(levelToNodes[level]);
    }
  }

  private void generateLevelToNodes() {
    if (lastFreezeSize == size()) {
      return;
    }
    int maxLevels = numLevels();
    levelToNodes = new IntArrayList[maxLevels];
    for (int i = 1; i < maxLevels; i++) {
      levelToNodes[i] = new IntArrayList();
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
      if (nonNullNode == size()) {
        break;
      }
    }
    lastFreezeSize = size();
  }

  /**
   * Provides an estimate of the current on-heap memory usage of the graph. This is not threadsafe,
   * meaning the heap utilization if building the graph concurrently may be inaccurate. The main
   * purpose of this method is during initial document indexing and flush.
   */
  @Override
  public long ramBytesUsed() {
    return graphRamBytesUsed.sum();
  }

  @Override
  public String toString() {
    return "OnHeapHnswGraph(size="
        + size()
        + ", numLevels="
        + numLevels()
        + ", entryNode="
        + entryNode()
        + ")";
  }
}
