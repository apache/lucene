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
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} that offers concurrent access; for typical graphs you will get significant
 * speedups in construction and searching as you add threads.
 *
 * <p>To search this graph, you should use a View obtained from {@link #getView()} to perform `seek`
 * and `nextNeighbor` operations. For convenience, you can use these methods directly on the graph
 * instance, which will give you a ThreadLocal View, but you can call `getView` directly if you need
 * more control, e.g. for performing a second search in the same thread while the first is still in
 * progress.
 */
public final class ConcurrentOnHeapHnswGraph extends HnswGraph implements Accountable {
  private final AtomicReference<NodeAtLevel>
      entryPoint; // the current graph entry node on the top level. -1 if not set

  // views for compatibility with HnswGraph interface; prefer creating views explicitly
  private final ThreadLocal<ConcurrentHnswGraphView> views =
      ThreadLocal.withInitial(ConcurrentHnswGraphView::new);

  // Unlike OnHeapHnswGraph (OHHG), we use the same data structure for Level 0 and higher node
  // lists,
  // a ConcurrentHashMap.  While the ArrayList used for L0 in OHHG is faster for single-threaded
  // workloads, it imposes an unacceptable contention burden for concurrent workloads.
  private final ConcurrentMap<Integer, ConcurrentNeighborSet> graphLevel0;
  private final ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentNeighborSet>>
      graphUpperLevels;

  // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
  private final int nsize;
  private final int nsize0;

  ConcurrentOnHeapHnswGraph(int M) {
    this.graphLevel0 = new ConcurrentHashMap<>();
    this.entryPoint =
        new AtomicReference<>(
            new NodeAtLevel(0, -1)); // Entry node should be negative until a node is added
    this.nsize = M;
    this.nsize0 = 2 * M;

    this.graphUpperLevels = new ConcurrentHashMap<>();
  }

  /**
   * Returns the neighbors connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public ConcurrentNeighborSet getNeighbors(int level, int node) {
    if (level == 0) return graphLevel0.get(node);
    return graphUpperLevels.get(level).get(node);
  }

  @Override
  public synchronized int size() {
    return graphLevel0.size(); // all nodes are located on the 0th level
  }

  /**
   * Add node on the given level with an empty set of neighbors.
   *
   * <p>Nodes can be inserted out of order, but it requires that the nodes preceded by the node
   * inserted out of order are eventually added.
   *
   * <p>Actually populating the neighbors, and establishing bidirectional links, is the
   * responsibility of the caller.
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (level > 0) {
      if (level >= graphUpperLevels.size()) {
        for (int i = graphUpperLevels.size(); i <= level; i++) {
          graphUpperLevels.putIfAbsent(i, new ConcurrentHashMap<>());
        }
      }

      graphUpperLevels.get(level).put(node, new ConcurrentNeighborSet(connectionsOnLevel(level)));
    } else {
      // Add nodes all the way up to and including "node" in the new graph on level 0. This will
      // cause the size of the
      // graph to differ from the number of nodes added to the graph. The size of the graph and the
      // number of nodes
      // added will only be in sync once all nodes from 0...last_node are added into the graph.
      if (node >= graphLevel0.size()) {
        for (int i = graphLevel0.size(); i <= node; i++) {
          graphLevel0.putIfAbsent(i, new ConcurrentNeighborSet(nsize0));
        }
      }
    }
  }

  /**
   * must be called after addNode to a level > 0
   *
   * <p>we don't do this as part of addNode itself, since it may not yet have been added to all the
   * levels
   */
  void maybeUpdateEntryNode(int level, int node) {
    while (true) {
      var oldEntry = entryPoint.get();
      if (oldEntry.node >= 0 && oldEntry.level >= level) {
        break;
      }
      entryPoint.compareAndSet(oldEntry, new NodeAtLevel(level, node));
    }
  }

  private int connectionsOnLevel(int level) {
    return level == 0 ? nsize0 : nsize;
  }

  @Override
  public void seek(int level, int target) throws IOException {
    views.get().seek(level, target);
  }

  @Override
  public int nextNeighbor() throws IOException {
    return views.get().nextNeighbor();
  }

  /**
   * @return the current number of levels in the graph where nodes have been added and we have a
   *     valid entry point.
   */
  @Override
  public int numLevels() {
    return entryPoint.get().level + 1;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public int entryNode() {
    return entryPoint.get().node;
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) {
    if (level == 0) {
      return new ArrayNodesIterator(size());
    } else {
      return new CollectionNodesIterator(graphUpperLevels.get(level).keySet());
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
    for (int l = 0; l <= entryPoint.get().level; l++) {
      if (l == 0) {
        total +=
            size() * neighborArrayBytes0 + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // for graph;
      } else {
        long numNodesOnLevel = graphUpperLevels.get(l).size();

        // For levels > 0, we represent the graph structure with a tree map.
        // A single node in the tree contains 3 references (left root, right root, value) as well
        // as an Integer for the key and 1 extra byte for the color of the node (this is actually 1
        // bit, but
        // because we do not have that granularity, we set to 1 byte). In addition, we include 1
        // more reference for
        // the tree map itself.
        total +=
            numNodesOnLevel * (3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES + 1)
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF;

        // Add the size neighbor of each node
        total += numNodesOnLevel * neighborArrayBytes;
      }
    }
    return total;
  }

  @Override
  public String toString() {
    return "ConcurrentOnHeapHnswGraph(size=%d, entryPoint=%s)".formatted(size(), entryPoint.get());
  }

  /**
   * Returns a view of the graph that is safe to use concurrently with updates performed on the
   * underlying graph.
   *
   * <p>Multiple Views may be searched concurrently.
   */
  public HnswGraph getView() {
    return new ConcurrentHnswGraphView();
  }

  private class ConcurrentHnswGraphView extends HnswGraph {
    private Iterator<Integer> remainingNeighbors;

    @Override
    public int size() {
      return ConcurrentOnHeapHnswGraph.this.size();
    }

    @Override
    public int numLevels() {
      return ConcurrentOnHeapHnswGraph.this.numLevels();
    }

    @Override
    public int entryNode() {
      return ConcurrentOnHeapHnswGraph.this.entryNode();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      return ConcurrentOnHeapHnswGraph.this.getNodesOnLevel(level);
    }

    @Override
    public void seek(int level, int targetNode) {
      remainingNeighbors = getNeighbors(level, targetNode).nodeIterator();
    }

    @Override
    public int nextNeighbor() {
      return remainingNeighbors.hasNext() ? remainingNeighbors.next() : NO_MORE_DOCS;
    }
  }

  static final class NodeAtLevel implements Comparable<NodeAtLevel> {
    public final int level;
    public final int node;

    public NodeAtLevel(int level, int node) {
      this.level = level;
      this.node = node;
    }

    @Override
    public int compareTo(NodeAtLevel o) {
      int cmp = Integer.compare(level, o.level);
      if (cmp == 0) {
        cmp = Integer.compare(node, o.node);
      }
      return cmp;
    }

    @Override
    public String toString() {
      return "NodeAtLevel [level=%d, node=%d]".formatted(level, node);
    }
  }
}
