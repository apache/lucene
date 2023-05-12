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
 * and `nextNeighbor` operations.
 */
public final class ConcurrentOnHeapHnswGraph extends HnswGraph implements Accountable {
  private final AtomicReference<NodeAtLevel>
      entryPoint; // the current graph entry node on the top level. -1 if not set

  // Unlike OnHeapHnswGraph (OHHG), we use the same data structure for Level 0 and higher node
  // lists,
  // a ConcurrentHashMap.  While the ArrayList used for L0 in OHHG is faster for single-threaded
  // workloads, it imposes an unacceptable contention burden for concurrent workloads.
  private final ConcurrentMap<Integer, ConcurrentMap<Integer, ConcurrentNeighborSet>> graphLevels;

  // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
  private final int nsize;
  private final int nsize0;

  ConcurrentOnHeapHnswGraph(int M) {
    this.entryPoint =
        new AtomicReference<>(
            new NodeAtLevel(0, -1)); // Entry node should be negative until a node is added
    this.nsize = M;
    this.nsize0 = 2 * M;

    this.graphLevels = new ConcurrentHashMap<>();
  }

  /**
   * Returns the neighbors connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public ConcurrentNeighborSet getNeighbors(int level, int node) {
    return graphLevels.get(level).get(node);
  }

  @Override
  public synchronized int size() {
    return graphLevels.get(0).size(); // all nodes are located on the 0th level
  }

  @Override
  public void addNode(int level, int node) {
    if (level >= graphLevels.size()) {
      for (int i = graphLevels.size(); i <= level; i++) {
        graphLevels.putIfAbsent(i, new ConcurrentHashMap<>());
      }
    }

    graphLevels.get(level).put(node, new ConcurrentNeighborSet(connectionsOnLevel(level)));
  }

  /**
   * must be called after addNode to a level > 0
   *
   * <p>we don't do this as part of addNode itself, since it may not yet have been added to all the
   * levels
   */
  void maybeUpdateEntryNode(int level, int node) {
    entryPoint.accumulateAndGet(
        new NodeAtLevel(level, node),
        (oldEntry, newEntry) -> {
          if (oldEntry.node >= 0 && oldEntry.level >= level) {
            return oldEntry;
          } else {
            return newEntry;
          }
        });
  }

  private int connectionsOnLevel(int level) {
    return level == 0 ? nsize0 : nsize;
  }

  @Override
  public void seek(int level, int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextNeighbor() throws IOException {
    throw new UnsupportedOperationException();
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
      return new CollectionNodesIterator(graphLevels.get(level).keySet());
    }
  }

  @Override
  public long ramBytesUsed() {
    // local vars here just to make it easier to keep lines short enough to read
    long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    long CORES = Runtime.getRuntime().availableProcessors();

    // skip list used by Neighbor Set
    long cskmNodesBytes = 3L * REF_BYTES; // K, V, index
    long cskmIndexBytes = 3L * REF_BYTES; // node, down, right
    long cskmBytes =
        REF_BYTES // head
            + AH_BYTES
            + CORES * Long.BYTES // longadder cells
            + 4L * REF_BYTES; // internal view refs
    long neighborSetBytes =
        cskmBytes
            + REF_BYTES // skiplist -> map reference
            + Integer.BYTES
            + Integer.BYTES
            + REF_BYTES
            + Integer.BYTES; // CNS fields
    // a CHM Node contains an int hash and a Node reference, as well as K and V references.
    long chmNodeBytes = 3L * REF_BYTES + Integer.BYTES;
    float chmLoadFactor = 0.75f; // this is hardcoded inside ConcurrentHashMap
    // CHM has a striped counter Cell implementation, we expect at most one per core
    long chmCounters = AH_BYTES + CORES * (REF_BYTES + Long.BYTES);

    long total = 0;
    for (int l = 0; l <= entryPoint.get().level; l++) {
      long numNodesOnLevel = graphLevels.get(l).size();

      // we represent the graph structure with a concurrent hash map.
      // we expect there to be nodesOnLevel / levelLoadFactor Nodes in its internal table.
      // there is also an entrySet reference, 3 ints, and a float for internal use.
      int nodeCount = (int) (numNodesOnLevel / chmLoadFactor);
      long chmSize =
          nodeCount * chmNodeBytes // nodes
              + nodeCount * REF_BYTES
              + AH_BYTES // nodes array
              + Long.BYTES
              + 3 * Integer.BYTES
              + 3 * REF_BYTES // extra internal fields
              + chmCounters
              + REF_BYTES; // the Map reference itself

      // Add the size neighbor of each node
      long neighborSize = 0;
      for (ConcurrentNeighborSet cns : graphLevels.get(l).values()) {
        neighborSize += neighborSetBytes + cns.size() * (cskmNodesBytes + cskmIndexBytes);
      }

      total += chmSize + neighborSize;
    }
    return total;
  }

  @Override
  public String toString() {
    return "ConcurrentOnHeapHnswGraph(size=" + size() + ", entryPoint=" + entryPoint.get();
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

  void validateEntryNode() {
    if (size() == 0) {
      return;
    }
    var en = entryPoint.get();
    if (!(en.level >= 0 && en.node >= 0 && graphLevels.get(en.level).containsKey(en.node))) {
      throw new IllegalStateException("Entry node was incompletely added! " + en);
    }
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
      return "NodeAtLevel(level=" + level + ", node=" + node + ")";
    }
  }
}
