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
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final Map<Integer, Map<Integer, ConcurrentNeighborSet>> graphLevels;
  private final Map<Integer, Integer> completedTime;
  private final AtomicInteger logicalClock;

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
    this.completedTime = new ConcurrentHashMap<>();
    logicalClock = new AtomicInteger(0);
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
  public int size() {
    Map<Integer, ConcurrentNeighborSet> levelZero = graphLevels.get(0);
    return levelZero == null ? 0 : levelZero.size(); // all nodes are located on the 0th level
  }

  @Override
  public void addNode(int level, int node) {
    if (level >= graphLevels.size()) {
      for (int i = graphLevels.size(); i <= level; i++) {
        graphLevels.putIfAbsent(i, new ConcurrentHashMap<>());
      }
    }

    graphLevels.get(level).put(node, new ConcurrentNeighborSet(node, connectionsOnLevel(level)));
  }

  /** must be called after addNode once neighbors are linked in all levels. */
  void markComplete(int level, int node) {
    entryPoint.accumulateAndGet(
        new NodeAtLevel(level, node),
        (oldEntry, newEntry) -> {
          if (oldEntry.node >= 0 && oldEntry.level >= level) {
            return oldEntry;
          } else {
            return newEntry;
          }
        });
    completedTime.put(node, logicalClock.getAndIncrement());
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

  NodeAtLevel entry() {
    return entryPoint.get();
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

    long neighborSetBytes =
        +REF_BYTES // atomicreference
            + Integer.BYTES
            + Integer.BYTES
            + REF_BYTES // NeighborArray
            + AH_BYTES * 2
            + REF_BYTES * 2
            + Integer.BYTES
            + 1; // NeighborArray internals
    long total = 0;

    // the main graph structure
    for (int l = 0; l <= entryPoint.get().level; l++) {
      Map<Integer, ConcurrentNeighborSet> level = graphLevels.get(l);
      if (level == null) {
        continue;
      }

      // size of nodes CHM
      int numNodesOnLevel = graphLevels.get(l).size();
      long chmSize = concurrentHashMapRamUsed(numNodesOnLevel);

      // size of the CNS of each node
      long neighborSize = 0;
      for (ConcurrentNeighborSet cns : graphLevels.get(l).values()) {
        neighborSize += neighborSetBytes + (long) cns.arrayLength() * (Integer.BYTES + Float.BYTES);
      }

      total += chmSize + neighborSize;
    }

    // logical clocks
    total += concurrentHashMapRamUsed(completedTime.size());

    return total;
  }

  private static long concurrentHashMapRamUsed(int externalNodeCount) {
    long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    long CORES = Runtime.getRuntime().availableProcessors();

    long chmNodeBytes =
        REF_BYTES // node itself in Node[]
            + 3L * REF_BYTES
            + Integer.BYTES; // node internals
    float chmLoadFactor = 0.75f; // this is hardcoded inside ConcurrentHashMap
    // CHM has a striped counter Cell implementation, we expect at most one per core
    long chmCounters = AH_BYTES + CORES * (REF_BYTES + Long.BYTES);

    long nodeCount = (long) (externalNodeCount / chmLoadFactor);

    long chmSize =
        nodeCount * chmNodeBytes // nodes
            + nodeCount * REF_BYTES
            + AH_BYTES // nodes array
            + Long.BYTES
            + 3 * Integer.BYTES
            + 3 * REF_BYTES // extra internal fields
            + chmCounters
            + REF_BYTES; // the Map reference itself
    return chmSize;
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

  /**
   * A concurrent View of the graph that is safe to search concurrently with updates and with other
   * searches. The View provides a limited kind of snapshot isolation: only nodes completely added
   * to the graph at the time the View was created will be visible (but the connections between them
   * are allowed to change, so you could potentially get different top K results from the same query
   * if concurrent updates are in progress.)
   */
  private class ConcurrentHnswGraphView extends HnswGraph {
    // It is tempting, but incorrect, to try to provide "adequate" isolation by
    // (1) keeping a bitset of complete nodes and giving that to the searcher as nodes to
    // accept -- but we need to keep incomplete nodes out of the search path entirely,
    // not just out of the result set, or
    // (2) keeping a bitset of complete nodes and restricting the View to those nodes
    // -- but we needs to consider neighbor diversity separately for concurrent
    // inserts and completed nodes; this allows us to keep the former out of the latter,
    // but not the latter out of the former (when a node completes while we are working,
    // that was in-progress when we started.)
    // The only really foolproof solution is to implement snapshot isolation as
    // we have done here.
    private final int timestamp;
    private PrimitiveIterator.OfInt remainingNeighbors;

    public ConcurrentHnswGraphView() {
      this.timestamp = logicalClock.get();
    }

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
      while (remainingNeighbors.hasNext()) {
        int next = remainingNeighbors.nextInt();
        if (completedTime.getOrDefault(next, Integer.MAX_VALUE) < timestamp) {
          return next;
        }
      }
      return NO_MORE_DOCS;
    }

    @Override
    public String toString() {
      return "ConcurrentOnHeapHnswGraphView(size=" + size() + ", entryPoint=" + entryPoint.get();
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
