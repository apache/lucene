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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.Accountable;

/**
 * An {@link HnswGraph} that is mutated while it is being built by an {@link HnswGraphBuilder}. The
 * neighbor lists either live on the java heap ({@link OnHeapHnswGraph}) or in native memory ({@link
 * OffHeapHnswGraph}).
 *
 * <p>Neighbor lists are mutated through {@code beginUpdate} / {@code endUpdate}: the on-heap graph
 * hands out its live {@link NeighborArray}, while the off-heap graph loads the slot into a
 * caller-owned scratch array and writes it back on {@code endUpdate}. Callers must hold the same
 * locks across the pair that they would hold while mutating a live array.
 *
 * <p>The graph must be {@link #close() closed} once it is no longer needed to release any native
 * memory.
 *
 * @lucene.experimental
 */
public abstract sealed class BuildableHnswGraph extends HnswGraph implements Accountable, Closeable
    permits OnHeapHnswGraph, OffHeapHnswGraph {

  private final AtomicReference<EntryNode> entryNode = new AtomicReference<>(new EntryNode(-1, 1));

  BuildableHnswGraph() {}

  /**
   * Creates a graph to be built.
   *
   * @param M max number of connections on upper levels
   * @param numNodes number of nodes that will be added to this graph, or -1 if unbounded
   * @param offHeap whether to keep the neighbor lists in native memory; requires a known {@code
   *     numNodes}
   */
  public static BuildableHnswGraph create(int M, int numNodes, boolean offHeap) {
    if (offHeap) {
      if (numNodes < 0) {
        throw new IllegalArgumentException("off-heap graph requires a known number of nodes");
      }
      return new OffHeapHnswGraph(M, numNodes);
    }
    return new OnHeapHnswGraph(M, numNodes);
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
  public abstract void addNode(int level, int node);

  /**
   * {@inheritDoc}
   *
   * <p>NOTE: calling this method while the graph is still building is prohibited
   */
  @Override
  public abstract NodesIterator getNodesOnLevel(int level);

  /** Returns true if the node has been added on the given level */
  public abstract boolean nodeExistAtLevel(int level, int node);

  /** Returns the number of neighbors of a node on the given level */
  public abstract int numNeighbors(int level, int node);

  /**
   * Copies the neighbors of a node into {@code dst}.
   *
   * @param dst destination, must have room for {@link #numNeighbors(int, int)} entries; {@code 2 *
   *     maxConn() + 1} entries are always enough
   * @return the number of neighbors copied
   */
  public abstract int copyNeighbors(int level, int node, int[] dst);

  /**
   * Returns a scratch array to pass to {@link #beginUpdate} for the given level, or null if this
   * graph mutates its neighbor lists in place.
   */
  abstract NeighborArray newUpdateScratch(int level);

  /**
   * Returns the neighbor list of a node for mutation. Must be followed by {@link #endUpdate} with
   * the returned array.
   *
   * @param scratch the array returned by {@link #newUpdateScratch} for this level
   */
  abstract NeighborArray beginUpdate(int level, int node, NeighborArray scratch);

  /** Publishes the mutations made on the array returned by {@link #beginUpdate}. */
  abstract void endUpdate(int level, int node, NeighborArray neighbors);

  /**
   * Returns the current number of levels in the graph
   *
   * @return the current number of levels in the graph
   */
  @Override
  public final int numLevels() {
    return entryNode.get().level + 1;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public final int entryNode() {
    return entryNode.get().node;
  }

  /**
   * Try to set the entry node if the graph does not have one
   *
   * @return True if the entry node is set to the provided node. False if the entry node already
   *     exists
   */
  public final boolean trySetNewEntryNode(int node, int level) {
    EntryNode current = entryNode.get();
    if (current.node == -1) {
      return entryNode.compareAndSet(current, new EntryNode(node, level));
    }
    return false;
  }

  /**
   * Try to promote the provided node to the entry node
   *
   * @param level should be larger than expectedOldLevel
   * @param expectOldLevel is the old entry node level the caller expect to be, the actual graph
   *     level can be different due to concurrent modification
   * @return True if the entry node is set to the provided node. False if expectOldLevel is not the
   *     same as the current entry node level. Even if the provided node's level is still higher
   *     than the current entry node level, the new entry node will not be set and false will be
   *     returned.
   */
  public final boolean tryPromoteNewEntryNode(int node, int level, int expectOldLevel) {
    assert level > expectOldLevel;
    EntryNode currentEntry = entryNode.get();
    if (currentEntry.level == expectOldLevel) {
      return entryNode.compareAndSet(currentEntry, new EntryNode(node, level));
    }
    return false;
  }

  /** Releases resources held by this graph. The default implementation does nothing. */
  @Override
  public void close() {}

  private record EntryNode(int node, int level) {}
}
