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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class OnHeapHnswGraph extends HnswGraph implements Accountable {

  // Level 0 is represented as List<NeighborArray> – nodes' connections on level 0.
  // Each entry in the list has the top maxConn/maxConn0 neighbors of a node. The nodes correspond
  // to vectors
  // added to HnswBuilder, and the node values are the ordinals of those vectors.
  // Thus, on all levels, neighbors expressed as the level 0's nodes' ordinals.
  private final List<NeighborArray> graphLevel0;
  // Represents levels 1-N. Each level is represented with a Map that maps a levels level 0
  // ordinal to its neighbors on that level. All nodes are in level 0, so we do not need to maintain
  // it in this list. However, to avoid changing list indexing, we always will make the first
  // element
  // null.
  private final List<Map<Integer, NeighborArray>> graphUpperLevels;
  private final int nsize;
  private final int nsize0;

  private final AtomicReference<EntryNode> entryNode;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  private OnHeapHnswGraph(int M, List<NeighborArray> level0) {
    entryNode = new AtomicReference<>(new EntryNode(-1, 1)); // Entry node is negative until a node is added
    // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    nsize = M + 1;
    nsize0 = nsize0(M);
    graphUpperLevels = new ArrayList<>(16);
    for (int i = 0; i < 16; i++) {
      // add more levels than we will ever need in order to avoid having to update
      graphUpperLevels.add(new ConcurrentHashMap<>());
    }
    graphLevel0 = level0;
  }

  OnHeapHnswGraph(int M) {
    this(M, new ArrayList<>());
  }

  OnHeapHnswGraph(int M, int graphSize) {
    this (M, preallocate(graphSize, nsize0(M)));
  }

  private static List<NeighborArray> preallocate(int size, int nsize0) {
    NeighborArray[] na = new NeighborArray[size];
    for (int i = 0; i < size; i++) {
      na[i] = new NeighborArray(nsize0, true);
    }
    return Arrays.asList(na);
  }

  private static int nsize0(int M) {
    return M * 2 + 1;
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public NeighborArray getNeighbors(int level, int node) {
    if (level == 0) {
      return graphLevel0.get(node);
    }
    Map<Integer, NeighborArray> levelMap = graphUpperLevels.get(level);
    assert levelMap.containsKey(node);
    return levelMap.get(node);
  }

  public NeighborIterator lockNeighbors(int level, int node) {
    NeighborArray neighbors = getNeighbors(level, node);
    // nocommit we were deadlocking? replace with simple lock()
    try {
      while (!neighbors.lock.readLock().tryLock(1, TimeUnit.SECONDS)) {
        System.out.println("timed out waiting for lock owned by " + neighbors.lock.getOwner());
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    //neighbors.lock.readLock().lock();
    return new NeighborIterator() {
      private int upto;
      @Override
      public int nextNeighbor() {
        if (upto < neighbors.size()) {
          return neighbors.node[upto++];
        }
        return NO_MORE_DOCS;
      }

      @Override
      public int size() {
        return neighbors.size();
      }

      @Override
      public void close() {
        neighbors.lock.readLock().unlock();
      }
    };
  }

  @Override
  public int size() {
    return graphLevel0.size(); // all nodes are located on the 0th level
  }

  /**
   * Add node on the given level. Nodes can be inserted out of order, but it requires that the nodes
   * preceded by the node inserted out of order are eventually added.
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (level > 0) {
      graphUpperLevels.get(level).put(node, new NeighborArray(nsize, true));
    } else {
      // Add nodes all the way up to and including "node" in the new graph on level 0. This will
      // cause the size of the
      // graph to differ from the number of nodes added to the graph. The size of the graph and the
      // number of nodes
      // added will only be in sync once all nodes from 0...last_node are added into the graph.
      while (node >= graphLevel0.size()) {
        graphLevel0.add(new NeighborArray(nsize0, true));
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
    // NPE: cur == null when multiple threads use this
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
    return entryNode.get().level + 1;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public int entryNode() {
    return entryNode.get().node;
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
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2
            + Integer.BYTES * 3;
    long neighborArrayBytes =
        nsize * (Integer.BYTES + Float.BYTES)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2
            + Integer.BYTES * 3;
    long total = 0;
    for (int l = 0; l < numLevels(); l++) {
      if (l == 0) {
        total +=
            graphLevel0.size() * neighborArrayBytes0
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // for graph;
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

  // nocommit - is this really necessary? seems like it shouldn't be needed. Although possibly if
  // the graph List is being enlarged then array elements won't yet have been carried over, and is this
  // done in a thread-safe manner??? We could make it safe by doing the array management ourselves
  // but otherwise we must synchronize or rely on ArrayList implementation - is it safe to read
  // concurrently with .add()?
  Closeable withWriteLock() {
    lock.writeLock().lock();
    return () -> lock.writeLock().unlock();
  }
  Closeable withReadLock() {
    lock.readLock().lock();
    return () -> lock.readLock().unlock();
  }
  public boolean trySetNewEntryNode(int node, int level) {
    EntryNode current = entryNode.get();
    if (current.node == -1) {
      return entryNode.compareAndSet(current, new EntryNode(node, level));
    }
    return false;
  }

  public boolean tryPromoteNewEntryNode(int node, int level, int expectOldLevel) {
    EntryNode currentEntry = entryNode.get();
    if (currentEntry.level == expectOldLevel) {
      return entryNode.compareAndSet(currentEntry, new EntryNode(node, level));
    }
    return false;
  }

  @Override
  public String toString() {
    return "OnHeapHnswGraph(size="
        + size()
        + ", numLevels="
        + numLevels()
        + ", entryNode="
        + entryNode
        + ")";
  }
  public NeighborArray getCur() {
    return cur;
  }

  private final class EntryNode {
    private final int node;
    private final int level;

    private EntryNode(int node, int level) {
      this.node = node;
      this.level = level;
    }
  }}
