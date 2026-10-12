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
import static org.apache.lucene.util.hnsw.NeighborArray.SLOT_NODES_OFFSET;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An {@link HnswGraph} whose neighbor lists are held in native memory while the graph is built. The
 * number of nodes must be known up front, as is the case when merging.
 *
 * <p>Every neighbor list is a fixed-size slot laid out as {@code [int size][int sortedSize][int
 * nodes[cap]][float scores[cap]]}. Level 0 is a single segment addressed by {@code node * stride0}.
 * A node on {@code n > 1} levels owns {@code n - 1} contiguous upper-level slots, allocated in
 * fixed-size chunks; the first slot index is kept in {@code upperBase[node]}.
 *
 * <p>Thread-safety: as for {@link OnHeapHnswGraph}, concurrent builders must serialize mutations of
 * a neighbor list (see {@code HnswLock}). Slot allocation is thread-safe. Allocations are published
 * to other threads through the same happens-before edges that make a newly linked node reachable,
 * i.e. a node is only discovered after the locks guarding the incoming link are released.
 *
 * <p>The native memory is released by {@link #close()}; the graph must not be used afterwards.
 *
 * @lucene.experimental
 */
public final class OffHeapHnswGraph extends BuildableHnswGraph {

  private static final long RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(OffHeapHnswGraph.class);

  private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED;

  private static final int MIN_UPPER_CHUNK_SHIFT = 8;
  private static final int MAX_UPPER_CHUNK_SHIFT = 16;

  private final int maxConn;
  private final int numNodes;
  private final int nsize; // neighbour array size at non-zero level
  private final int nsize0; // neighbour array size at zero level
  private final long stride; // slot bytes at non-zero level
  private final long stride0; // slot bytes at zero level

  private final Arena arena;
  private final MemorySegment level0;

  // number of levels each node is on, 0 if the node has not been added
  private final byte[] nodeLevels;
  // index of each node's level-1 slot; levels 1..nodeLevels[node]-1 are contiguous
  private final int[] upperBase;

  private final int upperChunkShift;
  private final int upperChunkMask;
  private final AtomicInteger upperNext = new AtomicInteger();
  private MemorySegment[] upperChunks; // grown under synchronized(this)

  private final AtomicInteger size = new AtomicInteger();
  private final AtomicLong offHeapBytesUsed = new AtomicLong();
  private boolean closed;

  // generated on demand by getNodesOnLevel for non-zero levels
  private IntArrayList[] levelToNodes;
  private int lastFreezeSize;

  // KnnGraphValues iterator members
  private MemorySegment curSegment;
  private long curNodesOffset;
  private int curSize;
  private int upto;

  /**
   * @param M max number of connections on upper levels
   * @param numNodes number of nodes that will be added to this graph; nodes must be in {@code [0,
   *     numNodes)}
   */
  OffHeapHnswGraph(int M, int numNodes) {
    if (numNodes < 0) {
      throw new IllegalArgumentException("numNodes must be >= 0, got " + numNodes);
    }
    this.maxConn = M;
    this.numNodes = numNodes;
    // We allocate extra space for neighbours, but then prune them to keep allowed maximum
    this.nsize = M + 1;
    this.nsize0 = M * 2 + 1;
    this.stride = NeighborArray.slotBytes(nsize);
    this.stride0 = NeighborArray.slotBytes(nsize0);
    this.nodeLevels = new byte[numNodes];
    this.upperBase = new int[numNodes];
    // size chunks so that a small graph doesn't reserve a large chunk: ~numNodes/M nodes are
    // expected on level 1
    int expectedUpperSlots = Math.max(1, numNodes / Math.max(1, M));
    this.upperChunkShift =
        Math.clamp(
            32 - Integer.numberOfLeadingZeros(expectedUpperSlots - 1),
            MIN_UPPER_CHUNK_SHIFT,
            MAX_UPPER_CHUNK_SHIFT);
    this.upperChunkMask = (1 << upperChunkShift) - 1;
    this.upperChunks = new MemorySegment[1];
    this.arena = Arena.ofShared();
    long level0Bytes = stride0 * numNodes;
    // Arena#allocate zeroes the memory, so every slot starts out empty
    this.level0 = arena.allocate(Math.max(level0Bytes, 1), Long.BYTES);
    offHeapBytesUsed.addAndGet(level0Bytes);
  }

  @Override
  public void addNode(int level, int node) {
    if (node >= numNodes) {
      throw new IllegalStateException(
          "The graph does not expect to grow when an initial size is given");
    }
    int levels = nodeLevels[node];
    if (levels == 0) {
      size.incrementAndGet();
      if (level > 0) {
        upperBase[node] = allocateUpperSlots(level);
      }
      nodeLevels[node] = toLevelCount(level);
    } else if (levels <= level) {
      // promoted to a new top level: move the existing upper slots into a larger run
      int newBase = allocateUpperSlots(level);
      if (levels > 1) {
        MemorySegment.copy(
            upperChunk(upperBase[node]),
            upperOffset(upperBase[node]),
            upperChunk(newBase),
            upperOffset(newBase),
            (levels - 1) * stride);
      }
      upperBase[node] = newBase;
      nodeLevels[node] = toLevelCount(level);
    }
    // start out with an empty neighbor list, as the on-heap graph does
    if (level == 0) {
      level0.set(INT, node * stride0, 0);
      level0.set(INT, node * stride0 + Integer.BYTES, 0);
    } else {
      int slot = upperBase[node] + level - 1;
      MemorySegment chunk = upperChunk(slot);
      long offset = upperOffset(slot);
      chunk.set(INT, offset, 0);
      chunk.set(INT, offset + Integer.BYTES, 0);
    }
  }

  private static byte toLevelCount(int level) {
    if (level >= Byte.MAX_VALUE) {
      throw new IllegalArgumentException("level too large: " + level);
    }
    return (byte) (level + 1);
  }

  /** Allocates {@code count} contiguous upper-level slots that don't straddle a chunk. */
  private int allocateUpperSlots(int count) {
    assert count > 0 && count <= upperChunkMask + 1;
    while (true) {
      int current = upperNext.get();
      int start = current;
      int chunkEnd = ((current >>> upperChunkShift) + 1) << upperChunkShift;
      if (start + count > chunkEnd) {
        // skip the tail of the current chunk
        start = chunkEnd;
      }
      if (start + count < 0) {
        throw new IllegalStateException("too many upper level slots");
      }
      if (upperNext.compareAndSet(current, start + count)) {
        ensureUpperChunk(start >>> upperChunkShift);
        return start;
      }
    }
  }

  private synchronized void ensureUpperChunk(int chunk) {
    if (chunk >= upperChunks.length) {
      upperChunks = ArrayUtil.grow(upperChunks, chunk + 1);
    }
    if (upperChunks[chunk] == null) {
      long bytes = stride << upperChunkShift;
      upperChunks[chunk] = arena.allocate(bytes, Long.BYTES);
      offHeapBytesUsed.addAndGet(bytes);
    }
  }

  private MemorySegment upperChunk(int slot) {
    return upperChunks[slot >>> upperChunkShift];
  }

  private long upperOffset(int slot) {
    return (slot & upperChunkMask) * stride;
  }

  private int upperSlot(int level, int node) {
    assert level > 0 && level < nodeLevels[node] : "level=" + level + ", node=" + node;
    return upperBase[node] + level - 1;
  }

  @Override
  public boolean nodeExistAtLevel(int level, int node) {
    return nodeLevels[node] > level;
  }

  @Override
  public int numNeighbors(int level, int node) {
    if (level == 0) {
      return level0.get(INT, node * stride0);
    }
    int slot = upperSlot(level, node);
    return upperChunk(slot).get(INT, upperOffset(slot));
  }

  @Override
  public int copyNeighbors(int level, int node, int[] dst) {
    MemorySegment segment;
    long offset;
    if (level == 0) {
      segment = level0;
      offset = node * stride0;
    } else {
      int slot = upperSlot(level, node);
      segment = upperChunk(slot);
      offset = upperOffset(slot);
    }
    int count = segment.get(INT, offset);
    MemorySegment.copy(segment, INT, offset + SLOT_NODES_OFFSET, dst, 0, count);
    return count;
  }

  @Override
  NeighborArray newUpdateScratch(int level) {
    return new NeighborArray(level == 0 ? nsize0 : nsize, true);
  }

  @Override
  NeighborArray beginUpdate(int level, int node, NeighborArray scratch) {
    assert scratch.maxSize() == (level == 0 ? nsize0 : nsize);
    if (level == 0) {
      scratch.loadFrom(level0, node * stride0);
    } else {
      int slot = upperSlot(level, node);
      scratch.loadFrom(upperChunk(slot), upperOffset(slot));
    }
    return scratch;
  }

  @Override
  void endUpdate(int level, int node, NeighborArray neighbors) {
    if (level == 0) {
      neighbors.storeTo(level0, node * stride0);
    } else {
      int slot = upperSlot(level, node);
      neighbors.storeTo(upperChunk(slot), upperOffset(slot));
    }
  }

  @Override
  public void seek(int level, int targetNode) {
    long offset;
    if (level == 0) {
      curSegment = level0;
      offset = targetNode * stride0;
    } else {
      int slot = upperSlot(level, targetNode);
      curSegment = upperChunk(slot);
      offset = upperOffset(slot);
    }
    curSize = curSegment.get(INT, offset);
    curNodesOffset = offset + SLOT_NODES_OFFSET;
    upto = -1;
  }

  @Override
  public int neighborCount() {
    return curSize;
  }

  @Override
  public int nextNeighbor() {
    if (++upto < curSize) {
      return curSegment.get(INT, curNodesOffset + (long) upto * Integer.BYTES);
    }
    return NO_MORE_DOCS;
  }

  @Override
  public int size() {
    return size.get();
  }

  @Override
  public int maxNodeId() {
    // the graph size is fixed and the graph can possibly be concurrently modified
    return numNodes - 1;
  }

  @Override
  public int maxConn() {
    return maxConn;
  }

  /**
   * NOTE: calling this method while the graph is still building is prohibited
   *
   * @see OnHeapHnswGraph#getNodesOnLevel(int)
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
    for (int node = 0; node < numNodes; node++) {
      for (int i = 1; i < nodeLevels[node]; i++) {
        levelToNodes[i].add(node);
      }
    }
    lastFreezeSize = size();
  }

  /** Returns the number of bytes of native memory reserved by this graph. */
  public long offHeapBytesUsed() {
    return offHeapBytesUsed.get();
  }

  /**
   * Returns the heap and native memory used by this graph, so that callers accounting for the graph
   * see the same total as with an {@link OnHeapHnswGraph}.
   */
  @Override
  public long ramBytesUsed() {
    return RAM_BYTES_USED
        + RamUsageEstimator.sizeOf(nodeLevels)
        + RamUsageEstimator.sizeOf(upperBase)
        + RamUsageEstimator.shallowSizeOf(upperChunks)
        + offHeapBytesUsed();
  }

  @Override
  public synchronized void close() {
    if (closed == false) {
      closed = true;
      arena.close();
    }
  }

  @Override
  public String toString() {
    return "OffHeapHnswGraph(size="
        + size()
        + ", numLevels="
        + numLevels()
        + ", entryNode="
        + entryNode()
        + ")";
  }
}
