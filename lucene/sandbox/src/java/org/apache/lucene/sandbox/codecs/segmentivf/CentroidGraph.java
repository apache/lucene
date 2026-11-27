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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongHeap;

/**
 * A single-layer neighbour graph over a segment's centroid codes, so a query can find its nearest
 * cells without scoring every centroid.
 *
 * <p>Each centroid links to at most {@link #MAX_NEIGHBOURS} others, chosen with the HNSW diversity
 * heuristic, and a query walks it best-first from {@code entryNode} by coarse distance between the
 * prepared query's centroid code and the centroid codes (see {@link CentroidCodes}). Only queries
 * use the graph: clustering moves the centroids every iteration.
 *
 * <p>Built nodes are fixed-size records in {@code nodes}, padded to a multiple of 64 bytes:
 *
 * <pre>
 *   [centroid code][degree: 2 bytes][neighbour ordinal: 2 bytes] x degree
 * </pre>
 *
 * The padding only aligns records to cache lines when {@code nodes} starts on one. A Java {@code
 * byte[]} does not: its data is only 8-byte aligned, at an offset that changes as the collector
 * moves it, so a record can span one more line than its size needs, and which nodes do varies from
 * run to run. For tiers whose centroid codes are compared by Hamming distance ({@link
 * CoarseCodec#nativeCentroidCodes}, S1), a built graph's {@code nodes} is therefore native memory
 * aligned to 64 bytes; for the others it wraps a heap array. The bytes written are the same either
 * way.
 *
 * <p>The 2-byte ordinals cap {@code nlist} at 65535. While the graph is being built, {@code nodes}
 * wraps the bare array of centroid codes and {@code buildAdjacency} holds the neighbour lists,
 * published atomically because parallel inserts search while others link; it is null once built.
 *
 * <p>Differences from Lucene's HNSW ({@code HnswGraphBuilder}, {@code HnswGraphSearcher}):
 *
 * <ul>
 *   <li>One layer and one fixed entry node, chosen by the insertion shuffle, instead of a hierarchy
 *       whose upper layers find a starting point near the query; walks from a far entry are longer.
 *   <li>At most {@link #MAX_NEIGHBOURS} (16) neighbours, where HNSW's bottom layer allows {@code 2
 *       * M} (32 by default). Doubling it would add 32 bytes to every node record.
 *   <li>A full neighbour list takes a back-link by replacing its farthest neighbour, only when the
 *       new node is nearer or must link (see {@link #linkBack}). HNSW adds the link and then drops
 *       the worst neighbour that fails the diversity check, so its full lists stay diverse; these
 *       drift towards plain nearest neighbours.
 *   <li>Inserts find candidates by coarse distance over centroid codes but choose and replace
 *       neighbours by exact distance between centroids; HNSW uses one scorer for both.
 *   <li>Unreachable nodes are attached by {@link #ensureReachable} to an adjacent ordinal or any
 *       reachable host, not to a searched near neighbour as HNSW's component repair does, so some
 *       repair edges are arbitrary.
 *   <li>{@link #search} returns every scored node for the caller to rank, where HNSW returns the
 *       top {@code k}.
 *   <li>Inserts follow a shuffle rather than document order, and parallel inserts publish
 *       copy-on-write lists so searches never lock, where HNSW's concurrent builder locks mutable
 *       neighbour arrays.
 * </ul>
 */
record CentroidGraph(
    CoarseCodec codec,
    int nlist,
    int stride,
    int entryNode,
    MemorySegment nodes,
    AtomicReferenceArray<int[]> buildAdjacency) {
  static final int MAX_NEIGHBOURS = 16;

  /** Beam width when inserting a node. */
  static final int EF_CONSTRUCTION = 64;

  /** A query's beam width is {@code max(MIN_EF, EF_MULTIPLIER * probes)}. */
  static final int EF_MULTIPLIER = 2;

  static final int MIN_EF = 32;

  private static final int NODE_ALIGNMENT = 64;
  private static final int ORD_BYTES = 2;
  private static final ValueLayout.OfShort ORD =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final int LOCK_STRIPES = 512;
  private static final int INSERT_GRAIN = 256;

  /**
   * Rounds of cheap connectivity repair after insertion, each followed by a fresh breadth-first
   * search; nodes still unreachable after them are attached by {@link #attachUnreachable}.
   */
  static final int MAX_REPAIR_ROUNDS = 8;

  /**
   * A thread's search state, reused by its every search so a search allocates nothing. In the
   * visited marks, {@code visited[0]} is a generation counter and {@code visited[node + 1] ==
   * generation} marks a node seen by the current search, so the array is only cleared when the
   * counter wraps (see {@link #nextGeneration}).
   */
  private static final class Walk {
    int[] visited = new int[1];
    final LongHeap toExpand = new LongHeap(64);
    final LongHeap kept = new LongHeap(64);
    final int[] scoreOffsets = new int[MAX_NEIGHBOURS];
    final int[] scoreDistances = new int[MAX_NEIGHBOURS];
    final int[] adjacent = new int[MAX_NEIGHBOURS];
  }

  private static final ThreadLocal<Walk> WALK = ThreadLocal.withInitial(Walk::new);

  /**
   * Builds the graph by inserting centroids in a fixed shuffled order. Each insert searches the
   * graph built so far for candidates, keeps a diverse subset as the new node's neighbours, and
   * links the new node back into theirs. The first inserts run on one thread so the parallel
   * inserts start from a graph large enough to find good candidates; the rest run on at most {@code
   * parallel}'s workers.
   */
  static CentroidGraph build(CentroidCodes codes, Clustering.Parallel parallel) throws IOException {
    int nlist = codes.nlist;
    // Ordinals are written in 2 bytes, so more cells would be silently truncated.
    if (nlist < 1 || nlist > SegmentIVFVectorsFormat.MAX_NLIST) {
      throw new IllegalArgumentException("nlist=" + nlist);
    }
    int codeBytes = codes.codeBytes;
    // neighbours() locates a built node's degree at codec.centroidBytes, so the two must agree.
    assert codeBytes == codes.codec.centroidBytes;
    int stride = alignNode(codeBytes + ORD_BYTES + MAX_NEIGHBOURS * ORD_BYTES);
    int[] empty = new int[0];
    AtomicReferenceArray<int[]> neighbours = new AtomicReferenceArray<>(nlist);
    for (int c = 0; c < nlist; c++) {
      neighbours.set(c, empty);
    }
    // Cell ordinals come out of clustering and may be spatially correlated, which would bias the
    // early graph, so inserts follow a fixed shuffle. Parallel inserts still race, so the graph
    // can differ between builds of the same centroids.
    int[] order = new int[nlist];
    for (int i = 0; i < nlist; i++) {
      order[i] = i;
    }
    // Fisher-Yates shuffle (every arrangement is equally likely)
    Random random = new Random(Clustering.SEED);
    for (int i = nlist - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      int swap = order[i];
      order[i] = order[j];
      order[j] = swap;
    }
    int entryNode = order[0];
    CentroidGraph partial =
        new CentroidGraph(
            codes.codec,
            nlist,
            codeBytes,
            entryNode,
            MemorySegment.ofArray(codes.codes),
            neighbours);
    Object[] locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
    // Inserts order[from + 1 .. to]; order[0] is the entry node and is never inserted.
    Clustering.Parallel.RangeTask insertRange =
        (from, to) -> {
          byte[] query = new byte[codes.codec.queryBytes];
          int[] candidates = new int[nlist];
          for (int i = from + 1; i <= to; i++) {
            int node = order[i];
            codes.codec.encodeCentroid(codes.centroids[node], query, 0);
            int found = partial.search(query, EF_CONSTRUCTION, candidates, null);
            int[] kept = selectDiverse(codes, codes.centroids[node], candidates, found);
            // Every list is written under its stripe's lock, the new node's too: another thread
            // can reach the node through a back-link below and link to it before this thread
            // returns, and must then extend this list, not the empty one.
            synchronized (locks[stripe(node)]) {
              neighbours.set(node, kept);
            }
            for (int k = 0; k < kept.length; k++) {
              int neighbour = kept[k];
              synchronized (locks[stripe(neighbour)]) {
                neighbours.set(
                    neighbour, linkBack(codes, neighbours.get(neighbour), neighbour, node, k == 0));
              }
            }
          }
        };
    int serialPrefix = Math.min(nlist - 1, Math.max(64, MAX_NEIGHBOURS * 4));
    insertRange.run(0, serialPrefix);
    parallel.run(
        nlist - 1 - serialPrefix,
        INSERT_GRAIN,
        (from, to) -> insertRange.run(serialPrefix + from, serialPrefix + to));
    int[][] lists = new int[nlist][];
    for (int c = 0; c < nlist; c++) {
      lists[c] = neighbours.get(c);
    }
    ensureReachable(lists, entryNode, MAX_REPAIR_ROUNDS);
    byte[] nodes = new byte[nlist * stride];
    for (int c = 0; c < nlist; c++) {
      System.arraycopy(codes.codes, c * codeBytes, nodes, c * stride, codeBytes);
      int degree = Math.min(MAX_NEIGHBOURS, lists[c].length);
      int at = c * stride + codeBytes;
      writeOrd(nodes, at, degree);
      for (int i = 0; i < degree; i++) {
        writeOrd(nodes, at + ORD_BYTES * (i + 1), lists[c][i]);
      }
    }
    MemorySegment placed = place(codes.codec, nodes);
    return new CentroidGraph(codes.codec, nlist, stride, entryNode, placed, null);
  }

  /**
   * Returns built {@code nodes} as the graph keeps them: copied into native memory aligned to
   * {@link #NODE_ALIGNMENT} when the codec allows it, else wrapped in place. The native copy is
   * freed once the graph is unreachable.
   */
  private static MemorySegment place(CoarseCodec codec, byte[] nodes) {
    if (codec.nativeCentroidCodes() == false || nodes.length == 0) {
      return MemorySegment.ofArray(nodes);
    }
    MemorySegment aligned = Arena.ofAuto().allocate(nodes.length, NODE_ALIGNMENT);
    MemorySegment.copy(nodes, 0, aligned, ValueLayout.JAVA_BYTE, 0, nodes.length);
    return aligned;
  }

  private static int stripe(int node) {
    return node & (LOCK_STRIPES - 1);
  }

  private static int alignNode(int bytes) {
    return (bytes + NODE_ALIGNMENT - 1) / NODE_ALIGNMENT * NODE_ALIGNMENT;
  }

  /**
   * Repairs connectivity after insertion so that every node is reachable from {@code entryNode}. A
   * full neighbour list can refuse a back-link and later displace a node's only in-link, leaving
   * that node unreachable; a query only probes cells its walk finds, so that cell would never be
   * probed.
   *
   * <p>Each of up to {@code repairRounds} rounds finds the reachable nodes by breadth-first search
   * from the entry and links every unreachable node from a host, and back to it when the node's own
   * list has room: the entry for ordinal 0, else ordinal {@code c - 1}, which is reachable or was
   * linked just before. A full host that does not already link to {@code c} gives up its last slot,
   * which can orphan that slot's old target, so the search repeats. Nodes still unreachable after
   * the rounds are attached by {@link #attachUnreachable}, which cannot orphan anything, so on
   * return every node is reachable, though not always by a good edge.
   */
  static void ensureReachable(int[][] neighbours, int entryNode, int repairRounds) {
    int n = neighbours.length;
    boolean[] reachable = new boolean[n];
    int[] queue = new int[n];
    for (int round = 0; round < repairRounds; round++) {
      Arrays.fill(reachable, false);
      int head = 0;
      int tail = 0;
      queue[tail] = entryNode;
      tail++;
      reachable[entryNode] = true;
      while (head < tail) {
        int expanded = queue[head];
        head++;
        for (int next : neighbours[expanded]) {
          if (reachable[next] == false) {
            reachable[next] = true;
            queue[tail] = next;
            tail++;
          }
        }
      }
      if (tail == n) {
        return;
      }
      int host = entryNode;
      for (int c = 0; c < n; c++) {
        if (reachable[c] == false) {
          neighbours[c] = appendUnique(neighbours[c], host);
          int[] hostList = neighbours[host];
          if (hostList.length < MAX_NEIGHBOURS) {
            neighbours[host] = appendUnique(hostList, c);
          } else if (contains(hostList, c) == false) {
            // A host attached earlier this round (c - 1) may already link to c; overwriting a
            // slot then would duplicate c and drop that slot's edge for nothing.
            neighbours[host] = hostList.clone();
            neighbours[host][MAX_NEIGHBOURS - 1] = c;
          }
          reachable[c] = true;
        }
        host = c;
      }
    }
    attachUnreachable(neighbours, entryNode);
  }

  /**
   * Links every node that is not reachable from {@code entryNode} from a reachable host, without
   * making any reachable node unreachable, so that afterwards every node is reachable.
   *
   * <p>A breadth-first search from the entry records, for each node it reaches, the slot that first
   * reached it: its tree edge. The search runs to completion before each attachment, so every
   * reachable node links only to reachable nodes. A host then gives up a free slot or, when full, a
   * slot that is not a tree edge; that slot's old target stays reachable through its own tree edge.
   * Such a host always exists: if every reachable node were full of tree edges there would be
   * {@link #MAX_NEIGHBOURS} tree edges per reachable node, but there is only one per reachable node
   * other than the entry. A host with neither kind of slot never gains one, since tree edges never
   * change, so a single cursor moves through the hosts in search order.
   */
  static void attachUnreachable(int[][] neighbours, int entryNode) {
    int n = neighbours.length;
    // treeHost[x] is the node whose slot treeSlot[x] first reached x, or -1 while x is unreached.
    // The entry is reached without an edge, so its slot -1 never matches a real slot.
    int[] treeHost = new int[n];
    int[] treeSlot = new int[n];
    Arrays.fill(treeHost, -1);
    treeHost[entryNode] = entryNode;
    treeSlot[entryNode] = -1;
    int[] queue = new int[n];
    int head = 0;
    int tail = 0;
    queue[tail] = entryNode;
    tail++;
    int hostCursor = 0;
    for (int c = 0; c < n; c++) {
      while (head < tail) {
        int expanded = queue[head];
        head++;
        int[] list = neighbours[expanded];
        for (int slot = 0; slot < list.length; slot++) {
          int next = list[slot];
          if (treeHost[next] == -1) {
            treeHost[next] = expanded;
            treeSlot[next] = slot;
            queue[tail] = next;
            tail++;
          }
        }
      }
      if (treeHost[c] != -1) {
        continue;
      }
      int slot = -1;
      while (slot == -1) {
        assert hostCursor < tail : "every reachable node is full of tree edges";
        int host = queue[hostCursor];
        slot = freeOrNonTreeSlot(neighbours[host], host, treeHost, treeSlot);
        if (slot == -1) {
          hostCursor++;
        }
      }
      int host = queue[hostCursor];
      int[] list = neighbours[host];
      if (slot == list.length) {
        list = ArrayUtil.growExact(list, list.length + 1);
      } else {
        list = list.clone();
      }
      list[slot] = c;
      neighbours[host] = list;
      treeHost[c] = host;
      treeSlot[c] = slot;
      queue[tail] = c;
      tail++;
    }
  }

  /**
   * Returns a slot of {@code host}'s neighbour list that can take a new link: {@code list.length}
   * if the list has room, else the first slot that is not the tree edge of its target, else -1.
   */
  private static int freeOrNonTreeSlot(int[] list, int host, int[] treeHost, int[] treeSlot) {
    if (list.length < MAX_NEIGHBOURS) {
      return list.length;
    }
    for (int slot = 0; slot < list.length; slot++) {
      int target = list[slot];
      boolean treeEdge = treeHost[target] == host && treeSlot[target] == slot;
      if (treeEdge == false) {
        return slot;
      }
    }
    return -1;
  }

  private static boolean contains(int[] list, int node) {
    for (int existing : list) {
      if (existing == node) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code list} with {@code node} appended, if it is absent and the list has room. Lists
   * are copied on write: they start as one shared empty array.
   */
  private static int[] appendUnique(int[] list, int node) {
    if (contains(list, node) || list.length >= MAX_NEIGHBOURS) {
      return list;
    }
    int[] grown = ArrayUtil.growExact(list, list.length + 1);
    grown[list.length] = node;
    return grown;
  }

  /**
   * Stable insertion sort of {@code ids} by ascending {@code dist}: quadratic, so for the short
   * lists of a search, and the candidate lists of a build, at most a few hundred long.
   */
  static void sortByDistance(float[] dist, int[] ids, int n) {
    for (int i = 1; i < n; i++) {
      float d = dist[i];
      int id = ids[i];
      int j = i - 1;
      for (; j >= 0 && dist[j] > d; j--) {
        dist[j + 1] = dist[j];
        ids[j + 1] = ids[j];
      }
      dist[j + 1] = d;
      ids[j + 1] = id;
    }
  }

  /**
   * Chooses up to {@link #MAX_NEIGHBOURS} neighbours for {@code vector} from its search candidates
   * with the HNSW diversity heuristic: visiting candidates nearest first, keep one only if it is no
   * nearer to an already kept neighbour than to {@code vector}. A rejected candidate is reachable
   * through that neighbour, so the kept edges point in different directions. Sorts {@code
   * candidates} in place.
   */
  // Core: HnswGraphBuilder#diversityCheck is this heuristic (ties differ), but it is private.
  private static int[] selectDiverse(
      CentroidCodes codes, float[] vector, int[] candidates, int count) {
    float[] dist = new float[count];
    for (int i = 0; i < count; i++) {
      dist[i] = codes.exactDistance(vector, candidates[i]);
    }
    sortByDistance(dist, candidates, count);
    int[] kept = new int[Math.min(MAX_NEIGHBOURS, count)];
    int keptCount = 0;
    for (int i = 0; i < count && keptCount < kept.length; i++) {
      float[] candidate = codes.centroids[candidates[i]];
      boolean diverse = true;
      for (int k = 0; k < keptCount && diverse; k++) {
        diverse = (codes.exactDistance(candidate, kept[k]) < dist[i]) == false;
      }
      if (diverse) {
        kept[keptCount++] = candidates[i];
      }
    }
    return ArrayUtil.copyOfSubArray(kept, 0, keptCount);
  }

  /**
   * Returns {@code node}'s neighbour list with a link to {@code newNode} added. A list with room
   * takes it; a full list replaces its farthest neighbour, either when {@code newNode} is nearer or
   * unconditionally when {@code mustLink}, which the caller sets for the new node's nearest
   * neighbour so every node keeps at least one in-link.
   */
  private static int[] linkBack(
      CentroidCodes codes, int[] list, int node, int newNode, boolean mustLink) {
    if (list.length < MAX_NEIGHBOURS) {
      return appendUnique(list, newNode);
    }
    for (int existing : list) {
      if (existing == newNode) {
        return list;
      }
    }
    float[] nodeVector = codes.centroids[node];
    int farthest = -1;
    float farthestDist = Float.NEGATIVE_INFINITY;
    for (int i = 0; i < list.length; i++) {
      float d = codes.exactDistance(nodeVector, list[i]);
      if (d > farthestDist) {
        farthestDist = d;
        farthest = i;
      }
    }
    boolean replace = mustLink || codes.exactDistance(nodeVector, newNode) < farthestDist;
    if (replace == false) {
      return list;
    }
    int[] updated = list.clone();
    updated[farthest] = newNode;
    return updated;
  }

  /**
   * Best-first search for the cells nearest a prepared {@code query}, by coarse distance.
   *
   * <p>From the entry node, the walk repeatedly expands the nearest node not yet expanded: it
   * scores that node's unvisited neighbours, queues each for expansion, and keeps the {@code ef}
   * nearest seen so far. It stops when nothing is left to expand, or when the nearest node left is
   * farther than the farthest of {@code ef} kept nodes, since it is unlikely to lead anywhere
   * nearer.
   *
   * <p>Every scored node, not only the {@code ef} nearest, is written to {@code visitedOut}, with
   * its distance in {@code visitedDist} when that is non-null, up to {@code visitedOut.length}:
   * callers rank these candidates themselves. Returns how many were written.
   */
  int search(byte[] query, int ef, int[] visitedOut, int[] visitedDist) {
    Walk walk = WALK.get();
    int[] visited = visitedMarks(walk);
    int generation = nextGeneration(visited);
    int keepSize = Math.min(Math.max(ef, 1), nlist);
    // Entries pack (distance << 32 | node), so they compare by distance first. The queue of nodes
    // to expand is a min-heap that grows on push, so no node is dropped: entries a nearer node has
    // since pushed out of kept stay queued until the walk stops at them. The kept nodes are
    // negated, so its top is the farthest kept.
    LongHeap toExpand = walk.toExpand;
    LongHeap kept = walk.kept;
    toExpand.clear();
    kept.clear();
    int[] scoreOffsets = walk.scoreOffsets;
    int[] scoreDistances = walk.scoreDistances;
    int[] adjacent = walk.adjacent;
    int toScore = 1;
    int written = 0;
    scoreOffsets[0] = entryNode * stride;
    visited[entryNode + 1] = generation;
    while (true) {
      codec.centroidDistancesAt(query, nodes, scoreOffsets, toScore, scoreDistances);
      for (int i = 0; i < toScore; i++) {
        int node = scoreOffsets[i] / stride;
        int distance = scoreDistances[i];
        if (written < visitedOut.length) {
          if (visitedDist != null) {
            visitedDist[written] = distance;
          }
          visitedOut[written++] = node;
        }
        if (kept.size() == keepSize && distance >= farthest(kept)) {
          continue;
        }
        long scored = ((long) distance << 32) | node;
        // Nearer than the farthest kept, so it replaces that once kept is full.
        if (kept.size() < keepSize) {
          kept.push(-scored);
        } else {
          kept.updateTop(-scored);
        }
        toExpand.push(scored);
      }
      if (toExpand.size() == 0) {
        return written;
      }
      long nearest = toExpand.pop();
      if (kept.size() == keepSize && (int) (nearest >>> 32) > farthest(kept)) {
        return written;
      }
      int degree = neighbours((int) nearest, adjacent);
      toScore = 0;
      for (int i = 0; i < degree; i++) {
        int next = adjacent[i];
        if (next < nlist && visited[next + 1] != generation) {
          visited[next + 1] = generation;
          scoreOffsets[toScore++] = next * stride;
        }
      }
    }
  }

  /** {@code walk}'s visited marks, grown to cover this graph's nodes. */
  private int[] visitedMarks(Walk walk) {
    if (walk.visited.length <= nlist) {
      walk.visited = new int[nlist + 1];
    }
    return walk.visited;
  }

  /**
   * Advances and returns the generation in {@code visited[0]}. After 2^32 searches on one thread
   * the counter wraps: generation 0 would match every slot never stamped, and a reused generation
   * could match a stale stamp, so the marks are cleared and the count restarts at 1.
   */
  private static int nextGeneration(int[] visited) {
    int generation = visited[0] + 1;
    if (generation == 0) {
      Arrays.fill(visited, 0);
      generation = 1;
    }
    visited[0] = generation;
    return generation;
  }

  /** Sets this thread's generation counter, so tests can force a wrap; the next search adds 1. */
  void setVisitedGenerationForTesting(int generation) {
    visitedMarks(WALK.get())[0] = generation;
  }

  /** The distance of the farthest node in {@code kept}, a heap of negated entries. */
  private static int farthest(LongHeap kept) {
    return (int) (-kept.top() >>> 32);
  }

  /** Copies {@code node}'s neighbours into {@code dest}, returning how many it has. */
  int neighbours(int node, int[] dest) {
    if (buildAdjacency != null) {
      int[] adjacent = buildAdjacency.get(node);
      System.arraycopy(adjacent, 0, dest, 0, adjacent.length);
      return adjacent.length;
    }
    int at = node * stride + codec.centroidBytes;
    int degree = readOrd(at);
    for (int i = 0; i < degree; i++) {
      dest[i] = readOrd(at + ORD_BYTES * (i + 1));
    }
    return degree;
  }

  private int readOrd(int at) {
    return nodes.get(ORD, at) & 0xFFFF;
  }

  private static void writeOrd(byte[] nodes, int at, int ord) {
    nodes[at] = (byte) ord;
    nodes[at + 1] = (byte) (ord >>> 8);
  }

  /** Writes the header (node count, stride, entry node) and the packed nodes. */
  void write(IndexOutput out) throws IOException {
    out.writeVInt(nlist);
    out.writeVInt(stride);
    out.writeVInt(entryNode);
    byte[] bytes =
        nodes.isNative()
            ? nodes.toArray(ValueLayout.JAVA_BYTE)
            : (byte[]) nodes.heapBase().orElseThrow();
    out.writeBytes(bytes, 0, bytes.length);
  }

  /**
   * Reads a graph written by {@link #write} for a field of {@code expectedNlist} cells, the {@code
   * length} bytes of {@code in}. Every value a search trusts is checked first: the header against
   * the field and the section length, and every node's degree and neighbour ordinals, so a corrupt
   * or mismatched section throws {@link CorruptIndexException} rather than overrunning an array or
   * proposing cells the field does not have.
   */
  static CentroidGraph read(RandomAccessInput in, CoarseCodec codec, long length, int expectedNlist)
      throws IOException {
    // The header is three vints, at most 15 bytes.
    byte[] headerBytes = new byte[(int) Math.min(16, length)];
    in.readBytes(0, headerBytes, 0, headerBytes.length);
    ByteArrayDataInput header = new ByteArrayDataInput(headerBytes);
    int nlist;
    int stride;
    int entryNode;
    try {
      nlist = header.readVInt();
      stride = header.readVInt();
      entryNode = header.readVInt();
    } catch (IndexOutOfBoundsException e) {
      throw new CorruptIndexException(
          "centroid graph: header truncated, length=" + length, in.toString(), e);
    }
    if (nlist != expectedNlist) {
      throw corrupt(in, "nlist=" + nlist + " but the field has " + expectedNlist + " cells");
    }
    int expectedStride = alignNode(codec.centroidBytes + ORD_BYTES + MAX_NEIGHBOURS * ORD_BYTES);
    if (stride != expectedStride) {
      throw corrupt(in, "stride=" + stride + " but the codec's nodes take " + expectedStride);
    }
    if (entryNode < 0 || entryNode >= nlist) {
      throw corrupt(in, "entryNode=" + entryNode + " outside nlist=" + nlist);
    }
    long nodesLength = (long) nlist * stride;
    if (header.getPosition() + nodesLength != length) {
      throw corrupt(
          in,
          "length="
              + length
              + " but the header and "
              + nlist
              + " nodes of "
              + stride
              + " bytes take "
              + (header.getPosition() + nodesLength));
    }
    // nlist <= MAX_NLIST and the stride is the codec's, so the nodes fit an array.
    byte[] nodes = new byte[Math.toIntExact(nodesLength)];
    in.readBytes(header.getPosition(), nodes, 0, nodes.length);
    for (int c = 0; c < nlist; c++) {
      int at = c * stride + codec.centroidBytes;
      int degree = (nodes[at] & 0xFF) | (nodes[at + 1] & 0xFF) << 8;
      if (degree > MAX_NEIGHBOURS) {
        throw corrupt(in, "node " + c + " has degree " + degree);
      }
      for (int i = 0; i < degree; i++) {
        int p = at + ORD_BYTES * (i + 1);
        int neighbour = (nodes[p] & 0xFF) | (nodes[p + 1] & 0xFF) << 8;
        if (neighbour >= nlist) {
          throw corrupt(in, "node " + c + " links to " + neighbour + " outside nlist=" + nlist);
        }
      }
    }
    return new CentroidGraph(codec, nlist, stride, entryNode, place(codec, nodes), null);
  }

  private static CorruptIndexException corrupt(RandomAccessInput in, String message) {
    return new CorruptIndexException("centroid graph: " + message, in.toString());
  }
}
