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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.util.Arrays;
import java.util.function.IntPredicate;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.VectorUtil;

/**
 * A single-layer navigable graph over the centroids. Hops are scored with Nitrox2 codes, which is
 * what makes them cheap; a 2-bit sketch only has to be directionally right, because every search
 * returns all the nodes it visited for the caller to {@link #rank} exactly.
 *
 * <p>Construction is incremental, HNSW-style: each node descends the graph built so far, and its
 * visited nodes are ranked by exact distance then pruned by the relative-neighbourhood rule (keep a
 * candidate only when it is nearer the new node than to any neighbour already kept), which leaves a
 * spread of directions rather than a cluster of near-duplicates. A final ring guarantees that no
 * centroid, and so no cell, is unreachable.
 */
final class CentroidGraph {
  private static final int MAX_NEIGHBORS = 16;
  private static final int BUILD_BEAM_WIDTH = 64;
  static final int BEAM_WIDTH = 32;

  private CentroidGraph() {}

  static int[][] build(float[][] centroids, byte[][] codes) {
    int[][] graph = new int[codes.length][];
    Arrays.setAll(graph, _ -> new int[0]);
    for (int node = 1; node < graph.length; node++) {
      int[] visited = search(graph, codes, codes[node], BUILD_BEAM_WIDTH, node, _ -> true);
      long[] ranked = rank(centroids, centroids[node], visited);
      int[] kept = new int[Math.min(MAX_NEIGHBORS, ranked.length)];
      int size = 0;
      for (int i = 0; i < ranked.length && size < kept.length; i++) {
        int candidate = (int) ranked[i];
        float distance = Float.intBitsToFloat((int) (ranked[i] >>> 32));
        boolean diverse = true;
        for (int j = 0; j < size && diverse; j++) {
          diverse = VectorUtil.squareDistance(centroids[candidate], centroids[kept[j]]) >= distance;
        }
        if (diverse) kept[size++] = candidate;
      }
      graph[node] = ArrayUtil.copyOfSubArray(kept, 0, size);
      // Forcing the nearest neighbour's back-edge is what makes the new node reachable.
      for (int i = 0; i < size; i++) link(graph, centroids, kept[i], node, i == 0);
    }
    if (graph.length > 1) {
      for (int node = 0; node < graph.length; node++) {
        int[] links = ArrayUtil.growExact(graph[node], graph[node].length + 2);
        links[links.length - 2] = (node + graph.length - 1) % graph.length;
        links[links.length - 1] = (node + 1) % graph.length;
        graph[node] = Arrays.stream(links).distinct().toArray();
      }
    }
    return graph;
  }

  /** Adds the back-edge; at capacity it replaces the farthest neighbour if nearer, or forced. */
  private static void link(int[][] graph, float[][] centroids, int from, int to, boolean force) {
    int[] links = graph[from];
    if (links.length < MAX_NEIGHBORS) {
      graph[from] = ArrayUtil.growExact(links, links.length + 1);
      graph[from][links.length] = to;
      return;
    }
    int worst = 0;
    float worstDistance = -1;
    for (int i = 0; i < links.length; i++) {
      float distance = VectorUtil.squareDistance(centroids[from], centroids[links[i]]);
      if (distance > worstDistance) {
        worstDistance = distance;
        worst = i;
      }
    }
    if (force || VectorUtil.squareDistance(centroids[from], centroids[to]) < worstDistance) {
      links[worst] = to;
    }
  }

  /**
   * Packs {@code (squared distance bits << 32 | node)}, nearest first. Squared distances are
   * non-negative, so their float bits sort in numeric order.
   */
  static long[] rank(float[][] centroids, float[] target, int[] nodes) {
    long[] ranked = new long[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      float distance = VectorUtil.squareDistance(target, centroids[nodes[i]]);
      ranked[i] = ((long) Float.floatToIntBits(distance) << 32) | nodes[i];
    }
    Arrays.sort(ranked);
    return ranked;
  }

  /**
   * Returns every accepted node visited by a beam search of width {@code max(beam, BEAM_WIDTH)},
   * unordered. The beam only steers the descent: a node it evicted may still be among the true
   * nearest, and it was already scored, so handing it to the exact ranking is free. Rejected nodes
   * (empty cells) are traversed but never returned.
   */
  static int[] search(byte[][] codes, int[][] graph, byte[] query, int beam, IntPredicate accept) {
    return search(graph, codes, query, beam, codes.length, accept);
  }

  /** Beam search from node 0 over the first {@code size} nodes. */
  private static int[] search(
      int[][] graph, byte[][] codes, byte[] query, int width, int size, IntPredicate accept) {
    if (size == 0) return new int[0];
    int beam = Math.min(size, Math.max(width, BEAM_WIDTH));
    FixedBitSet seen = new FixedBitSet(size);
    LongHeap candidates = new LongHeap(beam); // nearest first
    LongHeap best = new LongHeap(beam); // negated keys: farthest first, bounded to the beam
    int[] visited = new int[4 * beam];
    int count = 0;
    long entry = (long) VectorKernels.INSTANCE.hamming(query, codes[0]) << 32;
    candidates.push(entry);
    best.push(-entry);
    seen.set(0);
    if (accept.test(0)) visited[count++] = 0;
    while (candidates.size() > 0) {
      long current = candidates.pop();
      if (best.size() == beam && current > -best.top()) break;
      for (int link : graph[(int) current]) {
        if (link >= size || seen.getAndSet(link)) continue;
        if (accept.test(link)) {
          visited = ArrayUtil.grow(visited, count + 1);
          visited[count++] = link;
        }
        long neighbor = ((long) VectorKernels.INSTANCE.hamming(query, codes[link]) << 32) | link;
        if (best.insertWithOverflow(-neighbor)) {
          candidates.push(neighbor);
        }
      }
    }
    return ArrayUtil.copyOfSubArray(visited, 0, count);
  }
}
