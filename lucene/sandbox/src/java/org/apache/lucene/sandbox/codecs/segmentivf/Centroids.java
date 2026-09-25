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
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.sandbox.codecs.segmentivf.Clustering.Parallel;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.CodeRecord;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.FineCodec;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.Nitrox2;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

/** Encodes, ranks, and navigates the centroids used to route vectors into IVF cells. */
final class Centroids {
  private Centroids() {}

  /**
   * Keeps centroid representations together so routing can shortlist cheaply and verify accurately.
   */
  static final class CentroidCodes {
    private static final int TILE = 512;

    final float[][] centroids;
    final byte[] coarse;
    final int nlist, coarseBytes;
    private final int dim, fineStride;
    private final FineCodec fine;
    private final byte[] fineRecords;
    private final ThreadLocal<byte[]> gathered = ThreadLocal.withInitial(() -> new byte[0]);

    CentroidCodes(float[][] centroids, int dim, FineCodec fine) {
      this.dim = dim;
      this.nlist = centroids.length;
      this.centroids = centroids;
      this.coarseBytes = Nitrox2.bytesPerVector(dim);
      this.coarse = new byte[nlist * coarseBytes];
      this.fine = fine;
      this.fineStride = fine == null ? 0 : CodeRecord.length(fine.codeBytes);
      this.fineRecords = fine == null ? null : new byte[nlist * fineStride];
      encodeAll();
    }

    void encodeAll() {
      for (int c = 0; c < nlist; c++) {
        Nitrox2.encode(centroids[c], dim, coarse, c * coarseBytes);
        if (fine != null) fine.encode(centroids[c], fineRecords, c * fineStride);
      }
    }

    void rankCandidates(float[] vector, int[] cands, int count, float[] out) {
      if (fine == null) {
        for (int i = 0; i < count; i++) out[i] = exactDistance(vector, cands[i]);
        return;
      }
      byte[] flat = gathered.get();
      if (flat.length < count * fineStride) gathered.set(flat = new byte[count * fineStride]);
      for (int i = 0; i < count; i++) {
        System.arraycopy(fineRecords, cands[i] * fineStride, flat, i * fineStride, fineStride);
      }
      fine.query(vector, null).score(flat, fineStride, count, out);
      for (int i = 0; i < count; i++) out[i] = -out[i];
    }

    static final class Routing {
      final int[] cells;
      int count, cell2;
      float d1, d2;

      Routing(int capacity) {
        cells = new int[capacity];
      }
    }

    static final class Scratch {
      final int[] coarseDist = new int[TILE], verifyCells;
      final long[] heap;
      final float[] verifyDist;
      final byte[] qCode;

      Scratch(int dim, int nlist, int shortlist) {
        heap = new long[shortlist];
        verifyCells = new int[shortlist];
        verifyDist = new float[shortlist];
        qCode = new byte[Nitrox2.bytesPerVector(dim)];
      }
    }

    void routePacked(float[] vector, int shortlist, int keep, Routing out, Scratch scratch) {
      final int want = Math.min(shortlist, nlist);
      final long[] heap = scratch.heap;
      final int[] cd = scratch.coarseDist;
      int n = 0, worst = Integer.MAX_VALUE;
      for (int base = 0; base < nlist; base += TILE) {
        final int rows = Math.min(TILE, nlist - base);
        Kernels.INSTANCE.hamming(scratch.qCode, coarse, base * coarseBytes, rows, cd);
        for (int r = 0; r < rows; r++) {
          final int dist = cd[r];
          if (n < want) {
            heap[n++] = ((long) dist << 32) | (base + r);
            if (n == want) {
              for (int h = (n >>> 1) - 1; h >= 0; h--) {
                CentroidGraph.siftDown(heap, h, n, heap[h], true);
              }
              worst = (int) (heap[0] >>> 32);
            }
          } else if (dist < worst) {
            CentroidGraph.siftDown(heap, 0, n, ((long) dist << 32) | (base + r), true);
            worst = (int) (heap[0] >>> 32);
          }
        }
      }
      final int[] cells = scratch.verifyCells;
      final float[] dists = scratch.verifyDist;
      for (int i = 0; i < n; i++) {
        cells[i] = (int) heap[i];
        dists[i] = exactDistance(vector, cells[i]);
      }
      CentroidGraph.sortByDistance(dists, cells, n);
      System.arraycopy(cells, 0, out.cells, 0, out.count = Math.min(keep, n));
      out.cell2 = n > 1 ? cells[1] : -1;
      out.d1 = n > 0 ? dists[0] : Float.MAX_VALUE;
      out.d2 = n > 1 ? dists[1] : Float.MAX_VALUE;
    }

    float exactDistance(float[] vector, int c) {
      return -VectorUtil.dotProduct(vector, centroids[c]);
    }

    static boolean withinMargin(float d1, float d2, float margin) {
      return d2 != Float.MAX_VALUE && d2 - d1 <= (margin - 1f) * Math.abs(d1);
    }
  }

  /**
   * A compact graph over centroid codes that avoids scoring every cell when selecting query probes.
   */
  record CentroidGraph(
      int nlist, int coarseBytes, int stride, int entry, byte[] nodes, int[][] building) {
    static final int M = 16, EF_CONSTRUCTION = 64, EF_MULTIPLIER = 2, MIN_EF = 32;
    private static final int ALIGN = 64, ORD_BYTES = 2, LOCK_STRIPES = 512, INSERT_GRAIN = 256;
    private static final ThreadLocal<int[]> VISITED = ThreadLocal.withInitial(() -> new int[1]);

    static CentroidGraph build(CentroidCodes codes, int dim) throws IOException {
      int nlist = codes.nlist, coarseBytes = codes.coarseBytes;
      int stride = (coarseBytes + 2 + M * ORD_BYTES + ALIGN - 1) / ALIGN * ALIGN;
      int[][] neighbours = new int[nlist][];
      Arrays.fill(neighbours, new int[0]);
      int[] order = new int[nlist];
      for (int i = 0; i < nlist; i++) order[i] = i;
      Random random = new Random(0x5DEECE66DL);
      for (int i = nlist - 1; i > 0; i--) {
        int j = random.nextInt(i + 1), t = order[i];
        order[i] = order[j];
        order[j] = t;
      }
      int entry = order[0];
      CentroidGraph partial =
          new CentroidGraph(nlist, coarseBytes, coarseBytes, entry, codes.coarse, neighbours);
      Object[] locks = new Object[LOCK_STRIPES];
      for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
      Parallel.RangeTask insertRange =
          (from, to) -> {
            byte[] code = new byte[coarseBytes];
            int[] visited = new int[nlist];
            for (int idx = from + 1; idx <= to; idx++) {
              int node = order[idx];
              Nitrox2.encode(codes.centroids[node], dim, code, 0);
              int n = partial.search(code, EF_CONSTRUCTION, visited, null);
              int[] kept = neighbours[node] = prune(codes, codes.centroids[node], visited, n);
              for (int i = 0; i < kept.length; i++) {
                int x = kept[i];
                synchronized (locks[(x * 0x9E3779B9) >>> 1 & (LOCK_STRIPES - 1)]) {
                  neighbours[x] = link(codes, neighbours[x], x, node, i == 0);
                }
              }
            }
          };
      int seed = Math.min(nlist - 1, Math.max(64, M * 4));
      insertRange.run(0, seed);
      Parallel.overRange(
          nlist - 1 - seed, INSERT_GRAIN, (from, to) -> insertRange.run(seed + from, seed + to));
      connect(neighbours, entry);
      byte[] nodes = new byte[nlist * stride];
      for (int c = 0; c < nlist; c++) {
        System.arraycopy(codes.coarse, c * coarseBytes, nodes, c * stride, coarseBytes);
        int off = c * stride + coarseBytes, deg = Math.min(M, neighbours[c].length);
        nodes[off] = (byte) deg;
        for (int i = 0; i < deg; i++) {
          nodes[off += ORD_BYTES] = (byte) neighbours[c][i];
          nodes[off + 1] = (byte) (neighbours[c][i] >>> 8);
        }
      }
      return new CentroidGraph(nlist, coarseBytes, stride, entry, nodes, null);
    }

    private static void connect(int[][] neighbours, int entry) {
      int n = neighbours.length;
      boolean[] reachable = new boolean[n];
      int[] queue = new int[n];
      for (int guard = 0; guard <= 8; guard++) {
        Arrays.fill(reachable, false);
        int head = 0, tail = 0;
        queue[tail++] = entry;
        reachable[entry] = true;
        while (head < tail) {
          for (int x : neighbours[queue[head++]]) {
            if (reachable[x]) continue;
            reachable[x] = true;
            queue[tail++] = x;
          }
        }
        if (tail == n || guard == 8) return;
        for (int c = 0, host = entry; c < n; host = c++) {
          if (reachable[c]) continue;
          neighbours[c] = appendUnique(neighbours[c], host);
          if (neighbours[host].length < M) {
            neighbours[host] = appendUnique(neighbours[host], c);
          } else {
            neighbours[host] = neighbours[host].clone();
            neighbours[host][M - 1] = c;
          }
          reachable[c] = true;
        }
      }
    }

    private static int[] appendUnique(int[] a, int v) {
      for (int x : a) if (x == v) return a;
      if (a.length >= M) return a;
      int[] out = ArrayUtil.growExact(a, a.length + 1);
      out[a.length] = v;
      return out;
    }

    private static void sortByDistance(float[] dist, int[] ids, int n) {
      for (int i = 1; i < n; i++) {
        float d = dist[i];
        int c = ids[i], j = i - 1;
        for (; j >= 0 && dist[j] > d; j--) {
          dist[j + 1] = dist[j];
          ids[j + 1] = ids[j];
        }
        dist[j + 1] = d;
        ids[j + 1] = c;
      }
    }

    private static int[] prune(CentroidCodes codes, float[] vec, int[] cand, int n) {
      float[] dist = new float[n];
      for (int i = 0; i < n; i++) dist[i] = codes.exactDistance(vec, cand[i]);
      sortByDistance(dist, cand, n);
      int[] kept = new int[Math.min(M, n)];
      int nKept = 0;
      for (int i = 0; i < n && nKept < kept.length; i++) {
        boolean diverse = true;
        for (int k = 0; k < nKept && diverse; k++) {
          diverse = (codes.exactDistance(codes.centroids[cand[i]], kept[k]) < dist[i]) == false;
        }
        if (diverse) kept[nKept++] = cand[i];
      }
      return ArrayUtil.copyOfSubArray(kept, 0, nKept);
    }

    private static int[] link(CentroidCodes codes, int[] cur, int x, int node, boolean mustLink) {
      if (cur.length < M) return appendUnique(cur, node);
      for (int y : cur) if (y == node) return cur;
      float[] xVec = codes.centroids[x];
      int worst = -1;
      float worstD = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < cur.length; i++) {
        float d = codes.exactDistance(xVec, cur[i]);
        if (d > worstD) {
          worstD = d;
          worst = i;
        }
      }
      if ((mustLink || codes.exactDistance(xVec, node) < worstD) == false) return cur;
      int[] out = cur.clone();
      out[worst] = node;
      return out;
    }

    int search(byte[] qCode, int ef, int[] out, int[] outDist) {
      int[] visited = VISITED.get();
      if (visited.length <= nlist) VISITED.set(visited = new int[nlist + 1]);
      int gen = ++visited[0], nOut = 0, frontierN = 0, bestN = 0;
      int cap = Math.min(Math.max(ef, 1), nlist);
      long[] frontier = new long[Math.min(nlist, Math.max(64, cap * 4))], best = new long[cap];
      int[] fanOffsets = new int[M], fanDist = new int[M + 1];
      visited[entry + 1] = gen;
      fanOffsets[0] = entry * stride;
      for (int fan = 1; ; ) {
        int firstOut = nOut;
        for (int i = 0; i < fan && nOut < out.length; i++) out[nOut++] = fanOffsets[i] / stride;
        if (fan > 0) Kernels.INSTANCE.hammingAt(qCode, nodes, fanOffsets, fan, fanDist);
        if (outDist != null) System.arraycopy(fanDist, 0, outDist, firstOut, nOut - firstOut);
        for (int i = 0; i < fan; i++) {
          if (bestN == cap && fanDist[i] >= (int) (best[0] >>> 32)) continue;
          long e = ((long) fanDist[i] << 32) | (fanOffsets[i] / stride);
          if (bestN < cap) siftUp(best, bestN++, e, true);
          else siftDown(best, 0, bestN, e, true);
          if (frontierN < frontier.length) siftUp(frontier, frontierN++, e, false);
        }
        if (frontierN == 0) return nOut;
        long top = frontier[0];
        siftDown(frontier, 0, --frontierN, frontier[frontierN], false);
        if (bestN == cap && (int) (top >>> 32) > (int) (best[0] >>> 32)) return nOut;
        int node = (int) top;
        int[] adj = building == null ? null : building[node];
        int degOff = node * stride + coarseBytes;
        int deg =
            adj != null ? adj.length : (nodes[degOff] & 0xFF) | (nodes[degOff + 1] & 0xFF) << 8;
        fan = 0;
        for (int i = 0, off = degOff + 2; i < deg; i++, off += ORD_BYTES) {
          int next = adj != null ? adj[i] : (nodes[off] & 0xFF) | (nodes[off + 1] & 0xFF) << 8;
          if (next >= nlist || visited[next + 1] == gen) continue;
          visited[next + 1] = gen;
          fanOffsets[fan++] = next * stride;
        }
      }
    }

    void write(IndexOutput out) throws IOException {
      out.writeVInt(nlist);
      out.writeVInt(stride);
      out.writeVInt(entry);
      out.writeBytes(nodes, 0, nodes.length);
    }

    static CentroidGraph read(RandomAccessInput in, int dim, long length) throws IOException {
      byte[] head = new byte[(int) Math.min(16, length)];
      in.readBytes(0, head, 0, head.length);
      ByteArrayDataInput header = new ByteArrayDataInput(head);
      int nlist = header.readVInt(), stride = header.readVInt(), entry = header.readVInt();
      byte[] nodes = new byte[nlist * stride];
      in.readBytes(header.getPosition(), nodes, 0, nodes.length);
      return new CentroidGraph(nlist, Nitrox2.bytesPerVector(dim), stride, entry, nodes, null);
    }

    private static void siftUp(long[] h, int i, long v, boolean max) {
      for (int p; i > 0 && (max ? h[p = (i - 1) >>> 1] < v : h[p = (i - 1) >>> 1] > v); i = p) {
        h[i] = h[p];
      }
      h[i] = v;
    }

    private static void siftDown(long[] h, int i, int size, long v, boolean max) {
      for (int child; (child = (i << 1) + 1) < size; i = child) {
        if (child + 1 < size && (max ? h[child + 1] > h[child] : h[child + 1] < h[child])) child++;
        if (max ? h[child] <= v : h[child] >= v) break;
        h[i] = h[child];
      }
      h[i] = v;
    }
  }
}
