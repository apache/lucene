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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Random;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * A navigable graph over the centroids, for selecting cells in time logarithmic in {@code nlist}.
 *
 * <p>An exhaustive selection costs {@code nlist} comparisons per query. A greedy descent visits
 * {@code O(ef)} nodes, a count independent of {@code nlist}, so the saving grows with the cell
 * count.
 *
 * <h2>Layout</h2>
 *
 * <p>A pure search structure, which clustering never touches, so its whole cost is query-path
 * memory access. Payload and adjacency are interleaved into one cache-aligned record:
 *
 * <pre>
 *   dim=1024, M=16, 2-byte ordinals:
 *     [ coarse code 2 x 128 B ][ degree 2 B ][ neighbours 16 x 2 B ] = 290 B -> 320 B = 5 cache lines
 * </pre>
 *
 * <p>A visit is then one sequential run of cache lines, with no separate payload load and
 * pointer-chase, and the whole structure is small enough to stay cache-resident, which is what
 * makes a hop cheap. The payload leads because every visit scores it while only survivors read the
 * adjacency. Two-byte ordinals cap {@code nlist} at 65535 and are what fit five lines rather than
 * six. Degree is inline and the width is fixed, since the structure is cache-resident and size is
 * not the binding constraint.
 *
 * <p>Alignment leaves the tail of the last line unused, and neighbours are free until it fills: at
 * dim=1024 that is 30 spare bytes, room for 15 more ordinals, so any {@code M <= 31} occupies the
 * same five cache lines as 16 does.
 *
 * <h2>Construction</h2>
 *
 * <p>HNSW-style incremental insertion. Nodes are inserted in a shuffled order; each descends the
 * graph built so far, and the candidates that descent turns up are pruned by the
 * relative-neighbourhood rule, keeping {@code x} only when it is closer to {@code c} than to any
 * neighbour already kept, which leaves a spread of directions rather than a cluster of
 * near-duplicates. Back-edges make the edges effectively undirected.
 *
 * <p>The descent scores with the same coarse-then-exact cascade as the router ({@link
 * CentroidCodes#route}). If they disagreed, the graph would navigate toward cells the router would
 * not choose.
 *
 * <p>CONNECTIVITY IS NOT OPTIONAL: an unreachable centroid makes every document in its cell
 * permanently unretrievable, silently. Insertion links each node into a graph that already exists,
 * so the result is connected by construction, but the shrink rule can evict a node's only in-edge,
 * so a final sweep attaches whatever a traversal cannot reach.
 *
 * @lucene.experimental
 */
final class CentroidGraph {

  /**
   * Neighbours per node. Write-time: the record stride depends on it, so it is persisted with the
   * graph and a change reindexes. A smaller {@code M} shortens the node record and the adjacency
   * read per expansion, at some cost to navigability, which the exact rerank of the visited set
   * partly absorbs. {@code -Divfaster.graphM}.
   *
   * <p>The reader sizes its per-expansion fan scratch to this, so it must be at least any persisted
   * node's degree; keeping it in the index cache key (see KnnGraphTester) makes a mismatched reuse
   * impossible.
   */
  static final int M = Integer.getInteger("ivfaster.graphM", 16);

  /** Cache line, and the record alignment. */
  private static final int ALIGN = 64;

  /** Bytes of neighbour ordinal. Two bytes caps nlist at 65535; see the format's MAX_NLIST. */
  private static final int ORD_BYTES = 2;

  /**
   * Search beam width, as a multiple of the requested cell count. Search-time, since it bounds the
   * descent and not the graph, so {@code -Divfaster.efMultiplier} sweeps against one cached index.
   *
   * <p>A query at high {@code nlist} raises nprobe to recover the coverage lost to smaller cells,
   * and {@code ef} scales with nprobe, so this is the knob that trims the routing tax the way the
   * fine tier's {@code verifyMultiplier} trims the verify tax.
   */
  static final int EF_MULTIPLIER = Integer.getInteger("ivfaster.efMultiplier", 2);

  /** Minimum beam width, so small requests still explore enough to escape a local minimum. */
  static final int MIN_EF = Integer.getInteger("ivfaster.minEf", 32);

  /**
   * Beam width during construction. Wider than the search beam because a badly chosen edge is
   * permanent while an under-explored search is one query. Write-time, since it shapes the
   * persisted edges, so a change reindexes; {@code -Divfaster.efConstruction}.
   */
  static final int EF_CONSTRUCTION = Integer.getInteger("ivfaster.efConstruction", 64);

  /** Deterministic sampling: the same segment must yield the same graph. */
  private static final long SEED = 0x5DEECE66DL;

  /**
   * Lock stripes guarding concurrent back-edge updates. A power of two, so the stripe is a mask.
   *
   * <p>Striped rather than per-node: {@code nlist} can be 65535, and back-edge contention is low
   * because each insertion touches at most {@code M} nodes.
   */
  private static final int LOCK_STRIPES = 512;

  /**
   * Insertions per worker in the parallel build. One item is a whole HNSW insertion, so the grain
   * is far coarser than the {@link Parallel} per-document default. A build small enough to fall
   * below it stays sequential, and therefore deterministic; testDeterminism names this.
   */
  static final int PARALLEL_INSERT_GRAIN = 256;

  /**
   * Kill switch for parallel insertion ({@code -Divfaster.serialGraph}), so the concurrent build's
   * effect on recall is measurable against one binary. Concurrency changes topology, since a worker
   * descends whatever graph exists when it looks, and the sequential build is the control arm.
   */
  private static final boolean SERIAL_GRAPH = Boolean.getBoolean("ivfaster.serialGraph");

  private final int nlist;

  /** Bytes of coarse code per node: the record's payload prefix before the adjacency. */
  private final int coarseBytes;

  private final int stride;
  private final int entry;

  /** The interleaved node records, {@code nlist * stride} bytes. */
  private final byte[] nodes;

  private final MemorySegment nodesSeg;
  private final HammingKernel hamming = HammingKernel.get();

  /**
   * Per-thread visited set for {@link #search}, reused across queries. Generation-stamped: a node
   * is visited this query iff {@code visited[node] == gen}, so a new query bumps {@code gen} and
   * clears nothing. ThreadLocal, so it stays correct when a segment is searched concurrently; a
   * fresh array reads as gen 0, and gen starts at 1 and only increases, so no stale hit.
   */
  private final ThreadLocal<int[]> visitedScratch;

  private final ThreadLocal<int[]> visitedGen = ThreadLocal.withInitial(() -> new int[] {0});

  /**
   * Counts queries that descended the graph.
   *
   * <p>Cell selection falls back to an exact scan of the centroid matrix when there is no graph
   * ({@code nlist == 1}) and under {@code ivfaster.flatSelect}, which is the reference the graph's
   * selection quality is validated against. Both paths are wanted, but the fallback is silent and
   * costs {@code O(nlist)} per query, so this counter makes "the graph was used" provable.
   */
  static final java.util.concurrent.atomic.AtomicLong descents =
      new java.util.concurrent.atomic.AtomicLong();

  private CentroidGraph(int nlist, int planeBytes, int stride, int entry, byte[] nodes) {
    this.nlist = nlist;
    this.coarseBytes = Nitrox2.PLANES * planeBytes;
    this.stride = stride;
    this.entry = entry;
    this.nodes = nodes;
    this.nodesSeg = MemorySegment.ofArray(nodes);
    final int n = nlist;
    this.visitedScratch = ThreadLocal.withInitial(() -> new int[n]);
  }

  /** Bytes per node record at this dimension: payload, inline degree, neighbours, 64 B aligned. */
  static int strideFor(int dim) {
    final int raw = Nitrox2.bytesPerVector(dim) + 2 + M * ORD_BYTES;
    return (raw + ALIGN - 1) / ALIGN * ALIGN;
  }

  int nlist() {
    return nlist;
  }

  int entryPoint() {
    return entry;
  }

  int stride() {
    return stride;
  }

  /**
   * Builds the graph by HNSW-style incremental insertion.
   *
   * <p>Nodes are inserted one at a time in a shuffled order, seeded so that one segment yields one
   * index. For each, a greedy search of the graph built so far, scored by the coarse code and then
   * ranked exactly, produces its candidate neighbours; those are diversity-pruned to {@code M}, and
   * back-edges are added with the standard shrink rule when a neighbour is at capacity. The exact
   * rank is the verify half of the cascade, so the descent itself only has to be directionally
   * right.
   *
   * <p>INSERTION RUNS IN PARALLEL over batches guarded by striped locks, the shape
   * HnswConcurrentMergeBuilder uses: each insertion is a descent plus a diversity prune, which
   * serially is the dominant build stage. Safe because a node's adjacency array is REPLACED rather
   * than mutated in place, so a reader sees one consistent version, and the locks serialize
   * read-modify-write on a single node's slot. Edge choice depends on interleaving, since a worker
   * descends whatever graph exists when it looks, which is the tradeoff Lucene accepts for
   * concurrent merge.
   *
   * <p>A SERIAL SEED PREFIX runs first. Every worker descends from the same entry point, so with no
   * seed each range's first insertion would reach a graph holding only that node, prune to the
   * entry point and link there, giving a hub instead of a neighbourhood.
   *
   * @param codes the centroid tier, which supplies both the coarse payloads and the exact distances
   * @param dim vector dimension
   */
  static CentroidGraph build(CentroidCodes codes, int dim) throws IOException {
    final int nlist = codes.nlist();
    final int planeBytes = codes.planeBytes();
    final int stride = strideFor(dim);

    // Adjacency under construction. Bounded at M, which the record layout fixes.
    final int[][] neighbours = new int[nlist][];
    for (int c = 0; c < nlist; c++) {
      neighbours[c] = EMPTY;
    }

    final int[] order = new int[nlist];
    for (int i = 0; i < nlist; i++) {
      order[i] = i;
    }
    final Random random = new Random(SEED);
    for (int i = nlist - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final int t = order[i];
      order[i] = order[j];
      order[j] = t;
    }

    final int efc = Math.max(EF_CONSTRUCTION, M + 1);

    final int entry = order[0];

    final Object[] locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
    // One worker's share of the insertion order; `idx` runs (from, to], so ranges tile it once.
    final Parallel.RangeTask insertRange =
        (from, to) -> {
          // Per-worker scratch: shared it would be a data race, per-insertion it would dominate.
          final byte[] wCode = new byte[codes.coarseBytes()];
          final int[] wVisited = new int[nlist];
          final boolean[] wSeen = new boolean[nlist];
          final long[] wFrontier = new long[Math.min(nlist, Math.max(64, efc * 4))];
          final long[] wBest = new long[efc];
          for (int idx = from + 1; idx <= to; idx++) {
            final int node = order[idx];
            final float[] vec = codes.centroidAt(node);
            Nitrox2.encode(vec, dim, wCode, 0);

            final int nVisited =
                descendPartial(
                    codes,
                    neighbours,
                    entry,
                    wCode,
                    stride,
                    efc,
                    wSeen,
                    wFrontier,
                    wBest,
                    wVisited,
                    node);

            final float[] vDist = sortByExactDistance(codes, vec, wVisited, nVisited);
            final int[] kept = pruneVerified(codes, vec, wVisited, vDist, nVisited, node);
            neighbours[node] = kept;
            for (int i = 0; i < kept.length; i++) {
              final int x = kept[i];
              final boolean force = i == 0;
              synchronized (locks[(x * 0x9E3779B9) >>> 1 & (LOCK_STRIPES - 1)]) {
                neighbours[x] = link(codes, neighbours, x, node, force);
              }
            }
          }
        };

    final int seed = SERIAL_GRAPH ? nlist - 1 : Math.min(nlist - 1, Math.max(64, M * 4));
    insertRange.run(0, seed);

    Parallel.overRange(
        nlist - 1 - seed,
        PARALLEL_INSERT_GRAIN,
        (from, to) -> insertRange.run(seed + from, seed + to));

    connect(neighbours, entry);

    // Serialize into the interleaved records.
    final byte[] nodes = new byte[nlist * stride];
    final int coarseBytes = codes.coarseBytes();
    for (int c = 0; c < nlist; c++) {
      final int base = c * stride;
      // The whole coarse code as one contiguous copy: same layout in the code table and the record.
      System.arraycopy(codes.coarsePlane(), c * coarseBytes, nodes, base, coarseBytes);
      final int degOff = base + coarseBytes;
      final int deg = Math.min(M, neighbours[c].length);
      nodes[degOff] = (byte) deg;
      nodes[degOff + 1] = (byte) (deg >>> 8);
      for (int i = 0; i < deg; i++) {
        final int off = degOff + 2 + i * ORD_BYTES;
        nodes[off] = (byte) neighbours[c][i];
        nodes[off + 1] = (byte) (neighbours[c][i] >>> 8);
      }
    }
    return new CentroidGraph(nlist, planeBytes, stride, entry, nodes);
  }

  private static final int[] EMPTY = new int[0];

  /**
   * Ensures every node is reachable from {@code entry}, attaching any that are not.
   *
   * <p>An unreachable node is attached by giving a node that is reachable an edge to it, appending
   * when that node has room and otherwise replacing its last neighbour. Replacing can in principle
   * orphan something else, so the traversal is repeated; each round strictly increases the
   * reachable set, so it terminates, and in practice one round suffices.
   *
   * <p>Each orphan is handed to the most recently seen reachable node, which keeps a round {@code
   * O(n)} with no search for a nearest host. The edge exists to make the node findable at all, and
   * the descent's exact rerank fixes ordering.
   */
  private static void connect(int[][] neighbours, int entry) {
    final int n = neighbours.length;
    final boolean[] reachable = new boolean[n];
    int reached = traverse(neighbours, entry, reachable);
    int guard = 0;
    while (reached < n && guard++ < 8) {
      int host = entry;
      for (int c = 0; c < n; c++) {
        if (reachable[c]) {
          host = c;
          continue;
        }
        neighbours[c] = appendUnique(neighbours[c], host);
        if (neighbours[host].length < M) {
          neighbours[host] = appendUnique(neighbours[host], c);
        } else {
          final int[] cur = neighbours[host].clone();
          cur[cur.length - 1] = c;
          neighbours[host] = cur;
        }
        reachable[c] = true;
        host = c;
      }
      java.util.Arrays.fill(reachable, false);
      reached = traverse(neighbours, entry, reachable);
    }
  }

  /** Appends {@code v} if absent and there is room, else returns the array unchanged. */
  private static int[] appendUnique(int[] a, int v) {
    for (int x : a) {
      if (x == v) {
        return a;
      }
    }
    if (a.length >= M) {
      return a;
    }
    final int[] out = java.util.Arrays.copyOf(a, a.length + 1);
    out[a.length] = v;
    return out;
  }

  /** Breadth-first traversal; marks reachable nodes and returns how many. */
  private static int traverse(int[][] neighbours, int entry, boolean[] reachable) {
    final int[] queue = new int[neighbours.length];
    int head = 0;
    int tail = 0;
    queue[tail++] = entry;
    reachable[entry] = true;
    int count = 1;
    while (head < tail) {
      for (int x : neighbours[queue[head++]]) {
        if (reachable[x] == false) {
          reachable[x] = true;
          count++;
          queue[tail++] = x;
        }
      }
    }
    return count;
  }

  /**
   * Greedy descent over the partially built graph, collecting every node visited.
   *
   * <p>Returns the visit count, with the nodes in {@code visited}. The caller prunes from the
   * VISITED set rather than the beam's survivors: the beam ranks by coarse code, so a node it
   * discarded may still be among the true nearest, and a candidate that never reaches the exact
   * stage cannot be recovered there. Every visited node was already scored, so returning them is
   * one array write apiece.
   */
  private static int descendPartial(
      CentroidCodes codes,
      int[][] neighbours,
      int entry,
      byte[] qCode,
      int stride,
      int ef,
      boolean[] seen,
      long[] frontier,
      long[] best,
      int[] visited,
      int self) {
    java.util.Arrays.fill(seen, false);
    int nVisited = 0;
    int frontierN = 0;
    int bestN = 0;

    final int d0 = coarseDistanceOf(codes, qCode, entry);
    seen[entry] = true;
    if (entry != self) {
      visited[nVisited++] = entry;
    }
    frontier[frontierN++] = ((long) d0 << 32) | entry;
    best[bestN++] = ((long) d0 << 32) | entry;

    while (frontierN > 0) {
      final long top = frontier[0];
      frontier[0] = frontier[--frontierN];
      siftDownMin(frontier, 0, frontierN);
      if (bestN == best.length && (int) (top >>> 32) > (int) (best[0] >>> 32)) {
        break;
      }
      for (int next : neighbours[(int) top]) {
        if (seen[next]) {
          continue;
        }
        seen[next] = true;
        if (next != self && nVisited < visited.length) {
          visited[nVisited++] = next;
        }
        final int dist = coarseDistanceOf(codes, qCode, next);
        final boolean improves = bestN < best.length || dist < (int) (best[0] >>> 32);
        if (improves == false) {
          continue;
        }
        if (bestN < best.length) {
          best[bestN++] = ((long) dist << 32) | next;
          siftUpMax(best, bestN - 1);
        } else {
          best[0] = ((long) dist << 32) | next;
          siftDownMax(best, 0, bestN);
        }
        if (frontierN < frontier.length) {
          frontier[frontierN++] = ((long) dist << 32) | next;
          siftUpMin(frontier, frontierN - 1);
        }
      }
    }
    return nVisited;
  }

  /** Coarse distance between a query's code and a centroid's, straight from the code table. */
  private static int coarseDistanceOf(CentroidCodes codes, byte[] qCode, int node) {
    final byte[] table = codes.coarsePlane();
    final int coarseBytes = codes.coarseBytes();
    final int off = node * coarseBytes;
    int dist = 0;
    for (int i = 0; i < coarseBytes; i++) {
      dist += Integer.bitCount((table[off + i] ^ qCode[i]) & 0xFF);
    }
    return dist;
  }

  /**
   * Insertion-sorts {@code cand[0..n)} by exact distance to {@code vec}, nearest first, and returns
   * the distances in the same order.
   *
   * <p>Returned because the prune needs exactly these values, and recomputing them there would be
   * one float dot per candidate per insertion.
   */
  private static float[] sortByExactDistance(CentroidCodes codes, float[] vec, int[] cand, int n) {
    final float[] dist = new float[n];
    for (int i = 0; i < n; i++) {
      dist[i] = codes.exactDistance(vec, cand[i]);
    }
    for (int i = 1; i < n; i++) {
      final float d = dist[i];
      final int c = cand[i];
      int j = i - 1;
      while (j >= 0 && dist[j] > d) {
        dist[j + 1] = dist[j];
        cand[j + 1] = cand[j];
        j--;
      }
      dist[j + 1] = d;
      cand[j + 1] = c;
    }
    return dist;
  }

  /**
   * The relative-neighbourhood prune over an already distance-ordered candidate list.
   *
   * <p>Keeps {@code x} only when it is nearer {@code node} than any neighbour already kept, which
   * drops edges that are shortcuts through an existing neighbour and leaves a spread of directions.
   */
  private static int[] pruneVerified(
      CentroidCodes codes, float[] vec, int[] cand, float[] dist, int n, int node) {
    if (n == 0) {
      return EMPTY;
    }
    final int[] kept = new int[Math.min(M, n)];
    int nKept = 0;
    for (int i = 0; i < n && nKept < kept.length; i++) {
      final int x = cand[i];
      if (x == node) {
        continue;
      }
      // Already computed by the sort, in this order.
      final float dcx = dist[i];
      boolean diverse = true;
      final float[] xVec = codes.centroidAt(x);
      for (int k = 0; k < nKept; k++) {
        if (codes.exactDistance(xVec, kept[k]) < dcx) {
          diverse = false;
          break;
        }
      }
      if (diverse) {
        kept[nKept++] = x;
      }
    }
    // A node with no neighbours would dead-end the descent, so fall back to the nearest candidate.
    if (nKept == 0) {
      for (int i = 0; i < n; i++) {
        if (cand[i] != node) {
          kept[nKept++] = cand[i];
          break;
        }
      }
    }
    return nKept == kept.length ? kept : java.util.Arrays.copyOf(kept, nKept);
  }

  /**
   * Adds the back-edge {@code x -> node}, applying the shrink rule at capacity.
   *
   * <p>At degree {@code M} the new edge replaces {@code x}'s farthest neighbour, by default only
   * when {@code node} is nearer, so a late insertion does not displace a closer neighbour. {@code
   * mustLink} forces the eviction: callers force the first back-edge of an insertion, since one
   * existing node pointing at the new one is what makes it reachable, and the candidate list is
   * distance-ordered so that edge is its nearest neighbour anyway.
   */
  private static int[] link(
      CentroidCodes codes, int[][] neighbours, int x, int node, boolean mustLink) {
    final int[] cur = neighbours[x];
    for (int y : cur) {
      if (y == node) {
        return cur;
      }
    }
    if (cur.length < M) {
      final int[] out = java.util.Arrays.copyOf(cur, cur.length + 1);
      out[cur.length] = node;
      return out;
    }
    final float[] xVec = codes.centroidAt(x);
    int worst = -1;
    float worstD = Float.NEGATIVE_INFINITY;
    for (int i = 0; i < cur.length; i++) {
      final float d = codes.exactDistance(xVec, cur[i]);
      if (d > worstD) {
        worstD = d;
        worst = i;
      }
    }
    if (mustLink || codes.exactDistance(xVec, node) < worstD) {
      final int[] out = cur.clone();
      out[worst] = node;
      return out;
    }
    return cur;
  }

  // ---- search ----

  /**
   * Greedy best-first descent, scoring hops with the coarse code.
   *
   * <p>The coarse tier is what makes a hop cheap, and why the payload sits in the record at all.
   * Its imprecision is absorbed by the beam keeping {@code ef} candidates rather than committing to
   * the single best, and by the caller ranking the survivors exactly, so the descent only has to be
   * directionally right.
   *
   * <p>Returns every node VISITED. The beam ranks by coarse code, so a node it evicted may still be
   * among the true nearest, and the caller's exact rerank can never see an evicted node. Each
   * visited node was already scored to decide whether it entered the beam, so returning them costs
   * one array write apiece and hands the exact stage a strictly larger candidate set. The beam
   * still governs where the descent goes.
   *
   * @param qCode query coarse code
   * @param ef beam width, which bounds the descent rather than the result
   * @param out receives every visited cell, unordered; the caller reranks them exactly
   * @return how many candidates were written
   */
  int search(byte[] qCode, int ef, int[] out) {
    return search(qCode, ef, out, null);
  }

  /**
   * As {@link #search(byte[], int, int[])}, also reporting each visited node's coarse distance.
   *
   * <p>The descent computes that distance for every node it visits in order to decide the beam.
   * Handing it back lets the caller narrow the visited set to a candidate prefix for free, instead
   * of fine-verifying all of it.
   *
   * <p>THE VISITED SET is a generation-stamped {@code int[]} reused per thread across queries: a
   * node is visited this query iff {@code visited[node] == gen}, so bumping {@code gen} clears the
   * set in {@code O(1)}.
   *
   * <p>THE HEAPS are a candidate heap (min by distance, to expand nearest first) and a result heap
   * (max by distance, to evict the worst), both packed as {@code (distance << 32 | node)}. The
   * frontier holds more than {@code cap} at once, since every improving neighbour is pushed and a
   * node is popped only when expanded, so pushes outpace pops. It is bounded rather than grown:
   * once it holds several times the beam width the extra entries are all worse than everything in
   * it, and the early exit reaches them only after the answer is settled, which keeps the search
   * allocation-free and its cost independent of graph shape. The selection-quality test is the
   * guard, since a bound that cost recall would drop the overlap-versus-exact-scan assertion.
   *
   * <p>TWO PASSES OVER EACH ADJACENCY LIST, so the coarse scoring goes through the BULK kernel,
   * which loads the loop-invariant query vectors once per expansion rather than once per neighbour.
   * Pass 1 marks visited and records the ordinal; pass 2 consumes the batched distances in the SAME
   * ORDER. Bit-identical to the fused loop: scoring reads only the query and the node payload,
   * never the beam, so deferring it cannot change a beam decision.
   *
   * <p>The per-expansion fan scratch is sized to {@code M}, the record's fixed adjacency bound, so
   * it never grows, and it is LOCAL, since several queries share one reader and so one instance,
   * where per-search scratch on the instance would be a data race.
   *
   * @param outDist parallel to {@code out}; receives the coarse distance of each visited node, or
   *     null
   */
  int search(byte[] qCode, int ef, int[] out, int[] outDist) {
    descents.incrementAndGet();
    final int cap = Math.min(Math.max(ef, 1), nlist);
    final int[] visited = visitedScratch.get();
    final int[] genBox = visitedGen.get();
    final int gen = ++genBox[0];
    int nOut = 0;
    final long[] frontier = new long[Math.min(nlist, Math.max(64, cap * 4))];
    int frontierN = 0;
    final long[] best = new long[cap];
    int bestN = 0;
    final int[] fanNodes = new int[M];
    final int[] fanOffsets = new int[M];
    final int[] fanDist = new int[M];
    final int[] fanOutIdx = new int[M];

    final int d0 = coarseDistance(qCode, entry);
    visited[entry] = gen;
    if (nOut < out.length) {
      if (outDist != null) {
        outDist[nOut] = d0;
      }
      out[nOut++] = entry;
    }
    frontier[frontierN++] = ((long) d0 << 32) | entry;
    best[bestN++] = ((long) d0 << 32) | entry;

    while (frontierN > 0) {
      // Pop the nearest unexpanded candidate.
      long top = frontier[0];
      frontier[0] = frontier[--frontierN];
      siftDownMin(frontier, 0, frontierN);
      final int node = (int) top;
      final int nodeDist = (int) (top >>> 32);
      // Everything left is farther than the worst kept result, so no expansion can improve it.
      if (bestN == cap && nodeDist > (int) (best[0] >>> 32)) {
        break;
      }
      final int base = node * stride;
      final int degOff = base + coarseBytes;
      final int deg = (nodes[degOff] & 0xFF) | ((nodes[degOff + 1] & 0xFF) << 8);
      // Pass 1: gather the unvisited neighbours.
      int fan = 0;
      for (int i = 0; i < deg; i++) {
        final int off = degOff + 2 + i * ORD_BYTES;
        final int next = (nodes[off] & 0xFF) | ((nodes[off + 1] & 0xFF) << 8);
        if (next >= nlist || visited[next] == gen) {
          continue;
        }
        // Marked in the gather, so a repeated ordinal within one adjacency list is scored once.
        visited[next] = gen;
        fanNodes[fan] = next;
        fanOffsets[fan] = next * stride;
        // Where this node landed in `out`, or -1 when it did not fit.
        fanOutIdx[fan] = nOut < out.length ? nOut : -1;
        fan++;
        // Recorded on VISIT, before the beam decides its coarse-code verdict.
        if (nOut < out.length) {
          out[nOut++] = next;
        }
      }
      if (fan == 0) {
        continue;
      }
      // One Hamming over exactly coarseBytes at each node offset, never into the adjacency bytes.
      hamming.bulkDistancesAtBytes(qCode, nodes, fanOffsets, coarseBytes, fan, fanDist);
      if (outDist != null) {
        for (int i = 0; i < fan; i++) {
          if (fanOutIdx[i] >= 0) {
            outDist[fanOutIdx[i]] = fanDist[i];
          }
        }
      }
      for (int i = 0; i < fan; i++) {
        final int next = fanNodes[i];
        final int dist = fanDist[i];
        final boolean improves = bestN < cap || dist < (int) (best[0] >>> 32);
        if (improves == false) {
          continue;
        }
        if (bestN < cap) {
          best[bestN++] = ((long) dist << 32) | next;
          siftUpMax(best, bestN - 1);
        } else {
          best[0] = ((long) dist << 32) | next;
          siftDownMax(best, 0, bestN);
        }
        if (frontierN < frontier.length) {
          frontier[frontierN++] = ((long) dist << 32) | next;
          siftUpMin(frontier, frontierN - 1);
        }
      }
    }

    return nOut;
  }

  /** Coarse distance from the query code to one node's payload. */
  private int coarseDistance(byte[] qCode, int node) {
    final long base = (long) node * stride;
    return hamming.distance(qCode, nodesSeg, base, coarseBytes);
  }

  // ---- persistence ----

  /** Writes the node records verbatim; the reader maps them and scores in place. */
  void write(IndexOutput out) throws IOException {
    out.writeVInt(nlist);
    out.writeVInt(stride);
    out.writeVInt(entry);
    out.writeBytes(nodes, 0, nodes.length);
  }

  /** Reads a graph written by {@link #write}. */
  static CentroidGraph read(RandomAccessInput in, int dim, long length) throws IOException {
    // The header is vints, so it is read through a sequential view.
    final org.apache.lucene.store.ByteArrayDataInput header;
    final byte[] head = new byte[Math.min(16, (int) length)];
    for (int i = 0; i < head.length; i++) {
      head[i] = in.readByte(i);
    }
    header = new org.apache.lucene.store.ByteArrayDataInput(head);
    final int nlist = header.readVInt();
    final int stride = header.readVInt();
    final int entry = header.readVInt();
    final int headerLen = header.getPosition();
    final byte[] nodes = new byte[nlist * stride];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = in.readByte(headerLen + i);
    }
    return new CentroidGraph(nlist, Nitrox2.planeBytes(dim), stride, entry, nodes);
  }

  /** Neighbours of a node; test support. */
  int[] neighboursOf(int node) {
    final int degOff = node * stride + coarseBytes;
    final int deg = (nodes[degOff] & 0xFF) | ((nodes[degOff + 1] & 0xFF) << 8);
    final int[] out = new int[deg];
    for (int i = 0; i < deg; i++) {
      final int off = degOff + 2 + i * ORD_BYTES;
      out[i] = (nodes[off] & 0xFF) | ((nodes[off + 1] & 0xFF) << 8);
    }
    return out;
  }

  // ---- packed (distance, node) heaps; distance in the high 32 bits ----

  private static void siftUpMin(long[] h, int i) {
    final long v = h[i];
    while (i > 0) {
      final int parent = (i - 1) >>> 1;
      if (h[parent] <= v) {
        break;
      }
      h[i] = h[parent];
      i = parent;
    }
    h[i] = v;
  }

  private static void siftDownMin(long[] h, int i, int size) {
    final long v = h[i];
    while (true) {
      int child = (i << 1) + 1;
      if (child >= size) {
        break;
      }
      final int right = child + 1;
      if (right < size && h[right] < h[child]) {
        child = right;
      }
      if (h[child] >= v) {
        break;
      }
      h[i] = h[child];
      i = child;
    }
    h[i] = v;
  }

  private static void siftUpMax(long[] h, int i) {
    final long v = h[i];
    while (i > 0) {
      final int parent = (i - 1) >>> 1;
      if (h[parent] >= v) {
        break;
      }
      h[i] = h[parent];
      i = parent;
    }
    h[i] = v;
  }

  private static void siftDownMax(long[] h, int i, int size) {
    final long v = h[i];
    while (true) {
      int child = (i << 1) + 1;
      if (child >= size) {
        break;
      }
      final int right = child + 1;
      if (right < size && h[right] > h[child]) {
        child = right;
      }
      if (h[child] <= v) {
        break;
      }
      h[i] = h[child];
      i = child;
    }
    h[i] = v;
  }
}
