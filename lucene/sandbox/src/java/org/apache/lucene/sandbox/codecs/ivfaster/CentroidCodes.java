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

import java.lang.foreign.MemorySegment;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * The centroid tier: every centroid's coarse code, and the routing primitive built on it.
 *
 * <p>Routing is a two stage cascade. Stage 1 Hamming-scans all {@code nlist} coarse codes and keeps
 * a shortlist; stage 2 ranks that shortlist by exact float distance. Its callers are document
 * routing during Lloyd, spill cell selection, centroid graph construction, and the Reaper.
 *
 * <h2>Why the scan is exhaustive</h2>
 *
 * <p>Assignment is a write-time decision. An exhaustive scan keeps it independent of the centroid
 * graph's parameters ({@code M}, {@code efConstruction}, {@code ef}), which are search-time
 * choices, and its candidate set contains every beam's, so no beam can find a nearer cell. Cost is
 * {@code O(nlist * dim)} per document per Lloyd iteration.
 *
 * <h2>Tiling</h2>
 *
 * <p>The scan is blocked over centroids: one tile of coarse codes stays resident in L2 while every
 * document of a batch is scored against it, so RAM traffic is one pass over the code table per
 * batch instead of one per document.
 *
 * <p>Residency condition: {@code TILE * coarseBytes <= L2}. At dim=1024, {@code coarseBytes = 256}
 * and {@code TILE = 512} give a 128 KB tile, which a 1 MB L2 holds along with the document batch
 * and the output arrays. The whole table, {@code nlist * coarseBytes}, fits that L2 only up to
 * {@code nlist = 4096}, so tiling is what holds residency at any {@code nlist}.
 *
 * <h2>Distances</h2>
 *
 * <p>Every distance here means "smaller is nearer": squared Euclidean for EUCLIDEAN, negated dot
 * for the inner product family, so every comparison is a plain {@code <}. The negated dot is
 * negative, so margin tests take the difference form {@code d2 - d1 <= (margin - 1) * |d1|}; the
 * ratio {@code d2/d1} is not monotone across zero. Routing reports the nearest cell and the runner
 * up, the pair that spill selection and the Reaper's boundary test read.
 *
 * @lucene.experimental
 */
final class CentroidCodes {

  /** Centroids per scan tile. {@code TILE * coarseBytes} must fit L2; see the class javadoc. */
  static final int TILE = 512;

  private final int dim;
  private final int nlist;

  /**
   * Coarse codes, one contiguous {@code coarseBytes} record per centroid, the thermometer planes
   * concatenated. Same layout in memory, on disk, and in the graph node record: Hamming distance is
   * additive over the concatenation, so a coarse distance is one XOR+popcount over the whole
   * record. See {@link Nitrox2#encode}.
   */
  private final byte[] coarse;

  /** Bytes per centroid: the whole coarse code, and the Hamming kernel's row stride. */
  private final int coarseBytes;

  /** Bytes per plane. */
  private final int planeBytes;

  /** The centroid vectors, for the Lloyd mean, for encoding, and for exact distances. */
  private final float[][] centroids;

  /**
   * Centroid fine codes, one per centroid, in the field's configured fine encoding ({@link #fine}).
   *
   * <p>Read by {@link #rankCandidates}, the reader's cell rerank after a graph descent, where a
   * fine score is what ranks. {@link #route} orders its shortlist by exact distance and never reads
   * these, so they are built only when a fine quantizer is supplied; the build path passes {@code
   * null} and stays coarse only.
   */
  private final byte[] fineCodes;

  /** Per-centroid corrections for {@link #fineCodes}, four floats each. */
  private final float[][] fineCorrections;

  /** Bytes of fine code per centroid. */
  private final int fineBytes;

  /** The mean the fine codes are centred on, when the fine tier centres; {@code null} otherwise. */
  private final float[] fineMean;

  /**
   * The fine tier: the same {@link FineQuantizer} the documents use, so centroids are encoded and
   * scored symmetrically with documents. {@code null} on the build path.
   */
  private final FineQuantizer fine;

  /** Whether a fine tier is present and can represent this dimension; see the constructor. */
  private final boolean useFine;

  /** Whether {@link #fine} scores centred vectors, so the centroid mean is maintained. */
  private final boolean needsFineMean;

  /** Segment view of the coarse code table, for the Hamming kernel, which reads segments. */
  private final MemorySegment coarseSeg;

  private final HammingKernel hamming = HammingKernel.get();

  /**
   * A fine tier need not represent every dimension, since a bit-sliced code needs whole 64-bit
   * words. Where it does not, or where none is supplied, ranking falls back to the exact float dot.
   *
   * @param fine the field's configured fine quantizer, or {@code null} for a coarse-only instance
   *     (the build path, which ranks by exact float dot)
   */
  CentroidCodes(float[][] centroids, int dim, VectorSimilarityFunction sim, FineQuantizer fine) {
    this.dim = dim;
    this.nlist = centroids.length;
    this.centroids = centroids;
    this.planeBytes = Nitrox2.planeBytes(dim);
    this.coarseBytes = Nitrox2.bytesPerVector(dim);
    this.coarse = new byte[nlist * coarseBytes];
    this.coarseSeg = MemorySegment.ofArray(coarse);
    this.fine = fine;
    this.useFine = fine != null && fine.supports(dim);
    this.needsFineMean = useFine && fine.needsMean();
    this.fineBytes = useFine ? fine.codeBytes(dim) : 0;
    this.fineCodes = useFine ? new byte[nlist * fineBytes] : null;
    this.fineCorrections = useFine ? new float[nlist][] : null;
    this.fineMean = needsFineMean ? new float[dim] : null;
    encodeAll();
  }

  /**
   * Re-encodes every centroid from its current vector: {@code nlist x dim} work.
   *
   * <p>A code is a function of its centroid vector, so every Lloyd mean update invalidates all of
   * them.
   *
   * <p>The fine half runs only where a fine quantizer was supplied, which is the reader's instance;
   * the build path encodes coarse codes alone.
   *
   * <p>Where the fine tier centres, the reference is the mean of the centroids, which plays the
   * role the document mean plays for document codes and is recomputed here along with the codes.
   * Tiers that do not centre carry no mean.
   */
  void encodeAll() {
    if (useFine == false) {
      for (int c = 0; c < nlist; c++) {
        Nitrox2.encode(centroids[c], dim, coarse, c * coarseBytes);
      }
      return;
    }
    if (needsFineMean) {
      java.util.Arrays.fill(fineMean, 0f);
      for (int c = 0; c < nlist; c++) {
        final float[] v = centroids[c];
        for (int d = 0; d < dim; d++) {
          fineMean[d] += v[d];
        }
      }
      for (int d = 0; d < dim; d++) {
        fineMean[d] /= nlist;
      }
    }

    final byte[] code = new byte[fineBytes];
    final float[] centred = needsFineMean ? new float[dim] : null;
    for (int c = 0; c < nlist; c++) {
      final float[] v = centroids[c];
      Nitrox2.encode(v, dim, coarse, c * coarseBytes);
      final float[] toEncode;
      if (needsFineMean) {
        for (int d = 0; d < dim; d++) {
          centred[d] = v[d] - fineMean[d];
        }
        toEncode = centred;
      } else {
        toEncode = v;
      }
      fineCorrections[c] = new float[4];
      fine.encode(toEncode, dim, fineMean, code, fineCorrections[c]);
      System.arraycopy(code, 0, fineCodes, c * fineBytes, fineBytes);
    }
  }

  /**
   * Scores {@code count} candidate centroids into {@code out} as distances, smaller nearer, through
   * the fine tier.
   *
   * <p>For a caller that already holds a candidate list, such as the reader's cell rerank after a
   * graph descent.
   */
  void rankCandidates(float[] vector, int[] cands, int count, float[] out) {
    if (useFine == false) {
      for (int i = 0; i < count; i++) {
        out[i] = exactDistance(vector, cands[i]);
      }
      return;
    }
    if (rankScratch == null) {
      rankScratch = new VerifyScratch();
    }
    if (rankState == null || rankState.reset(vector, fineMean) == false) {
      rankState = prepareFine(vector);
    }
    verifyFine(rankState, cands, count, rankScratch);
    for (int i = 0; i < count; i++) {
      // The state reports raw dots, so the distance is their negation.
      out[i] = -rankScratch.scores[i];
    }
  }

  private VerifyScratch rankScratch;
  private FineQuantizer.QueryState rankState;

  /**
   * Per-query state for the fine tier, reusable across every candidate of one scan.
   *
   * <p>Configured to report raw dots, since this class ranks by distance and {@code -similarity} is
   * the same ordering on another scale.
   */
  FineQuantizer.QueryState prepareFine(float[] vector) {
    if (useFine == false) {
      return null;
    }
    final FineQuantizer.QueryState state =
        fine.prepareQuery(vector, dim, fineMean, VectorSimilarityFunction.DOT_PRODUCT);
    state.reportRawDots();
    return state;
  }

  /**
   * Scores {@code count} candidate centroids with the fine tier.
   *
   * <p>Bulk: the per-candidate cost is dominated by preparing the query side of the inner loop,
   * which is shared across the batch.
   *
   * <p>Scored in place out of the flat table. {@code scoreBulkAt} takes the table plus one byte
   * offset per candidate, so no candidate's code is copied, and the corrections are references, so
   * collecting those is a pointer copy.
   */
  void verifyFine(FineQuantizer.QueryState state, int[] cands, int count, VerifyScratch scratch) {
    scratch.ensure(count, fineBytes);
    final int[] offsets = scratch.offsets;
    for (int i = 0; i < count; i++) {
      offsets[i] = cands[i] * fineBytes;
      scratch.corr[i] = fineCorrections[cands[i]];
    }
    state.scoreBulkAt(
        fineCodes, offsets, count, fineBytes, scratch.corr, scratch.scores, scratch.recs);
  }

  /** Reusable buffers for {@link #verifyFine}, so a scan allocates nothing per candidate. */
  static final class VerifyScratch {
    byte[][] recs = new byte[0][];
    float[][] corr = new float[0][];
    float[] scores = new float[0];

    /** Byte offset of each candidate's code within the flat table; see {@link #verifyFine}. */
    int[] offsets = new int[0];

    /**
     * Grows every buffer to {@code count}.
     *
     * <p>{@code corr}, {@code scores} and {@code offsets} share one length, because callers index
     * all of them by candidate. The guard is {@code offsets}, since {@code recs} stays empty on the
     * {@code scoreBulkAt} path.
     *
     * <p>The {@code recs} rows serve a QueryState that does not override {@code scoreBulkAt} and
     * gathers instead, and are allocated to match so that path stays correct.
     */
    void ensure(int count, int fineBytes) {
      if (offsets.length >= count) {
        return;
      }
      final byte[][] grownRecs = new byte[count][];
      System.arraycopy(recs, 0, grownRecs, 0, recs.length);
      for (int i = recs.length; i < count; i++) {
        grownRecs[i] = new byte[fineBytes];
      }
      recs = grownRecs;
      corr = new float[count][];
      scores = new float[count];
      offsets = new int[count];
    }
  }

  int nlist() {
    return nlist;
  }

  /** The coarse code table; centroid {@code c}'s code starts at {@code c * coarseBytes()}. */
  byte[] coarsePlane() {
    return coarse;
  }

  int planeBytes() {
    return planeBytes;
  }

  int coarseBytes() {
    return coarseBytes;
  }

  /** Centroid {@code c}'s float vector, for exact comparisons during graph construction. */
  float[] centroidAt(int c) {
    return centroids[c];
  }

  /**
   * One vector's routing result: the nearest {@code count} cells with their exact distances,
   * nearest first, plus the top two distances {@code d1}, {@code d2} and the runner-up cell {@code
   * cell2}.
   *
   * <p>The runner-up's identity is reported because the Reaper's movement bound is per pair: it
   * bounds by the displacements of the two centroids that could trade places. Spill selection reads
   * the same pair.
   *
   * <p>{@code cell2 = -1} and {@code d2 =} {@link Float#MAX_VALUE} when there is no runner up.
   */
  static final class Routing {
    int[] cells;

    /** How many entries of {@link #cells} are valid. */
    int count;

    float[] dists;

    float d1;

    int cell2;

    float d2;

    Routing(int capacity) {
      this.cells = new int[capacity];
      this.dists = new float[capacity];
    }
  }

  /** Reusable per-thread scratch, so the scan allocates nothing per document. */
  static final class Scratch {
    final int[] coarseDist;
    final long[] heap;
    final int[] verifyCells;
    final float[] verifyDist;
    final int[] verifyIds;

    /** The query's coarse code, Hammed against the code table. */
    final byte[] qCode;

    Scratch(int dim, int nlist, int shortlist) {
      this.coarseDist = new int[TILE];
      this.heap = new long[shortlist];
      this.verifyCells = new int[shortlist];
      this.verifyDist = new float[shortlist];
      this.verifyIds = new int[shortlist];
      this.qCode = new byte[Nitrox2.bytesPerVector(dim)];
    }
  }

  /**
   * Routes one rotated vector: coarse-scans every centroid, keeps the best {@code shortlist},
   * orders those by exact distance, and reports the nearest {@code keep}.
   *
   * <p>{@code shortlist} is the oversample. Once it is a few times {@code keep}, the coarse tier
   * retains essentially all of the true nearest centroids, so accuracy is that of the exact stage
   * at the cost of the coarse one.
   *
   * @param vector the rotated vector to route
   * @param shortlist coarse survivors to rank exactly; clamped to {@code nlist}
   * @param keep how many ranked cells to report, nearest first
   * @param out receives the result
   *     <p>Derives the coarse code here. The build path packs every document's code once ({@link
   *     DocPlanes}) and calls {@link #routePacked} instead.
   * @param scratch per-thread scratch, sized for at least {@code shortlist}
   */
  void route(float[] vector, int shortlist, int keep, Routing out, Scratch scratch) {
    Nitrox2.encode(vector, dim, scratch.qCode, 0);
    routePacked(vector, shortlist, keep, out, scratch);
  }

  /**
   * As {@link #route}, with this vector's coarse code already in {@code scratch.qCode}.
   *
   * <p>A document's coarse code is a fixed function of its rotated vector, and documents do not
   * move between Lloyd iterations, so the build path packs all codes once ({@link DocPlanes}) and
   * copies the record in for each routing, reap and spill pass.
   *
   * <p>Stage 1 is a tiled coarse scan over every centroid, keeping the best {@code want} in a
   * max-heap keyed on (distance, cell) packed into one long, so the scan streams a single array.
   * Stage 2 orders those survivors by exact distance with an insertion sort, since the shortlist is
   * tens of entries and the output is wanted in order.
   *
   * <p>NO FINE-TIER CALL: the stage 2 sort is exact, so a fine score would be computed and
   * discarded. The fine tier's place in this codec is the reader's cell rerank ({@link
   * #rankCandidates}), where its score is what ranks.
   *
   * <p>ORDER AND DISTANCES COME FROM ONE METRIC. Were the sort keyed on the fine tier while {@code
   * d1}/{@code d2} came from {@link #exactDistance}, the two could disagree on near ties and report
   * {@code d2 < d1}, inverting the pair the Reaper reads. The exact distance therefore orders the
   * whole shortlist, and {@code d1}/{@code d2} are taken from that sort.
   */
  void routePacked(float[] vector, int shortlist, int keep, Routing out, Scratch scratch) {
    final int want = Math.min(shortlist, nlist);

    final long[] heap = scratch.heap;
    int heapSize = 0;
    int worst = Integer.MAX_VALUE;

    for (int base = 0; base < nlist; base += TILE) {
      final int rows = Math.min(TILE, nlist - base);
      hammingTile(scratch, base, rows);
      final int[] cd = scratch.coarseDist;
      for (int r = 0; r < rows; r++) {
        final int dist = cd[r];
        if (heapSize < want) {
          heap[heapSize++] = ((long) dist << 32) | (base + r);
          if (heapSize == want) {
            heapify(heap, heapSize);
            worst = (int) (heap[0] >>> 32);
          }
        } else if (dist < worst) {
          heap[0] = ((long) dist << 32) | (base + r);
          siftDown(heap, 0, heapSize);
          worst = (int) (heap[0] >>> 32);
        }
      }
    }

    final int[] cells = scratch.verifyCells;
    final float[] dists = scratch.verifyDist;
    for (int i = 0; i < heapSize; i++) {
      scratch.verifyIds[i] = (int) heap[i];
    }
    int n = 0;
    for (int i = 0; i < heapSize; i++) {
      final int c = scratch.verifyIds[i];
      final float d = exactDistance(vector, c);
      int j = n++;
      while (j > 0 && dists[j - 1] > d) {
        dists[j] = dists[j - 1];
        cells[j] = cells[j - 1];
        j--;
      }
      dists[j] = d;
      cells[j] = c;
    }

    final int k = Math.min(keep, n);
    if (out.cells.length < k) {
      out.cells = new int[k];
      out.dists = new float[k];
    }
    System.arraycopy(cells, 0, out.cells, 0, k);
    System.arraycopy(dists, 0, out.dists, 0, k);
    out.count = k;
    out.cell2 = n > 1 ? cells[1] : -1;
    out.d1 = n > 0 ? dists[0] : Float.MAX_VALUE;
    out.d2 = n > 1 ? dists[1] : Float.MAX_VALUE;
  }

  /**
   * Coarse-scores {@code rows} consecutive centroids from {@code base} into the scratch: one
   * Hamming over each centroid's whole {@code coarseBytes} record. See {@link #coarse}.
   */
  private void hammingTile(Scratch scratch, int base, int rows) {
    final long off = (long) base * coarseBytes;
    hamming.bulkDistances(scratch.qCode, coarseSeg, off, coarseBytes, rows, scratch.coarseDist);
  }

  /**
   * Exact distance from a rotated vector to centroid {@code c}, smaller nearer.
   *
   * <p>Every vector here is unit norm, since the writer normalizes documents and centroids and the
   * reader normalizes the query. So {@code ||q - c||^2 = 2 - 2*dot(q, c)}, and {@code -dot} is an
   * affine decreasing function of that: one expression ranks identically for every similarity this
   * codec supports, with no per-similarity branch and no norms to carry. The value is negative,
   * which is why margin tests take a difference; see the class javadoc.
   */
  float exactDistance(float[] vector, int c) {
    return -VectorUtil.dotProduct(vector, centroids[c]);
  }

  /**
   * The exact nearest centroid over all {@code nlist}, with no coarse filter, so the result is the
   * true argmin.
   *
   * <p>{@code O(nlist * dim)} per vector: a final-pass or boundary-only tool for placements that
   * must be exact rather than shortlist-limited.
   */
  int nearestExact(float[] vector) {
    int best = 0;
    float bestDist = Float.MAX_VALUE;
    for (int c = 0; c < nlist; c++) {
      final float d = exactDistance(vector, c);
      if (d < bestDist) {
        bestDist = d;
        best = c;
      }
    }
    return best;
  }

  /**
   * The margin test that spill selection and the Reaper's boundary test share:
   *
   * <p>{@code d2 - d1 <= (margin - 1) * |d1|}
   *
   * <p>For positive distances this is {@code d2 <= margin * d1}. The difference form against {@code
   * |d1|} stays well defined for the negated dot, where distances are negative and the ratio is not
   * monotone across zero.
   */
  static boolean withinMargin(float d1, float d2, float margin) {
    if (d2 == Float.MAX_VALUE) {
      return false;
    }
    return d2 - d1 <= (margin - 1f) * Math.abs(d1);
  }

  // ---- max-heap over packed (distance, cell) longs; distance in the high 32 bits ----

  private static void heapify(long[] heap, int size) {
    for (int i = (size >>> 1) - 1; i >= 0; i--) {
      siftDown(heap, i, size);
    }
  }

  private static void siftDown(long[] heap, int i, int size) {
    final long value = heap[i];
    while (true) {
      int child = (i << 1) + 1;
      if (child >= size) {
        break;
      }
      final int right = child + 1;
      if (right < size && heap[right] > heap[child]) {
        child = right;
      }
      if (heap[child] <= value) {
        break;
      }
      heap[i] = heap[child];
      i = child;
    }
    heap[i] = value;
  }
}
