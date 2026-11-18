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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IORunnable;

/**
 * Builds a segment's cells: where its clustering starts, and the Lloyd iterations from there.
 *
 * <p>Every clustered segment of a field trains the same {@code nlist} cells, so any such segment's
 * centroids can seed another's clustering; a segment too small to cluster is written as one cell
 * ({@link SegmentIVFVectorsFormat#MIN_VECTORS_PER_CELL}) and never seeds or is seeded. A segment
 * seeded from another inherits its <em>lineage</em>, the name of the segment that started the
 * chain. Centroids of one lineage correspond cell for cell, so they can be averaged and their
 * vectors can keep their cells; centroids of different lineages cannot. That gives three ways a
 * segment's clustering starts:
 *
 * <ol>
 *   <li><b>A new segment</b> ({@link #planNewSegment}): a flush starts from the remembered lineage
 *       holding the most vectors, its centroids averaged cell by cell; with nothing remembered it
 *       starts cold, from sampled vectors, and begins a new lineage.
 *   <li><b>A merge, same lineage</b> ({@link MergePlanner}): sources of the donor's lineage carry
 *       their vectors' cells into the merged segment, and their cells' live members weight the
 *       averaged seed centroids, so Lloyd starts almost converged.
 *   <li><b>A merge, different lineage</b>: sources of any other lineage, or written by another
 *       process, only contribute vectors, which are routed to the seed's nearest cells from
 *       scratch.
 * </ol>
 *
 * <p>From there {@link #cluster} runs balanced spherical k-means: Lloyd iterations over unit
 * vectors, each moving every centroid to the normalized mean of its members and reassigning vectors
 * to their nearest centroid, with a cell's size counted against it ({@link #BALANCE_WEIGHT}). Plain
 * k-means on high-dimensional embeddings grows a few hub cells near the data's mean direction, each
 * holding many vectors that are only narrowly nearer to it; a query probing them scans far more
 * than one probing ordinary cells. Movement bounds skip vectors whose nearest-cell decision cannot
 * have changed: a vector can only change cell if its centroids moved by more than the gap between
 * its nearest and second-nearest distance. Iteration stops after {@link #MAX_ITERS} or once fewer
 * than {@link #CONVERGE_FRACTION} of the vectors change cell. The final pass also gives each vector
 * near a boundary up to {@code spillBits} extra cells, chosen by SOAR loss.
 */
final class Clustering {
  static final long SEED = 42L;

  /**
   * Centroid sums accumulate as fixed-point longs: integer addition is associative, so the result
   * does not depend on how the work was split across threads.
   */
  static final long FIXED_POINT_SCALE = 1L << 30;

  /**
   * The most centroid updates a clustering runs; a final reassignment follows the last one. With a
   * HotStart seed clustering converges in a few passes (on a live index no clustering went past 10,
   * and the largest merges stopped at 4-6), so the cap only binds on cold starts of large segments,
   * which the writer's infoStream reports ({@link Result#hitCap}).
   */
  static final int MAX_ITERS = 10;

  /**
   * A vector spills when its second-nearest distance is within this ratio of its nearest. A wider
   * margin puts more boundary vectors in both cells, so each probed cell holds more true neighbors
   * at the cost of more slots to scan: at 1M 4096-dimension vectors and the default rerank depth,
   * 1.10 instead of 1.05 raised recall@100 from 0.940 to 0.952 at 96 probes, for 2.7% more index
   * and 13% more scan time.
   */
  static final float SPILL_MARGIN = 1.10f;

  /** Nearest and second-nearest distances within this ratio are too close to trust the bounds. */
  static final float TIE_MARGIN = 1.02f;

  /** Weight of the parallel residual in the SOAR spill loss. */
  static final float SOAR_LAMBDA = 1f;

  /** Clustering stops early once a pass moves fewer than this fraction of the vectors. */
  static final float CONVERGE_FRACTION = 0.005f;

  /**
   * The size penalty of assignment: a vector takes whichever of its two nearest cells minimizes
   * {@code distance + BALANCE_WEIGHT * size / meanSize}, with sizes as of the pass's start.
   * Distances are negative cosines, so this adds 0.05 to a cell a hundred times the mean, enough to
   * move its boundary vectors out, and next to nothing to a cell of ordinary size. At 20M
   * 4096-dimension vectors in 1M-vector segments it shrank the largest cells from up to 98k slots
   * to about 11k, with recall unchanged and spill as effective (1.53 slots per vector either way):
   * the 90th-percentile query latency fell from 17 ms to 5.6 ms.
   */
  static final float BALANCE_WEIGHT = 0.0005f;

  private Clustering() {}

  /**
   * Where a segment's clustering starts: {@code seed} centroids, or null to start cold; {@code
   * warm}, the cells merged vectors carry, or null; the {@code lineage} the segment joins; and, for
   * logging, the {@code donor} segment and its vectors, or "cold".
   */
  record Plan(float[][] seed, WarmState warm, String lineage, String donor) {
    static Plan cold(String segment) {
      return new Plan(null, null, segment, "cold");
    }
  }

  /**
   * The cells carried into a merge from its source segments: {@code assignment} is a vector's
   * previous primary cell, or -1 when it has none and is routed.
   */
  record WarmState(int[] assignment) {}

  /**
   * Final centroids and cells. Each vector owns {@code stride} consecutive entries of {@code
   * cells}: its primary cell, then its spill cells, then -1 padding. {@code passes} is the number
   * of centroid updates run and {@code movedFraction} the fraction of vectors the last of them
   * moved, for logging.
   */
  record Result(
      float[][] centroids,
      int[] cells,
      int stride,
      int[] assignment,
      int passes,
      float movedFraction) {

    /** A result with no clustering statistics, as tests build one. */
    Result(float[][] centroids, int[] cells, int stride, int[] assignment) {
      this(centroids, cells, stride, assignment, 0, 0f);
    }

    /** Whether clustering stopped at {@link #MAX_ITERS} rather than converging. */
    boolean hitCap() {
      return passes >= MAX_ITERS && movedFraction > CONVERGE_FRACTION;
    }

    /** The number of cells, primary and spill, that vector {@code i} is written to. */
    int cellCount(int i) {
      int n = 0;
      while (n < stride && cells[i * stride + n] >= 0) {
        n++;
      }
      return n;
    }

    /** Vector {@code i}'s {@code k}-th cell; {@code k == 0} is its primary. */
    int cell(int i, int k) {
      return cells[i * stride + k];
    }
  }

  // ---- 1. A new segment.

  /**
   * The plan for flushing segment {@code segment}: seeded from the remembered donor lineage when
   * there is one, else cold, beginning a lineage named after the segment.
   */
  static Plan planNewSegment(Directory dir, String segment, FieldInfo info, int nlist)
      throws IOException {
    HotStart.Seed donor = HotStart.seed(dir, segment, info, nlist);
    if (donor == null) {
      return Plan.cold(segment);
    }
    String description = "segment=" + donor.segment() + " vectors=" + donor.vectors();
    return new Plan(donor.centroids(), null, donor.lineage(), description);
  }

  // ---- 2 and 3. A merge.

  /**
   * Plans a merge. Each source segment written by a compatible SegmentIVF configuration offers its
   * centroids; the donor is chosen among them by {@link HotStart#donor}. As merged vectors are
   * staged, {@link #record} notes the cells of those from the donor's lineage (pathway 2); the
   * others are left to be routed (pathway 3).
   */
  static final class MergePlanner {
    private final String segment;
    private final IVFFieldReader[] fields;
    private final HotStart.Source[] sources;
    private final int donor;
    private final int[][] primaryCells;
    private int[] primary = new int[0];
    private int count;

    /**
     * @param fields each source's field, or null for a source that is not a compatible SegmentIVF
     *     segment
     * @param remembered each source's remembered clustering, or null
     * @param liveVectors each source's vectors that survive the merge
     */
    MergePlanner(
        String segment,
        int nlist,
        IVFFieldReader[] fields,
        HotStart.Seed[] remembered,
        int[] liveVectors) {
      this.segment = segment;
      this.fields = fields;
      int readers = fields.length;
      sources = new HotStart.Source[readers];
      for (int r = 0; r < readers; r++) {
        if (fields[r] != null) {
          String lineage = remembered[r] == null ? null : remembered[r].lineage();
          sources[r] = new HotStart.Source(fields[r].centroids, null, lineage, liveVectors[r]);
        }
      }
      donor = HotStart.donor(sources, nlist);
      primaryCells = new int[readers][];
      for (int r = 0; donor >= 0 && r < readers; r++) {
        if (HotStart.sameLineage(sources[r], sources[donor]) == false) {
          continue;
        }
        // Members are counted afresh from the vectors that survive the merge.
        HotStart.Source source = sources[r];
        sources[r] =
            new HotStart.Source(
                source.centroids(), new int[nlist], source.lineage(), source.vectors());
        if (remembered[r] == null) {
          continue;
        }
        int[] assignment = remembered[r].assignment();
        if (assignment != null && assignment.length == fields[r].count) {
          primaryCells[r] = assignment;
        }
      }
    }

    /** Records the next {@code n} merged vectors, from {@code sourceOf} readers' {@code ordOf}. */
    void record(int[] sourceOf, int[] ordOf, int n) throws IOException {
      if (donor < 0) {
        return;
      }
      primary = ArrayUtil.grow(primary, count + n);
      for (int j = 0; j < n; j++, count++) {
        int r = sourceOf[j];
        int[] members = sources[r] == null ? null : sources[r].members();
        if (members == null) {
          // Pathway 3: another lineage, routed from scratch.
          primary[count] = -1;
          continue;
        }
        // Pathway 2: the remembered cell if there is one, else the primary cell on disk.
        int ord = ordOf[j];
        int[] rememberedPrimary = primaryCells[r];
        int cell;
        if (rememberedPrimary != null && ord < rememberedPrimary.length) {
          cell = rememberedPrimary[ord];
        } else {
          cell = fields[r].primaryCell(ord);
        }
        if (cell < 0 || cell >= members.length) {
          // A remembered cell of another cell count: route it instead.
          primary[count] = -1;
          continue;
        }
        primary[count] = cell;
        members[cell]++;
      }
    }

    /**
     * The merge's plan: cold when no source can donate; else the donor lineage's averaged
     * centroids, with the cells the donor's lineage carries.
     */
    Plan plan() {
      if (donor < 0) {
        return Plan.cold(segment);
      }
      WarmState warm = new WarmState(ArrayUtil.copyOfSubArray(primary, 0, count));
      String lineage = sources[donor].lineage();
      float[][] seed = HotStart.weightedCentroids(sources, donor);
      String description = "source=" + donor + " vectors=" + sources[donor].vectors();
      return new Plan(seed, warm, lineage == null ? segment : lineage, description);
    }
  }

  // ---- Lloyd iterations.

  /** Clusters the staged vectors into {@code nlist} cells, starting from {@code plan}. */
  static Result cluster(
      SegmentIVFVectorsWriter.StagedVectors vectors,
      int nlist,
      Plan plan,
      int spillBits,
      IORunnable checkAborted)
      throws IOException {
    State state = new State(vectors, nlist, plan.seed(), spillBits);
    state.routeAll(plan.seed() == null ? null : plan.warm());
    int convergeAt = (int) (CONVERGE_FRACTION * state.count);
    int passes = 0;
    int moved;
    do {
      // A pass over a large segment takes seconds and writes nothing, so a rollback, which aborts
      // running merges, would otherwise wait for every pass to finish. Flushes are not aborted.
      checkAborted.run();
      state.updateCentroids();
      moved = state.reassign(false);
      passes++;
    } while (moved > convergeAt && passes < MAX_ITERS);
    // No centroid update after the final pass, so the written assignments match the centroids.
    state.reassign(true);
    // Vectors the final pass skipped, or all of them without spilling, have only a primary.
    for (int i = 0; i < state.count; i++) {
      if (state.cells[i * state.stride] < 0) {
        state.cells[i * state.stride] = state.assignment[i];
      }
    }
    float movedFraction = state.count == 0 ? 0f : (float) moved / state.count;
    return new Result(
        state.centroids, state.cells, state.stride, state.assignment, passes, movedFraction);
  }

  /**
   * The mutable state of one clustering run. Per-vector arrays are indexed by staged ordinal and
   * per-cell arrays by cell. {@code centroids} is shared with {@code codes}, which re-encodes it
   * after every update.
   */
  private static final class State {
    final SegmentIVFVectorsWriter.StagedVectors vectors;
    final int count;
    final int dim;
    final int nlist;
    final int spillBits;
    final int stride;
    final float[][] centroids;
    final CentroidCodes codes;
    final int[] assignment;
    final int[] cells;
    final int[] memberCount;
    final float[] nearestDist;
    final float[] secondDist;
    final float[] movement;

    /**
     * Movement since a vector was last routed. {@code gapSlack} bounds how much its nearest to
     * second-nearest gap can have shrunk, and {@code maxMoveSlack} how much any other centroid can
     * have approached it.
     */
    final float[] gapSlack;

    final float[] maxMoveSlack;

    /** Cell sizes at the start of the current pass, which the size penalty counts. */
    int[] passSizes;

    float meanSize;

    final long[] fixedPointSums;
    final Object[] locks;

    State(SegmentIVFVectorsWriter.StagedVectors vectors, int nlist, float[][] seed, int spillBits)
        throws IOException {
      this.vectors = vectors;
      this.nlist = nlist;
      this.spillBits = spillBits;
      count = vectors.count;
      dim = vectors.dim;
      stride = 1 + spillBits;
      if (seed == null) {
        centroids = sampleCentroids();
      } else {
        if (seed.length != nlist) {
          throw new IllegalArgumentException(
              "seed has " + seed.length + " centroids but nlist is " + nlist);
        }
        centroids = new float[nlist][];
        for (int c = 0; c < nlist; c++) {
          centroids[c] = ArrayUtil.copyOfSubArray(seed[c], 0, dim);
        }
      }
      for (float[] centroid : centroids) {
        normalize(centroid);
      }
      codes = new CentroidCodes(centroids, vectors.coarse);
      assignment = new int[count];
      nearestDist = new float[count];
      secondDist = new float[count];
      gapSlack = new float[count];
      maxMoveSlack = new float[count];
      movement = new float[nlist];
      memberCount = new int[nlist];
      fixedPointSums = new long[Math.multiplyExact(nlist, dim)];
      cells = new int[Math.multiplyExact(count, stride)];
      Arrays.fill(cells, -1);
      // Striped locks for addToSum: the smallest power of two above nlist, at most 1024.
      locks = new Object[Math.min(1024, Integer.highestOneBit(Math.max(1, nlist)) << 1)];
      for (int i = 0; i < locks.length; i++) {
        locks[i] = new Object();
      }
    }

    /**
     * Reservoir-samples {@code min(nlist, count)} vectors as the starting centroids. With fewer
     * vectors than cells the samples repeat, and the duplicate cells end up empty.
     */
    float[][] sampleCentroids() throws IOException {
      int samples = Math.min(nlist, count);
      Random random = new Random(SEED);
      int[] picks = new int[samples];
      for (int i = 0; i < samples; i++) {
        picks[i] = i;
      }
      for (int i = samples; i < count; i++) {
        int j = random.nextInt(i + 1);
        if (j < samples) {
          picks[j] = i;
        }
      }
      float[][] sampled = new float[nlist][dim];
      if (samples > 0) {
        SegmentIVFVectorsWriter.StagedVectors.Cursor cursor = vectors.cursor();
        for (int c = 0; c < nlist; c++) {
          cursor.load(picks[c % samples]);
          System.arraycopy(cursor.vector(), 0, sampled[c], 0, dim);
        }
      }
      return sampled;
    }

    /** Adds ({@code sign} 1) or removes ({@code sign} -1) a vector from a cell's running sum. */
    void addToSum(int cell, float[] vector, int sign) {
      int base = cell * dim;
      synchronized (locks[cell & (locks.length - 1)]) {
        for (int d = 0; d < dim; d++) {
          // The double argument selects Math.round(double), which returns a long.
          fixedPointSums[base + d] += sign * Math.round((double) vector[d] * FIXED_POINT_SCALE);
        }
        memberCount[cell] += sign;
      }
    }

    /**
     * Gives every vector its starting cell: the carried cell when {@code warm} has one, otherwise
     * the nearest centroid.
     */
    void routeAll(WarmState warm) throws IOException {
      int shortlist = Math.min(nlist, vectors.coarse.routingShortlist());
      vectors.parallel.run(
          count,
          (from, to) -> {
            CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(codes, shortlist);
            CentroidCodes.Routing routing = new CentroidCodes.Routing(2);
            SegmentIVFVectorsWriter.StagedVectors.Cursor cursor = vectors.cursor();
            for (int i = from; i < to; i++) {
              cursor.load(i);
              float[] vector = cursor.vector();
              int carried = warm == null ? -1 : warm.assignment[i];
              if (carried >= 0 && carried < nlist) {
                assignment[i] = carried;
                // The second-nearest cell is unknown, since the seed may average several
                // segments' centroids. Treating it as tied forces a reassignment on the first pass.
                nearestDist[i] = codes.exactDistance(vector, carried);
                secondDist[i] = nearestDist[i];
              } else {
                cursor.prepareRouting(scratch.query);
                codes.routeByShortlist(vector, shortlist, 2, routing, scratch);
                assignment[i] = routing.count > 0 ? routing.cells[0] : 0;
                nearestDist[i] = routing.nearestDist;
                secondDist[i] = routing.secondDist;
              }
              addToSum(assignment[i], vector, 1);
            }
          });
    }

    /**
     * Moves each non-empty centroid to the normalized mean of its members, then widens every
     * vector's movement bounds by how far its centroids moved.
     */
    void updateCentroids() throws IOException {
      vectors.parallel.run(
          nlist,
          (from, to) -> {
            float[] previous = new float[dim];
            for (int c = from; c < to; c++) {
              movement[c] = 0f;
              if (memberCount[c] == 0) {
                continue;
              }
              float[] centroid = centroids[c];
              System.arraycopy(centroid, 0, previous, 0, dim);
              double inverse = 1.0 / ((double) memberCount[c] * FIXED_POINT_SCALE);
              for (int d = 0; d < dim; d++) {
                centroid[d] = (float) (fixedPointSums[c * dim + d] * inverse);
              }
              normalize(centroid);
              double moved = 0;
              for (int d = 0; d < dim; d++) {
                double delta = (double) centroid[d] - previous[d];
                moved += delta * delta;
              }
              movement[c] = (float) Math.sqrt(moved);
            }
          });
      codes.encodeAll();
      // The largest movement, its cell, and the largest of any other cell.
      float maxMove = 0f;
      float secondMove = 0f;
      int maxCell = -1;
      for (int c = 0; c < nlist; c++) {
        if (movement[c] > maxMove) {
          secondMove = maxMove;
          maxMove = movement[c];
          maxCell = c;
        } else if (movement[c] > secondMove) {
          secondMove = movement[c];
        }
      }
      // By the triangle inequality, a centroid that moved m changes a vector's distance to it by
      // at most m. So the vector's own cell recedes by at most its movement, and any other cell,
      // the second-nearest or one beyond it, approaches by at most the largest other movement:
      // the gap to the nearest other cell shrinks by at most their sum (Hamerly's bound).
      for (int i = 0; i < count; i++) {
        int cell = assignment[i];
        gapSlack[i] += movement[cell] + (cell == maxCell ? secondMove : maxMove);
        maxMoveSlack[i] += maxMove;
      }
    }

    /**
     * Reroutes every vector whose movement bounds no longer prove its cell, and on the {@code last}
     * pass also assigns spill cells. Returns how many vectors changed primary cell.
     */
    int reassign(boolean last) throws IOException {
      boolean withSpill = last && spillBits > 0;
      int keep = withSpill ? stride : 2;
      int shortlist = Math.min(nlist, vectors.coarse.routingShortlist());
      passSizes = memberCount.clone();
      meanSize = Math.max(1f, (float) count / nlist);
      AtomicInteger changed = new AtomicInteger();
      vectors.parallel.run(
          count,
          (from, to) -> {
            CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(codes, shortlist);
            CentroidCodes.Routing routing = new CentroidCodes.Routing(keep);
            int[] spillCandidates = new int[1 + keep];
            float[] residual = new float[dim];
            float[] bestLoss = new float[spillBits];
            int changedHere = 0;
            SegmentIVFVectorsWriter.StagedVectors.Cursor cursor = vectors.cursor();
            for (int i = from; i < to; i++) {
              if (canSkip(i, withSpill)) {
                continue;
              }
              cursor.load(i);
              float[] vector = cursor.vector();
              cursor.prepareRouting(scratch.query);
              codes.routeByShortlist(vector, shortlist, keep, routing, scratch);
              int incumbent = assignment[i];
              int found = routing.count;
              float incumbentDist = codes.exactDistance(vector, incumbent);
              int[] candidates = routing.cells;
              if (found > 1 && balancedPrefersSecond(routing)) {
                // The second-nearest cell wins once both sizes are counted. Taken as the nearest,
                // so that the rest of the pass treats it as the vector's cell.
                int first = routing.cells[0];
                routing.cells[0] = routing.cells[1];
                routing.cells[1] = first;
                float nearest = routing.nearestDist;
                routing.nearestDist = routing.secondDist;
                routing.secondDist = nearest;
                incumbentDist = Math.max(incumbentDist, routing.nearestDist);
              }
              if (found > 0 && routing.nearestDist <= incumbentDist) {
                if (routing.cells[0] != incumbent) {
                  changedHere++;
                  addToSum(incumbent, vector, -1);
                  addToSum(routing.cells[0], vector, 1);
                }
                assignment[i] = routing.cells[0];
                nearestDist[i] = routing.nearestDist;
                secondDist[i] = routing.secondDist;
              } else {
                // The coarse shortlist missed the incumbent, which is still nearest by exact
                // distance: keep it, and demote the routed best to second and to the spill list.
                nearestDist[i] = incumbentDist;
                secondDist[i] = found > 0 ? routing.nearestDist : Float.MAX_VALUE;
                spillCandidates[0] = incumbent;
                System.arraycopy(routing.cells, 0, spillCandidates, 1, found);
                candidates = spillCandidates;
                found++;
              }
              gapSlack[i] = 0f;
              maxMoveSlack[i] = 0f;
              if (withSpill) {
                spill(vector, candidates, found, i, residual, bestLoss);
              }
            }
            changed.addAndGet(changedHere);
          });
      return changed.get();
    }

    /**
     * Whether vector {@code i} provably keeps its cell: its gap still exceeds the accumulated
     * movement, its two nearest are not near a tie, and on the spill pass it is too far from its
     * second cell to spill. {@link #cluster} relies on the last condition to give skipped vectors
     * only their primary.
     */
    private boolean canSkip(int i, boolean withSpill) {
      // The bounds assume nearest-centroid assignment: a vector in an oversized cell may move for
      // its size alone, so it is rerouted every pass.
      if (passSizes[assignment[i]] > 2 * meanSize) {
        return false;
      }
      float gap = secondDist[i] - nearestDist[i];
      if ((gap > gapSlack[i]) == false) {
        return false;
      }
      if (CentroidCodes.withinMargin(nearestDist[i], secondDist[i], TIE_MARGIN)) {
        return false;
      }
      if (withSpill == false) {
        return true;
      }
      float approach = maxMoveSlack[i] * (2f + SPILL_MARGIN);
      return gap > (SPILL_MARGIN - 1f) * Math.abs(nearestDist[i]) + approach;
    }

    /** Whether the second-nearest routed cell beats the nearest once each size is penalized. */
    private boolean balancedPrefersSecond(CentroidCodes.Routing routing) {
      float first = routing.nearestDist + BALANCE_WEIGHT * passSizes[routing.cells[0]] / meanSize;
      float second = routing.secondDist + BALANCE_WEIGHT * passSizes[routing.cells[1]] / meanSize;
      return second < first;
    }

    /**
     * Writes vector {@code i}'s primary and up to {@code spillBits} spill cells from its ranked
     * {@code candidates}. A vector spills only when its second-nearest cell is within {@link
     * #SPILL_MARGIN}. Spill cells are chosen by SOAR loss, {@code |v - c|^2 + lambda * (r . (v -
     * c))^2 / |r|^2} where {@code r} is the residual to the primary centroid: it prefers cells
     * whose error is orthogonal to the primary's, rather than just the next nearest. {@code
     * residual} and {@code bestLoss} are caller scratch.
     */
    void spill(
        float[] vector,
        int[] candidates,
        int candidateCount,
        int i,
        float[] residual,
        float[] bestLoss) {
      int base = i * stride;
      Arrays.fill(cells, base, base + stride, -1);
      int primary = candidateCount > 0 ? candidates[0] : 0;
      cells[base] = primary;
      if (candidateCount <= 1
          || CentroidCodes.withinMargin(nearestDist[i], secondDist[i], SPILL_MARGIN) == false) {
        return;
      }
      float[] primaryCentroid = centroids[primary];
      double residualNormSq = 0;
      for (int d = 0; d < dim; d++) {
        residual[d] = vector[d] - primaryCentroid[d];
        residualNormSq += (double) residual[d] * residual[d];
      }
      if (residualNormSq == 0) {
        // The vector is its primary centroid, where SOAR is undefined: take the next nearest.
        for (int k = 1; k < candidateCount && k <= spillBits; k++) {
          cells[base + k] = candidates[k];
        }
        return;
      }
      double inverseResidualNormSq = 1.0 / residualNormSq;
      // cells[base + 1 ..] holds the spill cells sorted by loss, bestLoss their losses.
      int filled = 0;
      for (int k = 1; k < candidateCount; k++) {
        int cell = candidates[k];
        if (cell == primary || cell < 0) {
          continue;
        }
        float[] centroid = centroids[cell];
        double errorNormSq = 0;
        double errorDotResidual = 0;
        for (int d = 0; d < dim; d++) {
          double error = vector[d] - centroid[d];
          errorNormSq += error * error;
          errorDotResidual += error * residual[d];
        }
        double parallel = errorDotResidual * errorDotResidual * inverseResidualNormSq;
        float loss = (float) (errorNormSq + SOAR_LAMBDA * parallel);
        if (filled == spillBits && loss >= bestLoss[spillBits - 1]) {
          continue;
        }
        // Insert into the sorted spill list: at its end while it has room, else over its worst.
        int position = spillBits - 1;
        if (filled < spillBits) {
          position = filled;
          filled++;
        }
        for (; position > 0 && bestLoss[position - 1] > loss; position--) {
          bestLoss[position] = bestLoss[position - 1];
          cells[base + 1 + position] = cells[base + position];
        }
        bestLoss[position] = loss;
        cells[base + 1 + position] = cell;
      }
    }
  }

  /**
   * Remembers the clustering of segments this process wrote, and picks donors among them.
   *
   * <p>Only segments this process flushed or merged are remembered, never segments found on disk: a
   * writer opened with {@code OpenMode.CREATE} still has the previous index's files in its
   * directory until its first commit, and must not cluster from them. State is held per directory,
   * weakly, and per field name, dimension and similarity.
   */
  static final class HotStart {
    private static final Map<Directory, Map<String, List<Seed>>> INDEXES = new WeakHashMap<>();

    /**
     * The most segments remembered per field. Merges publish without pruning segments that have
     * since left the index, so an unbounded list grew until the heap ran out. The smallest are
     * forgotten first: donors are always the largest.
     */
    static final int MAX_SEEDS = 32;

    /**
     * The most heap all remembered segments may hold, in bytes: a sixteenth of the maximum heap,
     * shared by every directory and field in the JVM. Past it the smallest segments anywhere are
     * forgotten first ({@link #evictToBudget}).
     *
     * <p>Each segment holds its centroids and one int per vector: a million 1024-dimension vectors
     * in 1000 cells is about 8 MB, at 4096 dimensions about 20 MB, so {@link #MAX_SEEDS} of them
     * for one field can reach hundreds of megabytes, and the fields of several indexes gigabytes.
     * Remembering only speeds up clustering: a forgotten segment costs a flush or merge a colder
     * start, never an incorrect index, though the cells a segment ends with, and so its recall,
     * depend on what was remembered, and so on the heap. So the cache takes a small, fixed share of
     * the heap, a judgment rather than a measured optimum. On a 31 GB heap that is about 2 GB, more
     * than one field's {@code MAX_SEEDS} largest segments, so it binds only with many fields or
     * indexes; on a small heap it keeps fewer, down to none when a single segment's clustering
     * exceeds it (a million 4096-dimension vectors on a 256 MB heap).
     */
    static final long MAX_BYTES = Runtime.getRuntime().maxMemory() / 16;

    /**
     * A remembered segment's clustering. {@code segment} is its name, by which the directory lists
     * it, and {@code id} its {@link org.apache.lucene.index.SegmentInfo#getId}, which identifies
     * it: a merge's sources may come from another index (addIndexes) under a name a segment here
     * also has, and a rolled-back name is reused. {@code members} counts each cell's primary
     * vectors; {@code assignment} is each vector's primary cell, by ordinal, for merges to carry.
     */
    record Seed(
        String segment,
        String id,
        String lineage,
        int vectors,
        float[][] centroids,
        int[] members,
        int[] assignment) {
      /** An estimate of the heap this seed holds. */
      long bytes() {
        long bytes = 0;
        for (float[] centroid : centroids) {
          bytes += (long) centroid.length * Float.BYTES;
        }
        if (members != null) {
          bytes += (long) members.length * Integer.BYTES;
        }
        if (assignment != null) {
          bytes += (long) assignment.length * Integer.BYTES;
        }
        return bytes;
      }
    }

    /**
     * A candidate source of seed centroids: a remembered segment, or a segment being merged. {@code
     * vectors} counts its live vectors, as far as they are known.
     */
    record Source(float[][] centroids, int[] members, String lineage, int vectors) {}

    private static String key(FieldInfo info) {
      return info.name + "/" + info.getVectorDimension() + "/" + info.getVectorSimilarityFunction();
    }

    /** Forgets everything; for tests. */
    static synchronized void clear() {
      INDEXES.clear();
    }

    /** The segments remembered for {@code dir}, over all fields; for tests. */
    static synchronized Set<String> segments(Directory dir) {
      Set<String> segments = new HashSet<>();
      for (List<Seed> seeds : INDEXES.getOrDefault(dir, Map.of()).values()) {
        for (Seed seed : seeds) {
          segments.add(seed.segment);
        }
      }
      return segments;
    }

    /**
     * Forgets the clusterings of field {@code info} remembered for {@code segment}, a name a
     * rolled-back segment may have left, and for the segments whose ids it {@code merged}, which
     * leave the index with it. For a segment that publishes none of its own: a single cell, or no
     * vectors.
     */
    static synchronized void forget(
        Directory dir, String segment, FieldInfo info, Collection<String> merged) {
      List<Seed> seeds = INDEXES.getOrDefault(dir, Map.of()).get(key(info));
      if (seeds != null) {
        seeds.removeIf(
            existing -> existing.segment.equals(segment) || merged.contains(existing.id));
      }
    }

    /**
     * Remembers the clustering of field {@code info} of segment {@code segment}, whose id is {@code
     * id}, and forgets the segments whose ids it {@code merged}, which leave the index with it.
     */
    static synchronized void publish(
        Directory dir,
        String segment,
        String id,
        String lineage,
        FieldInfo info,
        Result clustering,
        int vectors,
        Collection<String> merged) {
      int[] assignment = clustering.assignment();
      float[][] centroids = clustering.centroids();
      int[] members = new int[centroids.length];
      if (assignment != null) {
        for (int cell : assignment) {
          if (cell >= 0 && cell < members.length) {
            members[cell]++;
          }
        }
      }
      Seed seed = new Seed(segment, id, lineage, vectors, centroids, members, assignment);
      Map<String, List<Seed>> fields = INDEXES.computeIfAbsent(dir, _ -> new HashMap<>());
      List<Seed> seeds = fields.computeIfAbsent(key(info), _ -> new ArrayList<>());
      // Only flushes prune by the files on disk (seed), so without dropping a merge's
      // sources here, a build that ends in merges kept every merged-away segment's assignments.
      seeds.removeIf(existing -> existing.segment.equals(segment) || merged.contains(existing.id));
      seeds.add(seed);
      seeds.sort(Comparator.comparingInt(Seed::vectors).reversed());
      while (seeds.size() > MAX_SEEDS) {
        seeds.removeLast();
      }
      evictToBudget(MAX_BYTES);
    }

    /**
     * Forgets the smallest remembered segments, of any index and field, until all fit in {@code
     * budget}.
     */
    static synchronized void evictToBudget(long budget) {
      long total = 0;
      for (Map<String, List<Seed>> fields : INDEXES.values()) {
        for (List<Seed> seeds : fields.values()) {
          for (Seed seed : seeds) {
            total += seed.bytes();
          }
        }
      }
      while (total > budget) {
        // Every list is sorted largest first, so the smallest seed is some list's last.
        List<Seed> smallestList = null;
        for (Map<String, List<Seed>> fields : INDEXES.values()) {
          for (List<Seed> seeds : fields.values()) {
            if (seeds.isEmpty() == false
                && (smallestList == null
                    || seeds.getLast().vectors < smallestList.getLast().vectors)) {
              smallestList = seeds;
            }
          }
        }
        if (smallestList == null) {
          return;
        }
        total -= smallestList.removeLast().bytes();
      }
    }

    /** The remembered clustering of the segment whose id is {@code id}, or null. */
    static synchronized Seed snapshot(Directory dir, String id, FieldInfo info) {
      List<Seed> seeds = INDEXES.getOrDefault(dir, Map.of()).getOrDefault(key(info), List.of());
      for (Seed seed : seeds) {
        if (seed.id.equals(id)) {
          return seed;
        }
      }
      return null;
    }

    /**
     * Returns the index of the donor among the sources with {@code nlist} cells, or -1 if there are
     * none. Every clustered segment of a configuration trains the same cell count, so that skips
     * segments of another configuration and single-cell segments too small to cluster.
     *
     * <p>The donor lineage is the one whose sources hold the most live vectors in total, since its
     * averaged centroids summarize the most data; the donor is that lineage's largest source. Ties
     * go to the lineage with the larger donor, then to the earlier source.
     *
     * <p>A lineage with fewer vectors than cells is never a donor: its clustering started from
     * repeated samples, so most of its centroids are duplicates whose cells stay empty, and a large
     * segment seeded from it would crowd into the few distinct ones.
     */
    static int donor(Source[] sources, int nlist) {
      Map<String, Long> lineageVectors = new HashMap<>();
      for (Source source : sources) {
        if (compatible(source, nlist) && source.lineage != null) {
          lineageVectors.merge(source.lineage, (long) source.vectors, Long::sum);
        }
      }
      int donor = -1;
      long donorLineageVectors = -1;
      for (int i = 0; i < sources.length; i++) {
        Source source = sources[i];
        if (compatible(source, nlist) == false) {
          continue;
        }
        // A source without a lineage is a lineage of its own.
        long vectors = source.lineage == null ? source.vectors : lineageVectors.get(source.lineage);
        if (vectors < nlist) {
          continue;
        }
        boolean better =
            vectors > donorLineageVectors
                || (vectors == donorLineageVectors && source.vectors > sources[donor].vectors);
        if (better) {
          donor = i;
          donorLineageVectors = vectors;
        }
      }
      return donor;
    }

    private static boolean compatible(Source source, int nlist) {
      return source != null && source.centroids.length == nlist;
    }

    /** Whether {@code source}'s centroids correspond cell for cell with the donor's. */
    static boolean sameLineage(Source source, Source donor) {
      if (source == donor) {
        return true;
      }
      return source != null
          && source.lineage != null
          && source.lineage.equals(donor.lineage)
          && source.centroids.length == donor.centroids.length;
    }

    /**
     * The seed for a flush of segment {@code writing}: the donor among the remembered segments that
     * are still in the directory, with its centroids averaged over its lineage, or null.
     */
    static Seed seed(Directory dir, String writing, FieldInfo info, int nlist) throws IOException {
      synchronized (HotStart.class) {
        if (INDEXES.getOrDefault(dir, Map.of()).get(key(info)) == null) {
          return null;
        }
      }
      // Listed outside the lock, which every flush and merge of every index takes: a slow
      // directory would stall them all.
      Set<String> live = new HashSet<>();
      for (String file : dir.listAll()) {
        live.add(IndexFileNames.parseSegmentName(file));
      }
      return seed(dir, writing, info, nlist, live);
    }

    /** {@link #seed}, once the live segment names are known. */
    private static synchronized Seed seed(
        Directory dir, String writing, FieldInfo info, int nlist, Set<String> live) {
      List<Seed> seeds = INDEXES.getOrDefault(dir, Map.of()).get(key(info));
      if (seeds == null) {
        return null;
      }
      // Forget segments that were merged away or deleted.
      seeds.removeIf(seed -> seed.segment.equals(writing) || live.contains(seed.segment) == false);
      Source[] sources = new Source[seeds.size()];
      for (int i = 0; i < sources.length; i++) {
        Seed seed = seeds.get(i);
        sources[i] = new Source(seed.centroids, seed.members, seed.lineage, seed.vectors);
      }
      int donor = donor(sources, nlist);
      if (donor < 0) {
        return null;
      }
      Seed chosen = seeds.get(donor);
      float[][] centroids = weightedCentroids(sources, donor);
      return new Seed(
          chosen.segment, chosen.id, chosen.lineage, chosen.vectors, centroids, null, null);
    }

    /**
     * Averages each cell's centroid over the donor's lineage, weighted by the cell's members in
     * each source, and normalizes it. A cell with no members anywhere keeps the donor's centroid.
     *
     * <p>The weights count primary members only, not the cell's posting length: a centroid is the
     * mean of its primary members, since {@link Clustering#cluster} assigns spill cells on its
     * final pass, after the last centroid update. Weighting by primaries therefore pools the means
     * the centroids came from. Spill copies are boundary vectors kept for search, as in SOAR, and
     * stay out of Lloyd's means; counting them here would weight a source's centroid by vectors it
     * never averaged.
     */
    static float[][] weightedCentroids(Source[] sources, int donor) {
      float[][] donorCentroids = sources[donor].centroids;
      int nlist = donorCentroids.length;
      int dim = donorCentroids[0].length;
      float[][] seed = new float[nlist][dim];
      long[] weights = new long[nlist];
      for (Source source : sources) {
        // sameLineage is false for a null source, so this also guards the members check.
        if (sameLineage(source, sources[donor]) == false || source.members == null) {
          continue;
        }
        for (int c = 0; c < nlist; c++) {
          int weight = source.members[c];
          if (weight == 0) {
            continue;
          }
          weights[c] += weight;
          float[] centroid = source.centroids[c];
          float[] sum = seed[c];
          for (int d = 0; d < dim; d++) {
            sum[d] += weight * centroid[d];
          }
        }
      }
      for (int c = 0; c < nlist; c++) {
        if (weights[c] == 0) {
          System.arraycopy(donorCentroids[c], 0, seed[c], 0, dim);
        } else {
          normalize(seed[c]);
        }
      }
      return seed;
    }

    private HotStart() {}
  }

  /**
   * A cap on how many build threads one flush or merge may use, over the shared pool for
   * clustering, graph construction and encoding.
   *
   * <p>{@link #run} splits a range into at most {@link #workers} contiguous chunks of at least
   * {@code minPerTask} items and returns once every chunk has run, so callers can read the results
   * without further synchronization. Small ranges run inline on the caller.
   *
   * <p>The writer often shares its JVM with searchers, so a flush or merge takes at most {@link
   * SegmentIVFVectorsFormat#DEFAULT_BUILD_THREADS} workers, half the cores, unless the format is
   * configured otherwise. The cap changes only speed: every chunk's result is independent of how
   * the range is split (centroid sums are fixed-point), so clustering is identical at any cap.
   *
   * <p>The pool itself has one daemon thread per core, named {@code segmentivf-build-N}, shared by
   * every writer in the process and alive for its lifetime; each writer's cap bounds its own share.
   */
  static final class Parallel {
    private static final int MIN_PER_TASK = 4096;

    /** Threads in the shared pool: one per core, so a writer's cap is the only limit it sees. */
    private static final int POOL_THREADS = Runtime.getRuntime().availableProcessors();

    private static final TaskExecutor EXECUTOR =
        new TaskExecutor(
            Executors.newFixedThreadPool(
                POOL_THREADS,
                Thread.ofPlatform().name("segmentivf-build-", 0).daemon(true).factory()));

    /**
     * The default cap, used where no format is at hand (the centroid graph build). Equal to the
     * format's default, so a default-configured writer behaves the same everywhere.
     */
    static final Parallel DEFAULT = new Parallel(SegmentIVFVectorsFormat.DEFAULT_BUILD_THREADS);

    /** The most chunks one call runs at once; at least 1 and at most the pool's thread count. */
    final int workers;

    Parallel(int workers) {
      if (workers < 1) {
        throw new IllegalArgumentException("build threads must be >= 1; got " + workers);
      }
      this.workers = Math.min(workers, POOL_THREADS);
    }

    /** Work over the item range {@code [from, to)}. */
    interface RangeTask {
      void run(int from, int to) throws IOException;
    }

    void run(int count, RangeTask task) throws IOException {
      run(count, MIN_PER_TASK, task);
    }

    void run(int count, int minPerTask, RangeTask task) throws IOException {
      int tasks = Math.min(workers, Math.max(1, count / minPerTask));
      if (tasks <= 1) {
        task.run(0, count);
        return;
      }
      int chunk = (count + tasks - 1) / tasks;
      List<Callable<Void>> work = new ArrayList<>(tasks);
      for (int t = 0; t < tasks; t++) {
        int from = t * chunk;
        int to = Math.min(count, from + chunk);
        if (from >= to) {
          continue;
        }
        work.add(() -> {
          task.run(from, to);
          return null;
        });
      }
      EXECUTOR.invokeAll(work);
    }
  }

  /** Scales {@code vector} to unit length, leaving a zero vector unchanged. */
  static void normalize(float[] vector) {
    double norm = 0;
    for (float value : vector) {
      norm += (double) value * value;
    }
    if (norm == 0) {
      return;
    }
    float inverse = (float) (1.0 / Math.sqrt(norm));
    for (int d = 0; d < vector.length; d++) {
      vector[d] *= inverse;
    }
  }
}
