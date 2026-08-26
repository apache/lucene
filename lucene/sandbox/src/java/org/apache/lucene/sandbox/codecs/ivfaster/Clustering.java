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
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Lloyd clustering with exhaustive routing and the Reaper.
 *
 * <p>Produces {@code nlist} centroids and a primary cell per rotated document. Every segment gets
 * exactly {@code nlist} centroids, so a merge can always seed from one segment's centroids.
 *
 * <p>Assignment routes through {@link CentroidCodes}: a coarse scan of all centroids, then an exact
 * rank of an oversampled shortlist.
 *
 * <h2>The Reaper</h2>
 *
 * <p>After the means move, most documents cannot have changed cell. The Reaper re-routes only those
 * that could, on a SUFFICIENT condition, so the set provably contains every document that changes:
 *
 * <pre>
 *   a document's primary can change only if   d2 - d1 &lt;= delta(c1) + delta(c2)
 * </pre>
 *
 * <p>By the triangle inequality each cell's distance moves by at most its own centroid's
 * displacement, so a gap wider than their combined movement cannot be overtaken. {@code delta}
 * falls out of the mean update, so the test is free.
 *
 * <p>PER-PAIR, NOT GLOBAL. Comparing against {@code 2 * max(delta)} is also sufficient, but it is
 * dominated by the worst centroid in the index and reaps far more documents than the pairwise
 * bound. This is why {@link CentroidCodes.Routing} reports {@code cell2} and not only {@code d2}.
 *
 * <p>{@link #REAP_MARGIN} is unioned in as a near-tie safety net, and is separate from the spill
 * margin; see there.
 *
 * <p>Spill is chosen by a FINAL reap pass, since both ask whether a document is near a boundary
 * from the same {@code d1}/{@code d2}. Only that pass needs spill's width, and takes it from the
 * widened bound in {@link #reap}, which also corrects for the reap test reading a stale gap where
 * spill needs a fresh one.
 *
 * <h2>Convergence</h2>
 *
 * <p>The loop runs until a pass CHANGES fewer than {@link #CONVERGE_FRACTION} of the corpus's
 * assignments, and {@code maxIters} is a BACKSTOP rather than the schedule. Note the signal is the
 * changed count and NOT the reaped count, which plateaus well above zero on a fully converged
 * corpus; see {@link #CONVERGE_FRACTION}.
 *
 * <h2>Monotonicity</h2>
 *
 * <p>The objective is {@code J = sum_i dist(v_i, c_assignment(i))}, and Lloyd decreases it only
 * when BOTH steps minimize the SAME function. Two things secure that here, and each is load-bearing
 * for the convergence rule above: a non-monotone loop can cycle, never reach the threshold, and
 * spend the whole backstop every build.
 *
 * <ul>
 *   <li>ONE OBJECTIVE IN BOTH STEPS. Centroids are unit-norm for every similarity (see {@link
 *       #normalize}), so {@code argmin -dot} and {@code argmin ||v - c||^2} coincide, and {@code
 *       normalize(sum of members)} is the exact maximizer of {@code sum dot(v, c)} over the unit
 *       sphere. The mean update is then the constrained minimizer of what routing ranks by. Were
 *       centroids left unnormalized under EUCLIDEAN, the mean update would minimize {@code sum ||v
 *       - c||^2} while routing ranked by {@code -dot}: two different functions, and neither
 *       provably decreasing.
 *   <li>ASSIGNMENTS ONLY IMPROVE. Routing returns the best of a COARSE SHORTLIST, not the global
 *       argmin, so adopting it unconditionally can replace a good incumbent with a worse cell and
 *       raise {@code J}. {@link #reap} keeps whichever of the two is actually nearer.
 * </ul>
 *
 * <p>{@code J} is then non-increasing and strictly decreases unless the assignment is unchanged, so
 * the loop terminates. The backstop remains because the accumulators are {@code double} rounded to
 * {@code float}, so {@code J} can jitter at float precision; that cannot move a whole {@link
 * #CONVERGE_FRACTION} of the corpus, but it does leave a cycle formally possible.
 *
 * @lucene.experimental
 */
final class Clustering {

  /** Fixed seed: clustering must be reproducible, so that the same input yields the same index. */
  static final long SEED = 42L;

  /**
   * Coarse oversample for the routing cascade: the coarse scan keeps {@code OVERSAMPLE * keep}
   * candidates for the exact stage to rank.
   *
   * <p>It multiplies the number of cells the caller WANTS, so it scales with {@code keep}. A
   * constant would starve the exact stage at large {@code keep}.
   */
  static final int OVERSAMPLE = 3;

  /**
   * Minimum coarse shortlist, the floor {@link #shortlistFor} takes when the oversample is small.
   *
   * <p>BUILD-TIME ONLY: this feeds assignment routing, so raising it costs a wider exact rank per
   * document at build and nothing at search. It buys assignment quality, since the coarse scan can
   * rank the true-nearest centroid outside a small top-k and a document routed to a non-nearest
   * cell is recall the query side has to buy back with nprobe and spill. Each added candidate is
   * one more {@code exactDistance}, so the cost is linear in the floor.
   *
   * <p>At small {@code keep} the oversample falls below this floor, so the floor is what sets the
   * shortlist for the common routing and reap passes. Overridable, so the build and recall trade is
   * measurable.
   */
  static final int MIN_SHORTLIST = Integer.getInteger("ivfaster.minShortlist", 32);

  /** Coarse survivors to hand the exact stage, to select {@code keep} cells. */
  static int shortlistFor(int keep, int nlist) {
    return Math.min(nlist, Math.max(OVERSAMPLE * keep, MIN_SHORTLIST));
  }

  /**
   * Boundary margin for SPILL SELECTION.
   *
   * <p>Applied as a DIFFERENCE against {@code |d1|} (see {@link CentroidCodes#withinMargin}), since
   * the ratio {@code d2/d1} is not monotone for the negated-dot distances the inner-product family
   * uses.
   *
   * <p>WRITE-TIME ({@code -Divfaster.spillMargin}): it decides which documents spill, so it changes
   * the index contents and belongs in the cache key. Larger admits more documents as boundary
   * cases, and 1.0 spills nothing.
   *
   * <p>Distinct from {@code ivfaster.nprobeMargin}, which is SEARCH-time and prunes selected CELLS
   * on quality, and from {@link #REAP_MARGIN}; see there.
   */
  static final float MARGIN = Float.parseFloat(System.getProperty("ivfaster.spillMargin", "1.40"));

  /**
   * The Reaper's near-tie safety net: a small epsilon unioned with the movement bound, so a
   * document whose two best cells are all but exactly tied is re-measured even when the bound says
   * it need not be.
   *
   * <p>NOT A TUNING KNOB, and deliberately not a system property. It guards float slop in the
   * accumulated {@code docSlack} bound, which carries no epsilon of its own.
   *
   * <p>SEPARATE FROM {@link #MARGIN}. The Reaper skips documents whose assignment provably cannot
   * have changed, which {@code gap <= slack} decides on its own. Spill selection asks whether a
   * document is near a boundary at {@link #MARGIN}, and rides along on the FINAL pass so one route
   * answers both. Unioning the spill test into the reap test on every iteration would make the
   * whole Lloyd loop pay the spill margin's width, which at a wide margin admits nearly every
   * document and leaves the Reaper pruning nothing.
   *
   * <p>Decoupling them is safe by the derivation on {@link #reap}: the final pass widens to {@code
   * gap <= (m-1)*|d1| + s*(2+m)} with {@code s >= 0}, which is strictly wider than {@code
   * withinMargin(m)}, so the spill set is unchanged and only the non-final iterations regain their
   * pruning.
   */
  static final float REAP_MARGIN = 1.02f;

  /**
   * Convergence threshold, as a FRACTION OF THE CORPUS: the Lloyd loop stops once a pass CHANGES no
   * more than this share of assignments.
   *
   * <p>CHANGED, NOT REAPED, and the difference is not a refinement. The reaped count has an
   * IRREDUCIBLE FLOOR: {@link #REAP_MARGIN} unions in every document whose two best cells are
   * within a near-tie, and that population is a property of the corpus, not of how converged the
   * centroids are. So the reaped count plateaus at a corpus-dependent level and stays there
   * forever. On a clustered 6k corpus at {@code nlist=40} it settles at about 8% of documents while
   * {@code maxMove} is exactly zero and no cell is moving at all: fully converged, and no threshold
   * worth setting would ever fire.
   *
   * <p>The changed count instead reaches zero at the fixed point, and reaching it IS the fixed
   * point: if no assignment changed, the next mean update recomputes the same centroids, so every
   * later iteration is a no-op. A fractional threshold stops a little before that, where the
   * remaining moves are a handful of boundary documents not worth a full pass over the corpus.
   *
   * <p>WRITE-TIME ({@code -Divfaster.convergeFraction}): it decides how many iterations run and
   * therefore where the centroids land, so it changes the index contents and belongs in the cache
   * key, as {@link #MARGIN} does. Zero demands the exact fixed point.
   *
   * <p>The comparison is {@code changed <= fraction * count}, so below about {@code 1/fraction}
   * documents the threshold rounds to demanding the exact fixed point. That is the cheap end of the
   * range, where iterations cost nothing worth saving.
   */
  static final float CONVERGE_FRACTION =
      Float.parseFloat(System.getProperty("ivfaster.convergeFraction", "0.005"));

  /**
   * Diagnostic: count how many primaries an exact all-{@code nlist} scan would place differently
   * from the coarse shortlist's choice, without changing the index ({@code
   * -Divfaster.exactPlacementAudit}).
   *
   * <p>The direct measure of coarse-retention loss at assignment time, and therefore of whether an
   * exact-placement correction pass would buy anything. Kept because it is cheap and re-measures
   * that whenever the shortlist or the coarse tier changes.
   */
  static final boolean EXACT_PLACEMENT_AUDIT = Boolean.getBoolean("ivfaster.exactPlacementAudit");

  /**
   * Diagnostic: print per-iteration Lloyd convergence stats, namely mean and max centroid
   * displacement, how many cells are still moving, and how many are empty ({@code
   * -Divfaster.convergenceTrace}).
   *
   * <p>The signal for whether the loop exited on {@link #CONVERGE_FRACTION} or ran out the {@code
   * maxIters} backstop: displacement still large at the last iteration means the backstop bound the
   * build and raising it would sharpen the cells, which exact PLACEMENT cannot do, since that only
   * fixes doc-to-cell given fixed centroids.
   */
  static final boolean CONVERGENCE_TRACE = Boolean.getBoolean("ivfaster.convergenceTrace");

  private Clustering() {}

  /** The outcome of clustering: centroids, per-document primary cell, and the routing distances. */
  static final class Result {
    float[][] centroids;

    /** Primary cell of each document. */
    int[] assignment;

    /** Exact distance to the primary cell, per document. */
    float[] d1;

    /** Exact distance to the runner-up cell, per document. */
    float[] d2;

    /** The runner-up cell itself, per document; -1 when there is none. See the Reaper's bound. */
    int[] cell2;

    /** Documents re-routed by the final Reaper pass; diagnostic, and proves the Reaper engaged. */
    int reaped;

    /**
     * Lloyd iterations actually run, which {@link #CONVERGE_FRACTION} rather than {@code maxIters}
     * normally decides. Diagnostic, and what a test asserts against to show the loop stops early.
     */
    int iterations;

    /**
     * Whether the loop exited because a pass fell to {@link #CONVERGE_FRACTION}, as opposed to
     * running out the {@code maxIters} backstop.
     */
    boolean converged;

    /**
     * Primaries the exact-placement scan found to differ from the coarse shortlist's choice, in
     * documents. Set only when {@link #EXACT_PLACEMENT_AUDIT} ran.
     */
    int primariesMoved;

    /**
     * The cells each document is written into, primary first: its assignment plus its spill copies.
     *
     * <p>SPILL IS THE REAPER'S OTHER OUTPUT. The final {@link #reap} pass chooses both, since both
     * answer "is this document near a cell boundary?" from one route; the Reaper uses the answer to
     * MOVE the document and spill to COPY it. Answering them separately costs a second full route
     * of the corpus, at a wider shortlist.
     */
    int[][] cells;
  }

  /**
   * Clusters {@code vectors} into {@code nlist} cells, and chooses each document's spill cells.
   *
   * @param vectors rotated vectors, indexed by ordinal; not modified
   * @param seed initial centroids to warm-start from, or {@code null} to sample fresh ones
   */
  static Result cluster(
      float[][] vectors,
      int count,
      int dim,
      int nlist,
      int maxIters,
      VectorSimilarityFunction sim,
      float[][] seed)
      throws IOException {
    return cluster(vectors, count, dim, nlist, maxIters, sim, seed, null);
  }

  /**
   * As above, with documents' starting cells supplied.
   *
   * <p>Used by merge: a document from the donor segment already has a cell, and seed centroid
   * ordinal {@code c} IS donor cell {@code c}, so its assignment carries over. Only the other
   * segments' documents are routed, and the Reaper fixes up any carried document the refined
   * centroids moved away from, so merge needs no separate reassignment machinery.
   *
   * @param seedAssignment starting cell per document, or -1 to route it; {@code null} routes
   *     everything
   */
  static Result cluster(
      float[][] vectors,
      int count,
      int dim,
      int nlist,
      int maxIters,
      VectorSimilarityFunction sim,
      float[][] seed,
      int[] seedAssignment)
      throws IOException {
    return cluster(vectors, count, dim, nlist, maxIters, sim, seed, seedAssignment, 0, 0f);
  }

  /**
   * As above, additionally selecting spill cells in the final pass.
   *
   * @param spillBits maximum additional cells per document; 0 assigns each document to one cell
   * @param soarLambda SOAR weight for spill selection
   */
  static Result cluster(
      float[][] vectors,
      int count,
      int dim,
      int nlist,
      int maxIters,
      VectorSimilarityFunction sim,
      float[][] seed,
      int[] seedAssignment,
      int spillBits,
      float soarLambda)
      throws IOException {
    return cluster(
        vectors,
        count,
        dim,
        nlist,
        maxIters,
        sim,
        seed,
        seedAssignment,
        spillBits,
        soarLambda,
        null);
  }

  /**
   * As above, with every document's coarse planes already packed.
   *
   * <p>The writer supplies them, since it needs the same buffer to emit the on-disk plane sections,
   * so the codes are derived once per build and the routing scan and the index agree by
   * construction. {@code null} packs them here, which is what tests and any non-writer caller do.
   *
   * <p>The cell space is fixed by the seed: a seed with a different cell count than {@code nlist}
   * would let assignments index past the per-cell mean-update accumulators, so it is rejected.
   *
   * <p>CENTROIDS ARE NORMALIZED FOR EVERY SIMILARITY, not only the inner-product family; see {@link
   * #normalize} for why the Euclidean case needs it just as much.
   *
   * <p>Clustering is COARSE-ONLY: it routes by coarse code plus exact float dot and never scores
   * the fine tier, so fine centroid codes would be per-iteration work with no consumer.
   *
   * <p>LLOYD RUNS OVER THE FULL CORPUS. Subsampling exists to avoid a full assignment pass per
   * iteration, and the Reaper removes that cost instead, so these are full-corpus means.
   *
   * <p>TWO SLACK ACCUMULATORS, both per document rather than per cell, since a skipped document
   * keeps stale {@code d1}/{@code d2} and a stale gap compared against fresh movement is not a
   * sufficient bound. Clearing only for documents actually re-measured keeps the bound valid over
   * any number of iterations.
   *
   * <ul>
   *   <li>{@code docSlack} accumulates what the document's OWN PAIR of cells moved. The assignment
   *       bound asks only whether {@code c1} and {@code c2} can trade places, a statement about
   *       those two cells, so the tighter own-pair sum is valid there.
   *   <li>{@code docMaxSlack} accumulates the MAXIMUM movement of any cell. The spill bound asks
   *       whether the document's fresh top two are within the margin, and a THIRD cell moving
   *       toward the document changes that pair entirely; a third cell's movement appears in no
   *       document's own-pair sum, so {@code docSlack} is unsound for spill.
   * </ul>
   *
   * <p>NO FINAL RECOMPUTE. The loop ends in the consistency the query side needs: reap routes every
   * eligible document against the CURRENT centroids, so on exit each assignment is its
   * exact-nearest cell (pinned by {@code testReaperAssignmentMatchesFullReroute}). A trailing
   * recompute would move the centroids and desync that. Both conditions hold together at
   * convergence, which is what the loop now runs to rather than stopping on a count.
   *
   * <p>THE SPILL PASS IS ITS OWN PASS, because which iteration is last is no longer known in
   * advance: convergence is read off a Reaper count, so the loop learns it has finished only after
   * the pass that finished it. So the loop runs plain reaps, and one final reap follows at the SAME
   * centroids, with no mean update between them, to choose spill.
   *
   * <p>That extra pass is nearly free and is not extra work in the old sense. It routes only what
   * the widened bound admits, which for the documents the converging pass just re-measured (slack
   * zero) is exactly the spill-eligible set, work spill had to do in any case. What it costs is
   * re-routing that pass's own reap set a second time, and at convergence that set is under {@link
   * #CONVERGE_FRACTION} of the corpus.
   *
   * <p>Reading convergence off the PLAIN reap count is also what makes the counts comparable across
   * iterations: the spill pass widens its bound, so folding spill into the loop would make the
   * threshold mean something different on the pass that mattered.
   *
   * <p>Documents the final reap skipped get their primary cell alone: their gap exceeded their
   * accumulated slack, which is a strictly wider test than the spill margin, so they cannot spill.
   *
   * @param maxIters BACKSTOP on Lloyd iterations, {@code >= 1}; {@link #CONVERGE_FRACTION} normally
   *     ends the loop first
   * @param planesOrNull pre-packed coarse planes, or {@code null} to pack them internally
   */
  static Result cluster(
      float[][] vectors,
      int count,
      int dim,
      int nlist,
      int maxIters,
      VectorSimilarityFunction sim,
      float[][] seed,
      int[] seedAssignment,
      int spillBits,
      float soarLambda,
      DocPlanes planesOrNull)
      throws IOException {

    // Effectively final, so the parallel bodies below can capture it.
    final DocPlanes planes =
        planesOrNull != null
            ? planesOrNull
            : DocPlanes.encode(vectors, count, dim, null, null, null);
    final Result result = new Result();

    float[][] centroids =
        seed != null ? copyOf(seed, dim) : sampleCentroids(vectors, count, dim, nlist);
    if (centroids.length != nlist) {
      throw new IllegalArgumentException(
          "seed has "
              + centroids.length
              + " centroids but nlist is "
              + nlist
              + "; a donor's cell count must be adopted, not reinterpreted");
    }
    // Every similarity; see normalize(). A seed arrives normalized, so this is a no-op for merge.
    for (float[] c : centroids) {
      normalize(c);
    }

    final CentroidCodes codes = new CentroidCodes(centroids, dim, sim, null);
    final int[] assignment = new int[count];
    final float[] d1 = new float[count];
    final float[] d2 = new float[count];
    final int[] cell2 = new int[count];
    final float[] movement = new float[nlist];

    // Pass 1: route every document whose cell is not already known.
    final boolean trace = Parallel.TRACE;
    long tr = trace ? System.nanoTime() : 0L;
    routeAll(vectors, count, dim, codes, assignment, d1, d2, cell2, seedAssignment, nlist, planes);
    if (trace) {
      IvfDiag.err("[ivfaster-cluster] routeAll %.3f s%n", (System.nanoTime() - tr) / 1e9);
    }

    // Own-pair slack per document; see the javadoc.
    final float[] docSlack = new float[count];
    // Max-over-all-cells slack per document; see the javadoc.
    final float[] docMaxSlack = new float[count];
    // Spill is chosen by the FINAL reap pass, after the loop; see the javadoc.
    final int[][] cells = new int[count][];
    // The convergence threshold, in documents; see CONVERGE_FRACTION.
    final int convergeAt = (int) (CONVERGE_FRACTION * count);
    int reaped = count;
    int changed = count;
    int iterations = 0;
    boolean converged = false;
    // Whether a loop pass already chose spill, which the backstop-bound case does inline.
    boolean spillChosen = false;
    for (int it = 0; it < maxIters; it++) {
      final long tIter = trace ? System.nanoTime() : 0L;
      recomputeCentroids(vectors, count, dim, nlist, assignment, centroids, movement);
      final long tMean = trace ? System.nanoTime() : 0L;
      codes.encodeAll();
      final long tEnc = trace ? System.nanoTime() : 0L;
      // Slack grows here; a re-measured document's is reset inside reap.
      final float maxMove = maxOf(movement);
      for (int i = 0; i < count; i++) {
        final int c2i = cell2[i];
        docSlack[i] += c2i >= 0 ? movement[assignment[i]] + movement[c2i] : 2f * maxMove;
        docMaxSlack[i] += maxMove;
      }
      // When the BACKSTOP is what ends the loop, that is known in advance, so spill folds into this
      // pass exactly as it did before convergence existed, and the corpus is routed once rather
      // than
      // twice. Only an EARLY exit needs its own pass, since convergence is read off the count this
      // very pass returns. Non-final passes stay PLAIN, so the counts the threshold reads are
      // comparable across iterations; see the javadoc.
      final boolean lastAllowed = it == maxIters - 1;
      final Pass pass =
          reap(
              vectors,
              count,
              dim,
              codes,
              assignment,
              d1,
              d2,
              cell2,
              docSlack,
              docMaxSlack,
              lastAllowed ? cells : null,
              spillBits,
              soarLambda,
              centroids,
              sim,
              planes);
      reaped = pass.reaped;
      changed = pass.changed;
      iterations = it + 1;
      converged = changed <= convergeAt;
      spillChosen = lastAllowed;
      if (trace) {
        final long now = System.nanoTime();
        IvfDiag.err(
            "[ivfaster-cluster] iter=%d/%d mean=%.3f encodeAll=%.3f reap=%.3f reaped=%d (%.1f%%) changed=%d total=%.3f s%n",
            it + 1,
            maxIters,
            (tMean - tIter) / 1e9,
            (tEnc - tMean) / 1e9,
            (now - tEnc) / 1e9,
            reaped,
            100.0 * reaped / Math.max(1, count),
            changed,
            (now - tIter) / 1e9);
      }

      // Convergence trace; see CONVERGENCE_TRACE.
      if (CONVERGENCE_TRACE) {
        double sum = 0;
        float mx = 0;
        int moving = 0;
        int empty = 0;
        // Still moving means displaced more than 1% of the mean, a scale-free proxy.
        for (int c = 0; c < nlist; c++) {
          final float m = movement[c];
          sum += m;
          if (m > mx) {
            mx = m;
          }
        }
        final double mean = nlist > 0 ? sum / nlist : 0;
        final float movingThresh = (float) (0.01 * mean);
        for (int c = 0; c < nlist; c++) {
          if (movement[c] > movingThresh && movement[c] > 0f) {
            moving++;
          }
        }
        // Empty cells: those with no members this iteration, which recomputeCentroids left frozen.
        final int[] members = new int[nlist];
        for (int i = 0; i < count; i++) {
          members[assignment[i]]++;
        }
        for (int c = 0; c < nlist; c++) {
          if (members[c] == 0) {
            empty++;
          }
        }
        IvfDiag.err(
            "[ivfaster-converge] iter=%d/%d meanMove=%.6f maxMove=%.6f cellsMoving=%d/%d empty=%d reaped=%d changed=%d/%d converged=%b%n",
            it + 1,
            maxIters,
            mean,
            mx,
            moving,
            nlist,
            empty,
            reaped,
            changed,
            convergeAt,
            converged);
      }

      if (converged) {
        break;
      }
    }

    // The spill pass, at the SAME centroids the loop left; see the javadoc. Runs ONLY after an
    // early
    // exit: a backstop-bound loop chose spill on its last pass, and repeating it here would route
    // the
    // whole corpus a second time for an answer already in hand.
    final long tSpill = trace ? System.nanoTime() : 0L;
    if (spillChosen == false) {
      reaped =
          reap(
                  vectors,
                  count,
                  dim,
                  codes,
                  assignment,
                  d1,
                  d2,
                  cell2,
                  docSlack,
                  docMaxSlack,
                  cells,
                  spillBits,
                  soarLambda,
                  centroids,
                  sim,
                  planes)
              .reaped;
    }
    if (trace) {
      IvfDiag.err(
          "[ivfaster-cluster] iterations=%d/%d converged=%b spillPass=%.3f s reaped=%d (%.1f%%)%n",
          iterations,
          maxIters,
          converged,
          (System.nanoTime() - tSpill) / 1e9,
          reaped,
          100.0 * reaped / Math.max(1, count));
    }

    // Coarse-retention audit; see EXACT_PLACEMENT_AUDIT.
    if (EXACT_PLACEMENT_AUDIT) {
      final java.util.concurrent.atomic.AtomicInteger moved =
          new java.util.concurrent.atomic.AtomicInteger();
      Parallel.overRange(
          count,
          (lo, hi) -> {
            int local = 0;
            for (int i = lo; i < hi; i++) {
              if (codes.nearestExact(vectors[i]) != assignment[i]) {
                local++;
              }
            }
            moved.addAndGet(local);
          });
      result.primariesMoved = moved.get();
    }

    result.centroids = centroids;
    result.assignment = assignment;
    result.d1 = d1;
    result.d2 = d2;
    result.cell2 = cell2;
    result.reaped = reaped;
    result.iterations = iterations;
    result.converged = converged;
    // Documents the final reap skipped cannot spill; see the javadoc.
    for (int i = 0; i < count; i++) {
      if (cells[i] == null) {
        cells[i] = new int[] {assignment[i]};
      }
    }
    result.cells = cells;
    return result;
  }

  /**
   * Routes documents in {@code [from, to)}, in parallel over sub-ranges.
   *
   * <p>A CARRIED document keeps its cell but still has its distances recorded, since the Reaper's
   * bound reads {@code d1}/{@code d2} and without them a carried document could never be reaped and
   * would stay pinned to a cell the refined centroids have moved away from. Its runner-up is
   * unknown without a scan, so it is treated as adjacent (zero gap), which puts the document in the
   * reap set on the first iteration, where one scan establishes the real pair. That errs in the
   * safe direction.
   */
  private static void routeAll(
      float[][] vectors,
      int count,
      int dim,
      CentroidCodes codes,
      int[] assignment,
      float[] d1,
      float[] d2,
      int[] cell2,
      int[] seedAssignment,
      int nlist,
      DocPlanes planes)
      throws IOException {
    final int shortlist = shortlistFor(2, codes.nlist());
    Parallel.overRange(
        count,
        (lo, hi) -> {
          final CentroidCodes.Scratch scratch =
              new CentroidCodes.Scratch(dim, codes.nlist(), shortlist);
          final CentroidCodes.Routing routing = new CentroidCodes.Routing(2);
          for (int i = lo; i < hi; i++) {
            final int carried = seedAssignment == null ? -1 : seedAssignment[i];
            if (carried >= 0 && carried < nlist) {
              assignment[i] = carried;
              d1[i] = codes.exactDistance(vectors[i], carried);
              // Runner-up unknown, so treat it as adjacent; see the javadoc.
              d2[i] = d1[i];
              cell2[i] = -1;
              continue;
            }
            // Packed code straight into the kernel's query array.
            planes.copyInto(i, scratch.qCode);
            codes.routePacked(vectors[i], shortlist, 2, routing, scratch);
            assignment[i] = routing.count > 0 ? routing.cells[0] : 0;
            d1[i] = routing.d1;
            d2[i] = routing.d2;
            cell2[i] = routing.cell2;
          }
        });
  }

  /**
   * What one {@link #reap} pass did: how many documents it re-routed, and how many of those
   * actually changed cell.
   *
   * <p>Both are needed and they are not interchangeable. {@code reaped} is the pass's COST, which
   * is what the timing trace wants; {@code changed} is its PROGRESS, which is what convergence
   * reads. See {@link #CONVERGE_FRACTION} for why the two diverge permanently.
   */
  private static final class Pass {
    int reaped;
    int changed;
  }

  /**
   * The Reaper: re-routes only documents whose primary could have changed, and on the final pass
   * chooses spill cells for the documents it re-routed.
   *
   * <h2>Spill rides along</h2>
   *
   * <p>Spill asks a nearby question to the Reaper's, whether a document is close to a cell
   * boundary, against the same {@code d1}/{@code d2}. So when {@code cells} is non-null this pass
   * answers both from one route, saving a second full route of the corpus at a wider shortlist.
   *
   * <p>The two questions differ in WIDTH: spill's is {@link #MARGIN}, an index-size dial, and the
   * Reaper's is {@link #REAP_MARGIN}, a tie epsilon. Only the FINAL pass needs spill's width, and
   * it takes it from the widened bound below rather than from the per-iteration test.
   *
   * <p>THE REAP SET IS NOT AUTOMATICALLY A SUPERSET OF THE SPILL SET. The reap test reads a STALE
   * gap, last measured some iterations ago, while spill needs the FRESH one, and the two differ by
   * the accumulated slack. A document is spill-eligible when {@code gap_fresh <= m*|d1_fresh|}, and
   * the triangle inequality gives {@code gap_fresh >= gap_stale - slack} and {@code |d1_fresh| <=
   * |d1_stale| + slack}, so eligibility implies only
   *
   * <pre>
   *   gap_stale &lt;= m*|d1_stale| + slack*(1 + m)
   * </pre>
   *
   * <p>which is wider than {@code gap_stale <= slack || gap_stale <= m*|d1_stale|} by a band of
   * width {@code slack*(1+m)}: documents the plain reap test skips that should nonetheless spill.
   * The final pass therefore widens to the bound above, making the reap set a superset by
   * derivation.
   *
   * <h2>The two tests in the loop</h2>
   *
   * <p>The PER-PAIR bound, accumulated since the document was last measured: each cell's distance
   * moves by at most its own centroid's displacement, so the pair can trade places only if their
   * gap is within the sum of the two. It is unioned with {@link #REAP_MARGIN}, a near-tie epsilon.
   * A document failing it has stale distances but a provably unchanged ASSIGNMENT.
   *
   * <p>The FINAL-PASS widening, with {@code S} the accumulated maximum cell movement. Every cell's
   * distance has moved by at most {@code S}, so
   *
   * <pre>
   *   fresh gap  &gt;= stale gap - 2S      (best can gain S, runner-up can lose S)
   *   |fresh d1| &lt;= |stale d1| + S
   * </pre>
   *
   * <p>and spill-eligibility ({@code fresh gap <= m*|fresh d1|}) therefore implies {@code stale gap
   * <= m*|stale d1| + S*(2 + m)}. {@code S} is the MAX OVER ALL CELLS: the own-pair sum is sound
   * for the assignment bound but not here, since a third cell moving toward the document replaces
   * the top two entirely and its movement is in no document's own-pair sum.
   *
   * <p>A re-routed document spends both accumulators, since its {@code d1}/{@code d2} are current.
   * They are per document rather than per cell, because cells are shared by documents whose
   * measurements are from different iterations.
   *
   * <h2>The incumbent is defended</h2>
   *
   * <p>A route returns the best of a COARSE SHORTLIST, not the global argmin, so adopting it
   * unconditionally can replace a nearer incumbent with a farther cell whenever the coarse tier
   * misses. That RAISES the objective, and a step that can raise it can cycle, which would leave
   * {@link #CONVERGE_FRACTION} unreachable and spend the whole backstop on every build. So the
   * incumbent's exact distance is re-measured and the nearer of the two kept: one dot product on
   * top of the shortlist's tens, against a coarse scan of all {@code nlist}.
   *
   * <p>THE GUARD FIRES ONLY WHEN THE INCUMBENT WAS OUTSIDE THE SHORTLIST. Stage 2 ranks the whole
   * shortlist exactly, so an incumbent present in it would already be {@code routing.cells[0]} or
   * worse than it. The bookkeeping follows from that: when the incumbent wins it is the new {@code
   * d1}, and the shortlist's own best is exactly the runner-up.
   *
   * @param cells receives the chosen cells per re-routed document; {@code null} on the loop's plain
   *     passes, which skips spill selection entirely
   * @return what the pass did: documents re-routed, and how many of those changed cell
   */
  private static Pass reap(
      float[][] vectors,
      int count,
      int dim,
      CentroidCodes codes,
      int[] assignment,
      float[] d1,
      float[] d2,
      int[] cell2,
      float[] docSlack,
      float[] docMaxSlack,
      int[][] cells,
      int spillBits,
      float soarLambda,
      float[][] centroids,
      VectorSimilarityFunction sim,
      DocPlanes planes)
      throws IOException {

    final boolean withSpill = cells != null && spillBits > 0;
    // SOAR needs a complementary direction to choose from, so the final pass carries a wider keep.
    final int keep = withSpill ? 1 + spillBits : 2;
    final int shortlist =
        withSpill ? shortlistFor(keep, codes.nlist()) : shortlistFor(2, codes.nlist());
    final java.util.concurrent.atomic.AtomicInteger reaped =
        new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.atomic.AtomicInteger changed =
        new java.util.concurrent.atomic.AtomicInteger();

    Parallel.overRange(
        count,
        (lo, hi) -> {
          final CentroidCodes.Scratch scratch =
              new CentroidCodes.Scratch(dim, codes.nlist(), shortlist);
          final CentroidCodes.Routing routing = new CentroidCodes.Routing(keep);
          final int[] chosen = withSpill ? new int[1 + spillBits] : null;
          // Candidates for Spill, with a winning incumbent prepended; see the javadoc.
          final int[] cands = withSpill ? new int[1 + keep] : null;
          int local = 0;
          int localChanged = 0;
          for (int i = lo; i < hi; i++) {
            // The per-pair movement bound; see the javadoc.
            final float slack = docSlack[i];
            final float gap = d2[i] - d1[i];
            boolean couldFlip =
                gap <= slack || CentroidCodes.withinMargin(d1[i], d2[i], REAP_MARGIN);
            if (couldFlip == false && withSpill) {
              // Final pass: the widened bound a FRESH margin test needs; see the javadoc.
              final float s = docMaxSlack[i];
              couldFlip = gap <= (MARGIN - 1f) * Math.abs(d1[i]) + s * (2f + MARGIN);
            }
            if (couldFlip == false) {
              continue;
            }
            planes.copyInto(i, scratch.qCode);
            codes.routePacked(vectors[i], shortlist, keep, routing, scratch);

            // Defend the incumbent against a coarse-shortlist miss; see the javadoc.
            final int incumbent = assignment[i];
            final float incumbentDist = codes.exactDistance(vectors[i], incumbent);
            // Ties go to the ROUTE, which is what keeps the guard to its narrow case. The common
            // outcome is the route CONFIRMING the incumbent, where routing.d1 and incumbentDist are
            // the same dot and compare equal; taking the incumbent branch there would record
            // cell2 == assignment and a zero gap, so every confirmed document would be reaped again
            // on every later iteration and the loop would never converge.
            final boolean routeWins = routing.count > 0 && routing.d1 <= incumbentDist;
            int[] spillCands = routing.cells;
            int spillCount = routing.count;
            float bestDist;
            float runnerUpDist;
            if (routeWins) {
              if (routing.cells[0] != incumbent) {
                localChanged++;
              }
              assignment[i] = routing.cells[0];
              bestDist = routing.d1;
              runnerUpDist = routing.d2;
              cell2[i] = routing.cell2;
            } else {
              // The incumbent was outside the shortlist, so the shortlist's best IS the runner-up.
              bestDist = incumbentDist;
              runnerUpDist = routing.count > 0 ? routing.d1 : Float.MAX_VALUE;
              cell2[i] = routing.count > 0 ? routing.cells[0] : -1;
              if (withSpill) {
                // Spill requires candidates[0] to be the primary, which the route's list is not.
                cands[0] = incumbent;
                System.arraycopy(routing.cells, 0, cands, 1, routing.count);
                spillCands = cands;
                spillCount = routing.count + 1;
              }
            }
            d1[i] = bestDist;
            d2[i] = runnerUpDist;
            // Re-measured, so both accumulators are spent.
            docSlack[i] = 0f;
            docMaxSlack[i] = 0f;
            if (withSpill) {
              // FRESH d1/d2, straight from the route above, which is why spill belongs here.
              final int kept =
                  Spill.select(
                      vectors[i],
                      centroids,
                      dim,
                      spillCands,
                      spillCount,
                      bestDist,
                      runnerUpDist,
                      spillBits,
                      soarLambda,
                      MARGIN,
                      sim,
                      chosen);
              cells[i] = java.util.Arrays.copyOf(chosen, kept);
            } else if (cells != null) {
              // spillBits == 0: one cell per document, and no candidate list needed.
              cells[i] = new int[] {assignment[i]};
            }
            local++;
          }
          reaped.addAndGet(local);
          changed.addAndGet(localChanged);
        });
    final Pass pass = new Pass();
    pass.reaped = reaped.get();
    pass.changed = changed.get();
    return pass;
  }

  /**
   * Recomputes each centroid as the mean of its members, recording how far each moved.
   *
   * <p>Parallel over CELLS rather than documents, using a counting sort into cell order, so each
   * worker owns its cells outright and writes centroid accumulators without synchronization.
   * Parallel over documents would need per-thread {@code nlist x dim} partials, which do not fit at
   * high {@code nlist}.
   *
   * <p>An empty cell keeps its previous position rather than being reseeded. Reseeding would
   * renumber cells between iterations, and cell ids are what documents are assigned to.
   *
   * <p>MOVEMENT IS MEASURED AGAINST THE OLD POSITION AFTER RENORMALIZATION. Measuring it before
   * would understate the displacement, and the Reaper's bound would stop being sufficient, silently
   * dropping documents from the reap set.
   *
   * <p>THE NORMALIZED MEAN IS THE MINIMIZER, not an approximation of one: {@code normalize(sum of
   * members)} maximizes {@code sum dot(v, c)} over the unit sphere, which is the objective routing
   * ranks by. That is what makes this step monotone; see the class javadoc.
   */
  private static void recomputeCentroids(
      float[][] vectors,
      int count,
      int dim,
      int nlist,
      int[] assignment,
      float[][] centroids,
      float[] movement)
      throws IOException {

    // Counting sort documents into cell order, so each cell's members are one contiguous run.
    final int[] cellStart = new int[nlist + 1];
    for (int i = 0; i < count; i++) {
      cellStart[assignment[i] + 1]++;
    }
    for (int c = 0; c < nlist; c++) {
      cellStart[c + 1] += cellStart[c];
    }
    final int[] byCell = new int[count];
    final int[] cursor = new int[nlist];
    for (int i = 0; i < count; i++) {
      final int c = assignment[i];
      byCell[cellStart[c] + cursor[c]++] = i;
    }

    Parallel.overCells(
        nlist,
        count,
        cellStart,
        (from, to) -> {
          final double[] acc = new double[dim];
          float[] prev = new float[dim];
          for (int c = from; c < to; c++) {
            final int start = cellStart[c];
            final int end = cellStart[c + 1];
            final float[] cent = centroids[c];
            if (start == end) {
              movement[c] = 0f;
              continue;
            }
            java.util.Arrays.fill(acc, 0.0);
            for (int s = start; s < end; s++) {
              final float[] v = vectors[byCell[s]];
              for (int d = 0; d < dim; d++) {
                acc[d] += v[d];
              }
            }
            final double inv = 1.0 / (end - start);
            // Keep the OLD position to measure movement against; see the javadoc.
            if (prev.length < dim) {
              prev = new float[dim];
            }
            System.arraycopy(cent, 0, prev, 0, dim);
            for (int d = 0; d < dim; d++) {
              cent[d] = (float) (acc[d] * inv);
            }
            // Every similarity; see normalize() and the javadoc above.
            normalize(cent);
            double moved = 0;
            for (int d = 0; d < dim; d++) {
              final double delta = (double) cent[d] - prev[d];
              moved += delta * delta;
            }
            movement[c] = (float) Math.sqrt(moved);
          }
        });
  }

  /**
   * Picks initial centroids by reservoir-sampling distinct documents.
   *
   * <p>Deterministic, from a fixed seed. Sampling actual documents puts every initial centroid
   * inside the data distribution; the Lloyd iterations that follow are what separate them, and a
   * centroid seeded outside the data can start empty and stay empty.
   *
   * <p>When {@code nlist} exceeds the document count the surplus centroids duplicate sampled
   * documents. They hold empty cells and keep their position, which preserves the invariant that
   * every segment has exactly {@code nlist} centroids.
   */
  private static float[][] sampleCentroids(float[][] vectors, int count, int dim, int nlist) {
    final int[] pick = reservoirSample(count, Math.min(nlist, count));
    final float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      final int src = pick.length == 0 ? -1 : pick[c % pick.length];
      centroids[c] = new float[dim];
      if (src >= 0) {
        System.arraycopy(vectors[src], 0, centroids[c], 0, dim);
      }
    }
    return centroids;
  }

  /** Largest element, for the no-runner-up fallback. */
  private static float maxOf(float[] a) {
    float m = 0f;
    for (float x : a) {
      if (x > m) {
        m = x;
      }
    }
    return m;
  }

  /** Reservoir sample of {@code k} distinct indices from {@code [0, n)}, deterministic. */
  static int[] reservoirSample(int n, int k) {
    k = Math.min(k, n);
    final Random random = new Random(SEED);
    final int[] pick = new int[k];
    for (int i = 0; i < k; i++) {
      pick[i] = i;
    }
    for (int i = k; i < n; i++) {
      final int j = random.nextInt(i + 1);
      if (j < k) {
        pick[j] = i;
      }
    }
    return pick;
  }

  private static float[][] copyOf(float[][] src, int dim) {
    final float[][] out = new float[src.length][];
    for (int i = 0; i < src.length; i++) {
      out[i] = new float[dim];
      System.arraycopy(src[i], 0, out[i], 0, Math.min(dim, src[i].length));
    }
    return out;
  }

  private static void normalize(float[] v) {
    double norm = 0;
    for (float x : v) {
      norm += (double) x * x;
    }
    if (norm == 0) {
      return;
    }
    final float inv = (float) (1.0 / Math.sqrt(norm));
    for (int d = 0; d < v.length; d++) {
      v[d] *= inv;
    }
  }
}
