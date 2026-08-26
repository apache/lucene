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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests Lloyd clustering and, centrally, the Reaper's SUFFICIENCY claim.
 *
 * <p>The Reaper re-routes only documents that could have changed cell, on the argument that if the
 * gap between a document's two best cells exceeds twice the largest centroid movement, neither cell
 * can have overtaken the other. That is a claim about correctness, not a heuristic, so it is tested
 * as one: against a full re-route of every document, the Reaper's assignment must be IDENTICAL
 * rather than close. Otherwise documents land in the wrong cell silently, which shows up much later
 * as a recall shortfall that is hard to attribute back to here.
 */
public class TestClustering extends LuceneTestCase {

  private static final int DIM = 64;

  /**
   * THE KEY TEST. Reaper-based clustering must reach exactly the same assignment as re-routing
   * every document on every iteration.
   *
   * <p>Implemented by running the real clusterer and then, for the final centroids, checking every
   * document against a brute-force nearest search. Any document whose recorded assignment is not
   * its true nearest cell is a document the Reaper wrongly skipped.
   */
  public void testReaperAssignmentMatchesFullReroute() throws IOException {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int count = 3000;
      final int nlist = 40;
      float[][] vectors = clusteredCorpus(count, nlist, DIM);

      Clustering.Result r = Clustering.cluster(vectors, count, DIM, nlist, 4, sim, null);

      // Ground truth against the FINAL centroids: what a full re-route would have produced.
      CentroidCodes exact = new CentroidCodes(r.centroids, DIM, sim, null);
      int mismatches = 0;
      for (int i = 0; i < count; i++) {
        int best = -1;
        float bestD = Float.MAX_VALUE;
        for (int c = 0; c < nlist; c++) {
          float d = exact.exactDistance(vectors[i], c);
          if (d < bestD) {
            bestD = d;
            best = c;
          }
        }
        if (r.assignment[i] != best) {
          // Tolerate an exact tie: both cells are correct, and the winner is a tie-break detail.
          if (Math.abs(exact.exactDistance(vectors[i], r.assignment[i]) - bestD) > 1e-5f) {
            mismatches++;
          }
        }
      }
      assertEquals(
          "the Reaper must not skip any document whose cell changed, under " + sim, 0, mismatches);
    }
  }

  /**
   * The Reaper must actually SHRINK the work, and by a lot, or the per-pair bound has regressed to
   * something looser.
   *
   * <p>A performance property enforced as a test on purpose. Both the per-pair bound and the global
   * {@code 2*max(delta)} bound are CORRECT, so the sufficiency test above cannot catch a regression
   * from one to the other; it would show up only as a slower build. The per-pair bound reaps an
   * order of magnitude fewer documents per iteration than the global one, and the threshold below
   * sits between them, so the per-pair bound passes and the global one fails.
   */
  public void testReaperShrinksTheWorkSet() throws IOException {
    final int count = 8000;
    final int nlist = 100;
    float[][] vectors = clusteredCorpus(count, nlist, DIM);
    Clustering.Result r =
        Clustering.cluster(vectors, count, DIM, nlist, 6, VectorSimilarityFunction.EUCLIDEAN, null);
    // A ceiling that separates the two bounds without being sensitive to the corpus.
    final int ceiling = count / 4;
    assertTrue(
        "the final Reaper pass re-routed "
            + r.reaped
            + " of "
            + count
            + " documents (>"
            + ceiling
            + "); the per-pair movement bound appears to have regressed to a looser one",
        r.reaped < ceiling);
  }

  /**
   * SPILL COMPLETENESS. Every document that a full-strength margin test would spill must actually
   * have been spilled by the final Reaper pass.
   *
   * <p>The test for the trap in folding spill into reap. The reap test reads a STALE gap, spill
   * needs the FRESH one, and they differ by the accumulated slack, so "the reap set contains the
   * spill set" is false for the plain reap bound by a band of width {@code slack*(1+margin)}.
   * {@link Clustering#reap} widens the bound on the final pass to close it.
   *
   * <p>Without this test the failure is invisible: a missed document gets one cell instead of
   * several, no error is raised, and the only symptom is lower recall for near-boundary documents,
   * the exact population spill exists to serve.
   */
  public void testEveryBoundaryDocumentGetsSpilled() throws IOException {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int count = 4000;
      final int nlist = 60;
      final int spillBits = 2;
      float[][] vectors = clusteredCorpus(count, nlist, DIM);

      Clustering.Result r =
          Clustering.cluster(vectors, count, DIM, nlist, 4, sim, null, null, spillBits, 1.0f);

      // Ground truth against the FINAL centroids, asking the margin test directly.
      final CentroidCodes exact = new CentroidCodes(r.centroids, DIM, sim, null);
      int shouldSpill = 0;
      int missed = 0;
      for (int i = 0; i < count; i++) {
        float best = Float.MAX_VALUE;
        float second = Float.MAX_VALUE;
        for (int c = 0; c < nlist; c++) {
          final float d = exact.exactDistance(vectors[i], c);
          if (d < best) {
            second = best;
            best = d;
          } else if (d < second) {
            second = d;
          }
        }
        if (CentroidCodes.withinMargin(best, second, Clustering.MARGIN)) {
          shouldSpill++;
          if (r.cells[i].length < 2) {
            missed++;
          }
        }
      }
      // The corpus must exercise the property, or the assertion below is vacuous.
      assertTrue(
          "the corpus must contain boundary documents for this test to mean anything, got "
              + shouldSpill,
          shouldSpill > 20);
      // Nearly all, tolerating the cascade's coarse-retention misses; see the javadoc.
      final int tolerated = Math.max(1, shouldSpill / 100);
      assertTrue(
          "documents inside the spill margin that were never spilled, under "
              + sim
              + ": "
              + missed
              + " of "
              + shouldSpill
              + " eligible (tolerating "
              + tolerated
              + " for coarse-shortlist misses); a larger shortfall means the final reap pass's widened"
              + " bound is not covering the spill set",
          missed <= tolerated);
    }
  }

  /** Spill must stay within its cap, and every chosen cell must be real and distinct. */
  public void testSpillRespectsTheCap() throws IOException {
    final int count = 2000;
    final int nlist = 40;
    for (int spillBits : new int[] {0, 1, 2, 3}) {
      Clustering.Result r =
          Clustering.cluster(
              clusteredCorpus(count, nlist, DIM),
              count,
              DIM,
              nlist,
              3,
              VectorSimilarityFunction.EUCLIDEAN,
              null,
              null,
              spillBits,
              1.0f);
      for (int i = 0; i < count; i++) {
        final int[] cells = r.cells[i];
        assertNotNull("document " + i + " must have cells at spillBits=" + spillBits, cells);
        assertTrue("at least the primary", cells.length >= 1);
        assertTrue(
            "spill cap exceeded at spillBits=" + spillBits + ": " + cells.length,
            cells.length <= 1 + spillBits);
        assertEquals("cells[0] must be the primary", r.assignment[i], cells[0]);
        // Duplicate cells would break the reader's distinct-document accounting.
        for (int a = 0; a < cells.length; a++) {
          assertTrue("cell in range: " + cells[a], cells[a] >= 0 && cells[a] < nlist);
          for (int b = a + 1; b < cells.length; b++) {
            assertNotEquals("duplicate spill cell for document " + i, cells[a], cells[b]);
          }
        }
      }
    }
  }

  /**
   * The exact-placement primitive must return the TRUE nearest centroid, so it is pinned against
   * brute force directly rather than only through the flag-gated build path, which a test JVM
   * cannot toggle since the flag is read at class load.
   */
  public void testNearestExactIsTrueNearest() throws IOException {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int count = 500;
      final int nlist = 40;
      final float[][] vectors = clusteredCorpus(count, nlist, DIM);
      // Any centroids will do, since the primitive is a pure argmin over them.
      final Clustering.Result r = Clustering.cluster(vectors, count, DIM, nlist, 3, sim, null);
      final CentroidCodes codes = new CentroidCodes(r.centroids, DIM, sim, null);
      for (int i = 0; i < count; i++) {
        float bestD = Float.MAX_VALUE;
        for (int c = 0; c < nlist; c++) {
          final float d = codes.exactDistance(vectors[i], c);
          if (d < bestD) {
            bestD = d;
          }
        }
        final int got = codes.nearestExact(vectors[i]);
        // Tolerate an exact tie: equal-distance cells are both correct.
        assertEquals(
            "nearestExact must match brute force for doc " + i + " under " + sim,
            bestD,
            codes.exactDistance(vectors[i], got),
            1e-6f);
      }
    }
  }

  /** Clustering must be deterministic: identical input yields an identical index. */
  public void testDeterminism() throws IOException {
    final int count = 1500;
    final int nlist = 24;
    float[][] a = clusteredCorpus(count, nlist, DIM);
    float[][] b = new float[count][];
    for (int i = 0; i < count; i++) {
      b[i] = a[i].clone();
    }
    Clustering.Result r1 =
        Clustering.cluster(a, count, DIM, nlist, 3, VectorSimilarityFunction.EUCLIDEAN, null);
    Clustering.Result r2 =
        Clustering.cluster(b, count, DIM, nlist, 3, VectorSimilarityFunction.EUCLIDEAN, null);
    assertArrayEquals("assignments must be identical across runs", r1.assignment, r2.assignment);
    for (int c = 0; c < nlist; c++) {
      assertArrayEquals(
          "centroid " + c + " must be identical", r1.centroids[c], r2.centroids[c], 0f);
    }
  }

  /** Every segment must get exactly nlist centroids, even when documents are scarce. */
  public void testAlwaysProducesNlistCentroids() throws IOException {
    for (int count : new int[] {1, 5, 37}) {
      final int nlist = 16;
      float[][] vectors = new float[count][];
      for (int i = 0; i < count; i++) {
        vectors[i] = randomVector(DIM);
      }
      Clustering.Result r =
          Clustering.cluster(
              vectors, count, DIM, nlist, 2, VectorSimilarityFunction.EUCLIDEAN, null);
      assertEquals("nlist centroids at count=" + count, nlist, r.centroids.length);
      for (int c = 0; c < nlist; c++) {
        assertNotNull("centroid " + c + " must exist", r.centroids[c]);
        assertEquals(DIM, r.centroids[c].length);
      }
      for (int i = 0; i < count; i++) {
        assertTrue("assignment in range", r.assignment[i] >= 0 && r.assignment[i] < nlist);
      }
    }
  }

  /** Warm-starting from a seed must be honoured, which is what merge relies on. */
  public void testSeedWarmStart() throws IOException {
    final int count = 2000;
    final int nlist = 30;
    float[][] vectors = clusteredCorpus(count, nlist, DIM);

    Clustering.Result first =
        Clustering.cluster(vectors, count, DIM, nlist, 3, VectorSimilarityFunction.EUCLIDEAN, null);
    // Re-clustering from converged centroids must agree, since the seed already IS the answer.
    Clustering.Result second =
        Clustering.cluster(
            vectors, count, DIM, nlist, 1, VectorSimilarityFunction.EUCLIDEAN, first.centroids);
    int agree = 0;
    for (int i = 0; i < count; i++) {
      if (first.assignment[i] == second.assignment[i]) {
        agree++;
      }
    }
    assertTrue(
        "warm start from converged centroids should mostly reproduce the assignment, got "
            + agree
            + "/"
            + count,
        agree >= (int) (0.95 * count));
  }

  /** Clustering must reduce within-cell distortion versus the initial sample. */
  public void testDistortionDecreases() throws IOException {
    final int count = 3000;
    final int nlist = 40;
    float[][] vectors = clusteredCorpus(count, nlist, DIM);

    Clustering.Result one =
        Clustering.cluster(vectors, count, DIM, nlist, 1, VectorSimilarityFunction.EUCLIDEAN, null);
    Clustering.Result many =
        Clustering.cluster(vectors, count, DIM, nlist, 6, VectorSimilarityFunction.EUCLIDEAN, null);
    assertTrue(
        "more Lloyd iterations must not increase distortion: "
            + distortion(vectors, count, many)
            + " vs "
            + distortion(vectors, count, one),
        distortion(vectors, count, many) <= distortion(vectors, count, one) * 1.001);
  }

  /**
   * Centroids must be unit norm for EVERY similarity, not only the inner-product family.
   *
   * <p>The invariant three separate things assume: {@code exactDistance} ranks by {@code -dot} and
   * is a correct nearest-cell test only when every centroid shares one norm; {@code Nitrox2}'s grid
   * is a function of {@code dim} alone, derived for a unit-norm vector, so a shrunken centroid
   * quantizes into the middle level and the coarse tier loses the ability to tell centroids apart;
   * and the mean update is the constrained minimizer of the routed objective only on the sphere.
   *
   * <p>A mean of unit vectors has norm strictly below 1, so the Euclidean path is where this
   * regresses, and it regresses SILENTLY into a recall shortfall.
   */
  public void testCentroidsAreUnitNormForEverySimilarity() throws IOException {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int count = 2000;
      final int nlist = 32;
      final float[][] vectors = clusteredCorpus(count, nlist, DIM);
      final Clustering.Result r = Clustering.cluster(vectors, count, DIM, nlist, 5, sim, null);
      for (int c = 0; c < nlist; c++) {
        double norm = 0;
        for (int d = 0; d < DIM; d++) {
          norm += (double) r.centroids[c][d] * r.centroids[c][d];
        }
        assertEquals(
            "centroid " + c + " must be unit norm under " + sim, 1.0, Math.sqrt(norm), 1e-4);
      }
    }
  }

  /**
   * MONOTONICITY. The clustering objective must never increase with more iterations allowed.
   *
   * <p>Measured in the metric the algorithm actually routes by, {@code sum -dot(v, c_assigned)},
   * rather than raw Euclidean distortion, since that is the function both Lloyd steps minimize.
   *
   * <p>This is what makes the convergence stop well-founded rather than hopeful. A step that can
   * raise the objective can cycle, and a cycling loop never falls to {@link
   * Clustering#CONVERGE_FRACTION}, so it spends the whole backstop on every build; the symptom is a
   * slow indexer, not a wrong answer, which is why it needs a test rather than an assertion.
   *
   * <p>The specific way it would regress is {@link Clustering#reap} adopting the coarse shortlist's
   * best without comparing it to the incumbent: when the coarse tier misses, that swaps a nearer
   * cell for a farther one.
   */
  public void testObjectiveIsMonotoneInIterations() throws IOException {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int count = 4000;
      final int nlist = 50;
      final float[][] vectors = clusteredCorpus(count, nlist, DIM);
      double previous = Double.POSITIVE_INFINITY;
      for (int iters = 1; iters <= 8; iters++) {
        final Clustering.Result r =
            Clustering.cluster(vectors, count, DIM, nlist, iters, sim, null);
        final CentroidCodes codes = new CentroidCodes(r.centroids, DIM, sim, null);
        double objective = 0;
        for (int i = 0; i < count; i++) {
          objective += codes.exactDistance(vectors[i], r.assignment[i]);
        }
        // A float-precision tolerance, scaled to the objective, not an accuracy allowance.
        assertTrue(
            "objective rose from "
                + previous
                + " to "
                + objective
                + " at maxIters="
                + iters
                + " under "
                + sim
                + "; a Lloyd step is raising it, so the loop can cycle and never converge",
            objective <= previous + 1e-4 * Math.abs(previous));
        previous = objective;
      }
    }
  }

  /**
   * The loop must STOP on convergence rather than spending its backstop.
   *
   * <p>The whole point of the convergence rule, and a property no correctness test can see: a build
   * that ignores the threshold produces an equally valid index, just slower. So the iteration count
   * is asserted directly.
   */
  public void testStopsEarlyOnConvergence() throws IOException {
    final int count = 6000;
    final int nlist = 40;
    final int backstop = 60;
    final float[][] vectors = clusteredCorpus(count, nlist, DIM);
    final Clustering.Result r =
        Clustering.cluster(
            vectors, count, DIM, nlist, backstop, VectorSimilarityFunction.EUCLIDEAN, null);
    assertTrue(
        "clustering ran the full backstop of "
            + backstop
            + " iterations without converging; the Reaper's count never fell to "
            + Clustering.CONVERGE_FRACTION
            + " of the corpus",
        r.converged);
    assertTrue(
        "converged but reported " + r.iterations + " of " + backstop + " iterations",
        r.iterations < backstop);
    // The backstop must not be what decides a converging build, so a far larger one must agree.
    final Clustering.Result loose =
        Clustering.cluster(
            vectors, count, DIM, nlist, backstop * 4, VectorSimilarityFunction.EUCLIDEAN, null);
    assertEquals(
        "a larger backstop must not change a converged build's iteration count",
        r.iterations,
        loose.iterations);
    assertArrayEquals(
        "a larger backstop must not change a converged build's assignment",
        r.assignment,
        loose.assignment);
  }

  private double distortion(float[][] vectors, int count, Clustering.Result r) {
    double total = 0;
    for (int i = 0; i < count; i++) {
      final float[] c = r.centroids[r.assignment[i]];
      for (int d = 0; d < vectors[i].length; d++) {
        final double delta = vectors[i][d] - c[d];
        total += delta * delta;
      }
    }
    return total;
  }

  /** A corpus with real cluster structure, which is what a router actually sees. */
  private float[][] clusteredCorpus(int count, int clusters, int dim) {
    float[][] centres = new float[clusters][];
    for (int c = 0; c < clusters; c++) {
      centres[c] = randomVector(dim);
    }
    float[][] out = new float[count][];
    for (int i = 0; i < count; i++) {
      final float[] centre = centres[random().nextInt(clusters)];
      out[i] = new float[dim];
      final double std = 1.0 / Math.sqrt(dim);
      for (int d = 0; d < dim; d++) {
        out[i][d] = (float) (centre[d] + random().nextGaussian() * std * 0.3);
      }
    }
    return out;
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (random().nextGaussian() * std);
    }
    return v;
  }
}
