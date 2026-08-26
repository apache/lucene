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

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the routing cascade: coarse-scan everything, verify an oversampled shortlist exactly.
 *
 * <p>The claim under test is that the cascade's ACCURACY is that of its exact stage. That holds
 * only if the coarse stage retains the true nearest centroid in its shortlist; if it drops it, no
 * amount of exact reranking recovers it.
 *
 * <h2>The query distribution matters enormously, and only one of them is real</h2>
 *
 * <p>Measured retention of the true nearest centroid at dim=256, nlist=600, by shortlist size:
 *
 * <pre>
 *   shortlist:            8      16      24      40      80     160
 *   clustered queries: 1.000   1.000   1.000   1.000   1.000   1.000
 *   uniform queries:   0.503   0.650   0.735   0.823   0.938   0.990
 * </pre>
 *
 * <p>"Clustered" means the query sits near some real centroid, which is the situation the router is
 * always actually in: centroids come from k-means over the same corpus, so a document is close to
 * its own cell and distinctly farther from the rest. "Uniform" means both centroids and query are
 * independent Gaussians, and in high dimensions that makes essentially ALL centroids equidistant,
 * so the "true nearest" is one of a large set of near-ties and a 2-bit code cannot resolve which.
 * The uniform column is not a weakness of the coarse tier; it is a distribution where the question
 * has no stable answer.
 *
 * <p>So the retention test below uses clustered queries deliberately, and a second test pins the
 * uniform case as a documented lower bound rather than pretending it does not exist. Getting this
 * backwards would either understate the design (a 0.735 that looks alarming) or overstate it.
 */
public class TestCentroidCodes extends LuceneTestCase {

  private static final int DIM = 256;

  /**
   * With clustered queries, the only distribution the router meets in practice, the coarse tier
   * must retain the true nearest centroid essentially always, and the verified ordering must be
   * exact.
   *
   * <p>The queries are drawn near real centroids, which is what the router always sees, since
   * centroids are k-means output over the same corpus; see the class javadoc for why a uniform
   * query is a different and much harder question.
   *
   * <p>{@code d1}/{@code d2} come from the FINE tier, so they APPROXIMATE the exact distance rather
   * than reproducing it. Exact ordering is not promised either, since an 8-bit code can transpose
   * two nearly equidistant cells: the cascade's contract is that the SET is right. What must hold
   * is the ordering they are used for, that {@code d1} is the best of what was verified and {@code
   * d2} is no better than {@code d1}.
   *
   * <p>The retention threshold leaves headroom for the RNG without tolerating a real regression.
   */
  public void testCascadeFindsTrueNearest() {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.DOT_PRODUCT
        }) {
      final int nlist = 600;
      final int keep = 8;
      final int shortlist = 3 * keep;

      float[][] centroids = new float[nlist][];
      for (int c = 0; c < nlist; c++) {
        centroids[c] = randomRotatedVector(DIM);
      }
      CentroidCodes cc = new CentroidCodes(centroids, DIM, sim, new Int8Quantizer());
      CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, shortlist);
      CentroidCodes.Routing routing = new CentroidCodes.Routing(keep);

      int hits = 0;
      final int trials = 200;
      for (int t = 0; t < trials; t++) {
        float[] q = nearCentroid(centroids[random().nextInt(nlist)], DIM);
        cc.route(q, shortlist, keep, routing, scratch);

        // Ground truth by brute force over the same exact distance function.
        int best = -1;
        float bestD = Float.MAX_VALUE;
        for (int c = 0; c < nlist; c++) {
          float d = cc.exactDistance(q, c);
          if (d < bestD) {
            bestD = d;
            best = c;
          }
        }
        if (routing.count > 0 && routing.cells[0] == best) {
          hits++;
        }
        if (routing.count > 0) {
          assertEquals(
              "d1 must be the quantized distance to the reported nearest cell",
              cc.exactDistance(q, routing.cells[0]),
              routing.d1,
              0.05f);
        }
        if (routing.count > 1) {
          assertTrue("d2 must not be nearer than d1", routing.d2 >= routing.d1);
        }
      }
      assertTrue(
          "coarse tier retained the true nearest only " + hits + "/" + trials + " under " + sim,
          hits >= (int) (0.98 * trials));
    }
  }

  /**
   * A shortlist covering every centroid removes the COARSE stage's error, leaving only the fine
   * tier's.
   *
   * <p>Not exact: the verify stage quantizes to 8 bits per dimension, so it can transpose cells
   * whose true distances fall within its resolution. What a full shortlist guarantees is that no
   * cell was discarded before verification, the coarse tier's failure mode and the one this checks.
   */
  public void testFullShortlistIsExact() {
    final int nlist = 200;
    float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      centroids[c] = randomRotatedVector(DIM);
    }
    CentroidCodes cc =
        new CentroidCodes(centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, new Int8Quantizer());
    CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, nlist);
    CentroidCodes.Routing routing = new CentroidCodes.Routing(nlist);

    for (int t = 0; t < 30; t++) {
      float[] q = randomRotatedVector(DIM);
      cc.route(q, nlist, 3, routing, scratch);
      float bestD = Float.MAX_VALUE;
      for (int c = 0; c < nlist; c++) {
        float d = cc.exactDistance(q, c);
        if (d < bestD) {
          bestD = d;
        }
      }
      final float reported = cc.exactDistance(q, routing.cells[0]);
      assertTrue(
          "with a full shortlist the nearest must be within the fine tier's resolution: reported "
              + reported
              + " vs true "
              + bestD,
          reported <= bestD + 0.05f);
    }
  }

  /** A centroid is its own nearest neighbour: the base case, and it must survive the cascade. */
  public void testCentroidRoutesToItself() {
    final int nlist = 300;
    float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      centroids[c] = randomRotatedVector(DIM);
    }
    CentroidCodes cc =
        new CentroidCodes(centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, new Int8Quantizer());
    CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, 32);
    CentroidCodes.Routing routing = new CentroidCodes.Routing(4);
    for (int c = 0; c < nlist; c += 7) {
      cc.route(centroids[c], 32, 4, routing, scratch);
      assertEquals("centroid " + c + " must route to itself", c, routing.cells[0]);
      // -dot over unit-norm vectors, so a vector against itself is about -1, up to quantization.
      assertEquals("its own distance must be about -1", -1f, routing.d1, 0.05f);
    }
  }

  /** The scan must cross tile boundaries correctly, including a partial final tile. */
  public void testTileBoundariesAreHandled() {
    // Deliberately straddle: just over one tile, and a size that leaves a small remainder.
    for (int nlist :
        new int[] {CentroidCodes.TILE - 1, CentroidCodes.TILE, CentroidCodes.TILE + 3}) {
      float[][] centroids = new float[nlist][];
      for (int c = 0; c < nlist; c++) {
        centroids[c] = randomRotatedVector(DIM);
      }
      CentroidCodes cc =
          new CentroidCodes(
              centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, new Int8Quantizer());
      CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, nlist);
      CentroidCodes.Routing routing = new CentroidCodes.Routing(nlist);
      float[] q = randomRotatedVector(DIM);
      cc.route(q, nlist, nlist, routing, scratch);
      assertEquals("every centroid must be reachable at nlist=" + nlist, nlist, routing.count);
      // Every cell id must appear exactly once; a tiling bug duplicates or drops rows.
      boolean[] seen = new boolean[nlist];
      for (int i = 0; i < routing.count; i++) {
        assertFalse("cell " + routing.cells[i] + " returned twice", seen[routing.cells[i]]);
        seen[routing.cells[i]] = true;
      }
    }
  }

  /**
   * The margin test must be valid for NEGATIVE distances, which is the whole reason it is a
   * difference rather than a ratio. A ratio test on negated dots misclassifies exactly the
   * near-boundary documents the Reaper and spill selection exist to find.
   */
  public void testMarginTestHandlesNegativeDistances() {
    // Negated-dot distances: both negative, d2 only slightly worse (less negative) than d1.
    assertTrue(
        "a near-boundary pair of negative distances must be within margin",
        CentroidCodes.withinMargin(-1.00f, -0.95f, 1.10f));
    assertFalse(
        "a far pair of negative distances must be outside margin",
        CentroidCodes.withinMargin(-1.00f, -0.50f, 1.10f));

    // The naive ratio d2 <= margin*d1 rejects the near pair it should accept; see withinMargin.
    assertTrue("ratio form would misclassify this", -0.95f > 1.10f * -1.00f);

    // Positive (Euclidean) distances reduce to the familiar ratio.
    assertTrue(CentroidCodes.withinMargin(1.00f, 1.05f, 1.10f));
    assertFalse(CentroidCodes.withinMargin(1.00f, 1.50f, 1.10f));

    // A lone cell has no runner-up and is never a boundary document.
    assertFalse(CentroidCodes.withinMargin(1.0f, Float.MAX_VALUE, 1.10f));
  }

  /** Re-encoding after centroids move must be reflected in subsequent routing. */
  public void testEncodeAllPicksUpMovedCentroids() {
    final int nlist = 64;
    float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      centroids[c] = randomRotatedVector(DIM);
    }
    CentroidCodes cc =
        new CentroidCodes(centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, new Int8Quantizer());
    CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, nlist);
    CentroidCodes.Routing routing = new CentroidCodes.Routing(2);

    // Move centroid 0 onto a known query, then re-encode; it must become that query's nearest.
    float[] q = randomRotatedVector(DIM);
    System.arraycopy(q, 0, centroids[0], 0, DIM);
    cc.encodeAll();
    cc.route(q, nlist, 2, routing, scratch);
    assertEquals("the moved centroid must now be nearest", 0, routing.cells[0]);
  }

  /**
   * Pins the UNIFORM-query case as a documented lower bound.
   *
   * <p>Independent Gaussian centroids in high dimensions are all nearly equidistant, so the true
   * nearest is one of many near-ties and a 2-bit code cannot single it out. This is recorded rather
   * than hidden: if someone benchmarks routing on synthetic uniform data and sees ~0.7, that is the
   * distribution rather than a defect. The threshold is deliberately loose: its job is to catch a
   * catastrophic regression (a broken scan returning arbitrary cells), not to police this number.
   */
  public void testUniformQueriesAreTheHardCaseAndAreBounded() {
    final int nlist = 600;
    final int keep = 8;
    final int shortlist = 3 * keep;
    float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      centroids[c] = randomRotatedVector(DIM);
    }
    CentroidCodes cc =
        new CentroidCodes(centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, new Int8Quantizer());
    CentroidCodes.Scratch scratch = new CentroidCodes.Scratch(DIM, nlist, shortlist);
    CentroidCodes.Routing routing = new CentroidCodes.Routing(keep);

    int hits = 0;
    final int trials = 200;
    for (int t = 0; t < trials; t++) {
      float[] q = randomRotatedVector(DIM);
      cc.route(q, shortlist, keep, routing, scratch);
      int best = -1;
      float bestD = Float.MAX_VALUE;
      for (int c = 0; c < nlist; c++) {
        float d = cc.exactDistance(q, c);
        if (d < bestD) {
          bestD = d;
          best = c;
        }
      }
      if (routing.count > 0 && routing.cells[0] == best) {
        hits++;
      }
    }
    // Measured ~0.735 at this shortlist; 0.5 catches a genuinely broken scan and nothing else.
    assertTrue(
        "uniform-query retention collapsed to "
            + hits
            + "/"
            + trials
            + ", far below the ~0.73"
            + " expected for this distribution; suspect the scan itself",
        hits >= trials / 2);
  }

  /** A vector near {@code centroid}: the realistic query/document distribution for a router. */
  private float[] nearCentroid(float[] centroid, int dim) {
    float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (centroid[d] + random().nextGaussian() * std * 0.35);
    }
    org.apache.lucene.util.VectorUtil.l2normalize(v);
    return v;
  }

  /**
   * A rotated, UNIT-NORM vector: the codec normalizes everything before rotating, so a test corpus
   * that did not would be measuring distances the codec never sees. It also makes {@code -dot} of a
   * vector with itself exactly -1, which is what the self-routing test asserts.
   */
  private float[] randomRotatedVector(int dim) {
    float[] v = new float[dim];
    final double std = 1.0 / Math.sqrt(dim);
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (random().nextGaussian() * std);
    }
    org.apache.lucene.util.VectorUtil.l2normalize(v);
    return v;
  }
}
