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

/**
 * Spill-cell selection: which additional cells a document is written into.
 *
 * <p>A query probes {@code nprobe} cells. A document near a cell boundary can sit outside every
 * cell a query probes, and is then unreachable whatever the scoring does. Writing boundary
 * documents into more than one cell recovers them, at the cost of index size.
 *
 * <h2>The SOAR objective</h2>
 *
 * <p>SOAR (ScaNN, Sun et al., NeurIPS 2023) penalizes a spill cell whose residual is parallel to
 * the primary's:
 *
 * <pre>
 *   loss(c) = ||v - c||^2  +  lambda * (r1 . (v - c))^2 / ||r1||^2      where r1 = v - c_primary
 * </pre>
 *
 * <p>The penalty is large when {@code v - c} points the same way as the primary residual, so the
 * chosen cells are COMPLEMENTARY: they cover the directions the primary serves worst. The
 * next-nearest centroids tend to lie in the primary's own direction, so a query that misses the
 * primary misses them too.
 *
 * <h2>The margin</h2>
 *
 * <p>Only near-boundary documents are spilled, since a document deep inside its cell is found
 * whenever its cell is probed. The test is {@link CentroidCodes#withinMargin}, a difference against
 * {@code |d1|}, because the ratio {@code d2 / d1} is not monotone for the negated-dot distances the
 * inner-product family uses.
 *
 * <p>{@code spillBits} is therefore a cap rather than a quota: interior documents collapse to a
 * single cell.
 *
 * @lucene.experimental
 */
final class Spill {

  private Spill() {}

  /**
   * Chooses the cells for one document, primary first.
   *
   * @param vector the rotated document vector
   * @param candidates cells to consider, nearest-first, as produced by the routing scan. SOAR picks
   *     from among these, which keeps selection {@code O(nCand)}; the routing shortlist is an
   *     oversample, so the complementary cell is in it.
   * @param nCand valid entries in {@code candidates}
   * @param d1 exact distance to the primary
   * @param d2 exact distance to the runner-up
   * @param spillBits maximum ADDITIONAL cells beyond the primary
   * @param lambda SOAR weight; {@code <= 0} selects plain next-nearest
   *     <p>The interior-document test comes first: when the runner-up is far enough that a query
   *     finding it would find the primary first, a spill copy is bytes without recall.
   *     <p>The SOAR penalty is non-negative, so {@code ||v - c||^2} alone is a lower bound on the
   *     loss, and a candidate that already loses on that bound cannot win once the penalty is
   *     added.
   * @param out receives the chosen cells, primary at index 0
   * @return how many cells were chosen, always at least 1
   */
  static int select(
      float[] vector,
      float[][] centroids,
      int dim,
      int[] candidates,
      int nCand,
      float d1,
      float d2,
      int spillBits,
      float lambda,
      float margin,
      VectorSimilarityFunction sim,
      int[] out) {

    final int primary = nCand > 0 ? candidates[0] : 0;
    out[0] = primary;
    if (spillBits <= 0 || nCand <= 1) {
      return 1;
    }
    // Interior document: a spill copy would add bytes and no recall.
    if (CentroidCodes.withinMargin(d1, d2, margin) == false) {
      return 1;
    }

    if (lambda <= 0f) {
      // Plain next-nearest, still margin-gated.
      int kept = 1;
      for (int i = 1; i < nCand && kept <= spillBits; i++) {
        out[kept++] = candidates[i];
      }
      return kept;
    }

    // The primary residual r1; see the class javadoc.
    final float[] r1 = new float[dim];
    double r1NormSq = 0;
    final float[] pc = centroids[primary];
    for (int d = 0; d < dim; d++) {
      final float e = vector[d] - pc[d];
      r1[d] = e;
      r1NormSq += (double) e * e;
    }
    if (r1NormSq == 0) {
      // The document sits on its centroid, so there is no residual to complement. Take the nearest.
      int kept = 1;
      for (int i = 1; i < nCand && kept <= spillBits; i++) {
        out[kept++] = candidates[i];
      }
      return kept;
    }
    final double invR1NormSq = 1.0 / r1NormSq;

    // Rank the rest by SOAR loss, keeping the best `spillBits`; the list is short, so insert-sort.
    final float[] bestLoss = new float[spillBits];
    final int[] bestCell = new int[spillBits];
    java.util.Arrays.fill(bestLoss, Float.POSITIVE_INFINITY);
    java.util.Arrays.fill(bestCell, -1);
    int filled = 0;

    for (int ci = 1; ci < nCand; ci++) {
      final int c = candidates[ci];
      if (c == primary || c < 0) {
        continue;
      }
      final float[] cc = centroids[c];
      double resNormSq = 0;
      double dotR1 = 0;
      for (int d = 0; d < dim; d++) {
        final double e = vector[d] - cc[d];
        resNormSq += e * e;
        dotR1 += e * r1[d];
      }
      // ||v - c||^2 is a lower bound on the loss; see the javadoc.
      if (filled == spillBits && resNormSq >= bestLoss[spillBits - 1]) {
        continue;
      }
      final float loss = (float) (resNormSq + lambda * dotR1 * dotR1 * invR1NormSq);
      if (filled == spillBits && loss >= bestLoss[spillBits - 1]) {
        continue;
      }
      int pos = filled < spillBits ? filled : spillBits - 1;
      while (pos > 0 && bestLoss[pos - 1] > loss) {
        bestLoss[pos] = bestLoss[pos - 1];
        bestCell[pos] = bestCell[pos - 1];
        pos--;
      }
      bestLoss[pos] = loss;
      bestCell[pos] = c;
      if (filled < spillBits) {
        filled++;
      }
    }

    int kept = 1;
    for (int i = 0; i < filled; i++) {
      if (bestCell[i] >= 0) {
        out[kept++] = bestCell[i];
      }
    }
    return kept;
  }
}
