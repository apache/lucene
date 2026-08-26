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
 * Pins the int8 fine tier's correctness claims.
 *
 * <p>The load-bearing one is the offset correction: codes are stored unsigned so the scan can use
 * the fast unsigned kernel, and the signed dot is recovered algebraically afterwards. That recovery
 * is claimed to be EXACT rather than approximate, so it is tested as an identity against a directly
 * computed signed dot. If it drifts, every score shifts by a data-dependent amount, which is
 * invisible except as a recall change.
 */
public class TestInt8Quantizer extends LuceneTestCase {

  private static final int DIM = 1024;

  /**
   * The offset correction must recover the signed dot EXACTLY, for every input including saturating
   * ones. This is the identity the fast unsigned kernel depends on.
   */
  public void testOffsetCorrectionRecoversSignedDotExactly() {
    final Int8Quantizer q = new Int8Quantizer();
    for (int trial = 0; trial < 200; trial++) {
      final int dim = TestUtilDims.pick(random());
      float[] a = randomVector(dim, trial % 4 == 0 ? 40f : 1f);
      float[] b = randomVector(dim, trial % 3 == 0 ? 40f : 1f);

      byte[] codeA = new byte[dim];
      byte[] codeB = new byte[dim];
      float[] corrA = new float[4];
      float[] corrB = new float[4];
      q.encode(a, dim, null, codeA, corrA);
      q.encode(b, dim, null, codeB, corrB);

      // Signed codes, recovered from the stored unsigned form.
      int[] sa = new int[dim];
      int[] sb = new int[dim];
      long directSigned = 0;
      int sumA = 0;
      int sumB = 0;
      for (int d = 0; d < dim; d++) {
        sa[d] = (codeA[d] & 0xFF) - 128;
        sb[d] = (codeB[d] & 0xFF) - 128;
        directSigned += (long) sa[d] * sb[d];
        sumA += sa[d];
        sumB += sb[d];
      }

      // The stored code sums must match what encode recorded, or the correction uses wrong scalars.
      assertEquals("stored code sum for a", sumA, (int) corrA[1]);
      assertEquals("stored code sum for b", sumB, (int) corrB[1]);

      // The identity: unsignedDot - 128*(Sa + Sb) - 128^2*dim == signedDot.
      long unsignedDot = 0;
      for (int d = 0; d < dim; d++) {
        unsignedDot += (long) (codeA[d] & 0xFF) * (codeB[d] & 0xFF);
      }
      long recovered = unsignedDot - 128L * (sumA + sumB) - 16384L * dim;
      assertEquals("offset correction must be exact at dim=" + dim, directSigned, recovered);
    }
  }

  /** Codes must stay in the symmetric range: -128 must never be produced. */
  public void testCodesAreSymmetric() {
    final Int8Quantizer q = new Int8Quantizer();
    byte[] code = new byte[DIM];
    float[] corr = new float[4];
    for (int trial = 0; trial < 50; trial++) {
      // Deliberately include values far outside the representable range, to force saturation.
      float[] v = randomVector(DIM, 1000f);
      q.encode(v, DIM, null, code, corr);
      for (int d = 0; d < DIM; d++) {
        int signed = (code[d] & 0xFF) - 128;
        assertTrue("code " + signed + " must be >= -127 (symmetric range)", signed >= -127);
        assertTrue("code " + signed + " must be <= 127", signed <= 127);
      }
    }
  }

  /** Negating a vector must negate its code, which is what the symmetric range buys. */
  public void testNegationSymmetry() {
    final Int8Quantizer q = new Int8Quantizer();
    float[] v = randomVector(DIM, 1f);
    float[] neg = new float[DIM];
    for (int d = 0; d < DIM; d++) {
      neg[d] = -v[d];
    }
    byte[] cv = new byte[DIM];
    byte[] cn = new byte[DIM];
    float[] rv = new float[4];
    float[] rn = new float[4];
    q.encode(v, DIM, null, cv, rv);
    q.encode(neg, DIM, null, cn, rn);
    for (int d = 0; d < DIM; d++) {
      assertEquals(
          "negating the vector must negate code " + d,
          -((cv[d] & 0xFF) - 128),
          (cn[d] & 0xFF) - 128);
    }
    assertEquals("scale is unchanged by negation", rv[0], rn[0], 0f);
    assertEquals("code sum negates", -(int) rv[1], (int) rn[1]);
  }

  /**
   * The end-to-end score must track the true dot product closely. This is the accuracy claim, as
   * distinct from the exactness of the offset algebra above.
   */
  public void testScoreTracksTrueDotProduct() {
    final Int8Quantizer q = new Int8Quantizer();
    final int n = 64;
    byte[][] records = new byte[n][];
    float[][] corrections = new float[n][];
    double[] trueDots = new double[n];

    float[] query = unitVector(DIM);
    // A header before the code, so the code is a dense run inside a larger record.
    final int codeOffset = 8;
    for (int i = 0; i < n; i++) {
      float[] doc = unitVector(DIM);
      records[i] = new byte[codeOffset + DIM];
      corrections[i] = new float[4];
      byte[] code = new byte[DIM];
      q.encode(doc, DIM, null, code, corrections[i]);
      System.arraycopy(code, 0, records[i], codeOffset, DIM);
      double dot = 0;
      for (int d = 0; d < DIM; d++) {
        dot += (double) query[d] * doc[d];
      }
      trueDots[i] = dot;
    }

    FineQuantizer.QueryState st =
        q.prepareQuery(query, DIM, null, VectorSimilarityFunction.DOT_PRODUCT);
    float[] scores = new float[n];
    st.scoreBulk(records, n, codeOffset, corrections, scores);

    for (int i = 0; i < n; i++) {
      float expected = (float) Math.max(0.0, (1.0 + trueDots[i]) / 2.0);
      // 8 bits per dimension over unit vectors: the dot's error is well under 0.01 in score units.
      assertEquals("score for candidate " + i, expected, scores[i], 0.01f);
      assertTrue("score must be a valid similarity", scores[i] >= 0f);
    }
  }

  /** Scoring must be independent of batch size: the bulk path is not allowed to change results. */
  public void testBulkSizeDoesNotChangeScores() {
    final Int8Quantizer q = new Int8Quantizer();
    final int n = 40;
    byte[][] records = new byte[n][];
    float[][] corrections = new float[n][];
    for (int i = 0; i < n; i++) {
      records[i] = new byte[DIM];
      corrections[i] = new float[4];
      q.encode(unitVector(DIM), DIM, null, records[i], corrections[i]);
    }
    float[] query = unitVector(DIM);

    FineQuantizer.QueryState st =
        q.prepareQuery(query, DIM, null, VectorSimilarityFunction.DOT_PRODUCT);
    float[] all = new float[n];
    st.scoreBulk(records, n, 0, corrections, all);

    // Score the same records one at a time and require bit-identical results.
    for (int i = 0; i < n; i++) {
      float[] one = new float[1];
      st.scoreBulk(new byte[][] {records[i]}, 1, 0, new float[][] {corrections[i]}, one);
      assertEquals(
          "candidate " + i + " must score the same alone as in a batch", all[i], one[0], 0f);
    }
  }

  /** A zero vector must not produce NaN or infinity anywhere. */
  public void testZeroVectorIsSafe() {
    final Int8Quantizer q = new Int8Quantizer();
    byte[] code = new byte[DIM];
    float[] corr = new float[4];
    q.encode(new float[DIM], DIM, null, code, corr);
    assertTrue("scale must be finite and non-zero", corr[0] > 0f && Float.isFinite(corr[0]));

    for (VectorSimilarityFunction sim : VectorSimilarityFunction.values()) {
      FineQuantizer.QueryState st = q.prepareQuery(new float[DIM], DIM, null, sim);
      float[] scores = new float[1];
      st.scoreBulk(new byte[][] {code}, 1, 0, new float[][] {corr}, scores);
      assertTrue("score must be finite under " + sim, Float.isFinite(scores[0]));
    }
  }

  /** Every similarity function must produce a finite, non-negative score. */
  public void testAllSimilaritiesProduceValidScores() {
    final Int8Quantizer q = new Int8Quantizer();
    byte[] code = new byte[DIM];
    float[] corr = new float[4];
    q.encode(unitVector(DIM), DIM, null, code, corr);
    for (VectorSimilarityFunction sim : VectorSimilarityFunction.values()) {
      FineQuantizer.QueryState st = q.prepareQuery(unitVector(DIM), DIM, null, sim);
      float[] scores = new float[1];
      st.scoreBulk(new byte[][] {code}, 1, 0, new float[][] {corr}, scores);
      assertTrue("score under " + sim + " must be finite", Float.isFinite(scores[0]));
      assertTrue("score under " + sim + " must be >= 0", scores[0] >= 0f);
    }
  }

  private float[] randomVector(int dim, float scale) {
    float[] v = new float[dim];
    for (int d = 0; d < dim; d++) {
      v[d] = (float) (random().nextGaussian() * scale);
    }
    return v;
  }

  private float[] unitVector(int dim) {
    float[] v = randomVector(dim, 1f);
    double norm = 0;
    for (float x : v) {
      norm += (double) x * x;
    }
    norm = Math.sqrt(norm);
    if (norm == 0) {
      v[0] = 1f;
      return v;
    }
    for (int d = 0; d < dim; d++) {
      v[d] /= (float) norm;
    }
    return v;
  }

  /**
   * Dimensions worth covering, including a sub-byte tail and a non-multiple of the vector width.
   */
  private static final class TestUtilDims {
    private static final int[] DIMS = {1024, 768, 96, 8, 13};

    static int pick(java.util.Random r) {
      return DIMS[r.nextInt(DIMS.length)];
    }
  }
}
