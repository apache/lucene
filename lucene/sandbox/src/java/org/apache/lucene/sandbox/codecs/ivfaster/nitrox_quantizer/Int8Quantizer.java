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
 * Fine tier: per-vector symmetric int8, scored by an unsigned dot with an exact algebraic
 * correction.
 *
 * <h2>The encoding</h2>
 *
 * <p>Each vector gets its own max-abs scale, so {@code v[d] ~= scale * s[d]} with {@code s[d]} in
 * {@code [-127, 127]}. The stored byte is the UNSIGNED-OFFSET form {@code u[d] = s[d] + 128}, which
 * is what lets the scan use the fast unsigned kernel; see below.
 *
 * <p>Per-vector rather than a shared global scale because the rotation leaves the per-dimension
 * distribution near-Gaussian but does NOT equalize vector norms, and a shared scale would spend
 * most of its range on the few longest vectors.
 *
 * <h2>Why unsigned storage, and the correction that makes it exact</h2>
 *
 * <p>The vectorized kernel computes an UNSIGNED dot, because that is the form with four independent
 * accumulators and no sign-extension in the inner loop. Storing signed bytes and asking for a
 * signed dot would give up that kernel. Instead both sides are stored offset by 128 and the offset
 * is removed afterwards, exactly:
 *
 * <pre>
 *   Sum_d u_q[d]*u_d[d] = Sum_d (s_q[d]+128)(s_d[d]+128)
 *                       = Sum_d s_q*s_d + 128*Sum_d s_q + 128*Sum_d s_d + 128^2 * dim
 *
 *   =&gt;  Sum_d s_q[d]*s_d[d] = unsignedDot - 128*(Sq + Sd) - 16384*dim
 * </pre>
 *
 * <p>{@code Sd} (the signed code sum) is a per-document scalar stored in a correction slot; {@code
 * Sq} is computed once per query. This is an identity rather than an approximation: the recovered
 * signed dot is bit-identical to what a signed kernel would produce, so the fast path costs no
 * accuracy.
 *
 * <p>Values saturate at {@code +/-127} rather than using {@code -128}, keeping the range symmetric
 * so that negating a vector negates its code. An asymmetric range biases the dot by a term
 * proportional to how many coordinates clipped, which is data-dependent and therefore invisible in
 * testing.
 *
 * <h2>Correction slots</h2>
 *
 * <ul>
 *   <li>{@code [0]} = {@code scale}, the dequantization factor
 *   <li>{@code [1]} = {@code Sd}, the signed code sum, for the offset correction above
 *   <li>{@code [2]} = {@code ||v||^2}, the true squared norm, used only by EUCLIDEAN
 *   <li>{@code [3]} = unused
 * </ul>
 *
 * <p>The norm is stored from the FLOAT vector rather than reconstructed from the code, because
 * Euclidean distance is a difference of similar magnitudes and is the term most sensitive to
 * quantization error.
 *
 * @lucene.experimental
 */
final class Int8Quantizer implements FineQuantizer {

  /** Identifies this encoding on disk. */
  static final byte ENCODING_ID = 0;

  /** Offset applied to signed codes so the scan can use the unsigned kernel. */
  private static final int OFFSET = 128;

  Int8Quantizer() {}

  @Override
  public byte encodingId() {
    return ENCODING_ID;
  }

  @Override
  public String name() {
    return "int8";
  }

  @Override
  public int codeBytes(int dim) {
    return dim;
  }

  /**
   * Flat staging, so {@link State#scoreBulkStrided} can dot each candidate straight out of one
   * contiguous buffer with {@code BulkDotKernel.bulkDotStrided}, at a constant stride, rather than
   * gathering the shortlist into a {@code byte[][]} of unrelated addresses first.
   */
  @Override
  public boolean wantsStridedStaging() {
    return true;
  }

  /**
   * -Divfaster.int8Centre=true centres document codes on the persisted global mean.
   *
   * <p>WRITE-TIME: it changes every code byte and whether a mean is persisted at all, so it is part
   * of the index identity and a change needs a reindex.
   *
   * <p>WHY IT MIGHT HELP, and why it is not free. The rotation makes the per-dimension distribution
   * near-Gaussian but does NOT remove the corpus mean, so every document's code spends range on a
   * shared offset, and centring hands that range back to the residual, which is what distinguishes
   * documents. Against it: this tier's scale is PER VECTOR ({@code maxAbs/127}), so it already
   * adapts to each vector's magnitude, and where the mean is small relative to the residual spread,
   * centring buys precision that was not being lost.
   *
   * <p>THE QUERY IS NOT CENTRED, and the asymmetry is deliberate: {@code dot(q,v) = dot(q,mean) +
   * dot(q,v-mean)}, so the mean's contribution is a PER-QUERY CONSTANT folded in once rather than a
   * transformation of the query. Centring both sides subtracts it twice.
   */
  static final boolean CENTRE = Boolean.getBoolean("ivfaster.int8Centre");

  /**
   * A per-vector scale already adapts to each vector, so a shared mean is not needed for the GRID.
   * Under {@code -Divfaster.int8Centre} it is subtracted anyway, to spend the code's range on the
   * residual rather than on a shared offset; see {@link #CENTRE}.
   */
  @Override
  public boolean needsMean() {
    return CENTRE;
  }

  /**
   * Encodes one vector at its own max-abs scale, with the corrections the scan needs.
   *
   * <p>A zero vector codes as the offset itself, with an arbitrary scale that must not be zero, or
   * a NaN would propagate into every score it participates in.
   *
   * <p>Correction 2 is THE TRUE SQUARED NORM, not the residual's, since EUCLIDEAN computes {@code
   * ||q-d||^2 = |q|^2 + |d|^2 - 2*dot} and needs the document's own norm. Under centring {@code
   * vector} is {@code v - mean}, so the accumulated {@code sqNorm} is {@code |v - mean|^2} and
   *
   * <pre>
   *   |v|^2 = |v-mean|^2 + 2*dot(v-mean, mean) + |mean|^2
   * </pre>
   *
   * <p>recovers the exact term. Storing the residual norm instead would leave EUCLIDEAN scoring
   * against residual norms, which throws nothing and does not affect DOT_PRODUCT.
   */
  @Override
  public void encode(float[] vector, int dim, float[] mean, byte[] code, float[] corrections) {
    float maxAbs = 0f;
    double sqNorm = 0;
    for (int d = 0; d < dim; d++) {
      final float v = vector[d];
      final float a = Math.abs(v);
      if (a > maxAbs) {
        maxAbs = a;
      }
      sqNorm += (double) v * v;
    }
    if (maxAbs == 0f) {
      java.util.Arrays.fill(code, 0, dim, (byte) OFFSET);
      corrections[0] = 1f;
      corrections[1] = 0f;
      corrections[2] = 0f;
      corrections[3] = 0f;
      return;
    }
    final float scale = maxAbs / 127f;
    final float inv = 127f / maxAbs;
    int sum = 0;
    for (int d = 0; d < dim; d++) {
      int q = Math.round(vector[d] * inv);
      if (q > 127) {
        q = 127;
      } else if (q < -127) {
        q = -127;
      }
      sum += q;
      code[d] = (byte) (q + OFFSET);
    }
    corrections[0] = scale;
    corrections[1] = sum;
    // The TRUE squared norm, reconstructed under centring; see the javadoc.
    if (mean == null) {
      corrections[2] = (float) sqNorm;
    } else {
      double cross = 0;
      double muSq = 0;
      for (int d = 0; d < dim; d++) {
        cross += (double) vector[d] * mean[d];
        muSq += (double) mean[d] * mean[d];
      }
      corrections[2] = (float) (sqNorm + 2.0 * cross + muSq);
    }
    corrections[3] = 0f;
  }

  /**
   * The inverse of {@link #encode}: undo the offset, undo the scale, and add the mean back.
   *
   * <p>The mean is added back only when it was subtracted: {@code encode} receives an
   * already-centred vector, so a faithful inverse must undo that rather than leave it to the
   * caller. Without a mean this is a no-op branch.
   */
  @Override
  public void decode(
      byte[] code, int codeOffset, int dim, float[] mean, float[] corrections, float[] dest) {
    final float scale = corrections[0];
    if (mean == null) {
      for (int d = 0; d < dim; d++) {
        dest[d] = ((code[codeOffset + d] & 0xFF) - OFFSET) * scale;
      }
    } else {
      for (int d = 0; d < dim; d++) {
        dest[d] = ((code[codeOffset + d] & 0xFF) - OFFSET) * scale + mean[d];
      }
    }
  }

  @Override
  public QueryState prepareQuery(
      float[] rotated, int dim, float[] mean, VectorSimilarityFunction sim) {
    return new State(rotated, dim, mean, sim);
  }

  /** Per-query state: the quantized query, its scale and code sum, and its true norm. */
  private static final class State implements QueryState {

    private final byte[] q;
    private final int dim;
    private final float qScale;
    private final int qSum;
    private final double qSqNorm;

    /** {@code dot(query, mean)}: the term the centred codes leave out. Zero when uncentred. */
    private final double meanDot;

    private final VectorSimilarityFunction sim;
    private final BulkDotKernel kernel = BulkDotKernel.get();

    /**
     * Quantizes the query once, with the per-query constants the batch scoring needs.
     *
     * <p>{@code dot(q, v) = dot(q, mean) + dot(q, v - mean)}, and the codes hold the RESIDUAL, so
     * the mean's contribution is a per-query constant added once in {@code finishAll} rather than
     * folded into the query; centring the query too would subtract it twice. It is zero when not
     * centring.
     */
    State(float[] rotated, int dim, float[] mean, VectorSimilarityFunction sim) {
      this.dim = dim;
      this.sim = sim;
      double md = 0;
      if (mean != null) {
        for (int d = 0; d < dim; d++) {
          md += (double) rotated[d] * mean[d];
        }
      }
      this.meanDot = md;
      this.q = new byte[dim];
      float maxAbs = 0f;
      double sq = 0;
      for (int d = 0; d < dim; d++) {
        final float v = rotated[d];
        final float a = Math.abs(v);
        if (a > maxAbs) {
          maxAbs = a;
        }
        sq += (double) v * v;
      }
      this.qSqNorm = sq;
      if (maxAbs == 0f) {
        java.util.Arrays.fill(q, (byte) OFFSET);
        this.qScale = 1f;
        this.qSum = 0;
        return;
      }
      this.qScale = maxAbs / 127f;
      final float inv = 127f / maxAbs;
      int sum = 0;
      for (int d = 0; d < dim; d++) {
        int v = Math.round(rotated[d] * inv);
        if (v > 127) {
          v = 127;
        } else if (v < -127) {
          v = -127;
        }
        sum += v;
        q[d] = (byte) (v + OFFSET);
      }
      this.qSum = sum;
    }

    /** Scratch for the kernel's integer dots, grown on demand and reused across queries. */
    private int[] dots = new int[0];

    @Override
    public void scoreBulk(
        byte[][] records, int count, int codeOffset, float[][] corrections, float[] scores) {
      if (dots.length < count) {
        dots = new int[org.apache.lucene.util.ArrayUtil.oversize(count, Integer.BYTES)];
      }
      // One call for the whole batch: the query widening is shared across all `count` records.
      kernel.bulkDotAt(q, records, count, codeOffset, dim, dots);
      finishAll(count, corrections, scores);
    }

    /**
     * The FLAT-BUFFER form: an unsigned int8 dot per candidate straight out of the staged buffer.
     *
     * <p>One kernel call for the whole batch, so the query widening is shared across every
     * candidate and the four code runs a tile reads are a fixed stride apart in one allocation.
     * Integer dot is exact, so the score does not depend on which kernel the platform selected.
     */
    @Override
    public void scoreBulkStrided(
        byte[] recs,
        int count,
        int stride,
        int codeOffset,
        float[][] corrections,
        float[] scores,
        byte[][] scratch) {
      if (dots.length < count) {
        dots = new int[org.apache.lucene.util.ArrayUtil.oversize(count, Integer.BYTES)];
      }
      kernel.bulkDotStrided(q, recs, count, stride, codeOffset, dim, dots);
      finishAll(count, corrections, scores);
    }

    /**
     * Turns the batch's unsigned dots into scores: undo the offset exactly, dequantize, transform.
     *
     * <p>Adding {@code meanDot} restores {@code dot(q, mean)}, which the centred codes leave out.
     * It is zero when uncentred, so the uncentred tier's arithmetic is bit-for-bit unchanged.
     */
    private void finishAll(int count, float[][] corrections, float[] scores) {
      final long offsetConst = (long) OFFSET * qSum + 16384L * dim;
      for (int i = 0; i < count; i++) {
        final float[] c = corrections[i];
        // Recover the signed dot exactly; see the class javadoc for the derivation.
        final long signedDot = (long) dots[i] - offsetConst - (long) OFFSET * (long) c[1];
        scores[i] = finish((double) signedDot * qScale * c[0] + meanDot, c[2]);
      }
    }

    /** Applies the similarity transform, matching Lucene's score conventions. */
    private float finish(double dot, float docSqNorm) {
      switch (sim) {
        case EUCLIDEAN -> {
          // Exact norms, so only the cross term carries quantization error.
          double sq = qSqNorm + docSqNorm - 2.0 * dot;
          if (sq < 0) {
            sq = 0; // quantization error can push a near-coincident pair slightly negative
          }
          return (float) (1.0 / (1.0 + sq));
        }
        case DOT_PRODUCT, COSINE -> {
          // Unit-length inputs for both, so the dot is in [-1,1] and this maps it to [0,1].
          return (float) Math.max(0.0, (1.0 + dot) / 2.0);
        }
        case MAXIMUM_INNER_PRODUCT -> {
          return scaleMaxInnerProduct((float) dot);
        }
      }
      throw new IllegalStateException("unhandled similarity: " + sim);
    }

    /** Lucene's MIP scaling: shifted for non-negative dots, inverted for negative ones. */
    private static float scaleMaxInnerProduct(float dot) {
      if (dot < 0) {
        return 1f / (1f + -1f * dot);
      }
      return dot + 1f;
    }
  }
}
