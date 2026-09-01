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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * The exact fine tier: NO quantization. Each document's code is its ROTATED float vector stored
 * verbatim ({@code dim} little-endian floats), and the rerank is a plain {@code
 * VectorUtil.dotProduct}. Selected by a {@code null} fine tier on the format, meaning rerank on
 * full precision.
 *
 * <p>Because the Hadamard rotation is orthonormal, {@code dot(rotated_q, rotated_d) == dot(q, d)},
 * so storing rotated vectors and dotting against the rotated query recovers the true dot exactly.
 * It also makes {@link #decode} exact, so {@code getFloatVectorValues} returns the original vector
 * rather than a reconstruction.
 *
 * <p>It slots into the existing rerank machinery like any other {@link FineQuantizer}: the record
 * is simply {@code 4*dim} bytes of code instead of {@code dim}, and the scorer is an exact float
 * dot rather than an unsigned int8 dot. The trade is size and bandwidth (4x the fine bytes of int8)
 * for zero rerank error, which fits a caller whose recall must be exact modulo routing, or who
 * wants the full-precision vectors on disk anyway.
 *
 * @lucene.experimental
 */
final class Fp32Quantizer implements FineQuantizer {

  /** Identifies this encoding on disk. */
  static final byte ENCODING_ID = 20;

  /**
   * Little-endian float view over the code {@code byte[]}, matching {@link CodeRecord}'s LE ints.
   */
  private static final VarHandle VH_FLOAT_LE =
      MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.LITTLE_ENDIAN);

  Fp32Quantizer() {}

  @Override
  public byte encodingId() {
    return ENCODING_ID;
  }

  @Override
  public String name() {
    return "fp32";
  }

  @Override
  public int codeBytes(int dim) {
    return dim * Float.BYTES;
  }

  @Override
  public boolean wantsStridedStaging() {
    return true;
  }

  @Override
  public boolean needsMean() {
    return false;
  }

  @Override
  public void encode(float[] vector, int dim, float[] mean, byte[] code, float[] corrections) {
    double sqNorm = 0;
    for (int d = 0; d < dim; d++) {
      final float v = vector[d];
      VH_FLOAT_LE.set(code, d * Float.BYTES, v);
      sqNorm += (double) v * v;
    }
    // Only EUCLIDEAN reads a correction (the doc's squared norm); the dot itself needs none.
    corrections[0] = 0f;
    corrections[1] = 0f;
    corrections[2] = (float) sqNorm;
    corrections[3] = 0f;
  }

  @Override
  public void decode(
      byte[] code, int codeOffset, int dim, float[] mean, float[] corrections, float[] dest) {
    for (int d = 0; d < dim; d++) {
      dest[d] = (float) VH_FLOAT_LE.get(code, codeOffset + d * Float.BYTES);
    }
  }

  @Override
  public QueryState prepareQuery(
      float[] rotated, int dim, float[] mean, VectorSimilarityFunction sim) {
    return new State(rotated, dim, sim);
  }

  /**
   * Per-query state: the rotated query and its norm, plus a decode scratch reused across
   * candidates.
   */
  private static final class State implements QueryState {

    private final float[] q;
    private final int dim;
    private final double qSqNorm;
    private final VectorSimilarityFunction sim;
    private float[] scratch;
    private boolean rawDots;

    State(float[] rotated, int dim, VectorSimilarityFunction sim) {
      this.dim = dim;
      this.sim = sim;
      this.q = java.util.Arrays.copyOf(rotated, dim);
      this.scratch = new float[dim];
      double sq = 0;
      for (int d = 0; d < dim; d++) {
        sq += (double) rotated[d] * rotated[d];
      }
      this.qSqNorm = sq;
    }

    @Override
    public void reportRawDots() {
      rawDots = true;
    }

    @Override
    public void scoreBulk(
        byte[][] records, int count, int codeOffset, float[][] corrections, float[] scores) {
      for (int i = 0; i < count; i++) {
        final byte[] rec = records[i];
        for (int d = 0; d < dim; d++) {
          scratch[d] = (float) VH_FLOAT_LE.get(rec, codeOffset + d * Float.BYTES);
        }
        final double dot = VectorUtil.dotProduct(q, scratch);
        scores[i] = rawDots ? (float) dot : finish(dot, corrections[i][2]);
      }
    }

    @Override
    public void scoreBulkStrided(
        byte[] recs,
        int count,
        int stride,
        int codeOffset,
        float[][] corrections,
        float[] scores,
        byte[][] scratchRows) {
      for (int i = 0; i < count; i++) {
        final int base = i * stride + codeOffset;
        for (int d = 0; d < dim; d++) {
          scratch[d] = (float) VH_FLOAT_LE.get(recs, base + d * Float.BYTES);
        }
        final double dot = VectorUtil.dotProduct(q, scratch);
        scores[i] = rawDots ? (float) dot : finish(dot, corrections[i][2]);
      }
    }

    /**
     * Applies the similarity transform, matching Lucene's score conventions (see Int8Quantizer).
     */
    private float finish(double dot, float docSqNorm) {
      switch (sim) {
        case EUCLIDEAN -> {
          double sq = qSqNorm + docSqNorm - 2.0 * dot;
          if (sq < 0) {
            sq = 0;
          }
          return (float) (1.0 / (1.0 + sq));
        }
        case DOT_PRODUCT, COSINE -> {
          return (float) Math.max(0.0, (1.0 + dot) / 2.0);
        }
        case MAXIMUM_INNER_PRODUCT -> {
          final float d = (float) dot;
          return d < 0 ? 1f / (1f - d) : d + 1f;
        }
      }
      throw new IllegalStateException("unhandled similarity: " + sim);
    }
  }
}
