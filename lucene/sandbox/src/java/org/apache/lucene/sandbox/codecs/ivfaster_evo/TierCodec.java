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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.ivfaster_evo.IVFasterEvoVectorsFormat.Tier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Fixed, segment-independent encodings. U8 intervals are per vector, never per IVF centroid. */
final class TierCodec {
  final Tier tier;
  final int dim;
  final int bytes;
  final HadamardRotation rotation;

  /** Size in bytes of a Nitrox2 code: two bit planes of {@code dim} bits each. */
  static int nitroxBytes(int dim) {
    return 2 * ((dim + 7) / 8);
  }

  TierCodec(Tier tier, int dim) {
    this.tier = tier;
    this.dim = dim;
    bytes =
        switch (tier) {
          case FP32 -> 4 * dim;
          case U8 -> dim + 12;
          case NITROX2 -> nitroxBytes(dim);
        };
    rotation = HadamardRotation.create(dim, 42);
  }

  byte[] encode(float[] vector) {
    byte[] code = new byte[bytes];
    var buf = ByteBuffer.wrap(code).order(ByteOrder.LITTLE_ENDIAN);
    if (tier == Tier.FP32) {
      buf.asFloatBuffer().put(vector);
      return code;
    }
    float[] rotated = new float[dim];
    if (tier == Tier.NITROX2) {
      encodeNitrox(vector, code, rotated);
    } else {
      rotation.rotate(vector, rotated);
      byte[] levels = new byte[dim];
      var q =
          new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT)
              .scalarQuantize(rotated, levels, (byte) 8, new float[dim]);
      buf.putFloat(q.lowerInterval())
          .putFloat((q.upperInterval() - q.lowerInterval()) / 255f)
          .putInt(q.quantizedComponentSum())
          .put(levels);
    }
    return code;
  }

  /** Allocation-free Nitrox2 encoding for streaming construction workers. */
  void encodeNitrox(float[] vector, byte[] code, float[] rotated) {
    assert tier == Tier.NITROX2;
    rotation.rotate(vector, rotated);
    encodeRotatedNitrox(rotated, code);
  }

  /** Encodes an already rotated vector; scratch is normalized in place. */
  void encodeRotatedNitrox(float[] rotated, byte[] code) {
    java.util.Arrays.fill(code, (byte) 0);
    double norm = Math.sqrt(VectorUtil.dotProduct(rotated, rotated));
    if (norm != 0) for (int i = 0; i < dim; i++) rotated[i] /= (float) norm;
    float clip = (float) (1 / Math.sqrt(dim));
    int planeBytes = (dim + 7) / 8;
    for (int p = 0; p < 2; p++) {
      float threshold = (p == 0 ? -0.5f : 0.5f) * clip;
      for (int i = 0; i < dim; i++) {
        if (rotated[i] >= threshold) code[p * planeBytes + i / 8] |= (byte) (1 << (i & 7));
      }
    }
  }

  void decode(byte[] code, float[] out) {
    if (tier == Tier.U8) {
      float[] rotated = new float[dim];
      decodeUnrotated(code, 0, rotated);
      rotation.inverseRotate(rotated, out);
    } else {
      decodeUnrotated(code, 0, out);
    }
  }

  /** Decodes a fine record without undoing the rotation: U8 stays in rotated space. */
  void decodeUnrotated(byte[] code, int offset, float[] out) {
    if (tier == Tier.NITROX2) throw new UnsupportedOperationException("Nitrox2 is a coarse tier");
    var buf = ByteBuffer.wrap(code, offset, bytes).order(ByteOrder.LITTLE_ENDIAN);
    if (tier == Tier.FP32) {
      buf.asFloatBuffer().get(out);
      return;
    }
    float lower = buf.getFloat(), step = buf.getFloat();
    for (int i = 0; i < dim; i++) out[i] = lower + step * (code[offset + 12 + i] & 255);
  }

  Scorer scorer(float[] query, VectorSimilarityFunction similarity) {
    return new Scorer(encode(query), similarity);
  }

  final class Scorer {
    final byte[] query;
    private final VectorSimilarityFunction similarity;
    private final float[] qFloat, scratch;
    private final byte[] qBytes, dBytes;
    private final float qa, qs;
    private final int qSum;
    private final double qNorm;
    private final boolean needsNorm;

    Scorer(byte[] query, VectorSimilarityFunction similarity) {
      this.query = query;
      this.similarity = similarity;
      needsNorm =
          similarity == VectorSimilarityFunction.COSINE
              || similarity == VectorSimilarityFunction.EUCLIDEAN;
      qFloat = tier == Tier.FP32 ? new float[dim] : null;
      scratch = qFloat == null ? null : new float[dim];
      if (qFloat != null) decode(query, qFloat);
      qBytes = tier == Tier.U8 ? ArrayUtil.copyOfSubArray(query, 12, bytes) : null;
      dBytes = qBytes == null ? null : new byte[dim];
      var buf = ByteBuffer.wrap(query).order(ByteOrder.LITTLE_ENDIAN);
      qa = qBytes == null ? 0 : buf.getFloat();
      qs = qBytes == null ? 0 : buf.getFloat();
      qSum = qBytes == null ? 0 : buf.getInt();
      qNorm =
          qBytes == null || needsNorm == false
              ? 0
              : norm(qa, qs, qSum, VectorUtil.uint8DotProduct(qBytes, qBytes));
    }

    double score(byte[] doc) {
      if (tier == Tier.NITROX2) return -VectorKernels.INSTANCE.hamming(query, doc);
      if (tier == Tier.FP32) {
        decode(doc, scratch);
        return similarity.compare(qFloat, scratch);
      }
      var buf = ByteBuffer.wrap(doc).order(ByteOrder.LITTLE_ENDIAN);
      float da = buf.getFloat(), ds = buf.getFloat();
      int dSum = buf.getInt();
      buf.get(dBytes);
      double dot =
          (double) qa * da * dim
              + (double) qa * ds * dSum
              + (double) da * qs * qSum
              + (double) qs * ds * VectorUtil.uint8DotProduct(qBytes, dBytes);
      double dNorm = needsNorm ? norm(da, ds, dSum, VectorUtil.uint8DotProduct(dBytes, dBytes)) : 0;
      return switch (similarity) {
        case EUCLIDEAN -> 1 / (1 + Math.max(0, qNorm + dNorm - 2 * dot));
        case COSINE ->
            qNorm == 0 || dNorm == 0
                ? 0
                : Math.max(0, Math.min(1, (1 + dot / Math.sqrt(qNorm * dNorm)) / 2));
        case DOT_PRODUCT -> Math.max(0, (1 + dot) / 2);
        case MAXIMUM_INNER_PRODUCT -> dot < 0 ? 1 / (1 - dot) : dot + 1;
      };
    }

    private double norm(float a, float step, int sum, int squareSum) {
      return Math.max(
          0, (double) a * a * dim + 2d * a * step * sum + (double) step * step * squareSum);
    }
  }

  /**
   * A randomized orthogonal rotation {@code R = F * P * S}: random sign flips {@code S}, a random
   * permutation {@code P}, then a normalized Fast Walsh-Hadamard Transform {@code F} over each
   * power-of-two block composing the dimension (its set bits; e.g. {@code 768 -> [512][256]}).
   *
   * <p>Every factor is orthogonal, so the rotation preserves norms and dot products and its inverse
   * is its transpose. It spreads each input component across a whole block, which evens out the
   * per-dimension value distribution so that fixed quantization grids fit every vector. The
   * rotation is fully determined by {@code (dim, seed)}, immutable and thread-safe.
   */
  static final class HadamardRotation {
    private final int dim;
    private final float[] signs;

    /** {@code perm[i]} is the source index gathered into position {@code i}. */
    private final int[] perm;

    static HadamardRotation create(int dim, long seed) {
      if (dim < 1) throw new IllegalArgumentException("dim must be >= 1, got " + dim);
      return new HadamardRotation(dim, new Random(seed));
    }

    private HadamardRotation(int dim, Random random) {
      this.dim = dim;
      signs = new float[dim];
      for (int i = 0; i < dim; i++) signs[i] = random.nextBoolean() ? 1f : -1f;
      // Fisher-Yates.
      perm = new int[dim];
      for (int i = 0; i < dim; i++) perm[i] = i;
      for (int i = dim - 1; i > 0; i--) {
        int j = random.nextInt(i + 1);
        int tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
      }
    }

    /** {@code out = R * in}. The arrays must be distinct and of the rotation's dimension. */
    void rotate(float[] in, float[] out) {
      checkArgs(in, out);
      for (int i = 0; i < dim; i++) {
        int src = perm[i];
        out[i] = signs[src] * in[src];
      }
      fwht(out);
    }

    /** {@code out = R^T * in}. The arrays must be distinct and of the rotation's dimension. */
    void inverseRotate(float[] in, float[] out) {
      checkArgs(in, out);
      // F is symmetric and self-inverse, so apply it first, on a copy.
      float[] f = in.clone();
      fwht(f);
      for (int i = 0; i < dim; i++) {
        int dst = perm[i];
        out[dst] = signs[dst] * f[i];
      }
    }

    private void checkArgs(float[] in, float[] out) {
      if (in.length != dim || out.length != dim || in == out) {
        throw new IllegalArgumentException("in/out must be distinct arrays of length " + dim);
      }
    }

    /** In-place normalized FWHT over each power-of-two block of the dimension, largest first. */
    private void fwht(float[] a) {
      int offset = 0;
      for (int len = Integer.highestOneBit(dim); len != 0; len >>>= 1) {
        if ((dim & len) == 0) continue;
        for (int h = 1; h < len; h <<= 1) {
          for (int i = offset; i < offset + len; i += h << 1) {
            for (int p = i; p < i + h; p++) {
              float x = a[p];
              float y = a[p + h];
              a[p] = x + y;
              a[p + h] = x - y;
            }
          }
        }
        if (len > 1) {
          float scale = (float) (1.0 / Math.sqrt(len));
          for (int i = offset; i < offset + len; i++) a[i] *= scale;
        }
        offset += len;
      }
    }
  }
}
