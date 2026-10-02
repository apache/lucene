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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/** Compact coarse and fine vector representations plus the shared rotation. */
final class Tiers {
  private static final Kernels K = Kernels.INSTANCE;
  private static final ValueLayout.OfInt INT_LE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfFloat FLOAT_LE =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  static final class FineCodec {
    final FineTier tier;
    final int dim, codeBytes;
    private final int scaleOffset;

    FineCodec(FineTier tier, int dim) {
      this.tier = tier;
      this.dim = dim;
      codeBytes = tier == FineTier.FP32 ? dim * Float.BYTES : dim;
      scaleOffset = CodeRecord.scaleOffset(codeBytes);
    }

    void encode(float[] rotated, byte[] record, int offset) {
      if (tier == FineTier.FP32) {
        floats(record, offset).put(rotated, 0, dim);
        return;
      }
      float scale = quantize(rotated, record, offset);
      BitUtil.VH_LE_INT.set(record, offset + scaleOffset, Float.floatToIntBits(scale));
    }

    void decode(byte[] record, int offset, float[] dest) {
      if (tier == FineTier.FP32) {
        floats(record, offset).get(dest, 0, dim);
        return;
      }
      float scale = scale(record, offset);
      for (int d = 0; d < dim; d++) dest[d] = scale * record[offset + d];
    }

    private FloatBuffer floats(byte[] record, int offset) {
      return ByteBuffer.wrap(record, offset, codeBytes)
          .order(ByteOrder.LITTLE_ENDIAN)
          .asFloatBuffer();
    }

    private float scale(byte[] record, int offset) {
      return Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(record, offset + scaleOffset));
    }

    Query query(float[] rotated, VectorSimilarityFunction similarity) {
      return new Query(rotated, similarity);
    }

    final class Query {
      private final VectorSimilarityFunction similarity;
      private final float[] query, decoded;
      private final byte[] levels;
      private final float queryScale;
      private int[] dots = new int[0];

      private Query(float[] rotated, VectorSimilarityFunction similarity) {
        this.similarity = similarity;
        query = rotated.clone();
        levels = tier == FineTier.INT8 ? new byte[dim] : null;
        decoded = levels == null ? new float[dim] : null;
        queryScale = levels == null ? 1f : quantize(query, levels, 0);
      }

      void score(byte[] records, int stride, int count, float[] scores) {
        if (tier == FineTier.FP32) {
          // One view per batch; records start at multiples of stride (a cache-line multiple).
          assert stride % Float.BYTES == 0;
          FloatBuffer all = ByteBuffer.wrap(records).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
          for (int r = 0; r < count; r++) {
            all.get(r * stride / Float.BYTES, decoded, 0, dim);
            scores[r] = finish(VectorUtil.dotProduct(query, decoded));
          }
          return;
        }
        dots = ArrayUtil.growNoCopy(dots, count);
        K.dotProducts(levels, records, stride, count, dots);
        for (int r = 0; r < count; r++) {
          scores[r] = finish((double) queryScale * scale(records, r * stride) * dots[r]);
        }
      }

      /** Scores the records starting at {@code offsets} of {@code records}, read in place. */
      void score(MemorySegment records, long[] offsets, int count, float[] scores) {
        if (tier == FineTier.FP32) {
          for (int r = 0; r < count; r++) {
            MemorySegment.copy(records, FLOAT_LE, offsets[r], decoded, 0, dim);
            scores[r] = finish(VectorUtil.dotProduct(query, decoded));
          }
          return;
        }
        dots = ArrayUtil.growNoCopy(dots, count);
        K.dotProducts(levels, records, offsets, count, dots);
        for (int r = 0; r < count; r++) {
          float scale = Float.intBitsToFloat(records.get(INT_LE, offsets[r] + scaleOffset));
          scores[r] = finish((double) queryScale * scale * dots[r]);
        }
      }

      private float finish(double dot) {
        return switch (similarity) {
          case null -> (float) dot;
          case EUCLIDEAN -> (float) (1 / (1 + Math.max(0, 2 - 2 * dot)));
          case DOT_PRODUCT, COSINE -> (float) Math.max(0, (1 + dot) / 2);
          case MAXIMUM_INNER_PRODUCT -> (float) (dot < 0 ? 1 / (1 - dot) : dot + 1);
        };
      }
    }

    private float quantize(float[] vector, byte[] code, int offset) {
      float maxAbs = 0f;
      for (int d = 0; d < dim; d++) maxAbs = Math.max(maxAbs, Math.abs(vector[d]));
      if (maxAbs == 0f) {
        Arrays.fill(code, offset, offset + dim, (byte) 0);
        return 1f;
      }
      float scale = maxAbs / 127f, inverse = 127f / maxAbs;
      for (int d = 0; d < dim; d++) {
        code[offset + d] = (byte) Math.max(-127, Math.min(127, Math.round(vector[d] * inverse)));
      }
      return scale;
    }
  }

  /**
   * Extended Hamming code with a sign bit and magnitude bit where popcount is the similarity score.
   *
   * <p>The representation is crafted for a high-CPU-memory-bandwidth XOR and popcount scan, which
   * lets SIMD reject candidates with very low latency before fine reranking.
   */
  static final class Nitrox2 {
    /** Sign and magnitude bit planes of {@code ceil(dim / 8)} bytes each. */
    static int bytesPerVector(int dim) {
      return 2 * ((dim + 7) >>> 3);
    }

    static void encode(float[] vector, int dim, byte[] dest, int offset) {
      float clip = (float) (1 / Math.sqrt(dim));
      K.pack2(vector, dim, dest, offset, -0.5f * clip, 0.5f * clip);
    }
  }

  static final class CodeRecord {
    static int length(int codeBytes) {
      return (codeBytes + 24 + 63) / 64 * 64;
    }

    static int primaryCellOffset(int codeBytes) {
      return codeBytes + 4;
    }

    static int scaleOffset(int codeBytes) {
      return codeBytes + 8;
    }
  }

  record HadamardRotation(float[] signs, int[] perm) {
    static HadamardRotation create(int dim, long seed) {
      Random random = new Random(seed);
      float[] signs = new float[dim];
      int[] perm = new int[dim];
      for (int i = 0; i < dim; i++) signs[i] = random.nextBoolean() ? 1f : -1f;
      for (int i = 0; i < dim; i++) perm[i] = i;
      for (int i = dim - 1; i > 0; i--) {
        int j = random.nextInt(i + 1), tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
      }
      return new HadamardRotation(signs, perm);
    }

    void rotate(float[] in, float[] out) {
      for (int i = 0; i < perm.length; i++) out[i] = signs[perm[i]] * in[perm[i]];
      transform(out);
    }

    void inverseRotate(float[] in, float[] out) {
      float[] f = in.clone();
      transform(f);
      for (int i = 0; i < perm.length; i++) out[perm[i]] = signs[perm[i]] * f[i];
    }

    private void transform(float[] a) {
      int dim = perm.length, offset = 0;
      for (int len = Integer.highestOneBit(dim); len != 0; len >>>= 1) {
        if ((dim & len) == 0) continue;
        for (int h = 1; h < len; h <<= 1)
          for (int i = offset; i < offset + len; i += h << 1)
            for (int p = i; p < i + h; p++) {
              float x = a[p], y = a[p + h];
              a[p] = x + y;
              a[p + h] = x - y;
            }
        float norm = (float) (1.0 / Math.sqrt(len));
        for (int i = offset; i < offset + len; i++) a[i] *= norm;
        offset += len;
      }
    }
  }
}
