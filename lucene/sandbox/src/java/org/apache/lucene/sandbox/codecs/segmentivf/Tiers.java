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
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

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
   * A coarse code compared against a prepared query by an integer distance in {@code [0, bins)},
   * lower is nearer. Document codes may be relative to an anchor (a cell's centroid, or the mean
   * centroid in the graph); a query is encoded once and then adjusted per anchor. Distances
   * estimate the same similarity whatever the anchor, so they compare across cells and segments.
   */
  abstract static sealed class CoarseCodec permits Nitrox2, Bbq {
    final int dim, codeBytes, queryBytes, bins;

    CoarseCodec(int dim, int codeBytes, int queryBytes, int bins) {
      this.dim = dim;
      this.codeBytes = codeBytes;
      this.queryBytes = queryBytes;
      this.bins = bins;
    }

    static CoarseCodec of(CoarseTier tier, int dim) {
      return switch (tier) {
        case NITROX2 -> new Nitrox2(dim);
        case BBQ -> new Bbq(dim);
      };
    }

    /** Whether codes depend on the anchor, so they must be re-encoded when it changes. */
    abstract boolean anchored();

    /** Encodes {@code vector} relative to {@code anchor} into {@link #codeBytes} at {@code off}. */
    abstract void encode(float[] vector, float[] anchor, byte[] dest, int off);

    /**
     * Encodes {@code query} into {@link #queryBytes} at {@code off}, to be {@link #anchorQuery
     * anchored} before use.
     */
    abstract void encodeQuery(float[] query, byte[] dest, int off);

    /** Adjusts {@code q}, an encoding of {@code query}, to codes relative to {@code anchor}. */
    abstract void anchorQuery(byte[] q, float[] query, float[] anchor);

    /** Distances to {@code rows} consecutive codes starting at {@code offset}. */
    abstract void distances(byte[] q, MemorySegment codes, long offset, int rows, int[] out);

    abstract int distance(byte[] q, MemorySegment codes, long offset);

    abstract void distances(byte[] q, byte[] codes, int offset, int rows, int[] out);

    abstract int distance(byte[] q, byte[] codes, int offset);

    final void distancesAt(byte[] q, byte[] codes, int[] offsets, int count, int[] out) {
      for (int i = 0; i < count; i++) out[i] = distance(q, codes, offsets[i]);
    }

    /** The mean of {@code vectors} when codes are anchored, else null. */
    float[] anchor(float[][] vectors) {
      if (anchored() == false) return null;
      double[] sum = new double[dim];
      for (float[] v : vectors) for (int d = 0; d < dim; d++) sum[d] += v[d];
      float[] mean = new float[dim];
      for (int d = 0; d < dim && vectors.length > 0; d++) {
        mean[d] = (float) (sum[d] / vectors.length);
      }
      return mean;
    }
  }

  /**
   * Extended Hamming code with a sign bit and magnitude bit where popcount is the similarity score.
   *
   * <p>The representation is crafted for a high-CPU-memory-bandwidth XOR and popcount scan, which
   * lets SIMD reject candidates with very low latency before fine reranking. Codes need no anchor,
   * and a query is coded as a document.
   */
  static final class Nitrox2 extends CoarseCodec {
    Nitrox2(int dim) {
      super(dim, bytesPerVector(dim), bytesPerVector(dim), bytesPerVector(dim) * 8 + 2);
    }

    /** Sign and magnitude bit planes of {@code ceil(dim / 8)} bytes each. */
    static int bytesPerVector(int dim) {
      return 2 * ((dim + 7) >>> 3);
    }

    @Override
    boolean anchored() {
      return false;
    }

    @Override
    void encode(float[] vector, float[] anchor, byte[] dest, int off) {
      float clip = (float) (1 / Math.sqrt(dim));
      K.pack2(vector, dim, dest, off, -0.5f * clip, 0.5f * clip);
    }

    @Override
    void encodeQuery(float[] query, byte[] dest, int off) {
      encode(query, null, dest, off);
    }

    @Override
    void anchorQuery(byte[] q, float[] query, float[] anchor) {}

    @Override
    void distances(byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
      K.hamming(q, codes, offset, rows, out);
    }

    @Override
    int distance(byte[] q, MemorySegment codes, long offset) {
      return K.hamming(q, codes, offset);
    }

    @Override
    void distances(byte[] q, byte[] codes, int offset, int rows, int[] out) {
      K.hamming(q, codes, offset, rows, out);
    }

    @Override
    int distance(byte[] q, byte[] codes, int offset) {
      return K.hamming(q, codes, offset);
    }
  }

  /**
   * Better Binary Quantization: one bit per dimension of a vector's offset from an anchor, from
   * Lucene's {@link OptimizedScalarQuantizer}, scored against a 4-bit query by AND and popcount
   * over the query's bit planes ({@link ScalarEncoding#SINGLE_BIT_QUERY_NIBBLE}).
   *
   * <p>A code is {@code ceil(dim / 8)} packed bits {@code b}, then the interval's lower bound
   * {@code ax}, its step {@code lx} and {@code lx} times the bit count, so {@code x - a ~ ax + lx
   * b}. The query is quantized once, uncentered, to {@code ay + ly qq}: four bit planes of {@code
   * qq}, then {@code sum(q)}, {@code ay}, {@code ly} and the exact {@code q.a} for the current
   * anchor. Then {@code q.x ~ q.a + ax sum(q) + ay lx sum(b) + lx ly qq.b}, the correction of
   * {@code Lucene104ScalarQuantizedVectorScorer} with the query side exact, so a new anchor costs
   * one dot product rather than a quantization. Vectors are unit length, so the estimate maps
   * linearly to {@link #SCALE}-wide bins.
   *
   * <p>Quantizing the query against each probed centroid instead, as Lucene104 does against its one
   * centroid, measured at 1M Cohere 1024d, nprobe 8 to 64, 0 to 0.002 higher recall@100 (within the
   * +-0.003 between builds) for 50-60% higher latency, since every probe then runs the quantizer.
   *
   * <p>Scoring uses {@link Kernels#int4BitDots}, the computation of {@link
   * VectorUtil#int4BitDotProduct}, because that takes only whole arrays and codes here are read in
   * place from mapped or pinned segments; it can be swapped in once Lucene offers an offset or
   * {@code MemorySegment} form.
   */
  static final class Bbq extends CoarseCodec {
    static final int SCALE = 1 << 11;
    private static final int DOC_CORRECTIONS = 3 * Float.BYTES, QUERY_CORRECTIONS = 4 * Float.BYTES;
    private static final ScalarEncoding ENCODING = ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE;
    private static final byte DOC_BITS = ENCODING.getBits(), QUERY_BITS = ENCODING.getQueryBits();
    private static final OptimizedScalarQuantizer QUANTIZER =
        new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);

    final int bitBytes;
    private final int planeBytes;
    private final float[] origin;
    private final ThreadLocal<Scratch> scratch;

    private record Scratch(float[] centered, byte[] levels, byte[] packed) {}

    Bbq(int dim) {
      super(
          dim,
          ENCODING.getDocPackedLength(dim) + DOC_CORRECTIONS,
          ENCODING.getQueryPackedLength(dim) + QUERY_CORRECTIONS,
          2 * SCALE + 2);
      bitBytes = codeBytes - DOC_CORRECTIONS;
      planeBytes = queryBytes - QUERY_CORRECTIONS;
      origin = new float[dim];
      int discrete = ENCODING.getDiscreteDimensions(dim);
      scratch =
          ThreadLocal.withInitial(
              () -> new Scratch(new float[dim], new byte[discrete], new byte[planeBytes]));
    }

    @Override
    boolean anchored() {
      return true;
    }

    @Override
    void encode(float[] vector, float[] anchor, byte[] dest, int off) {
      Scratch s = scratch.get();
      System.arraycopy(vector, 0, s.centered, 0, dim); // the quantizer centers in place
      var r = QUANTIZER.scalarQuantize(s.centered, s.levels, DOC_BITS, anchor);
      Arrays.fill(s.packed, 0, bitBytes, (byte) 0);
      OptimizedScalarQuantizer.packAsBinary(s.levels, s.packed);
      System.arraycopy(s.packed, 0, dest, off, bitBytes);
      float lx = (r.upperInterval() - r.lowerInterval()) / ((1 << DOC_BITS) - 1);
      putFloat(dest, off + bitBytes, r.lowerInterval());
      putFloat(dest, off + bitBytes + 4, lx);
      putFloat(dest, off + bitBytes + 8, lx * r.quantizedComponentSum());
    }

    @Override
    void encodeQuery(float[] query, byte[] dest, int off) {
      Scratch s = scratch.get();
      System.arraycopy(query, 0, s.centered, 0, dim);
      var r = QUANTIZER.scalarQuantize(s.centered, s.levels, QUERY_BITS, origin);
      OptimizedScalarQuantizer.transposeHalfByte(s.levels, s.packed);
      System.arraycopy(s.packed, 0, dest, off, planeBytes);
      float ay = r.lowerInterval(), ly = (r.upperInterval() - ay) / ((1 << QUERY_BITS) - 1);
      float sum = 0;
      for (int d = 0; d < dim; d++) sum += query[d];
      putFloat(dest, off + planeBytes, sum);
      putFloat(dest, off + planeBytes + 4, ay);
      putFloat(dest, off + planeBytes + 8, ly);
      putFloat(dest, off + planeBytes + 12, 0f);
    }

    @Override
    void anchorQuery(byte[] q, float[] query, float[] anchor) {
      putFloat(q, planeBytes + 12, VectorUtil.dotProduct(query, anchor));
    }

    @Override
    void distances(byte[] q, MemorySegment codes, long offset, int rows, int[] out) {
      K.int4BitDots(q, bitBytes, codes, offset, codeBytes, rows, out);
      float sum = getFloat(q, planeBytes), ay = getFloat(q, planeBytes + 4);
      float ly = getFloat(q, planeBytes + 8), qa = getFloat(q, planeBytes + 12);
      for (int r = 0; r < rows; r++) {
        long at = offset + (long) r * codeBytes + bitBytes;
        float ax = codes.get(FLOAT_LE, at), lx = codes.get(FLOAT_LE, at + 4);
        float lxx1 = codes.get(FLOAT_LE, at + 8);
        out[r] = bin(qa + ax * sum + ay * lxx1 + lx * ly * out[r]);
      }
    }

    @Override
    int distance(byte[] q, MemorySegment codes, long offset) {
      int[] out = new int[1];
      distances(q, codes, offset, 1, out);
      return out[0];
    }

    @Override
    void distances(byte[] q, byte[] codes, int offset, int rows, int[] out) {
      for (int r = 0; r < rows; r++) out[r] = distance(q, codes, offset + r * codeBytes);
    }

    @Override
    int distance(byte[] q, byte[] codes, int offset) {
      int at = offset + bitBytes, dot = K.int4BitDot(q, bitBytes, codes, offset);
      return bin(
          getFloat(q, planeBytes + 12)
              + getFloat(codes, at) * getFloat(q, planeBytes)
              + getFloat(q, planeBytes + 4) * getFloat(codes, at + 8)
              + getFloat(codes, at + 4) * getFloat(q, planeBytes + 8) * dot);
    }

    /** Maps a dot-product estimate in {@code [-1, 1]} to a bin, lower is nearer. */
    static int bin(float estimate) {
      float d = (1f - estimate) * SCALE;
      return d <= 0 ? 0 : d >= 2 * SCALE ? 2 * SCALE : (int) (d + 0.5f);
    }

    private static float getFloat(byte[] b, int off) {
      return Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(b, off));
    }

    private static void putFloat(byte[] b, int off, float v) {
      BitUtil.VH_LE_INT.set(b, off, Float.floatToIntBits(v));
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
