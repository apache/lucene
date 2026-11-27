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
import java.util.Arrays;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * The fine tier: one code per vector, read only to rerank a shortlist, and scored by a dot product
 * with the query.
 *
 * <p>Every fine tier obeys two rules:
 *
 * <ul>
 *   <li><b>No data dependency.</b> A code depends only on its own rotated vector and on constants
 *       of the tier, never on the segment's other vectors (no trained codebooks, no segment-wide
 *       centering). A merge copies fine codes unchanged and derives everything else from them,
 *       including the coarse codes, so a code must mean the same thing in every segment. The
 *       original float vectors are not kept, and re-quantizing a quantized vector would lose
 *       precision on every merge.
 *   <li><b>No room for metadata.</b> A code is padded to a cache line, {@link #recordBytes}, and
 *       codes start on a page boundary, so a 4096-dimension INT8 code is exactly one page and is
 *       read uncached in one I/O. A tier may only use the padding its code leaves below the cache
 *       line, which for that dimension is none; per-vector metadata that does not fit there would
 *       break the page multiples. INT8 keeps none: it uses one frozen scale instead of a scale per
 *       vector.
 * </ul>
 */
abstract sealed class FineCodec permits FineCodec.Int8 {
  private static final int CACHE_LINE = 64;

  final FineTier tier;
  final int dim;

  /** Bytes of a code. */
  final int codeBytes;

  /** Bytes of a code on disk, padded to a cache line. */
  final int recordBytes;

  FineCodec(FineTier tier, int dim, int codeBytes) {
    this.tier = tier;
    this.dim = dim;
    this.codeBytes = codeBytes;
    this.recordBytes = (codeBytes + CACHE_LINE - 1) / CACHE_LINE * CACHE_LINE;
  }

  static FineCodec of(FineTier tier, int dim) {
    return switch (tier) {
      case INT8 -> new Int8(dim);
    };
  }

  /** Encodes a rotated unit vector into the code at {@code offset}. */
  abstract void encode(float[] rotated, byte[] dest, int offset);

  /** Decodes the code at {@code offset} back to a rotated vector. */
  abstract void decode(byte[] code, int offset, float[] dest);

  /**
   * Prepares a rotated query, at its own length, for scoring codes. A null {@code similarity}
   * returns the dot product estimate instead of a similarity score.
   */
  abstract Query query(float[] rotated, VectorSimilarityFunction similarity);

  /**
   * As {@link #query(float[], VectorSimilarityFunction)}, reusing {@code levels}, the INT8 levels
   * of the same {@code rotated}, where this tier scores with them. By default it ignores them.
   */
  Query query(float[] rotated, VectorSimilarityFunction similarity, QueryLevels levels) {
    return query(rotated, similarity);
  }

  /** A query prepared for one search. */
  abstract static class Query {
    private final VectorSimilarityFunction similarity;
    private final float normSquared;

    Query(float[] rotated, VectorSimilarityFunction similarity) {
      this.similarity = similarity;
      this.normSquared = VectorUtil.dotProduct(rotated, rotated);
    }

    /** Scores {@code count} codes stored {@code stride} bytes apart from the array's start. */
    abstract void score(byte[] codes, int stride, int count, float[] scores);

    /** Scores the codes starting at {@code offsets} of {@code codes}, read in place. */
    abstract void score(MemorySegment codes, long[] offsets, int count, float[] scores);

    /**
     * Converts the query's dot product with a unit code to the field's similarity score, as {@link
     * VectorSimilarityFunction#compare} scores the query against the unit vector, using {@code |q -
     * v|^2 = |q|^2 + 1 - 2 q.v} for Euclidean. Every search path scores this way, so scores from
     * the exact and approximate paths, and from different segments, compare.
     */
    final float toScore(double dot) {
      float d = (float) dot;
      return switch (similarity) {
        case null -> d;
        case EUCLIDEAN ->
            VectorUtil.normalizeDistanceToUnitInterval(Math.max(0f, normSquared + 1 - 2 * d));
        case DOT_PRODUCT -> VectorUtil.normalizeToUnitInterval(d);
        case COSINE ->
            VectorUtil.normalizeToUnitInterval(
                normSquared == 0 ? 0 : d / (float) Math.sqrt(normSquared));
        case MAXIMUM_INNER_PRODUCT -> VectorUtil.scaleMaxInnerProductScore(d);
      };
    }
  }

  /**
   * INT8: {@code dim} signed bytes, one <em>level</em> per component of the rotated vector.
   *
   * <p><b>Levels.</b> A level is a component counted in steps of one {@code scale}: component
   * {@code x} is stored as {@code l = round(x / scale)}, clamped to [-127, 127], and read back as
   * {@code l * scale}. The scale sets the trade-off: a larger one covers bigger components before
   * they clip at 127, but with coarser steps for all the rest. The best scale follows the spread of
   * the components, their standard deviation.
   *
   * <p><b>Why the scale can be frozen.</b> Codes carry no per-vector scale, so one scale must fit
   * every vector, which holds only if every vector's components spread the same way. Two steps make
   * them do so:
   *
   * <ul>
   *   <li><b>Normalization</b> fixes each vector's total energy: a unit vector's squared components
   *       sum to 1, so they average {@code 1 / dim}.
   *   <li><b>The randomized Hadamard rotation</b> ({@link
   *       SegmentIVFVectorsWriter.HadamardRotation}: random signs, a permutation, then normalized
   *       Walsh-Hadamard transforms that together mix every component, for any dimension) spreads
   *       that energy evenly. Each output component is a signed sum of input components, so it is
   *       close to normal with mean 0 and variance about {@code 1 / dim}, however unevenly the raw
   *       embedding put its energy, as in a few outlier dimensions. When {@code dim} is not a power
   *       of two, the last transform spans {@code P > dim / 2} components, so energy in a few raw
   *       dimensions still spreads over at least {@code P}: a variance of at most about {@code 2 /
   *       dim}. The rotation is orthonormal, so dot products are unchanged.
   * </ul>
   *
   * <p>Together they make every component of every vector close to {@code N(0, 1 / dim)}, known
   * from the dimension alone. The scale is {@link #CLIP_DEVIATIONS} standard deviations over 127
   * levels, {@code 4 / sqrt(dim) / 127}: about one component in 16,000 is clipped, and the step is
   * about 15% coarser than a per-vector max-abs scale's at 1024 dimensions, whose largest component
   * is typically 3.4 deviations. A frozen scale needs no per-vector metadata, and a code means the
   * same thing in every segment, so a merge copies it unchanged.
   *
   * <p><b>The query</b> is not stored, so it is quantized with its own max-abs scale ({@link
   * #queryLevels}): its largest component sits at 127, so nothing clips and the steps are as fine
   * as they can be. The coarse tier's tables are built from the same levels ({@link
   * CoarseCodec.S1}), and are coarser than a level, so they gain from the finer steps: quantizing
   * the query at the frozen scale instead cost about 0.001 recall@100 at 1024 dimensions, and saved
   * no time. A score is the integer dot product of the two codes, from the INT8 kernels, times both
   * scales.
   */
  static final class Int8 extends FineCodec {
    /** Components beyond this many standard deviations are clipped. */
    static final float CLIP_DEVIATIONS = 4f;

    /** The scale of every code of this dimension. */
    final float scale;

    Int8(int dim) {
      super(FineTier.INT8, dim, dim);
      scale = frozenScale(dim);
    }

    /** The scale of every {@code dim}-dimensional INT8 code. */
    static float frozenScale(int dim) {
      return (float) (CLIP_DEVIATIONS / Math.sqrt(dim) / 127);
    }

    @Override
    void encode(float[] rotated, byte[] dest, int offset) {
      quantize(rotated, dim, 1 / scale, dest, offset);
    }

    @Override
    void decode(byte[] code, int offset, float[] dest) {
      for (int d = 0; d < dim; d++) {
        dest[d] = scale * code[offset + d];
      }
    }

    /** Writes {@code vector / scale}, rounded and clamped to [-127, 127], at {@code offset}. */
    static void quantize(float[] vector, int dim, float inverseScale, byte[] code, int offset) {
      for (int d = 0; d < dim; d++) {
        int level = Math.round(vector[d] * inverseScale);
        code[offset + d] = (byte) Math.max(-127, Math.min(127, level));
      }
    }

    /**
     * Quantizes a query into {@code levels} with its own max-abs scale, {@code max |x_d| / 127},
     * which it returns: a query is not stored, so it need not share the frozen scale. A search
     * quantizes its query once for every tier, through {@link QueryLevels}.
     */
    static float queryLevels(float[] rotated, int dim, byte[] levels) {
      float maxAbs = 0f;
      for (int d = 0; d < dim; d++) {
        maxAbs = Math.max(maxAbs, Math.abs(rotated[d]));
      }
      if (maxAbs == 0f) {
        Arrays.fill(levels, 0, dim, (byte) 0);
        return 1f;
      }
      quantize(rotated, dim, 127f / maxAbs, levels, 0);
      return maxAbs / 127f;
    }

    @Override
    Query query(float[] rotated, VectorSimilarityFunction similarity) {
      byte[] levels = new byte[dim];
      return query(rotated, similarity, levels, queryLevels(rotated, dim, levels));
    }

    @Override
    Query query(float[] rotated, VectorSimilarityFunction similarity, QueryLevels levels) {
      assert levels.rotated() == rotated;
      return query(rotated, similarity, levels.levels(), levels.scale());
    }

    private Query query(
        float[] rotated, VectorSimilarityFunction similarity, byte[] levels, float queryScale) {
      double scales = (double) queryScale * scale;
      return new Query(rotated, similarity) {
        private int[] dots = new int[0];

        @Override
        void score(byte[] codes, int stride, int count, float[] scores) {
          dots = ArrayUtil.growNoCopy(dots, count);
          Kernels.INSTANCE.dotProducts(levels, codes, stride, count, dots);
          for (int r = 0; r < count; r++) {
            scores[r] = toScore(scales * dots[r]);
          }
        }

        @Override
        void score(MemorySegment codes, long[] offsets, int count, float[] scores) {
          dots = ArrayUtil.growNoCopy(dots, count);
          Kernels.INSTANCE.dotProducts(levels, codes, offsets, count, dots);
          for (int r = 0; r < count; r++) {
            scores[r] = toScore(scales * dots[r]);
          }
        }
      };
    }
  }
}
