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

/**
 * The nitrox2 coarse tier: a symmetric 3-level thermometer code over two bit planes, 2 bits/dim.
 *
 * <p>Named for the Hot Wheels Acceleracers fuel. This class IS the encoding rather than an
 * interface over it: the thermometer form, the shared grid and the no-lookup-table rule leave no
 * room for a second implementation, so an abstraction would only add indirection to the hottest
 * path.
 *
 * <h2>The encoding</h2>
 *
 * <pre>
 *   level L in {0,1,2};   plane hi = (L &gt; 0),  plane lo = (L &gt; 1)
 *   L=0 -&gt; hi=0 lo=0      L=1 -&gt; hi=1 lo=0     L=2 -&gt; hi=1 lo=1
 * </pre>
 *
 * <p>THE REASON FOR THE THERMOMETER, and it is the whole design:
 *
 * <pre>
 *   popcount(q_hi ^ d_hi) + popcount(q_lo ^ d_lo)  ==  Sum_d |L_q(d) - L_d(d)|
 * </pre>
 *
 * <p>The identity is exact: the double Hamming IS the summed level distance rather than an
 * estimate. Two consequences follow. It is SYMMETRIC, so there is no per-document correction term
 * to carry, where an asymmetric 2-bit dot needs one and omitting it is not merely less accurate but
 * biased. And the whole comparison is one XOR and popcount over the concatenated code, which is
 * fewer operations than an asymmetric dot over the same bytes.
 *
 * <h2>The grid</h2>
 *
 * <p>Data-blind: the Hadamard rotation makes every dimension's standard deviation analytically
 * {@code 1/sqrt(dim)}, so the half-width is a function of {@code dim} alone, with no fitting and no
 * persisted statistic.
 *
 * <p>{@link #level} is the ONLY place that decision is made, and document, centroid and query
 * encode all route through it. A writer packing at one threshold while a reader quantizes at
 * another produces a wrong ranking with no error anywhere, and one function makes that impossible.
 *
 * @lucene.experimental
 */
final class Nitrox2 {

  /**
   * BITS PER DIMENSION, which is the plane count, and the unit to configure in.
   *
   * <p>{@code -Divfaster.coarseBits=1} is the sign sketch, {@code 2} the default thermometer. The
   * class's internal vocabulary is LEVELS ({@code {0,1}} at 1 bit, {@code {0,1,2}} at 2), one more
   * than the bit count, so the knob is in bits and {@link #LEVELS} is derived. {@code
   * -Divfaster.coarseLevels} is honoured too and means levels, that is bits + 1.
   *
   * <p>ONE BIT IS EXACTLY A SIGN SKETCH. {@link #thresholdFor} is {@code ((2t+1)/(LEVELS-1) - 1) *
   * clip}, so at {@code LEVELS=2} there is one plane whose single threshold is {@code (1/1 - 1) *
   * clip = 0}, the sign bit, independent of clip. The thermometer identity {@code popcount(q^d) ==
   * sum|Lq - Ld|} holds trivially there, since levels are 0/1 and the per-dimension distance is 0
   * or 1. So the tier degrades to the 1-bit case with no special-casing: {@code bytesPerVector}
   * halves and the same Hamming kernel scores the shorter code.
   *
   * <p>THE TRADE: a 1-bit code at the same byte budget covers twice the dimensions but needs a
   * larger shortlist for the same shortlist recall. Since the coarse scan dominates query CPU and
   * the shortlist path is a small term, paying more on the small term to halve the big one can be
   * favourable, which is what this flag exists to test end to end.
   *
   * <p>WRITE-TIME. It fixes the bytes on disk, so it is persisted and the reader REFUSES a
   * mismatch, as with {@link #CLIP_SIGMA}: a plane-count disagreement would misread every record
   * boundary rather than misplace a bucket.
   */
  private static final int PLANES_PROPERTY = resolvePlanes();

  private static int resolvePlanes() {
    final Integer bits = Integer.getInteger("ivfaster.coarseBits");
    final Integer levels = Integer.getInteger("ivfaster.coarseLevels");
    if (bits != null && levels != null && levels != bits + 1) {
      throw new IllegalArgumentException(
          "ivfaster.coarseBits="
              + bits
              + " and ivfaster.coarseLevels="
              + levels
              + " disagree: levels must be bits + 1. Set one.");
    }
    if (bits != null) {
      return bits;
    }
    if (levels != null) {
      return levels - 1;
    }
    return 2;
  }

  /** Thermometer levels: one more than the bit count. See {@link #PLANES_PROPERTY} for the knob. */
  static final int LEVELS = PLANES_PROPERTY + 1;

  /**
   * Bit planes, one per level boundary; equals BITS PER DIMENSION. See {@link #PLANES_PROPERTY}.
   */
  static final int PLANES = LEVELS - 1;

  static {
    if (LEVELS < 2) {
      throw new IllegalArgumentException(
          "coarse code needs >= 1 bit per dimension (ivfaster.coarseBits, or coarseLevels = bits + 1);"
              + " got bits="
              + PLANES);
    }
  }

  /**
   * Grid half-width, in units of the rotated per-dimension standard deviation.
   *
   * <p>Expressed against the ANALYTIC per-dimension std {@code 1/sqrt(dim)}, which is the std
   * {@link #level} uses, so the number and its scale agree.
   *
   * <p>Data-blind by construction: the rotation makes every dimension's std analytically {@code
   * 1/sqrt(dim)}, so this needs no fitting and no persisted statistic. It is nonetheless PERSISTED
   * per field, because it is a WRITE-TIME decision that fixes the grid the planes were packed on,
   * and a reader assuming a different one would score queries against buckets that were never
   * encoded.
   */
  static final float CLIP_SIGMA = 1.0f;

  /*
   * CLIP DOES NOT ADAPT TO LEVELS, which is a trap for anyone raising the level count.
   *
   * The clip is a HALF-WIDTH, and thresholdFor spaces the boundaries evenly across [-clip, +clip],
   * so raising LEVELS moves every boundary INWARD rather than adding boundaries beyond the existing
   * ones:
   *
   *   LEVELS=2 -> thresholds/clip = [0.0]                    (clip-INVARIANT: the sign bit)
   *   LEVELS=3 -> [-0.5, +0.5]                               (the default)
   *   LEVELS=4 -> [-0.667, 0.0, +0.667]
   *   LEVELS=5 -> [-0.75, -0.25, +0.25, +0.75]
   *
   * So at a FIXED clip, 4 levels saturates MORE of the tail than 3 does: the outer boundaries land
   * inside the 3-level pair. A clip tuned for 3 levels is therefore wrong for 4, and more levels
   * want a WIDER clip. LEVELS=2 is the safe case, since its single threshold is exactly 0 whatever
   * the clip, so the sign sketch inherits this constant harmlessly.
   */

  private Nitrox2() {}

  /** Bytes per plane for a given dimension. A code holds {@link #PLANES} of them. */
  static int planeBytes(int dim) {
    return (dim + 7) >>> 3;
  }

  /** Total coarse bytes per vector: one plane per level boundary. */
  static int bytesPerVector(int dim) {
    return PLANES * planeBytes(dim);
  }

  /**
   * The thermometer level in {@code [0, LEVELS)} for one rotated coordinate.
   *
   * <p><b>The single source of truth for the coarse grid.</b> Document, centroid, and query
   * encoding all call this; see the class javadoc for why that is load-bearing.
   */
  static int level(float v, int dim) {
    return levelOnGrid(v, clipFor(dim), invStepFor(dim));
  }

  /**
   * Grid half-width for {@code dim}: {@code CLIP_SIGMA / sqrt(dim)}.
   *
   * <p>Split out so a loop over coordinates can hoist it: recomputing it inside {@link #level}
   * costs a {@code Math.sqrt} and three float operations PER COORDINATE for a value fixed by {@code
   * dim}.
   */
  static float clipFor(int dim) {
    return (float) (CLIP_SIGMA / Math.sqrt(dim));
  }

  /** Reciprocal of the level step, so the hot loop multiplies instead of dividing. */
  static float invStepFor(int dim) {
    return (LEVELS - 1) / (2f * clipFor(dim));
  }

  /**
   * Threshold at which thermometer plane {@code t} fires, in units of {@code clip}.
   *
   * <p>THE ENCODING IS TWO COMPARISONS, not an affine map plus a rounding plus two clamps. Plane
   * {@code t} carries {@code (L > t)}, and {@code L = round((v + clip) * invStep)} with {@code
   * invStep = (LEVELS-1)/(2*clip)}, so
   *
   * <pre>
   *   L &gt;= t+1  &lt;=&gt;  (v + clip) * invStep &gt;= t + 0.5
   *             &lt;=&gt;  v &gt;= ((2t+1)/(LEVELS-1) - 1) * clip
   * </pre>
   *
   * <p>At {@code LEVELS=3} that is {@code -clip/2} and {@code +clip/2}. The general form is kept
   * rather than the two constants so that a wider code (4 levels over 3 planes) inherits it by
   * changing {@link #LEVELS} alone, since the derivation holds for any level count.
   *
   * <p>THE THRESHOLD FORM IS THE EXACT ONE, and the arithmetic form is not: {@code (v + clip) *
   * invStep} can round a coordinate that lies within a few ULP below a threshold up through it. The
   * comparison form has no such rounding, so it is both correct and cheaper, and the equivalence is
   * pinned by a test because the difference is format-affecting at ULP scale.
   *
   * <p>NaN fails every comparison and lands on level 0, matching {@code Math.round(NaN) == 0};
   * infinities saturate at the ends. Both are checked by that test.
   */
  static float thresholdFor(int t, float clip) {
    return ((2f * t + 1f) / (LEVELS - 1) - 1f) * clip;
  }

  /**
   * The thermometer level, given a pre-computed grid.
   *
   * <p>IN THRESHOLD FORM, in place rather than as a parallel fast path. Every caller (document
   * encode, centroid encode, query encode) routes through here, and this class exists so that there
   * is exactly one grid decision; a second implementation alongside it would reintroduce the writer
   * and reader disagreement the design prevents.
   *
   * <p>{@code invStep} is unused and retained only so the signature is stable for callers that
   * pre-compute it. See {@link #thresholdFor} for the derivation.
   */
  static int levelOnGrid(float v, float clip, float invStep) {
    int l = 0;
    for (int t = 0; t < LEVELS - 1; t++) {
      if (v >= thresholdFor(t, clip)) {
        l++;
      }
    }
    return l;
  }

  /**
   * Encodes {@code vector[0..dim)} into {@code PLANES} consecutive thermometer planes at {@code
   * destOff}, each {@code planeBytes(dim)} long.
   *
   * <p>THE PACKING PRIMITIVE every encode path routes through, and the one place the bit layout is
   * decided. Three properties:
   *
   * <ol>
   *   <li><b>Accumulates into a long and stores once per 64 dimensions</b>, so there is no
   *       read-modify-write per set bit and no zero-fill: each 64-bit word is built in a register
   *       and written once.
   *   <li><b>Branchless bit setting.</b> On rotated data the levels are near-uniformly distributed,
   *       the worst case for a predictor, and {@code word |= mask & -(v >= t ? 1 : 0)} has no
   *       branch to mispredict.
   *   <li><b>One comparison per plane per coordinate</b>, in place of an add, a multiply, a round
   *       and two clamps; see {@link #thresholdFor}, which is also the exact form.
   * </ol>
   *
   * <p>Byte-granularity tail handling keeps the on-disk layout well defined for any {@code dim},
   * including ones that are not a multiple of 64.
   */
  static void packPlanes(float[] vector, int dim, byte[] dest, int destOff, int planeBytes) {
    PACK.pack(vector, dim, dest, destOff, planeBytes, thresholdsFor(dim));
  }

  /** The packing kernel: vectorized where available, scalar otherwise. */
  private static final PlanePackKernel PACK = PlanePackKernel.get();

  /**
   * The {@code PLANES} thresholds for {@code dim}, in plane order.
   *
   * <p>Cached per dimension because a build calls this once per vector and the value depends only
   * on {@code dim}. Two entries at {@code LEVELS=3}.
   */
  static float[] thresholdsFor(int dim) {
    Grid g = CACHED_GRID;
    if (g == null || g.dim != dim) {
      final float clip = clipFor(dim);
      final float[] t = new float[PLANES];
      for (int i = 0; i < PLANES; i++) {
        t[i] = thresholdFor(i, clip);
      }
      g = new Grid(dim, t);
      CACHED_GRID = g;
    }
    return g.thresholds;
  }

  /**
   * The grid for one dimension: the {@code PLANES} thresholds, paired with the dimension they are
   * for.
   *
   * <p>ONE IMMUTABLE PAIR UNDER ONE VOLATILE, rather than a separate thresholds array and dim
   * field. Two fields can be torn whatever the write order: a reader loads the array, loses a race
   * to a thread caching a DIFFERENT dimension, re-reads the dim field, sees a value matching its
   * own request, and returns the other dimension's grid with no error anywhere. Reading both as one
   * reference removes that; racing recomputation stays benign, since the value is a pure function
   * of {@code dim}.
   */
  private record Grid(int dim, float[] thresholds) {}

  private static volatile Grid CACHED_GRID;

  /**
   * Encodes {@code vector[0..dim)} into the thermometer planes, written into {@code dest} back to
   * back: {@code hi} at {@code destOff}, then {@code lo}.
   *
   * <p>THE SOLE ENCODE ENTRY POINT, and the coarse code has ONE layout everywhere: the planes
   * concatenated into a {@code bytesPerVector(dim)}-byte string, in memory (the centroid graph's
   * node record, the routing buffer) and on disk (one coarse section, one record per vector) alike.
   * Because Hamming distance is additive over that string, the scan is a single XOR and popcount
   * over the whole code, with no plane-major transpose and no two-plane fusion to carry. See {@link
   * #level} for the thermometer identity that makes the concatenated Hamming exactly the summed
   * per-dimension level distance.
   */
  static void encode(float[] vector, int dim, byte[] dest, int destOff) {
    packPlanes(vector, dim, dest, destOff, planeBytes(dim));
  }

  /**
   * Decodes the level of dimension {@code d} from a packed code. Test and diagnostic support; the
   * scan never decodes, it XORs.
   *
   * <p>Sums the set bits across all {@link #PLANES} planes, which IS the level under the
   * thermometer representation. A loop over the actual plane count rather than a hi/lo pair, since
   * at one plane the pair form would read one byte past the end of the code.
   */
  static int decodeLevel(byte[] code, int off, int dim, int d) {
    final int pb = planeBytes(dim);
    final int mask = 1 << (d & 7);
    final int byteIdx = d >>> 3;
    int level = 0;
    for (int t = 0; t < PLANES; t++) {
      if ((code[off + t * pb + byteIdx] & mask) != 0) {
        level++;
      }
    }
    return level;
  }
}
