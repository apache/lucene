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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.util.VectorUtil;

/**
 * Panama multi-target unsigned int8 dot product: the query code is loaded and widened ONCE per
 * 8-byte chunk and multiply-accumulated against {@code TILE} centroid codes in {@code TILE}
 * independent accumulators, so the loop-invariant query load is shared and the accumulator chains
 * overlap. One horizontal reduce per target at the end (unavoidable), amortized over {@code
 * dim/laneCount} mul-adds.
 *
 * <p>WHY THE TILE. A chain of {@code VectorUtil.uint8DotProduct} calls re-loads and re-widens the
 * query for every centroid and exposes one dependent accumulate chain at a time. Widening the byte
 * lanes to int is the bulk of the per-element cost here, so paying it once per query chunk instead
 * of once per (query, target) pair is the win; the independent per-target accumulators additionally
 * let the CPU issue the multiply-adds in parallel instead of draining one chain before the next
 * call starts.
 *
 * <p>BIT-IDENTICAL to {@code VectorUtil.uint8DotProduct}. The product is a sum of independent
 * per-lane int32 terms; splitting the byte vector into 8-byte chunks and reassociating the lane
 * sums cannot change the total (integer addition is associative and cannot overflow at these
 * magnitudes: {@code dim * 255 * 255 < 2^31} at every dimension this codec supports). This kernel
 * therefore does not change clustering, and {@code TestBulkDotKernel} pins every target and every
 * dim against core while asserting {@code simdEngaged} advanced, so a scalar fallback cannot pass
 * silently.
 *
 * <p>256-bit layout mirrors core's {@code PanamaVectorUtilSupport.dotProductBody256}: load 8 bytes
 * ({@code SPECIES_64}), zero-extend to 8 int lanes ({@code SPECIES_256}), multiply, add. Widths
 * other than 256-bit and the sub-tile or dim tail delegate to core's {@code uint8DotProduct}, which
 * is correct on every platform. This kernel is a throughput optimization for the common shape, not
 * a new distance.
 *
 * <p>Loaded reflectively by name from {@link BulkDotKernel#get()} only after the incubator module
 * is confirmed readable; never reference it from a class that must load without {@code
 * jdk.incubator.vector}.
 *
 * @lucene.experimental
 */
final class PanamaBulkDotKernel implements BulkDotKernel {

  /**
   * Package-private ctor, reached via {@code getDeclaredConstructor} from {@link
   * BulkDotKernel#get()}.
   */
  PanamaBulkDotKernel() {}

  /** Byte chunk width: 8 bytes widen to one 256-bit int vector (8 lanes). */
  private static final VectorSpecies<Byte> B_SPECIES = ByteVector.SPECIES_64;

  /** Int accumulator width. */
  private static final VectorSpecies<Integer> I_SPECIES = IntVector.SPECIES_256;

  /** Targets scored per pass. 4 independent accumulators keep register pressure sane on 256-bit. */
  private static final int TILE = 4;

  /** True only when the platform vector is 256-bit; other widths delegate to core per target. */
  private static final boolean IS_256 = IntVector.SPECIES_PREFERRED.vectorBitSize() == 256;

  @Override
  public boolean isVectorized() {
    return true;
  }

  @Override
  public void bulkDot(byte[] q, byte[][] targets, int numTargets, int dim, int[] out) {
    if (IS_256 == false) {
      // Off the tuned width: core's per-target kernel, correct everywhere.
      for (int t = 0; t < numTargets; t++) {
        out[t] = VectorUtil.uint8DotProduct(q, targets[t]);
      }
      return;
    }
    int t = 0;
    for (; t + TILE <= numTargets; t += TILE) {
      tile4(q, targets, t, dim, out);
    }
    // Remainder targets (fewer than TILE): core per-target, bit-identical.
    for (; t < numTargets; t++) {
      out[t] = VectorUtil.uint8DotProduct(q, targets[t]);
    }
  }

  @Override
  public void bulkDotAt(
      byte[] q, byte[][] recs, int numTargets, int docOffset, int dim, int[] out) {
    if (IS_256 == false) {
      // Off the tuned width: core's SIMD ladder via a copy, one scratch for the whole batch.
      final byte[] scratch = new byte[dim];
      for (int t = 0; t < numTargets; t++) {
        out[t] = BulkDotKernel.udotAtViaCore(q, recs[t], docOffset, dim, scratch);
      }
      return;
    }
    int t = 0;
    for (; t + TILE <= numTargets; t += TILE) {
      tile4At(q, recs, t, docOffset, dim, out);
    }
    // Remainder targets: the single-target kernel at the same offset, bit-identical.
    for (; t < numTargets; t++) {
      out[t] = dotAt(q, recs[t], docOffset, dim);
    }
  }

  @Override
  public void bulkDotStrided(
      byte[] q, byte[] flat, int count, int stride, int codeOffset, int dim, int[] out) {
    if (IS_256 == false) {
      final byte[] scratch = new byte[dim];
      for (int i = 0; i < count; i++) {
        out[i] = BulkDotKernel.udotAtViaCore(q, flat, i * stride + codeOffset, dim, scratch);
      }
      return;
    }
    int i = 0;
    for (; i + TILE <= count; i += TILE) {
      tile4Strided(q, flat, i, stride, codeOffset, dim, out);
    }
    for (; i < count; i++) {
      out[i] = dotAt(q, flat, i * stride + codeOffset, dim);
    }
    simdEngaged.addAndGet(count);
  }

  /**
   * {@link #tile4At} against four records in ONE flat buffer, {@code stride} bytes apart. Identical
   * arithmetic; the only difference is that the four code runs are four offsets into a single array
   * instead of index 0 of four arrays, so the loads are a fixed distance apart and stay sequential.
   */
  private static void tile4Strided(
      byte[] q, byte[] flat, int base, int stride, int codeOffset, int dim, int[] out) {
    final int o0 = base * stride + codeOffset;
    final int o1 = o0 + stride;
    final int o2 = o1 + stride;
    final int o3 = o2 + stride;
    IntVector a0 = IntVector.zero(I_SPECIES);
    IntVector a1 = IntVector.zero(I_SPECIES);
    IntVector a2 = IntVector.zero(I_SPECIES);
    IntVector a3 = IntVector.zero(I_SPECIES);
    int i = 0;
    int limit = B_SPECIES.loopBound(dim); // multiple of 8
    for (; i < limit; i += B_SPECIES.length()) {
      // Query chunk: loaded and widened ONCE for all four records (the whole point).
      IntVector vq =
          (IntVector)
              ByteVector.fromArray(B_SPECIES, q, i)
                  .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
      a0 = fma(vq, flat, o0 + i, a0);
      a1 = fma(vq, flat, o1 + i, a1);
      a2 = fma(vq, flat, o2 + i, a2);
      a3 = fma(vq, flat, o3 + i, a3);
    }
    int r0 = a0.reduceLanes(VectorOperators.ADD);
    int r1 = a1.reduceLanes(VectorOperators.ADD);
    int r2 = a2.reduceLanes(VectorOperators.ADD);
    int r3 = a3.reduceLanes(VectorOperators.ADD);
    // Dim tail (dim not a multiple of 8): scalar, unsigned, bit-identical to core's tail.
    for (; i < dim; i++) {
      int vqi = q[i] & 0xFF;
      r0 += vqi * (flat[o0 + i] & 0xFF);
      r1 += vqi * (flat[o1 + i] & 0xFF);
      r2 += vqi * (flat[o2 + i] & 0xFF);
      r3 += vqi * (flat[o3 + i] & 0xFF);
    }
    out[base] = r0;
    out[base + 1] = r1;
    out[base + 2] = r2;
    out[base + 3] = r3;
  }

  /**
   * SINGLE-target dot of {@code q} against a {@code dim}-byte run starting at {@code off} in {@code
   * buf}. The offset-taking twin of {@link #dot}, for the sub-tile remainder; same four-accumulator
   * shape, and bit-identical because integer dot is associative.
   */
  private static int dotAt(byte[] q, byte[] buf, int off, int dim) {
    IntVector a0 = IntVector.zero(I_SPECIES);
    IntVector a1 = IntVector.zero(I_SPECIES);
    IntVector a2 = IntVector.zero(I_SPECIES);
    IntVector a3 = IntVector.zero(I_SPECIES);
    final int step = B_SPECIES.length(); // 8 bytes
    int i = 0;
    final int limit = dim - 4 * step;
    for (; i <= limit; i += 4 * step) {
      a0 = fma(vq(q, i), buf, off + i, a0);
      a1 = fma(vq(q, i + step), buf, off + i + step, a1);
      a2 = fma(vq(q, i + 2 * step), buf, off + i + 2 * step, a2);
      a3 = fma(vq(q, i + 3 * step), buf, off + i + 3 * step, a3);
    }
    for (; i + step <= dim; i += step) {
      a0 = fma(vq(q, i), buf, off + i, a0);
    }
    int res = a0.add(a1).add(a2.add(a3)).reduceLanes(VectorOperators.ADD);
    for (; i < dim; i++) {
      res += (q[i] & 0xFF) * (buf[off + i] & 0xFF);
    }
    return res;
  }

  /** One 8-byte query chunk, zero-extended to {@link #I_SPECIES}. */
  private static IntVector vq(byte[] q, int i) {
    return (IntVector)
        ByteVector.fromArray(B_SPECIES, q, i)
            .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
  }

  /**
   * {@link #tile4} against four RECORDS whose codes start at {@code docOffset}. Separate from
   * {@code tile4} rather than a parameterized version of it so the offset-0 assignment path keeps
   * its exact shape; the two are otherwise identical.
   */
  private static void tile4At(
      byte[] q, byte[][] recs, int base, int docOffset, int dim, int[] out) {
    byte[] t0 = recs[base], t1 = recs[base + 1], t2 = recs[base + 2], t3 = recs[base + 3];
    IntVector a0 = IntVector.zero(I_SPECIES);
    IntVector a1 = IntVector.zero(I_SPECIES);
    IntVector a2 = IntVector.zero(I_SPECIES);
    IntVector a3 = IntVector.zero(I_SPECIES);
    int i = 0;
    int limit = B_SPECIES.loopBound(dim); // multiple of 8
    for (; i < limit; i += B_SPECIES.length()) {
      // Query chunk: loaded and widened ONCE for all four records (the whole point).
      IntVector vq =
          (IntVector)
              ByteVector.fromArray(B_SPECIES, q, i)
                  .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
      a0 = fma(vq, t0, docOffset + i, a0);
      a1 = fma(vq, t1, docOffset + i, a1);
      a2 = fma(vq, t2, docOffset + i, a2);
      a3 = fma(vq, t3, docOffset + i, a3);
    }
    int r0 = a0.reduceLanes(VectorOperators.ADD);
    int r1 = a1.reduceLanes(VectorOperators.ADD);
    int r2 = a2.reduceLanes(VectorOperators.ADD);
    int r3 = a3.reduceLanes(VectorOperators.ADD);
    // Dim tail (dim not a multiple of 8): scalar, unsigned, bit-identical to core's tail.
    for (; i < dim; i++) {
      int vqi = q[i] & 0xFF;
      r0 += vqi * (t0[docOffset + i] & 0xFF);
      r1 += vqi * (t1[docOffset + i] & 0xFF);
      r2 += vqi * (t2[docOffset + i] & 0xFF);
      r3 += vqi * (t3[docOffset + i] & 0xFF);
    }
    out[base] = r0;
    out[base + 1] = r1;
    out[base + 2] = r2;
    out[base + 3] = r3;
  }

  /**
   * Single-target dot with FOUR independent accumulators over four interleaved 8-byte chunks of the
   * same doc.
   *
   * <p>Core's {@code dotProductBody256} uses ONE accumulator, a dependent add chain over {@code
   * dim/8} steps whose drain shows up in the trailing {@code reduceLanes}. Four chains overlap in
   * the pipeline, with one reduce each at the end.
   *
   * <p>Bit-identical, since integer dot is associative: splitting the sum into four partials cannot
   * change the int32 total, and {@code dim * 255 * 255 < 2^31} leaves no overflow.
   */
  @Override
  public int dot(byte[] q, byte[] doc, int dim) {
    if (IS_256 == false) {
      return VectorUtil.uint8DotProduct(q, doc);
    }
    IntVector a0 = IntVector.zero(I_SPECIES);
    IntVector a1 = IntVector.zero(I_SPECIES);
    IntVector a2 = IntVector.zero(I_SPECIES);
    IntVector a3 = IntVector.zero(I_SPECIES);
    int step = B_SPECIES.length(); // 8 bytes
    int i = 0;
    int limit = dim - 4 * step; // room for one full unrolled group
    for (; i <= limit; i += 4 * step) {
      a0 = fma2(q, doc, i, a0);
      a1 = fma2(q, doc, i + step, a1);
      a2 = fma2(q, doc, i + 2 * step, a2);
      a3 = fma2(q, doc, i + 3 * step, a3);
    }
    // Remainder full 8-byte chunks (fewer than 4 left).
    for (; i + step <= dim; i += step) {
      a0 = fma2(q, doc, i, a0);
    }
    int res = a0.add(a1).add(a2.add(a3)).reduceLanes(VectorOperators.ADD);
    // Scalar tail (dim not a multiple of 8).
    for (; i < dim; i++) {
      res += (q[i] & 0xFF) * (doc[i] & 0xFF);
    }
    return res;
  }

  /** q·doc chunk at byte offset {@code i}: widen both 8-byte lanes to int, multiply, accumulate. */
  private static IntVector fma2(byte[] q, byte[] doc, int i, IntVector acc) {
    IntVector vq =
        (IntVector)
            ByteVector.fromArray(B_SPECIES, q, i)
                .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
    IntVector vd =
        (IntVector)
            ByteVector.fromArray(B_SPECIES, doc, i)
                .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
    return acc.add(vq.mul(vd));
  }

  /** Scores four consecutive targets against {@code q}, sharing the query load across all four. */
  private static void tile4(byte[] q, byte[][] targets, int base, int dim, int[] out) {
    byte[] t0 = targets[base],
        t1 = targets[base + 1],
        t2 = targets[base + 2],
        t3 = targets[base + 3];
    IntVector a0 = IntVector.zero(I_SPECIES);
    IntVector a1 = IntVector.zero(I_SPECIES);
    IntVector a2 = IntVector.zero(I_SPECIES);
    IntVector a3 = IntVector.zero(I_SPECIES);
    int i = 0;
    int limit = B_SPECIES.loopBound(dim); // multiple of 8
    for (; i < limit; i += B_SPECIES.length()) {
      // Query chunk: loaded and widened ONCE, reused for all four targets (the whole point).
      IntVector vq =
          (IntVector)
              ByteVector.fromArray(B_SPECIES, q, i)
                  .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
      a0 = fma(vq, t0, i, a0);
      a1 = fma(vq, t1, i, a1);
      a2 = fma(vq, t2, i, a2);
      a3 = fma(vq, t3, i, a3);
    }
    int r0 = a0.reduceLanes(VectorOperators.ADD);
    int r1 = a1.reduceLanes(VectorOperators.ADD);
    int r2 = a2.reduceLanes(VectorOperators.ADD);
    int r3 = a3.reduceLanes(VectorOperators.ADD);
    // Dim tail (dim not a multiple of 8): scalar, unsigned, bit-identical to core's tail.
    for (; i < dim; i++) {
      int vqi = q[i] & 0xFF;
      r0 += vqi * (t0[i] & 0xFF);
      r1 += vqi * (t1[i] & 0xFF);
      r2 += vqi * (t2[i] & 0xFF);
      r3 += vqi * (t3[i] & 0xFF);
    }
    out[base] = r0;
    out[base + 1] = r1;
    out[base + 2] = r2;
    out[base + 3] = r3;
  }

  /**
   * Widen 8 target bytes at {@code i} to int lanes, multiply by the (already-widened) query,
   * accumulate.
   */
  private static IntVector fma(IntVector vq, byte[] target, int i, IntVector acc) {
    IntVector vt =
        (IntVector)
            ByteVector.fromArray(B_SPECIES, target, i)
                .convertShape(VectorOperators.ZERO_EXTEND_B2I, I_SPECIES, 0);
    return acc.add(vq.mul(vt));
  }

  /** Test/benchmark introspection: the platform vector width in bits. */
  static int vectorBitSize() {
    return IntVector.SPECIES_PREFERRED.vectorBitSize();
  }
}
