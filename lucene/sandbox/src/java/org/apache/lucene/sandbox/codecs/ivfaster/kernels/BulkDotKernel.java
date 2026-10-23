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
 * Multi-target unsigned int8 dot product for the writer's centroid assignment: one query code
 * against several centroid codes at once, computing {@code VectorUtil.uint8DotProduct(q, codes[t])}
 * for each target {@code t}.
 *
 * <p>AN SPI because the int8 dot is ALREADY vectorized in core, so what is left to win is the
 * per-call overhead: the query vector is loop-invariant across the targets, so a batch loads and
 * widens it ONCE per chunk and reuses it for every target, and independent accumulator chains give
 * the pipeline more to overlap than a sequence of separate {@code uint8DotProduct} calls.
 *
 * <p>BIT-IDENTICAL to {@code VectorUtil.uint8DotProduct} for every target, which is load-bearing:
 * assignment decides CLUSTERING, so a wrong dot moves doc-to-cell placement and shows up only as
 * shifted recall. Integer dot is exact and associative, so hoisting the query load and
 * reassociating the lane sums cannot change the int32 result, and {@code TestBulkDotKernel} pins
 * every target against core.
 *
 * <p>Same {@code requires static jdk.incubator.vector} loading discipline as {@link HammingKernel}:
 * the Panama implementation is named by STRING and loaded reflectively only after the incubator
 * module is confirmed readable, so a JVM without {@code --add-modules jdk.incubator.vector} never
 * triggers its verification.
 *
 * @lucene.experimental
 */
interface BulkDotKernel {

  /**
   * For each {@code t} in {@code [0, numTargets)}, computes {@code uint8DotProduct(q, targets[t])}
   * over {@code dim} bytes and writes it to {@code out[t]}. All codes are unsigned-offset int8
   * ({@code [0,255]}), {@code dim} bytes each; {@code q} and every {@code targets[t]} have length
   * {@code >= dim}.
   *
   * @param out must have length {@code >= numTargets}
   */
  void bulkDot(byte[] q, byte[][] targets, int numTargets, int dim, int[] out);

  /**
   * As {@link #bulkDot}, but each target's code starts at {@code docOffset} INSIDE its record
   * rather than at index 0, so the rerank can dot straight out of the code records it just read
   * without copying each doc's code into its own array first.
   *
   * <p>The shape the RERANK has: it reads whole records (code, docId, corrective terms) and the
   * code is a dense {@code dim}-byte run at a fixed offset. Sharing the query widening across
   * {@code numTargets} records is what this buys, since the query load and ZERO_EXTEND are the bulk
   * of the per-element cost and calling {@link #dot} per candidate repays them every time.
   *
   * <p>Only valid for PLAIN unsigned-int8 codes (one byte per dim, no codebook, no bit-plane
   * packing). Bit-identical to {@link #dot} per target.
   *
   * @param recs the first {@code numTargets} entries are records whose code runs at {@code
   *     docOffset}
   * @param out must have length {@code >= numTargets}
   */
  void bulkDotAt(byte[] q, byte[][] recs, int numTargets, int docOffset, int dim, int[] out);

  /**
   * As {@link #bulkDotAt}, but the records live in ONE flat buffer at a fixed {@code stride}:
   * record {@code i}'s code runs at {@code i * stride + codeOffset}. This is the shape the rerank
   * actually stages into, so it is the shape the hot path scores from.
   *
   * <p>One buffer rather than {@code numTargets} separate arrays lets the tile read SEQUENTIALLY,
   * four codes a fixed distance apart in one allocation instead of four objects at unrelated
   * addresses, on top of sharing the query widening the way {@link #bulkDotAt} does.
   *
   * @param flat holds {@code count} records of {@code stride} bytes, code at {@code codeOffset} in
   *     each
   * @param out must have length {@code >= count}
   */
  void bulkDotStrided(
      byte[] q, byte[] flat, int count, int stride, int codeOffset, int dim, int[] out);

  /**
   * SINGLE-target unsigned int8 dot of {@code q · doc} over {@code dim} bytes. Core's {@code
   * VectorUtil.uint8DotProduct} accumulates into ONE IntVector, a dependent add chain over {@code
   * dim/laneCount} iterations; this uses FOUR independent accumulators over four chunks of the same
   * doc, so the mul-adds overlap and the chain does not stall. Bit-identical to core, since integer
   * dot is associative.
   */
  int dot(byte[] q, byte[] doc, int dim);

  /** True for the vectorized implementation; false for the scalar fallback. */
  boolean isVectorized();

  /** Counts how many batches ran through the VECTORIZED kernel; benchmarks assert on this. */
  java.util.concurrent.atomic.AtomicLong simdEngaged = new java.util.concurrent.atomic.AtomicLong();

  /** -Divfaster.noSimdBulkDot=true forces the scalar kernel, to A/B the vectorized one. */
  boolean SIMD_DISABLED = Boolean.getBoolean("ivfaster.noSimdBulkDot");

  /**
   * Offset dot via core's PUBLIC {@code uint8DotProduct}, by copying the code run into {@code
   * scratch} first.
   *
   * <p>Core's udot takes two whole equal-length arrays, so scoring a {@code dim}-byte run out of a
   * larger record needs the run in an array of its own, and this is what that costs. Used off the
   * tuned path (a vector width the local tile is not written for, and the sub-tile remainder),
   * where inheriting core's full SIMD ladder beats hand-rolling every width. The caller owns {@code
   * scratch} and passes the SAME one for a whole batch, so this is one {@code dim}-byte copy per
   * record with no allocation.
   */
  static int udotAtViaCore(byte[] q, byte[] buf, int offset, int dim, byte[] scratch) {
    System.arraycopy(buf, offset, scratch, 0, dim);
    return org.apache.lucene.util.VectorUtil.uint8DotProduct(q, scratch);
  }

  /**
   * The kernel for this JVM: Panama when the incubator module is readable, else scalar. Resolved
   * once.
   *
   * <p>THE LOCAL TILE IS THE ONLY VECTORIZED PATH. It does not fork to core's uint8 dot when
   * Lucene's provider chain has a NATIVE one ({@code -Dlucene.useNativeDotProduct} plus {@code
   * libdotProduct} on the library path), because scoring a code run inside a record needs an OFFSET
   * entry point core does not expose: its public udot takes two whole equal-length arrays, so the
   * fork would need a new core API. With the native dot off, which is the default, the tile is
   * faster than core's per-call udot anyway, since it shares one query widening across four targets
   * where core re-widens per call.
   *
   * <p>If a future core gains an offset-taking or MemorySegment udot, the fork is a small change
   * and worth taking for anyone running with the native library.
   */
  static BulkDotKernel get() {
    return Holder.KERNEL;
  }

  /**
   * Lazy holder: the lookup runs once, on first assignment scan. Mirrors {@link
   * HammingKernel.Holder}.
   */
  final class Holder {
    static final BulkDotKernel KERNEL = load();

    private Holder() {}

    private static BulkDotKernel load() {
      Scalar scalar = new Scalar();
      if (SIMD_DISABLED) {
        return scalar;
      }
      try {
        var layer = BulkDotKernel.class.getModule().getLayer();
        var incubator =
            (layer == null ? ModuleLayer.boot() : layer).findModule("jdk.incubator.vector");
        if (incubator.isEmpty()) {
          return scalar;
        }
        BulkDotKernel.class.getModule().addReads(incubator.get());
        var clazz =
            Class.forName(
                "org.apache.lucene.sandbox.codecs.ivfaster.PanamaBulkDotKernel",
                true,
                BulkDotKernel.class.getClassLoader());
        return (BulkDotKernel) clazz.getDeclaredConstructor().newInstance();
      } catch (Throwable t) {
        if (Boolean.getBoolean("ivfaster.simdDebug")) {
          IvfDiag.errThrowable("[ivfaster] Panama bulk-dot kernel not loaded", t);
        }
        return scalar;
      }
    }
  }

  /**
   * Scalar fallback: just loops core's {@code uint8DotProduct}, which is itself SIMD but per-call.
   */
  final class Scalar implements BulkDotKernel {
    @Override
    public boolean isVectorized() {
      return false;
    }

    @Override
    public void bulkDot(byte[] q, byte[][] targets, int numTargets, int dim, int[] out) {
      for (int t = 0; t < numTargets; t++) {
        out[t] = org.apache.lucene.util.VectorUtil.uint8DotProduct(q, targets[t]);
      }
    }

    /**
     * ONE scratch for the whole batch; see {@link BulkDotKernel#udotAtViaCore}.
     *
     * <p>No shortcut for {@code docOffset == 0}, since a record still carries its trailing
     * corrective bytes, so {@code recs[t].length > dim} and core's udot rejects the length
     * mismatch. Offset zero means the code starts at the front of the record, not that the record
     * is the code.
     */
    @Override
    public void bulkDotAt(
        byte[] q, byte[][] recs, int numTargets, int docOffset, int dim, int[] out) {
      final byte[] scratch = new byte[dim];
      for (int t = 0; t < numTargets; t++) {
        out[t] = udotAtViaCore(q, recs[t], docOffset, dim, scratch);
      }
    }

    @Override
    public void bulkDotStrided(
        byte[] q, byte[] flat, int count, int stride, int codeOffset, int dim, int[] out) {
      final byte[] scratch = new byte[dim];
      for (int i = 0; i < count; i++) {
        out[i] = udotAtViaCore(q, flat, i * stride + codeOffset, dim, scratch);
      }
    }

    @Override
    public int dot(byte[] q, byte[] doc, int dim) {
      return org.apache.lucene.util.VectorUtil.uint8DotProduct(q, doc);
    }
  }
}
