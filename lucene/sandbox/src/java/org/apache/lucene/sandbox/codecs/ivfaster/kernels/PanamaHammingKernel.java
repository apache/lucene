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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/**
 * Panama SIMD implementation of the coarse 1-bit Hamming kernel: a true vector popcount over {@code
 * LongVector} lanes ({@code VectorOperators.BIT_COUNT}), replacing a word-at-a-time scalar loop.
 *
 * <p>WHY THIS EXISTS. {@code VectorUtil.xorBitCount} is scalar on every platform, and on aarch64
 * {@code Long.bitCount} does not auto-vectorize (JDK-8336000), which is why the scalar fallback
 * strides 4 bytes there.
 *
 * <p>THE QUERY MUST BE LOADED WITH {@code ByteVector.fromArray}, NOT VIA A HEAP MemorySegment.
 * {@code MemorySegment.ofArray} allocates a wrapper whose bounds and liveness checks do not fold
 * away, and the per-load overhead swamps the vector win; hoisting the wrapper out of the inner loop
 * does not recover it. Do not "simplify" these loads back.
 *
 * <p>MUST NOT be referenced directly by any class that can load without {@code
 * jdk.incubator.vector}: {@link HammingKernel#get()} loads it by name, only after checking the
 * module is readable.
 *
 * <p>ACCUMULATION IS IN LONG LANES. The popcount of a 64-bit lane is at most 64, so summing into
 * the same long lanes cannot overflow for any code length this codec sees, at one {@code add} per
 * step. {@code reduceLanes(ADD)} is paid once per row.
 *
 * <p>BIT-IDENTICAL to {@code VectorUtil.xorBitCount}: a popcount is a sum over independent words,
 * so lane-splitting and reassociating cannot change the total. Byte order never enters, since both
 * operands are loaded as raw byte lanes and XOR is bytewise, so the reinterpretation to long lanes
 * is a view of bytes that already agree. {@code TestHammingKernel} pins this against core for every
 * length, and {@code TestHammingKernelEquivalence} pins the wired reader's top-k.
 *
 * @lucene.experimental
 */
final class PanamaHammingKernel implements HammingKernel {

  /**
   * Package-private ctor, reached via {@code getDeclaredConstructor} from {@link
   * HammingKernel#get()}.
   */
  PanamaHammingKernel() {}

  /**
   * The STRIDED heap-array form, specialized for the two shapes a production coarse code takes.
   *
   * <p>Both branches hold the query vectors in named locals, so they stay in registers across every
   * row, run one independent XOR/BIT_COUNT chain each, and pay one tree reduce and one store per
   * row. Do NOT hoist them into a {@code LongVector[]}, which spills.
   *
   * <p>Four vectors covers a 512-bit machine, and eight covers 256-bit. Without the four-vector
   * branch the interface fallback is the scalar per-row loop, so on AVX512 the centroid-graph
   * descent would run its coarse distance with no SIMD at all. Any other width takes that scalar
   * fallback, which is correct everywhere.
   *
   * <p>The loads are {@code fromArray} rather than a heap MemorySegment; see {@link
   * HammingKernel#bulkDistancesAtBytes} for why that distinction is the point of this method.
   */
  @Override
  public void bulkDistancesAtBytes(
      byte[] q, byte[] nodes, int[] offsets, int len, int rows, int[] out) {
    if (len == 4 * STEP) {
      LongVector a0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
      LongVector a1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
      LongVector a2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
      LongVector a3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
      for (int r = 0; r < rows; r++) {
        final int o = offsets[r];
        LongVector s0 =
            a0.lanewise(VectorOperators.XOR, rowVec(nodes, o)).lanewise(VectorOperators.BIT_COUNT);
        LongVector s1 =
            a1.lanewise(VectorOperators.XOR, rowVec(nodes, o + STEP))
                .lanewise(VectorOperators.BIT_COUNT);
        LongVector s2 =
            a2.lanewise(VectorOperators.XOR, rowVec(nodes, o + 2 * STEP))
                .lanewise(VectorOperators.BIT_COUNT);
        LongVector s3 =
            a3.lanewise(VectorOperators.XOR, rowVec(nodes, o + 3 * STEP))
                .lanewise(VectorOperators.BIT_COUNT);
        out[r] = (int) s0.add(s1).add(s2.add(s3)).reduceLanes(VectorOperators.ADD);
      }
      strided2Rows.addAndGet(rows);
      return;
    }
    if (len != 8 * STEP) {
      // Off the production shape: the interface's scalar loop is correct at every width.
      HammingKernel.super.bulkDistancesAtBytes(q, nodes, offsets, len, rows, out);
      return;
    }
    LongVector a0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
    LongVector a1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
    LongVector a2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
    LongVector a3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
    LongVector a4 = ByteVector.fromArray(B_SPECIES, q, 4 * STEP).reinterpretAsLongs();
    LongVector a5 = ByteVector.fromArray(B_SPECIES, q, 5 * STEP).reinterpretAsLongs();
    LongVector a6 = ByteVector.fromArray(B_SPECIES, q, 6 * STEP).reinterpretAsLongs();
    LongVector a7 = ByteVector.fromArray(B_SPECIES, q, 7 * STEP).reinterpretAsLongs();
    for (int r = 0; r < rows; r++) {
      final int o = offsets[r];
      LongVector s0 =
          a0.lanewise(VectorOperators.XOR, rowVec(nodes, o)).lanewise(VectorOperators.BIT_COUNT);
      LongVector s1 =
          a1.lanewise(VectorOperators.XOR, rowVec(nodes, o + STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s2 =
          a2.lanewise(VectorOperators.XOR, rowVec(nodes, o + 2 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s3 =
          a3.lanewise(VectorOperators.XOR, rowVec(nodes, o + 3 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s4 =
          a4.lanewise(VectorOperators.XOR, rowVec(nodes, o + 4 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s5 =
          a5.lanewise(VectorOperators.XOR, rowVec(nodes, o + 5 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s6 =
          a6.lanewise(VectorOperators.XOR, rowVec(nodes, o + 6 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector s7 =
          a7.lanewise(VectorOperators.XOR, rowVec(nodes, o + 7 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      out[r] =
          (int)
              s0.add(s1)
                  .add(s2.add(s3))
                  .add(s4.add(s5).add(s6.add(s7)))
                  .reduceLanes(VectorOperators.ADD);
    }
    strided2Rows.addAndGet(rows);
  }

  /** Rows scored through the SINGLE-plane 4-step branch (the 1-bit sketch's production path). */
  static final java.util.concurrent.atomic.AtomicLong single4Rows =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Rows scored through the 8-step cell-scan branch (the 2-bit coarse code's production path at
   * 256-bit).
   */
  static final java.util.concurrent.atomic.AtomicLong single8Rows =
      new java.util.concurrent.atomic.AtomicLong();

  /** Rows scored through the STRIDED heap-array branch, so a benchmark can prove it engaged. */
  static final java.util.concurrent.atomic.AtomicLong strided2Rows =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Loads {@code STEP} bytes of a heap-resident row as long lanes.
   *
   * <p>The reinterpret is kept here, unlike in {@link #docVecNative}: {@code fromArray} on a {@code
   * byte[]} has no segment bounds or liveness check for the conversion to obstruct.
   */
  private static LongVector rowVec(byte[] a, int off) {
    return ByteVector.fromArray(B_SPECIES, a, off).reinterpretAsLongs();
  }

  private static final VectorSpecies<Byte> B_SPECIES =
      VectorSpecies.of(byte.class, VectorShape.preferredShape());

  /** Long view of the same width, where the popcount accumulates. */
  private static final VectorSpecies<Long> L_SPECIES =
      VectorSpecies.of(long.class, VectorShape.preferredShape());

  /**
   * Int view of the preferred width, for the admission filter's compare/compress over the
   * distances.
   */
  private static final VectorSpecies<Integer> I_SPECIES =
      VectorSpecies.of(int.class, VectorShape.preferredShape());

  /** Int lanes per vector: how many distances the admission filter tests per {@code compare}. */
  private static final int I_LANES = I_SPECIES.length();

  /** Bytes consumed per vector step. */
  private static final int STEP = B_SPECIES.vectorByteSize();

  /**
   * Doc rows are off-heap. Byte lanes have no endianness, so this order is nominal: what matters is
   * only that both operands are viewed as the same raw bytes, which they are.
   */
  private static final ByteOrder NATIVE = ByteOrder.nativeOrder();

  /**
   * Vector admission filter: {@code compare(LE, thr)} over a whole int-lane width per step, then
   * {@code compress} the surviving LOCAL indices. See {@link HammingKernel#filterAtMost} for the
   * correctness argument (thr snapshot per sub-block can only over-admit, never drop a survivor).
   *
   * <p>An all-reject step, the common case, costs one masked compare and one {@code anyTrue}; only
   * a step with at least one survivor pays the {@code compress} and store. Indices are compressed
   * against a per-lane iota, so the store is a single vector op rather than a scalar extraction
   * loop.
   */
  @Override
  public int filterAtMost(int[] rowDist, int from, int count, int thr, int[] outIdx) {
    final IntVector threshold = IntVector.broadcast(I_SPECIES, thr);
    int k = 0;
    int i = 0;
    for (final int upper = count - I_LANES; i <= upper; i += I_LANES) {
      final IntVector d = IntVector.fromArray(I_SPECIES, rowDist, from + i);
      final VectorMask<Integer> keep = d.compare(VectorOperators.LE, threshold);
      final int survivors = keep.trueCount();
      if (survivors == 0) {
        continue;
      }
      // Compressing the iota under `keep` packs surviving indices low; one store lands them.
      final IntVector local = IOTA.add(i);
      local.compress(keep).intoArray(outIdx, k);
      k += survivors;
    }
    // Scalar tail: the lanes that do not fill a full vector.
    for (; i < count; i++) {
      if (rowDist[from + i] <= thr) {
        outIdx[k++] = i;
      }
    }
    return k;
  }

  /**
   * Per-lane index [0,1,2,...], the base the admission filter offsets by the sub-block position.
   */
  private static final IntVector IOTA = IntVector.zero(I_SPECIES).addIndex(1);

  @Override
  public boolean isVectorized() {
    return true;
  }

  @Override
  public int vectorBits() {
    return vectorBitSize();
  }

  /**
   * Below this many bytes the PER-ROW vector form loses to the scalar loop and delegates: a single
   * load, XOR and CNT cannot amortize the {@code reduceLanes} horizontal reduction, so the
   * crossover is one vector's worth of work. Only the per-row entry point is affected, since {@link
   * #bulkDistances} hoists the query load and pays the same single reduce per row at any width.
   */
  private static final int MIN_VECTOR_BYTES = 2 * STEP;

  @Override
  public int distance(byte[] q, MemorySegment seg, long off, int len) {
    if (len < MIN_VECTOR_BYTES) {
      // A static call so it inlines, rather than a delegate instance.
      return HammingKernel.Scalar.scalarDistance(q, seg, off, len);
    }
    int i = 0;
    LongVector acc = LongVector.zero(L_SPECIES);
    // Two independent chains, since BIT_COUNT then ADD is dependent per accumulator.
    LongVector acc2 = LongVector.zero(L_SPECIES);
    for (final int upper = len - 2 * STEP; i <= upper; i += 2 * STEP) {
      acc = acc.add(xorPopcnt(q, seg, off, i));
      acc2 = acc2.add(xorPopcnt(q, seg, off, i + STEP));
    }
    for (final int upper = len - STEP; i <= upper; i += STEP) {
      acc = acc.add(xorPopcnt(q, seg, off, i));
    }
    int distance = (int) acc.add(acc2).reduceLanes(VectorOperators.ADD);
    // Scalar tails, empty whenever len is a whole number of vectors.
    for (; i + Long.BYTES <= len; i += Long.BYTES) {
      distance += Long.bitCount(qLong(q, i) ^ seg.get(ValueLayout.JAVA_LONG_UNALIGNED, off + i));
    }
    for (; i < len; i++) {
      distance += Integer.bitCount((q[i] ^ seg.get(ValueLayout.JAVA_BYTE, off + i)) & 0xFF);
    }
    return distance;
  }

  /**
   * Bulk form. The QUERY vectors are loop-invariant across rows, so at the common sketch widths
   * they are loaded ONCE and held in registers for every row of the cell, instead of re-loaded per
   * row. This is why the interface has a bulk entry point at all.
   *
   * <p>DISPATCH ONLY: each hot shape runs in its OWN small method ({@link #bulk8Native} and
   * friends), never inline here. With all eight specialized loops ({@code len} in {@code
   * {1,2,4,8}*STEP} times native or heap) inline in one method, C2 gives the buried production loop
   * poor register allocation and it runs issue-bound rather than memory-bandwidth bound. Keeping
   * this method tiny, so each hot loop is compiled on its own, is what avoids that. Do NOT fold the
   * bodies back inline.
   */
  @Override
  public void bulkDistances(byte[] q, MemorySegment seg, long off, int len, int rows, int[] out) {
    if (len == STEP) {
      // Sketch is exactly one vector wide.
      LongVector vq = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
      for (int r = 0; r < rows; r++) {
        long base = off + (long) r * len;
        out[r] =
            (int)
                vq.lanewise(VectorOperators.XOR, docVecHeap(seg, base))
                    .lanewise(VectorOperators.BIT_COUNT)
                    .reduceLanes(VectorOperators.ADD);
      }
      return;
    }
    if (len == 2 * STEP) {
      // Two vectors, both staying in registers across every row of the cell.
      LongVector vq0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
      LongVector vq1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
      for (int r = 0; r < rows; r++) {
        long base = off + (long) r * len;
        LongVector p0 =
            vq0.lanewise(VectorOperators.XOR, docVecHeap(seg, base))
                .lanewise(VectorOperators.BIT_COUNT);
        LongVector p1 =
            vq1.lanewise(VectorOperators.XOR, docVecHeap(seg, base + STEP))
                .lanewise(VectorOperators.BIT_COUNT);
        out[r] = (int) p0.add(p1).reduceLanes(VectorOperators.ADD);
      }
      return;
    }
    if (len == 4 * STEP) {
      // Four vectors, in its own method so C2 compiles the loop cleanly; see the javadoc.
      if (seg.isNative()) {
        bulk4Native(q, seg, off, rows, out);
      } else {
        bulk4Heap(q, seg, off, rows, out);
      }
      single4Rows.addAndGet(rows);
      return;
    }
    if (len == 8 * STEP) {
      // The 2-bit code's production path at 256-bit, in its own method; see the javadoc.
      if (seg.isNative()) {
        bulk8Native(q, seg, off, rows, out);
      } else {
        bulk8Heap(q, seg, off, rows, out);
      }
      single8Rows.addAndGet(rows);
      return;
    }
    if (len < MIN_VECTOR_BYTES) {
      // Narrower than the profitable vector width, so the scalar loop over the whole run.
      for (int r = 0; r < rows; r++) {
        out[r] = HammingKernel.Scalar.scalarDistance(q, seg, off + (long) r * len, len);
      }
      return;
    }
    for (int r = 0; r < rows; r++) {
      out[r] = distance(q, seg, off + (long) r * len, len);
    }
  }

  /**
   * The 2-bit production cell-scan loop: 256 B code (8 SIMD steps at 256-bit), off-heap (mmap /
   * io_uring) rows via {@link #docVecNative}.
   *
   * <p>ITS OWN METHOD ON PURPOSE. Eight query vectors held in named-local registers across every
   * row, eight independent XOR/BIT_COUNT chains reassociated in a balanced tree, one {@code
   * reduceLanes} and one store per row. Compiled standalone the loop is memory-bandwidth bound;
   * inline in the eight-branch {@link #bulkDistances} C2 spills the vector registers under the
   * whole method's live-range pressure. Do NOT hoist the vq locals into a {@code LongVector[]},
   * which spills, and do NOT inline this back into the dispatcher.
   */
  private static void bulk8Native(byte[] q, MemorySegment seg, long off, int rows, int[] out) {
    final int len = 8 * STEP;
    LongVector vq0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
    LongVector vq1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
    LongVector vq2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
    LongVector vq3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
    LongVector vq4 = ByteVector.fromArray(B_SPECIES, q, 4 * STEP).reinterpretAsLongs();
    LongVector vq5 = ByteVector.fromArray(B_SPECIES, q, 5 * STEP).reinterpretAsLongs();
    LongVector vq6 = ByteVector.fromArray(B_SPECIES, q, 6 * STEP).reinterpretAsLongs();
    LongVector vq7 = ByteVector.fromArray(B_SPECIES, q, 7 * STEP).reinterpretAsLongs();
    for (int r = 0; r < rows; r++) {
      long base = off + (long) r * len;
      LongVector p0 =
          vq0.lanewise(VectorOperators.XOR, docVecNative(seg, base))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p1 =
          vq1.lanewise(VectorOperators.XOR, docVecNative(seg, base + STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p2 =
          vq2.lanewise(VectorOperators.XOR, docVecNative(seg, base + 2 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p3 =
          vq3.lanewise(VectorOperators.XOR, docVecNative(seg, base + 3 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p4 =
          vq4.lanewise(VectorOperators.XOR, docVecNative(seg, base + 4 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p5 =
          vq5.lanewise(VectorOperators.XOR, docVecNative(seg, base + 5 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p6 =
          vq6.lanewise(VectorOperators.XOR, docVecNative(seg, base + 6 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p7 =
          vq7.lanewise(VectorOperators.XOR, docVecNative(seg, base + 7 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      out[r] =
          (int)
              p0.add(p1)
                  .add(p2.add(p3))
                  .add(p4.add(p5).add(p6.add(p7)))
                  .reduceLanes(VectorOperators.ADD);
    }
  }

  /** Heap-segment counterpart of {@link #bulk8Native} ({@link #docVecHeap} loads). */
  private static void bulk8Heap(byte[] q, MemorySegment seg, long off, int rows, int[] out) {
    final int len = 8 * STEP;
    LongVector vq0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
    LongVector vq1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
    LongVector vq2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
    LongVector vq3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
    LongVector vq4 = ByteVector.fromArray(B_SPECIES, q, 4 * STEP).reinterpretAsLongs();
    LongVector vq5 = ByteVector.fromArray(B_SPECIES, q, 5 * STEP).reinterpretAsLongs();
    LongVector vq6 = ByteVector.fromArray(B_SPECIES, q, 6 * STEP).reinterpretAsLongs();
    LongVector vq7 = ByteVector.fromArray(B_SPECIES, q, 7 * STEP).reinterpretAsLongs();
    for (int r = 0; r < rows; r++) {
      long base = off + (long) r * len;
      LongVector p0 =
          vq0.lanewise(VectorOperators.XOR, docVecHeap(seg, base))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p1 =
          vq1.lanewise(VectorOperators.XOR, docVecHeap(seg, base + STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p2 =
          vq2.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 2 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p3 =
          vq3.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 3 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p4 =
          vq4.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 4 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p5 =
          vq5.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 5 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p6 =
          vq6.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 6 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p7 =
          vq7.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 7 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      out[r] =
          (int)
              p0.add(p1)
                  .add(p2.add(p3))
                  .add(p4.add(p5).add(p6.add(p7)))
                  .reduceLanes(VectorOperators.ADD);
    }
  }

  /**
   * The 1-bit sign-sketch production loop: 128 B code (4 SIMD steps at 256-bit), off-heap rows.
   * Standalone per-shape method for the same register-allocation reason as {@link #bulk8Native}.
   */
  private static void bulk4Native(byte[] q, MemorySegment seg, long off, int rows, int[] out) {
    final int len = 4 * STEP;
    LongVector vq0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
    LongVector vq1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
    LongVector vq2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
    LongVector vq3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
    for (int r = 0; r < rows; r++) {
      long base = off + (long) r * len;
      LongVector p0 =
          vq0.lanewise(VectorOperators.XOR, docVecNative(seg, base))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p1 =
          vq1.lanewise(VectorOperators.XOR, docVecNative(seg, base + STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p2 =
          vq2.lanewise(VectorOperators.XOR, docVecNative(seg, base + 2 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p3 =
          vq3.lanewise(VectorOperators.XOR, docVecNative(seg, base + 3 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      out[r] = (int) p0.add(p1).add(p2.add(p3)).reduceLanes(VectorOperators.ADD);
    }
  }

  /** Heap-segment counterpart of {@link #bulk4Native} ({@link #docVecHeap} loads). */
  private static void bulk4Heap(byte[] q, MemorySegment seg, long off, int rows, int[] out) {
    final int len = 4 * STEP;
    LongVector vq0 = ByteVector.fromArray(B_SPECIES, q, 0).reinterpretAsLongs();
    LongVector vq1 = ByteVector.fromArray(B_SPECIES, q, STEP).reinterpretAsLongs();
    LongVector vq2 = ByteVector.fromArray(B_SPECIES, q, 2 * STEP).reinterpretAsLongs();
    LongVector vq3 = ByteVector.fromArray(B_SPECIES, q, 3 * STEP).reinterpretAsLongs();
    for (int r = 0; r < rows; r++) {
      long base = off + (long) r * len;
      LongVector p0 =
          vq0.lanewise(VectorOperators.XOR, docVecHeap(seg, base))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p1 =
          vq1.lanewise(VectorOperators.XOR, docVecHeap(seg, base + STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p2 =
          vq2.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 2 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      LongVector p3 =
          vq3.lanewise(VectorOperators.XOR, docVecHeap(seg, base + 3 * STEP))
              .lanewise(VectorOperators.BIT_COUNT);
      out[r] = (int) p0.add(p1).add(p2.add(p3)).reduceLanes(VectorOperators.ADD);
    }
  }

  /** XOR of the query and doc vectors at byte offset {@code i}, popcounted lanewise. */
  private static LongVector xorPopcnt(byte[] q, MemorySegment seg, long off, int i) {
    return ByteVector.fromArray(B_SPECIES, q, i)
        .reinterpretAsLongs()
        .lanewise(VectorOperators.XOR, docVecHeap(seg, off + i))
        .lanewise(VectorOperators.BIT_COUNT);
  }

  /**
   * Loads {@code STEP} bytes of a document row as longs, where the popcount accumulates.
   *
   * <p>LOADED DIRECTLY AS LONGS rather than as a reinterpreted ByteVector. The reinterpret is
   * nominally a free bitcast, but it is not elided: it goes through {@code
   * AbstractVector.convert0}. Reading the segment as {@code LongVector} skips the conversion.
   *
   * <p>Bit-identical by construction: XOR is lanewise and {@code BIT_COUNT} sums over all bits of
   * the lane, so the result depends only on the raw bytes both operands see and not on how they are
   * grouped into lanes. Both sides use {@link #NATIVE} order, so they see the same bytes. The query
   * side keeps its ByteVector load, since {@code fromArray} on a {@code byte[]} has no segment to
   * reinterpret.
   *
   * <p>Unaligned-safe: the Vector API's segment loads do not require element alignment, and the
   * coarse plane sections start at arbitrary file offsets, so an alignment requirement here would
   * throw rather than mis-read. The parity tests cover it.
   *
   * <p>ONLY FOR NATIVE SEGMENTS. {@code LongVector.fromMemorySegment} on a HEAP segment cannot take
   * the direct path: it goes through {@code unsafeGetBase} and a per-element load loop. The
   * reader's planes are mmapped and take the direct path, while {@link CentroidCodes} wraps a heap
   * {@code byte[]} with {@code MemorySegment.ofArray} and does not, so the same call has opposite
   * outcomes decided by where the bytes live.
   *
   * <p>THE CHOICE MUST BE MADE OUTSIDE THE ROW LOOP. A branch on {@code isNative()} inside this
   * method is not hoisted as loop-invariant, and it blocks the load from folding into the XOR, so
   * {@link #bulkDistances} tests the segment ONCE and runs a specialized loop where each of these
   * two helpers contains exactly one kind of load.
   */
  private static LongVector docVecNative(MemorySegment seg, long off) {
    return LongVector.fromMemorySegment(L_SPECIES, seg, off, NATIVE);
  }

  /** Byte load plus reinterpret, for heap segments and the single-plane paths. */
  private static LongVector docVecHeap(MemorySegment seg, long off) {
    return ByteVector.fromMemorySegment(B_SPECIES, seg, off, NATIVE).reinterpretAsLongs();
  }

  /**
   * Native-order long read of the query for the scalar tail. A VarHandle rather than {@code
   * MemorySegment.ofArray}; see the class comment on why heap segments are avoided here.
   */
  private static long qLong(byte[] q, int i) {
    return (long) VH_Q_LONG.get(q, i);
  }

  private static final java.lang.invoke.VarHandle VH_Q_LONG =
      java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

  /** Test/benchmark introspection: the vector width actually in use, in bits. */
  static int vectorBitSize() {
    return L_SPECIES.vectorBitSize();
  }
}
