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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * The 1-bit Hamming kernel for the ivfaster coarse scan: XOR + popcount of a query sign sketch
 * against doc sketch rows living in an off-heap {@link MemorySegment} (an mmap slice, or the
 * io_uring batch buffer).
 *
 * <p>AN SPI RATHER THAN A METHOD. The coarse popcount is the largest term in query CPU, and unlike
 * every dot-product path it has no SIMD implementation in core: {@code VectorUtil.xorBitCount} is
 * scalar everywhere. A vector popcount needs {@code jdk.incubator.vector}, which the sandbox module
 * declares {@code requires static}, so it is optional at runtime. The Panama code therefore sits in
 * its own class ({@link PanamaHammingKernel}), loaded only after the incubator module is known to
 * be readable; touching it on a JVM started without {@code --add-modules jdk.incubator.vector}
 * would throw {@link NoClassDefFoundError}. That check plus the load is {@link #get()}.
 *
 * <p>Implementations MUST be bit-identical to {@code VectorUtil.xorBitCount} for every length. A
 * wrong Hamming distance does not crash: it reorders the coarse shortlist and surfaces only as
 * slightly-off recall. {@code TestHammingKernel} pins every implementation against core's
 * reference, and {@link #simdEngaged} exists so a benchmark cannot report a SIMD number having run
 * the scalar path.
 *
 * @lucene.experimental
 */
interface HammingKernel {

  /**
   * XOR bit count of {@code q} against the {@code len} bytes at {@code seg[off]}.
   *
   * <p>Reads straight from the segment, so the scan never copies a sketch row onto the heap.
   */
  int distance(byte[] q, MemorySegment seg, long off, int len);

  /**
   * Bulk form: Hammings {@code q} against {@code rows} CONSECUTIVE sketch rows of {@code len}
   * bytes, the r-th at {@code seg[off + r*len]}, writing distance r to {@code out[r]}.
   *
   * <p>The shape the coarse scan has, since under cell-order a probed cell's sketches are one
   * contiguous run, and where the SIMD win is largest: the QUERY vectors are loop invariant across
   * rows, so they load once per tile rather than once per row. The scalar implementation calls
   * {@link #distance} in a loop; only the Panama one exploits this.
   *
   * @param out must have length >= {@code rows}
   */
  void bulkDistances(byte[] q, MemorySegment seg, long off, int len, int rows, int[] out);

  /**
   * STRIDED bulk form over a HEAP {@code byte[]}: Hammings {@code q} against {@code rows} node
   * codes of {@code len} bytes whose starts are given by {@code offsets}, writing distance {@code
   * r} to {@code out[r]}.
   *
   * <p>The coarse code is a single {@code len}-byte string, the thermometer planes concatenated,
   * and Hamming distance is additive over that string, so the summed per-dimension level distance
   * IS this one Hamming. See {@link Nitrox2} for the thermometer identity.
   *
   * <p>A byte[] OVERLOAD RATHER THAN A MemorySegment ONE, because this is the CENTROID GRAPH's
   * shape and the graph lives on the heap: a small table ({@code nlist * stride}) read once at
   * open. Reaching a heap array through {@code MemorySegment.ofArray} cannot take Panama's direct
   * load path, so it degrades to {@code unsafeGetBase} plus a per-element loop and the wrapper's
   * bounds and liveness checks do not fold. {@code ByteVector.fromArray} is the right load for a
   * {@code byte[]}, with no segment to bounds-check per access.
   *
   * <p>STRIDED because a descent scores the unvisited NEIGHBOURS of one node, an arbitrary set of
   * ordinals that {@link #bulkDistances} cannot express. Bulk so the loop-invariant query vectors
   * load once per node EXPANSION rather than once per neighbour, which is where a fan-out of up to
   * {@code M} pays.
   *
   * <p>The default implementation is the per-row scalar loop, so a kernel that does not specialize
   * stays correct.
   *
   * @param offsets byte offset of each row's code within {@code nodes}; length >= {@code rows}
   * @param len bytes per code
   * @param out must have length >= {@code rows}
   */
  default void bulkDistancesAtBytes(
      byte[] q, byte[] nodes, int[] offsets, int len, int rows, int[] out) {
    for (int r = 0; r < rows; r++) {
      final int off = offsets[r];
      int d = 0;
      for (int i = 0; i < len; i++) {
        d += Integer.bitCount((nodes[off + i] ^ q[i]) & 0xFF);
      }
      out[r] = d;
    }
  }

  /**
   * ADMISSION FILTER for the streaming coarse select. Scans {@code count} distances starting at
   * {@code rowDist[from]} and appends, to {@code outIdx} starting at 0, the LOCAL index {@code i}
   * (relative to {@code from}, i.e. {@code 0..count}) of every row with {@code rowDist[from + i] <=
   * thr}, in ascending {@code i} order. Returns the number appended.
   *
   * <p>ON THE SPI because the streaming admission loop rejects most candidates against a threshold
   * that only ever tightens, and the scalar form of that test is a short dependent chain (three
   * mins then a compare) per group, which the scalar loop cannot widen. A vector {@code
   * compare(LE)} evaluates a whole SIMD width against one threshold snapshot in a single lane op,
   * and an all-reject block then costs one {@code anyTrue}.
   *
   * <p>CALLED PER SUB-BLOCK rather than per cell: the caller re-tightens {@code thr} between
   * sub-blocks, so a snapshot governs at most one sub-block's rows. Filtering against a snapshot
   * looser than the fully-tightened threshold can only ADMIT rows a stricter test would have
   * rejected, never drop a survivor, which is the tolerance the group-of-four unroll already relies
   * on: {@code hist} and {@code packed} stay exact for every admitted row, and the final histogram
   * cut re-derives the true threshold, so top-k is unchanged.
   *
   * <p>The default is the scalar scan, so a kernel without a vector path stays correct.
   *
   * @param rowDist the distances; entries {@code [from, from+count)} are read
   * @param outIdx receives the surviving LOCAL indices; length must be >= {@code count}
   * @return number of survivors written to {@code outIdx}
   */
  default int filterAtMost(int[] rowDist, int from, int count, int thr, int[] outIdx) {
    int k = 0;
    for (int i = 0; i < count; i++) {
      if (rowDist[from + i] <= thr) {
        outIdx[k++] = i;
      }
    }
    return k;
  }

  /** True for an actual vectorized implementation; false for the scalar fallback. */
  boolean isVectorized();

  /**
   * Vector width in bits actually in use, or 0 for a scalar kernel. Reported at reader close so a
   * benchmark can prove which kernel ran (see {@link #simdEngaged}). On the interface so callers
   * never reference {@code PanamaHammingKernel} directly, which must stay reachable only by name.
   */
  default int vectorBits() {
    return 0;
  }

  /**
   * Counts how many queries ran a coarse scan through the VECTORIZED kernel. Benchmarks assert on
   * this: an unreadable incubator module or a missed {@code --add-modules} silently yields the
   * scalar path at a plausible latency, so "SIMD was measured" has to be provable.
   */
  java.util.concurrent.atomic.AtomicLong simdEngaged = new java.util.concurrent.atomic.AtomicLong();

  /** -Divfaster.noSimdHamming=true forces the scalar kernel, to A/B the vectorized one. */
  boolean SIMD_DISABLED = Boolean.getBoolean("ivfaster.noSimdHamming");

  /**
   * The kernel for this JVM: Panama when {@code jdk.incubator.vector} is readable and C2-backed
   * vectors are trustworthy, else scalar.
   *
   * <p>Resolved once. The Panama class is named by STRING and loaded reflectively so that a JVM
   * without the incubator module never triggers its verification; referencing it directly from here
   * would make this interface itself unloadable there.
   */
  static HammingKernel get() {
    return Holder.KERNEL;
  }

  /**
   * Lazy holder: the lookup runs once, on first coarse scan.
   *
   * <p>{@code requires static} is a COMPILE-TIME edge only. At runtime the module is resolved when
   * the JVM was started with {@code --add-modules jdk.incubator.vector}, but this module does not
   * automatically READ it, so loading {@link PanamaHammingKernel} would fail with {@code
   * IllegalAccessError}. The read edge is therefore added explicitly, as core's {@code
   * VectorizationProvider} does, and an absent module means no SIMD. THIS module's layer is
   * searched first, falling back to the boot layer for the unnamed and classpath case, mirroring
   * core's {@code lookupVectorModule}.
   *
   * <p>The constructor is reached through {@code getDeclaredConstructor} rather than {@code
   * getConstructor}, since the kernel and its constructor are package-private and {@code
   * getConstructor} finds only public ones; same package, so no {@code setAccessible}.
   *
   * <p>ANY failure, whether an unreadable module, no C2, or a verification error, falls back rather
   * than failing a query, since the scalar kernel is always correct. {@code
   * -Divfaster.simdDebug=true} surfaces why, so a silent scalar fallback can be diagnosed.
   */
  final class Holder {
    static final HammingKernel KERNEL = load();

    private Holder() {}

    private static HammingKernel load() {
      Scalar scalar = new Scalar();
      if (SIMD_DISABLED) {
        return scalar;
      }
      try {
        var layer = HammingKernel.class.getModule().getLayer();
        var incubator =
            (layer == null ? ModuleLayer.boot() : layer).findModule("jdk.incubator.vector");
        if (incubator.isEmpty()) {
          return scalar;
        }
        HammingKernel.class.getModule().addReads(incubator.get());
        var clazz =
            Class.forName(
                "org.apache.lucene.sandbox.codecs.ivfaster.PanamaHammingKernel",
                true,
                HammingKernel.class.getClassLoader());
        return (HammingKernel) clazz.getDeclaredConstructor().newInstance();
      } catch (Throwable t) {
        if (Boolean.getBoolean("ivfaster.simdDebug")) {
          IvfDiag.errThrowable("[ivfaster] Panama Hamming kernel not loaded", t);
        }
        return scalar;
      }
    }
  }

  /**
   * Scalar fallback: a four-accumulator XOR and popcount loop.
   *
   * <p>FOUR INDEPENDENT ACCUMULATORS. With one {@code distance +=} per word every add waits on the
   * previous one, so the CPU cannot pipeline the independent XOR and CNT work. Four chains issue in
   * parallel and let C2 auto-vectorize the body, with the partials summed once at the end.
   * Bit-identical to the single-chain form, since addition is associative over the partials and the
   * byte order is unchanged.
   *
   * <p>On aarch64 it strides 4 bytes with {@code Integer.bitCount} rather than 8 with {@code
   * Long.bitCount}: {@code Long.bitCount} is NOT vectorized on ARM (JDK-8336000), which is exactly
   * why core's {@code VectorUtil} picks the int stride there.
   */
  final class Scalar implements HammingKernel {

    /**
     * Native-order long view of a byte[], for pairing with Panama segment reads. NOT {@code
     * BitUtil.VH_NATIVE_LONG}, which is documented as native order only in production environments
     * and randomized during testing, while Panama's {@code JAVA_*_UNALIGNED} is always true native
     * order. Pairing the two makes the popcount wrong whenever the randomized order disagrees with
     * the platform's, because the xor of two longs read with opposite endianness is not the xor of
     * the bytes.
     */
    private static final VarHandle VH_QUERY_LONG =
        MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    /** The 4-byte counterpart to {@link #VH_QUERY_LONG}, both explicitly native order. */
    private static final VarHandle VH_QUERY_INT =
        MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    /**
     * True when {@code Integer.bitCount} is the faster stride (aarch64), mirroring core's {@code
     * VectorUtil.XOR_BIT_COUNT_STRIDE_AS_INT}.
     */
    private static final boolean STRIDE_AS_INT =
        System.getProperty("os.arch", "").contains("aarch64");

    /**
     * -Divfaster.scalarPopcount=true restores the single-accumulator loop, to A/B the unrolled one.
     */
    private static final boolean UNROLL = Boolean.getBoolean("ivfaster.scalarPopcount") == false;

    @Override
    public boolean isVectorized() {
      return false;
    }

    @Override
    public void bulkDistances(byte[] q, MemorySegment seg, long off, int len, int rows, int[] out) {
      for (int r = 0; r < rows; r++) {
        out[r] = distance(q, seg, off + (long) r * len, len);
      }
    }

    @Override
    public int distance(byte[] q, MemorySegment seg, long off, int len) {
      return scalarDistance(q, seg, off, len);
    }

    /**
     * The loop itself, as a static so {@link PanamaHammingKernel} can call it directly for lengths
     * too short to vectorize without paying a virtual dispatch to another kernel instance per row.
     */
    static int scalarDistance(byte[] q, MemorySegment seg, long off, int len) {
      int i = 0;
      int d0 = 0, d1 = 0, d2 = 0, d3 = 0;
      if (UNROLL && STRIDE_AS_INT) {
        // 4 x 4 B = 16 B per iteration, four independent chains.
        for (final int upper = len & -16; i < upper; i += 16) {
          d0 += Integer.bitCount((int) VH_QUERY_INT.get(q, i) ^ segInt(seg, off + i));
          d1 += Integer.bitCount((int) VH_QUERY_INT.get(q, i + 4) ^ segInt(seg, off + i + 4));
          d2 += Integer.bitCount((int) VH_QUERY_INT.get(q, i + 8) ^ segInt(seg, off + i + 8));
          d3 += Integer.bitCount((int) VH_QUERY_INT.get(q, i + 12) ^ segInt(seg, off + i + 12));
        }
        for (final int upper = len & -Integer.BYTES; i < upper; i += Integer.BYTES) {
          d0 += Integer.bitCount((int) VH_QUERY_INT.get(q, i) ^ segInt(seg, off + i));
        }
      } else if (UNROLL) {
        // x86-64: Long.bitCount is optimal there, so stride 8 with four chains, 32 B per iteration.
        for (final int upper = len & -32; i < upper; i += 32) {
          d0 += Long.bitCount((long) VH_QUERY_LONG.get(q, i) ^ segLong(seg, off + i));
          d1 += Long.bitCount((long) VH_QUERY_LONG.get(q, i + 8) ^ segLong(seg, off + i + 8));
          d2 += Long.bitCount((long) VH_QUERY_LONG.get(q, i + 16) ^ segLong(seg, off + i + 16));
          d3 += Long.bitCount((long) VH_QUERY_LONG.get(q, i + 24) ^ segLong(seg, off + i + 24));
        }
        for (final int upper = len & -Long.BYTES; i < upper; i += Long.BYTES) {
          d0 += Long.bitCount((long) VH_QUERY_LONG.get(q, i) ^ segLong(seg, off + i));
        }
      } else {
        // Control arm (-Divfaster.scalarPopcount): the original single-accumulator 8-byte loop.
        for (final int upper = len & -Long.BYTES; i < upper; i += Long.BYTES) {
          d0 += Long.bitCount((long) VH_QUERY_LONG.get(q, i) ^ segLong(seg, off + i));
        }
      }
      int distance = d0 + d1 + d2 + d3;
      for (; i < len; i++) {
        distance += Integer.bitCount((q[i] ^ seg.get(ValueLayout.JAVA_BYTE, off + i)) & 0xFF);
      }
      return distance;
    }

    private static int segInt(MemorySegment seg, long off) {
      return seg.get(ValueLayout.JAVA_INT_UNALIGNED, off);
    }

    private static long segLong(MemorySegment seg, long off) {
      return seg.get(ValueLayout.JAVA_LONG_UNALIGNED, off);
    }
  }
}
