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

import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * An IVF (inverted file) vector format built around two quantization tiers and an exhaustive
 * router.
 *
 * <p>Documents are clustered into {@code nlist} cells by Lloyd iteration; a query selects a few
 * cells and scores only their documents. Both steps are two-tier cascades: a cheap 2-bit
 * <i>coarse</i> code narrows the field, and a wider <i>fine</i> code ranks the survivors.
 *
 * <ul>
 *   <li><b>Coarse, 2 bits/dim</b> (256 B at dim=1024): the nitrox2 3-level thermometer code, whose
 *       summed per-dimension level distance equals {@code popcount(q ^ d)} over the whole code.
 *       Symmetric, so there is no per-document correction term. The bit planes are concatenated per
 *       document, so the scan is one XOR+popcount over a contiguous byte string, with no
 *       plane-major transpose (see {@code Nitrox2}).
 *   <li><b>Fine, 8 bits/dim</b> (1024 B at dim=1024): pluggable, see {@code FineQuantizer}. Ranks
 *       the coarse survivors.
 * </ul>
 *
 * <h2>Design choices</h2>
 *
 * <ol>
 *   <li><b>Routing is an exhaustive scan.</b> Its candidate set contains every beam's, and
 *       assignment stays independent of the centroid graph's search-time parameters.
 *   <li><b>The centroid graph is purely a search structure</b>, built once at the end of flush or
 *       merge. Clustering never touches it, so its memory layout is its entire cost.
 *   <li><b>The Reaper</b>: after a centroid update only documents that could have changed cell are
 *       re-scanned, on a bound that provably contains every document that changes.
 * </ol>
 *
 * <h2>Scope limits</h2>
 *
 * <p>PAGE-CACHE RESIDENT IS THE TUNED CASE, and the layout admits a colder one. The scan reads the
 * coarse and fine sections through random-access slices of the data file. Slots are grouped by
 * cell, so the byte range of every probed cell is known before the scan begins, and the reader
 * hints all {@code nprobe} of them through {@code IndexInput#prefetch} up front, letting cold
 * faults overlap in place of one synchronous fault per cell.
 *
 * <p>AN ASYNC OR BATCHED I/O PATH IS DELIBERATELY ABSENT. It pays when nearly every page is cold,
 * and costs more than it saves in the page-cache-resident case this codec tunes for. The prefetch
 * hint has the opposite profile, since it collapses to a counter increment once pages are resident
 * (see the reader's {@code PREFETCH}), which is what makes it the one safe to leave on. Behaviour
 * on an index larger than the page cache is untested.
 *
 * <p>Indexing buffers the field's float vectors in heap, because the Lloyd mean needs them. The
 * coarse scan that opens every routing pass reads the packed planes alone, {@code 1/16} of that;
 * the exact stage that ranks the resulting shortlist reads the float vectors. Merging reconstructs
 * float vectors and needs comparable heap.
 *
 * <p>float32 vectors only. Full-precision storage is optional and takes two forms. A {@code null}
 * fine tier makes FP32 the fine tier: the code is the rotated vector at {@code 4*dim} bytes, and
 * the rerank is an exact float dot. {@code keepFullPrecision} stores an inert FP32 section beside a
 * quantized fine tier: search never reads it, merge copies it by concatenation, and it serves exact
 * {@code getFloatVectorValues}. With both options off, {@code getFloatVectorValues} reconstructs
 * approximations from the fine codes, and merging consumes those reconstructions.
 *
 * <p>No backwards-compatibility guarantee. Two files per segment: {@code .ivfm} (metadata) and
 * {@code .ivfd} (centroids, mean, fine codes, coarse planes, graph, slot map, optional FP32
 * vectors, posting offsets).
 *
 * @lucene.experimental
 */
public final class IVFasterVectorsFormat extends KnnVectorsFormat {

  /** The SPI name of this format. */
  public static final String NAME = "IVFasterVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static final String META_CODEC_NAME = NAME + "Meta";
  static final String DATA_CODEC_NAME = NAME + "Data";
  static final String META_EXTENSION = "ivfm";
  static final String DATA_EXTENSION = "ivfd";
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /** Default number of cells / posting lists. */
  public static final int DEFAULT_NLIST = 1000;

  /** Default number of cells scanned per query. */
  public static final int DEFAULT_NPROBE = 32;

  /**
   * Default spill bits. Each document is written into its primary cell plus up to this many
   * next-best cells, trading index size for recall at a fixed {@code nprobe}.
   *
   * <p>A SEARCH-LATENCY default. Spill lets a query reach a boundary document without probing deep,
   * so more spill buys the same recall at a lower {@code nprobe}, and {@code nprobe} is the
   * dominant term in query cost.
   *
   * <p>It is also the dominant term in INDEX SIZE: slot count scales as {@code 1 + spillBits}, and
   * both the fine code table and the coarse planes replicate per slot, so the whole vector payload
   * scales with it. Lowering it is the most immediate dial for size, at some latency to recover
   * through {@code nprobe}.
   *
   * <p>Interacts with {@code -Divfaster.spillMargin}, which gates eligibility: at a wide margin
   * most documents take the full {@code spillBits} copies and this behaves as flat {@code 1 +
   * spillBits} replication, while a tighter margin spends the budget on near-boundary documents
   * alone.
   */
  public static final int DEFAULT_SPILL_BITS = 3;

  /**
   * Default SOAR lambda. Spill cells are chosen by the SOAR objective (ScaNN, Sun et al., NeurIPS
   * 2023), which steers spill copies toward the directions the primary cell serves poorly, so the
   * copies are complementary. Zero selects plain next-nearest; the paper's tuned range is about 1.0
   * to 1.5.
   */
  public static final float DEFAULT_SOAR_LAMBDA = 1.0f;

  /**
   * Default BACKSTOP on Lloyd iterations. Each iteration is one centroid recompute and one Reaper
   * pass.
   *
   * <p>THIS IS A CEILING. Clustering's own exit is convergence: it stops once a pass CHANGES at
   * most {@code Clustering.CONVERGE_FRACTION} of the corpus's assignments. The signal is that
   * changed count. The number of documents the Reaper re-routes is a separate and far larger
   * quantity, one that plateaus well above zero on a fully converged corpus, so it is unusable as a
   * stopping rule; see {@code Clustering.CONVERGE_FRACTION} for the derivation.
   *
   * <p>MEASURED, and a large real field does reach this ceiling. On 1M 1024d embeddings at {@code
   * nlist=8000} the final merge's clustering still changes about 0.9% of assignments at iteration
   * 10, against a 0.5% threshold, so the backstop is what ends that loop and every added iteration
   * is paid in full. Small flush segments converge on their own, around iterations four to seven,
   * and those are the cheap ones.
   *
   * <p>So on the fields that dominate a build, this value buys cell quality with build time. The
   * objective is non-increasing (see {@code Clustering}), so raising it can only slow the build
   * while leaving cells at least as good. {@code -Divfaster.convergenceTrace} reports which case a
   * field fell into, and is the thing to read before changing this.
   *
   * <p>Write-time: {@code -Divfaster.lloydIters}, or the constructor.
   */
  public static final int DEFAULT_LLOYD_ITERS = 10;

  /**
   * The coarse tier: the cheap code that narrows the field before the fine rerank.
   *
   * <p>{@link #NITROX2} IS the coarse tier of this codec: a symmetric 2-bit thermometer code scored
   * by an XOR+popcount Hamming distance (see {@code Nitrox2}). Every ivfaster index is built on it,
   * and the enum exists so that the coarse tier is an explicit, self-describing property of the
   * format and of a segment's metadata.
   */
  public enum CoarseTier {
    /** 2-bit symmetric thermometer code, scored by one XOR+popcount Hamming over the whole code. */
    NITROX2
  }

  /**
   * The fine tier: the per-dimension int8 code that ranks the coarse survivors, scored by an
   * unsigned int8 dot ({@link BulkDotKernel}).
   *
   * <p>{@link #INT8} is the sole fine tier: full 8-bit doc levels, one byte per dimension. The enum
   * makes the fine tier an explicit, self-describing property of the format, persisted as an
   * encoding id, with room for future fine codes at the same constructor shape. A {@code null} fine
   * tier selects exact FP32 rerank (see the {@code keepFullPrecision} constructor).
   */
  public enum FineTier {
    /** Full 8-bit doc levels; the default. */
    INT8
  }

  /** Default coarse tier. */
  public static final CoarseTier DEFAULT_COARSE_TIER = CoarseTier.NITROX2;

  /** Default fine tier. */
  public static final FineTier DEFAULT_FINE_TIER = FineTier.INT8;

  /**
   * Default for {@link #IVFasterVectorsFormat(int, int, int, float, int, CoarseTier, FineTier,
   * boolean)}.
   */
  public static final boolean DEFAULT_KEEP_FULL_PRECISION = false;

  // Write-time parameters.
  private final int nlist;
  private final int spillBits;
  private final float soarLambda;
  private final int lloydIters;
  private final CoarseTier coarseTier;

  /**
   * The fine (rerank) tier, or {@code null} for exact FP32 rerank on the stored full-precision
   * vectors.
   */
  private final FineTier fineTier;

  /**
   * Whether to store the original FP32 vectors as an inert, search-unused, merge-by-concat section.
   */
  private final boolean keepFullPrecision;

  // Search-time parameter, persisted per field.
  private final int nprobe;

  /** Constructs a format using all default parameters. Required for SPI loading. */
  public IVFasterVectorsFormat() {
    this(DEFAULT_NLIST, DEFAULT_NPROBE);
  }

  /** Constructs a format with the given cell count and probe count, and default tiers. */
  public IVFasterVectorsFormat(int nlist, int nprobe) {
    this(nlist, nprobe, DEFAULT_SPILL_BITS, DEFAULT_SOAR_LAMBDA, DEFAULT_LLOYD_ITERS);
  }

  /** Constructs a format with the given clustering parameters and default tiers. */
  public IVFasterVectorsFormat(
      int nlist, int nprobe, int spillBits, float soarLambda, int lloydIters) {
    this(nlist, nprobe, spillBits, soarLambda, lloydIters, DEFAULT_COARSE_TIER, DEFAULT_FINE_TIER);
  }

  /** Constructs a format with the given tiers and default full-precision handling. */
  public IVFasterVectorsFormat(
      int nlist,
      int nprobe,
      int spillBits,
      float soarLambda,
      int lloydIters,
      CoarseTier coarseTier,
      FineTier fineTier) {
    this(
        nlist,
        nprobe,
        spillBits,
        soarLambda,
        lloydIters,
        coarseTier,
        fineTier,
        DEFAULT_KEEP_FULL_PRECISION);
  }

  /**
   * Constructs a fully configured format.
   *
   * @param nlist number of cells / posting lists (write-time; persisted)
   * @param nprobe number of cells scanned per query (search-time; persisted)
   * @param spillBits number of additional cells each document is written into, {@code >= 0}
   * @param soarLambda SOAR spill-selection weight, {@code >= 0}; zero selects plain nearest
   * @param lloydIters BACKSTOP on Lloyd iterations, {@code >= 1}; clustering normally stops
   *     earlier, on convergence, so this bounds only a field that does not converge
   * @param coarseTier the coarse tier; write-time, self-describing per field
   * @param fineTier the fine (rerank) tier, or {@code null} for EXACT FP32 rerank, where documents
   *     store their full-precision vector as the fine code and the shortlist is ranked by a plain
   *     float dot product. Write-time, persisted per field via its encoding id.
   * @param keepFullPrecision when true, store the original FP32 vectors as an INERT on-disk
   *     section: unread by the coarse and fine scan, merged by concatenation, and used only to
   *     serve exact {@code getFloatVectorValues}. IGNORED when {@code fineTier == null}, since
   *     exact FP32 rerank already stores and uses the full-precision vectors.
   */
  public IVFasterVectorsFormat(
      int nlist,
      int nprobe,
      int spillBits,
      float soarLambda,
      int lloydIters,
      CoarseTier coarseTier,
      FineTier fineTier,
      boolean keepFullPrecision) {
    super(NAME);
    if (coarseTier == null) {
      throw new IllegalArgumentException("coarseTier must not be null");
    }
    if (nlist < 1) {
      throw new IllegalArgumentException("nlist must be >= 1, got " + nlist);
    }
    // A structural bound, since exceeding it would silently truncate adjacency; see MAX_NLIST.
    if (nlist > MAX_NLIST) {
      throw new IllegalArgumentException("nlist must be <= " + MAX_NLIST + ", got " + nlist);
    }
    if (nprobe < 1) {
      throw new IllegalArgumentException("nprobe must be >= 1, got " + nprobe);
    }
    if (spillBits < 0) {
      throw new IllegalArgumentException("spillBits must be >= 0, got " + spillBits);
    }
    if (soarLambda < 0 || Float.isNaN(soarLambda)) {
      throw new IllegalArgumentException("soarLambda must be >= 0, got " + soarLambda);
    }
    if (lloydIters < 1) {
      throw new IllegalArgumentException("lloydIters must be >= 1, got " + lloydIters);
    }
    this.nlist = nlist;
    this.nprobe = nprobe;
    this.spillBits = spillBits;
    this.soarLambda = soarLambda;
    this.lloydIters = lloydIters;
    this.coarseTier = coarseTier;
    this.fineTier = fineTier;
    // keepFullPrecision is ignored under exact FP32 rerank; see the constructor javadoc.
    this.keepFullPrecision = keepFullPrecision && fineTier != null;
  }

  /**
   * Largest supported {@code nlist}, set by the centroid graph's two-byte neighbour ordinals.
   *
   * <p>Raising it means widening the ordinal field in the node record, which changes the record
   * stride and therefore the on-disk format.
   */
  public static final int MAX_NLIST = 0xFFFF;

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new IVFasterVectorsWriter(
        state, nlist, nprobe, spillBits, soarLambda, lloydIters, fineTier, keepFullPrecision);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new IVFasterVectorsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%s(nlist=%d nprobe=%d spillBits=%d soarLambda=%s lloydIters=%d coarseTier=%s fineTier=%s"
            + " keepFullPrecision=%s)",
        NAME,
        nlist,
        nprobe,
        spillBits,
        soarLambda,
        lloydIters,
        coarseTier,
        fineTier == null ? "fp32(exact)" : fineTier,
        keepFullPrecision);
  }
}
