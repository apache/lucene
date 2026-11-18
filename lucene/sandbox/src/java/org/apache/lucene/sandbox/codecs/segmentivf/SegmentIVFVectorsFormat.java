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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * SegmentIVF: an inverted-file (IVF) vector format, one IVF index per segment, with two tiers of
 * codes.
 *
 * <h2>Design</h2>
 *
 * <p>Each segment clusters its vectors into {@code nlist} cells. A query picks the cells nearest to
 * it, scans the compact coarse tier codes of every vector in them, keeps a bounded shortlist, and
 * reranks only the shortlist with the more precise fine tier codes. The coarse tier is {@link
 * CoarseTier#S1}, a sign bit per dimension relative to the cell's centroid, and the fine tier
 * {@link FineTier#INT8}, a byte per dimension. Vectors near a cell boundary are also written to up
 * to {@code spillBits} neighbouring cells, so a query probing either cell finds them; each such
 * copy is referred to as a slot.
 *
 * <p>The format is built around Lucene's segments being immutable and merged repeatedly:
 *
 * <ul>
 *   <li><b>Only codes are stored.</b> The original float vectors are never kept: {@link
 *       org.apache.lucene.index.FloatVectorValues} read back the normalized vectors the fine codes
 *       decode to, and a merge into another format carries those. Every vector is normalized and
 *       rotated by a fixed randomized Hadamard transform before it is encoded, which spreads its
 *       energy evenly over the dimensions, so per-dimension quantization works without training.
 *       Normalizing keeps only a vector's direction, so the format indexes FLOAT32 fields with
 *       {@code COSINE} or {@code DOT_PRODUCT} similarity only; a document with any other vector
 *       field is rejected, with an IllegalArgumentException, and the field is written empty.
 *   <li><b>The fine tier is merge-stable.</b> A fine code should only depend on the vector and
 *       optionally, a frozen scale ({@link FineCodec}), so a merge copies fine codes as they are
 *       and derives everything else from them, and no segment ever re-quantizes a quantized vector.
 *   <li><b>Every clustered segment of a field has the same cell count</b>, derived from {@code
 *       maxVectorsPerSegment}, so any such segment's centroids can seed another's clustering:
 *       flushes and merges warm-start from segments this process already wrote ({@link
 *       Clustering}). The exception is a segment too small to cluster, fewer than {@link
 *       #MIN_VECTORS_PER_CELL} vectors per cell, which is written as a single cell and never seeds.
 *   <li><b>Search cost does not grow with the segment count.</b> {@link SegmentIVFKnnQuery} merges
 *       every segment's coarse shortlist and reranks only the index-wide best.
 * </ul>
 *
 * <h2>Files</h2>
 *
 * <p>A segment has a metadata file ({@code .ivfm}) and a data file ({@code .ivfd}). The metadata
 * file holds one {@link FieldMetadata} entry per field, ending with field number -1. The data file
 * holds each field's sections, in this order:
 *
 * <ol>
 *   <li><b>Centroids</b> ({@link #SECTION_CENTROIDS}): {@code nlist} rotated unit vectors as
 *       floats. Searches rank candidate cells exactly against them, and merges seed from them.
 *   <li><b>Fine codes</b> ({@link #SECTION_FINE}): one per vector, in ordinal order, each padded to
 *       a cache line. The section starts on a {@link #PAGE_BYTES} boundary, so that a
 *       4096-dimension code is exactly one page and a 1024-dimension code a quarter of one, which a
 *       rerank reads uncached off disk ({@link FineCodeIO}), and no code straddles more cache lines
 *       than it must. A code carries no header or scale, which keeps the page multiples: its
 *       document and cell are found through the slot sections, and a fine tier keeps no per-vector
 *       metadata ({@link FineCodec}).
 *   <li><b>Coarse codes</b> ({@link #SECTION_COARSE}): one per slot, grouped by cell, so a probed
 *       cell is one contiguous run that a SIMD kernel scans in place, followed by the tier's
 *       per-field parameters ({@link CoarseCodec#parameters}). Each slot is coded relative to its
 *       own cell's centroid. A spill slot therefore holds a real copy, not a reference to its
 *       vector's primary code (see {@code SegmentIVFVectorsWriter}'s {@code writeAnchoredCoarse}
 *       for the measurement).
 *   <li><b>Centroid graph</b> ({@link #SECTION_GRAPH}): a neighbour graph over the centroid codes,
 *       which a query walks to find its cells without scoring every centroid; absent when {@code
 *       nlist == 1}.
 *   <li><b>Ordinal to primary slot</b> ({@link #SECTION_ORD_TO_SLOT}): one int per vector. It maps
 *       an ordinal to its document, through the slot, and to its primary cell.
 *   <li><b>Slot to document</b> ({@link #SECTION_SLOT_DOCS}): one int per slot, read by every scan
 *       to deduplicate spill copies and apply filters.
 *   <li><b>Cell offsets</b> ({@link #SECTION_CELL_OFFSETS}): {@code nlist + 1} monotonic values,
 *       the byte offset of each cell's first slot in the slot-to-document section, so cell {@code
 *       c} holds slots {@code [start(c), start(c + 1))}.
 * </ol>
 *
 * <h2>Configuration</h2>
 *
 * <p>{@code maxVectorsPerSegment} sizes the cells: a field gets {@code nlist =
 * sqrt(maxVectorsPerSegment)} cells, the usual IVF balance between ranking centroids and scanning
 * cells, whatever the dimension or tiers. {@code nprobe} is the default number of cells a query
 * visits, {@code spillBits} the most extra cells a vector is written to (at most {@link
 * #MAX_SPILL_BITS}), {@link CoarseTier} the scan encoding and {@link FineTier} the rerank encoding.
 * {@link SearchStrategy} overrides the search per query. {@code nprobe} is recorded in each segment
 * as it is written, so changing it affects only segments written afterwards: searches without a
 * strategy probe each segment as it was configured, until a merge rewrites it.
 *
 * <p><b>Warm up before concurrent load.</b> The scan's kernels use the Vector API, which HotSpot's
 * C2 compiles to SIMD instructions only when it compiles them from a profile that has seen them
 * run. If many threads search a freshly started JVM at once, C2 can compile them early, from a thin
 * profile, with their vectors boxed; that code runs several times slower and stays for the life of
 * the JVM. Run some searches on one thread before taking concurrent traffic: with such a warmup no
 * slow compile was seen in 40 starts, against about one start in ten without.
 *
 * @lucene.experimental
 */
public final class SegmentIVFVectorsFormat extends KnnVectorsFormat {
  static final String NAME = "SegmentIVFVectorsFormat";
  static final String META_CODEC_NAME = NAME + "Meta";
  static final String DATA_CODEC_NAME = NAME + "Data";
  static final String META_EXTENSION = "ivfm";
  static final String DATA_EXTENSION = "ivfd";

  static final int VERSION_START = 0;

  static final int VERSION_CURRENT = VERSION_START;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /** The fine-code section starts on a page boundary, so a page-sized code is one read. */
  static final int PAGE_BYTES = 4096;

  // Data sections, in file order.
  static final int SECTION_CENTROIDS = 0;
  static final int SECTION_FINE = 1;
  static final int SECTION_COARSE = 2;
  static final int SECTION_GRAPH = 3;
  static final int SECTION_ORD_TO_SLOT = 4;
  static final int SECTION_SLOT_DOCS = 5;
  static final int SECTION_CELL_OFFSETS = 6;
  static final int SECTION_COUNT = 7;

  /**
   * The most spill cells a vector can get. Spill cells are drawn from the candidate cells routing
   * ranks for the vector, 16 for S1, while memory grows with {@code 1 + spillBits}.
   */
  public static final int MAX_SPILL_BITS = 7;

  /** The most cells a field can have: the centroid graph stores 2-byte cell ordinals. */
  public static final int MAX_NLIST = 0xFFFF;

  /**
   * The most dimensions a field can have, above Lucene's default of 1024: at 4096 an INT8 fine code
   * is exactly one page ({@link #PAGE_BYTES}), the case the page-aligned fine section serves.
   */
  public static final int MAX_DIMENSIONS = 4096;

  /**
   * The default number of vectors the largest segment is expected to hold, so {@code nlist}, its
   * square root, is 1000. The format keeps no float vectors: a 1024-dimension vector takes about
   * 1.2 KB of its files, a 1 KB INT8 fine code, a 4-byte ordinal-to-slot entry, and a 128-byte S1
   * coarse code with a 4-byte document id per cell it is in (its primary cell, and a spill cell for
   * about half of vectors, so about 1.5 slots per vector). A million is then about 1.2 GB, inside a
   * {@link org.apache.lucene.index.TieredMergePolicy} segment's default 5 GB with room for the rest
   * of each document. Set it to the vector count of the largest segment expected.
   */
  public static final int DEFAULT_MAX_VECTORS_PER_SEGMENT = 1_000_000;

  /**
   * Default oversample refers to the number of vectors reranked with the fine tier code. The
   * default value 3 shows a good balance between coarse scan speed and SSD rerank performance.
   */
  public static final float DEFAULT_OVERSAMPLE = 3f;

  /** The default number of cells a search probes, for a format created without one. */
  public static final int DEFAULT_NPROBE = 40;

  /** A search never fine-reranks fewer coarse candidates than this. */
  static final int MIN_RERANK = 100;

  /**
   * By default a flush or merge clusters and encodes with at most this many threads, half the
   * cores: the writer often shares its JVM with searchers, and a writer that took every core would
   * starve them during large flushes and merges.
   */
  public static final int DEFAULT_BUILD_THREADS =
      Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  /**
   * A segment with fewer than this many vectors per configured cell is written as a single cell.
   * Flushes are often tiny (a median of about 100 vectors on a live index), and training the full
   * cell count for them writes thousands of mostly duplicate, empty centroids and a graph over them
   * on every flush. A single cell needs no clustering, no graph and no seeding; a search scans it
   * in full, which costs no more than the full scan a search already does of any segment no larger
   * than its candidate pool. Such a segment never seeds another (its cell count differs), so the
   * rule changes nothing for the segments that do.
   */
  static final int MIN_VECTORS_PER_CELL = 16;

  final int maxVectorsPerSegment;
  final int nprobe;
  final int spillBits;
  final FineTier fineTier;
  final CoarseTier coarseTier;

  /**
   * The most build threads one flush or merge uses. Not written to the index: it changes only how
   * fast a segment is written, never what is written.
   */
  final int buildThreads;

  /**
   * The coarse tier's encoding. The cell scan, the centroid graph and clustering's routing all use
   * it.
   */
  public enum CoarseTier {
    /**
     * S1: 1-bit sign per dimension of a vector's offset from its cell's centroid, with no
     * per-vector metadata, scored against the query by table lookup ({@link CoarseCodec.S1}).
     */
    S1(2);

    /** The id the metadata records, fixed whatever the order of the constants. */
    final byte id;

    CoarseTier(int id) {
      this.id = (byte) id;
    }
  }

  /** The fine tier's encoding, used to rerank. */
  public enum FineTier {
    /** One signed byte per dimension at a scale frozen per dimension ({@link FineCodec.Int8}). */
    INT8(0);

    /** The id the metadata records, fixed whatever the order of the constants. */
    final byte id;

    FineTier(int id) {
      this.id = (byte) id;
    }
  }

  /**
   * One field's entry in the metadata file:
   *
   * <pre>
   *   field number    int      (-1 ends the file)
   *   fine tier       byte     FineTier id
   *   coarse tier     byte     CoarseTier id
   *   dimension       vint
   *   nlist           vint     0 for a field without vectors, which has no other data
   *   count           vint     vectors
   *   nprobe          vint     the default probes, from the format
   *   spill bits      vint     the most extra cells per vector
   *   section starts  vlong    x SECTION_COUNT, each where its section starts in the data file
   *   cell offsets    DirectMonotonic metadata, when nlist > 0
   * </pre>
   *
   * A section ends where the next starts. The rotation is not recorded: every index uses the same
   * fixed seed ({@link SegmentIVFVectorsWriter.HadamardRotation}).
   */
  record FieldMetadata(
      FineTier fineTier,
      CoarseTier coarseTier,
      int dim,
      int nlist,
      int count,
      int nprobe,
      int spillBits,
      long[] sections,
      DirectMonotonicReader.Meta cellOffsets) {

    /** Writes the entry up to the section starts, which the writer adds as it writes each one. */
    static void writeHeader(
        DataOutput meta, FieldInfo info, SegmentIVFVectorsFormat format, int nlist, int count)
        throws IOException {
      meta.writeInt(info.number);
      meta.writeByte(format.fineTier.id);
      meta.writeByte(format.coarseTier.id);
      meta.writeVInt(info.getVectorDimension());
      meta.writeVInt(nlist);
      meta.writeVInt(count);
      meta.writeVInt(format.nprobe);
      meta.writeVInt(format.spillBits);
    }

    /** Reads the rest of an entry whose field number was just read. */
    static FieldMetadata read(IndexInput meta, FieldInfo info, int maxDoc) throws IOException {
      byte fineId = meta.readByte();
      byte coarseId = meta.readByte();
      FineTier fineTier = null;
      for (FineTier tier : FineTier.values()) {
        fineTier = tier.id == fineId ? tier : fineTier;
      }
      CoarseTier coarseTier = null;
      for (CoarseTier tier : CoarseTier.values()) {
        coarseTier = tier.id == coarseId ? tier : coarseTier;
      }
      if (fineTier == null || coarseTier == null) {
        throw new CorruptIndexException(
            "unknown tiers " + fineId + ", " + coarseId + " for field " + info.name, meta);
      }
      int dim = meta.readVInt();
      int nlist = meta.readVInt();
      int count = meta.readVInt();
      int nprobe = meta.readVInt();
      int spillBits = meta.readVInt();
      long[] sections = new long[SECTION_COUNT];
      for (int s = 0; s < SECTION_COUNT; s++) {
        sections[s] = meta.readVLong();
      }
      // A checksum-valid entry can still be wrong (a writer bug, a mismatched build): reject it as
      // corrupt here rather than fail later, or misread, at search time.
      if (dim != info.getVectorDimension()) {
        throw new CorruptIndexException("dimension " + dim + " for field " + info.name, meta);
      }
      if (nlist < 0 || nlist > MAX_NLIST || count < 0 || count > maxDoc) {
        throw new CorruptIndexException(
            "nlist " + nlist + ", count " + count + " for field " + info.name, meta);
      }
      if ((nlist == 0) != (count == 0)
          || nprobe < 1
          || spillBits < 0
          || spillBits > MAX_SPILL_BITS) {
        throw new CorruptIndexException(
            "nlist "
                + nlist
                + ", count "
                + count
                + ", nprobe "
                + nprobe
                + ", spillBits "
                + spillBits
                + " for field "
                + info.name,
            meta);
      }
      for (int s = 1; s < SECTION_COUNT; s++) {
        if (sections[s] < sections[s - 1]) {
          throw new CorruptIndexException("section starts out of order for " + info.name, meta);
        }
      }
      DirectMonotonicReader.Meta cellOffsets = null;
      if (nlist > 0) {
        cellOffsets = DirectMonotonicReader.loadMeta(meta, nlist + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
      }
      return new FieldMetadata(
          fineTier, coarseTier, dim, nlist, count, nprobe, spillBits, sections, cellOffsets);
    }
  }

  /**
   * Creates a format for segments of up to {@link #DEFAULT_MAX_VECTORS_PER_SEGMENT} vectors, with
   * {@link #DEFAULT_NPROBE} probes, one spill cell, INT8 reranking and S1 coarse codes.
   */
  public SegmentIVFVectorsFormat() {
    this(DEFAULT_MAX_VECTORS_PER_SEGMENT, DEFAULT_NPROBE);
  }

  /** Creates a format with one spill cell, INT8 reranking and S1 coarse codes. */
  public SegmentIVFVectorsFormat(int maxVectorsPerSegment, int nprobe) {
    this(maxVectorsPerSegment, nprobe, 1, FineTier.INT8, CoarseTier.S1);
  }

  /** Creates a format with {@link #DEFAULT_BUILD_THREADS} build threads. */
  public SegmentIVFVectorsFormat(
      int maxVectorsPerSegment,
      int nprobe,
      int spillBits,
      FineTier fineTier,
      CoarseTier coarseTier) {
    this(maxVectorsPerSegment, nprobe, spillBits, fineTier, coarseTier, DEFAULT_BUILD_THREADS);
  }

  /**
   * Creates a fully configured format; {@code buildThreads} caps the threads each flush or merge
   * uses to cluster and encode, and is capped in turn by the number of cores.
   */
  public SegmentIVFVectorsFormat(
      int maxVectorsPerSegment,
      int nprobe,
      int spillBits,
      FineTier fineTier,
      CoarseTier coarseTier,
      int buildThreads) {
    super(NAME);
    if (maxVectorsPerSegment < 1
        || nprobe < 1
        || spillBits < 0
        || spillBits > MAX_SPILL_BITS
        || fineTier == null
        || coarseTier == null
        || buildThreads < 1) {
      throw new IllegalArgumentException(
          "require maxVectorsPerSegment >= 1, nprobe >= 1, 0 <= spillBits <= "
              + MAX_SPILL_BITS
              + ", non-null tiers and"
              + " buildThreads >= 1; got"
              + " maxVectorsPerSegment="
              + maxVectorsPerSegment
              + " nprobe="
              + nprobe
              + " spillBits="
              + spillBits
              + " fineTier="
              + fineTier
              + " coarseTier="
              + coarseTier
              + " buildThreads="
              + buildThreads);
    }
    this.maxVectorsPerSegment = maxVectorsPerSegment;
    this.nprobe = nprobe;
    this.spillBits = spillBits;
    this.fineTier = fineTier;
    this.coarseTier = coarseTier;
    this.buildThreads = buildThreads;
  }

  /** The configured cell count, the same for every field. */
  int nlist() {
    return configuredNlist(maxVectorsPerSegment);
  }

  /**
   * The cell count a segment of {@code count} vectors is written with: none when empty, one when it
   * has fewer than {@link #MIN_VECTORS_PER_CELL} vectors per configured cell, else the configured
   * {@link #nlist()}.
   */
  int nlist(int count) {
    if (count == 0) {
      return 0;
    }
    int configured = nlist();
    if (count < (long) MIN_VECTORS_PER_CELL * configured) {
      return 1;
    }
    return configured;
  }

  /**
   * The rounded square root of {@code maxVectorsPerSegment}, at least 1. It never exceeds {@link
   * #MAX_NLIST}: the square root of the largest int is 46341.
   */
  static int configuredNlist(int maxVectorsPerSegment) {
    return (int) Math.max(1, Math.round(Math.sqrt(maxVectorsPerSegment)));
  }

  /**
   * Per-query search options.
   *
   * <ul>
   *   <li>{@code numProbes}: how many cells a query visits, the nearest by centroid similarity.
   *   <li>{@code filterMode}: how a filter is applied ({@link FilterPlan.Mode}); {@link
   *       FilterPlan.Mode#PUSHDOWN} is the only mode.
   *   <li>{@code widen}: a filtered search that finds too few matches in its cells keeps probing
   *       more, in doubling rounds, the more the rarer the filter; without it, the selected cells
   *       are probed once.
   *   <li>{@code oversample}: how many coarse candidates per requested neighbor are fine-reranked,
   *       at least {@value #MIN_RERANK} in all.
   * </ul>
   */
  public static final class SearchStrategy extends KnnSearchStrategy {
    final int numProbes;
    final FilterPlan.Mode filterMode;
    final boolean widen;
    final float oversample;

    /** Creates a search strategy with pushdown filtering and the default oversample. */
    public SearchStrategy(int numProbes) {
      this(numProbes, FilterPlan.Mode.PUSHDOWN, false, DEFAULT_OVERSAMPLE);
    }

    /** Creates a fully configured search strategy. */
    public SearchStrategy(
        int numProbes, FilterPlan.Mode filterMode, boolean widen, float oversample) {
      // Written as a negation so that NaN is rejected.
      boolean validOversample = oversample >= 1 && Float.isFinite(oversample);
      if (numProbes < 1 || filterMode == null || validOversample == false) {
        throw new IllegalArgumentException(
            "require numProbes >= 1, a filter mode and finite oversample >= 1; got numProbes="
                + numProbes
                + " filterMode="
                + filterMode
                + " oversample="
                + oversample);
      }
      this.numProbes = numProbes;
      this.filterMode = filterMode;
      this.widen = widen;
      this.oversample = oversample;
    }

    /** How many coarse candidates to fine-rerank for a top-{@code k} search. */
    long rerankCount(int k) {
      return Math.max(MIN_RERANK, (long) Math.ceil((double) oversample * k));
    }

    /** Does nothing: cells are probed in a single pass. */
    @Override
    public void nextVectorsBlock() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof SearchStrategy strategy
          && strategy.numProbes == numProbes
          && strategy.filterMode == filterMode
          && strategy.widen == widen
          && Float.compare(strategy.oversample, oversample) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(numProbes, filterMode, widen, oversample);
    }
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new SegmentIVFVectorsWriter(state, this);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new SegmentIVFVectorsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return NAME
        + "(maxVectorsPerSegment="
        + maxVectorsPerSegment
        + " nprobe="
        + nprobe
        + " spillBits="
        + spillBits
        + " fineTier="
        + fineTier
        + " coarseTier="
        + coarseTier
        + " buildThreads="
        + buildThreads
        + ")";
  }
}
