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

import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.DATA_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.ivfaster.IVFasterVectorsFormat.VERSION_START;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Reads and searches an ivfaster index.
 *
 * <h2>The query path</h2>
 *
 * <ol>
 *   <li>Normalize the query, for every similarity, and rotate it with the persisted rotation.
 *   <li>Quantize it ONCE into both tiers: the 2-bit coarse planes and the fine tier's query form.
 *   <li>Select {@code nprobe} cells by greedy descent over the centroid graph's 2-bit node
 *       payloads, then rerank the visited set exactly. Falls back to an exact scan of the centroid
 *       matrix only when there is no graph ({@code nlist == 1}) or under {@code
 *       ivfaster.flatSelect}.
 *   <li>Coarse-scan the selected cells' contiguous slot runs, selecting a shortlist by counting
 *       sort.
 *   <li>Rerank the shortlist with the fine tier and collect.
 * </ol>
 *
 * <h2>Filters are resolved first, doc-at-a-time</h2>
 *
 * <p>A filtered query never post-filters a shortlist. The filter is resolved BEFORE the coarse scan
 * and consumed doc-at-a-time: every probed cell is a posting list ({@link CellPostings}, its slots
 * ascending by doc id), each probed run is walked in doc order with the filter's bit set tested per
 * document, and only the surviving slots are Hamming-scored, so a filter that admits one document
 * in a thousand costs one coarse distance per admitted document rather than per slot, and the
 * shortlist is drawn from filter-accepted documents alone. See {@link #filteredScanAndRerank} for
 * why the cells lead the intersection rather than the filter.
 *
 * <p>NPROBE IS DYNAMIC under a filter. Cells are consumed nearest-first in growing batches until
 * {@link #FILTERED_TARGET} distinct accepted documents have been scored, or every cell has been
 * probed, so a selective filter widens the probe instead of starving the shortlist. A filter whose
 * whole accepted set is no larger than the shortlist ({@link #EXACT_FILTER_COST}) skips cell
 * selection entirely and reranks every accepted document.
 *
 * <p>Live docs alone are NOT a filter in this sense: deletions are bounded by merging and cannot
 * starve the pool, so an unfiltered query on a segment with deletions keeps the bulk scan and drops
 * deleted documents at the dedup, the way it always has.
 *
 * <h2>Dedup inside the selection</h2>
 *
 * <p>Under spill a document occupies several slots, so a shortlist of {@code bruteN} SLOTS can hold
 * far fewer than {@code bruteN} distinct documents, and spill would then dilute the shortlist. The
 * selection therefore takes a pool of {@code bruteN * (1 + spillBits)} slots, which the writer's
 * spill cap makes a hard guarantee of at least {@code bruteN} distinct documents, and keeps the
 * first {@code bruteN} distinct ones in distance order. An exact bound rather than a tuned
 * overselect.
 *
 * @lucene.experimental
 */
final class IVFasterVectorsReader extends KnnVectorsReader {

  /**
   * Coarse shortlist size: documents handed to the fine tier per query.
   *
   * <p>The primary recall and latency dial, alongside {@code nprobe}. The two trade against each
   * other: a wider shortlist reaches a target recall at LOWER {@code nprobe}, so it buys
   * coarse-scan savings with extra rerank work, and which side wins depends on the recall being
   * targeted.
   */
  private static final int BRUTE_N = Integer.getInteger("ivfaster.bruteN", 700);

  /**
   * Filtered queries: distinct filter-accepted documents the coarse scan gathers before cell
   * selection stops; see the class javadoc.
   *
   * <p>Defaults to {@link #BRUTE_N}, so the walk stops as soon as it can fill the shortlist.
   *
   * <p>IT USED TO DEFAULT TO TWICE THAT, on the reasoning that reranking exactly what was gathered
   * leaves the coarse ranking with nothing to select, and on a measurement that twice the shortlist
   * lifted 5%-selectivity recall from 0.866 to 0.926. But that measurement held {@code nprobe}
   * fixed, and raising this target is only an indirect way of buying more cells: measured at
   * MATCHED RECALL on a 1M x 1024-d segment, 700 against 1400 against 2800 is a wash, inside
   * run-to-run noise at every recall point (5%: 1.070 / 1.078 / 1.091 ms at recall ~0.915, 1.243 /
   * 1.237 / 1.267 ms at 0.936; 10%: 1.338 / 1.339 / 1.372 ms at 0.936). Buying the cells directly
   * with {@code nprobe} costs the same and is one dial instead of two, so the smaller target is the
   * default and the extra rerank work is not spent.
   *
   * <p>COUPLED TO {@link #exactFilterBound}, which scales as its square root, so halving this also
   * narrows the band of filters that skip cell selection entirely by about 1.4x.
   */
  private static final int FILTERED_TARGET = Integer.getInteger("ivfaster.filteredTarget", BRUTE_N);

  /**
   * ABSOLUTE cap on cells probed by a filtered query; {@code 0} defers to {@link
   * #FILTERED_PROBE_MULTIPLIER}. Set it to bound a filtered query in cells regardless of the {@code
   * nprobe} it was asked for.
   */
  private static final int FILTERED_MAX_PROBE = Integer.getInteger("ivfaster.filteredMaxProbe", 0);

  /**
   * RELATIVE cap on cells probed by a filtered query, as a multiple of the query's own {@code
   * nprobe}; {@code 0} means every cell. Ignored when {@link #FILTERED_MAX_PROBE} is set.
   *
   * <p>WHY RELATIVE. The widening loop stops on {@link #FILTERED_TARGET}, and a filter selective
   * enough that the target is unreachable walked to the absolute ceiling instead, which was every
   * cell in the field. That makes the worst-case filtered query cost a full scan of the partition
   * and makes it unbounded in {@code nlist} — at {@code nlist = 65535} a query asking for 32 cells
   * could be served 65535. Tying the ceiling to what the caller asked for keeps the bound
   * proportional to the query's own budget, so the same setting means the same thing at every
   * {@code nlist}.
   *
   * <p>The cost of the bound is the recall held by the cells it declines to probe. Filters
   * selective enough to reach this ceiling are largely already served by the exact path (see {@link
   * #exactFilterBound}), which reranks the whole accepted set and never walks cells at all, so the
   * band this actually binds on is narrow.
   */
  private static final int FILTERED_PROBE_MULTIPLIER =
      Integer.getInteger("ivfaster.filteredProbeMultiplier", 8);

  /**
   * Floor on the filter cardinality at or below which every accepted document is fine-reranked
   * directly, with no cell selection and no coarse scan; see {@link #exactFilterBound}.
   *
   * <p>Defaults to {@link #BRUTE_N}: below it the accepted set is no larger than the shortlist the
   * fine tier would score anyway, so probing cells to find a subset of it could only lose recall.
   */
  private static final int EXACT_FILTER_COST =
      Integer.getInteger("ivfaster.exactFilterCost", BRUTE_N);

  /**
   * The cardinality up to which reranking every accepted document reads FEWER BYTES than walking
   * cells to find {@link #FILTERED_TARGET} of them would, so the exact path is chosen on cost as
   * well as on the floor.
   *
   * <p>A filter accepting {@code cost} of the segment lands about {@code cost / nlist} documents in
   * each cell, so reaching the target takes {@code target * nlist / cost} cells, each a run of
   * {@code slots / nlist} coarse codes: {@code target * slots * coarseBytes / cost} bytes of coarse
   * scan, over cells a selective filter drives past the warm working set. Reranking everything
   * instead reads {@code cost * recordLen} bytes. The two are equal at {@code cost = sqrt(target *
   * slots * coarseBytes / recordLen)}, about 25K documents on a 1M-document 1024-d segment with
   * three spill copies; below that the exact path is both cheaper and complete.
   */
  private static int exactFilterBound(FieldEntry e) {
    final double slots = (double) e.codeTableLength / e.recordLen;
    final double parity = Math.sqrt(FILTERED_TARGET * slots * e.coarseBytes / e.recordLen);
    return (int) Math.max(EXACT_FILTER_COST, Math.min(Integer.MAX_VALUE, parity));
  }

  /**
   * Cells a filtered query may probe before the widening loop must stop, for a query that asked for
   * {@code probe} of them.
   *
   * <p>{@link #FILTERED_MAX_PROBE} wins when set, since an absolute bound is an explicit
   * instruction about this field. Otherwise the bound is {@link #FILTERED_PROBE_MULTIPLIER} times
   * the query's own probe, and {@code 0} for either means the whole field.
   *
   * <p>ON THE MULTIPLIER PATH the result is never more than {@code nlist} and never less than
   * {@code probe}, so it can decline to WIDEN but cannot retract cells the caller asked for. THE
   * ABSOLUTE OVERRIDE DELIBERATELY CAN: {@link #FILTERED_MAX_PROBE} is documented to bound a
   * filtered query regardless of the {@code nprobe} it was asked for, so setting it below {@code
   * nprobe} does cut the base probe, and the recall that costs is the point of setting it.
   *
   * <p>Widened in long arithmetic: {@code probe} is caller-supplied and the multiplier is a system
   * property, so the product overflows int for large enough values of either.
   */
  private static int filteredMaxProbe(FieldEntry e, int probe) {
    if (FILTERED_MAX_PROBE > 0) {
      return Math.min(FILTERED_MAX_PROBE, e.nlist);
    }
    if (FILTERED_PROBE_MULTIPLIER <= 0) {
      return e.nlist;
    }
    final long widened = (long) Math.max(1, probe) * FILTERED_PROBE_MULTIPLIER;
    return (int) Math.min(e.nlist, Math.max(widened, Math.min(probe, e.nlist)));
  }

  /**
   * Diagnostic: select cells by exact scan of the centroid matrix rather than the graph.
   *
   * <p>The exact-select reference the graph's selection quality is validated against, the way the
   * {@code noSimd*} switches are references for the kernels.
   */
  private static final boolean FLAT_SELECT = Boolean.getBoolean("ivfaster.flatSelect");

  /**
   * Streaming admission in the coarse select: reject candidates above the running threshold before
   * storing them.
   *
   * <p>Whether this pays depends on how much the running threshold rejects: a threshold near the
   * mean candidate distance rejects little, and the added branch and its dependency chain then cost
   * more than the stores they save. {@code -Divfaster.noStreamSelect=true} runs the control arm.
   */
  private static final boolean STREAM_SELECT =
      Boolean.getBoolean("ivfaster.noStreamSelect") == false;

  /**
   * The streaming-admission reject runs through the Hamming kernel's vector {@code filterAtMost}
   * rather than the scalar min-of-four unroll; {@code -Divfaster.noSimdAdmit=true} restores the
   * scalar arm.
   *
   * <p>The admission test is where the scan spends its scalar time, and the vector form replaces
   * the dependency chain rather than the store: a rejected block collapses to one masked {@code
   * compare(LE)} plus an {@code anyTrue}, where the scalar min-of-four still serializes. Recall is
   * unchanged either way, since both arms admit the same rows.
   */
  private static final boolean SIMD_ADMIT = Boolean.getBoolean("ivfaster.noSimdAdmit") == false;

  /**
   * Sub-block over which the SIMD admission filter tests one threshold snapshot before
   * re-tightening. Large enough to amortize the per-call dispatch and keep the compare/compress in
   * a tight loop; small enough that the threshold does not go stale within a cell (staleness only
   * over-admits, but a wildly loose snapshot would store rows the next tightening immediately
   * evicts). 256 rows is one L1-resident int block.
   */
  private static final int SIMD_ADMIT_BLOCK = Integer.getInteger("ivfaster.simdAdmitBlock", 256);

  /**
   * Hint the page cache for every probed cell's byte range before scanning any of them ({@code
   * -Divfaster.noPrefetch=true} runs the control arm).
   *
   * <p>WHY THIS CODEC CAN DO IT AT ALL. Slots are grouped by cell, so as soon as the posting
   * directory is read the scan knows the exact contiguous byte range of every cell it is about to
   * touch, before touching the first one. Issuing all {@code nprobe} hints up front lets the kernel
   * fault those runs concurrently, so a cold scan pays one round of I/O latency in place of one per
   * cell. A graph descent has no equivalent: the next hop's address is unknown until the current
   * node is scored, so its misses are serial by construction. This is the property that makes the
   * layout suited to a disk-resident index.
   *
   * <p>NEARLY FREE WHEN WARM, which is what makes it safe to leave on by default. {@link
   * IndexInput#prefetch} is adaptive: it counts calls and skips the {@code madvise} syscall unless
   * the counter is zero or a power of two, and even then advises only when a page is genuinely
   * missing, resetting the counter on a miss. A warm index therefore pays an atomic increment and a
   * bit test per probed cell, and a cold one gets the hints it needs.
   *
   * <p>Both sections the scan reads are hinted: the coarse planes it Hammings, and the code table
   * that holds the fine codes the rerank reads. Nothing else on the query path is hinted, since the
   * centroid graph is sized to stay cache-resident.
   */
  private static final boolean PREFETCH = Boolean.getBoolean("ivfaster.noPrefetch") == false;

  /**
   * Per-query nprobe override; 0 uses the value persisted at write time.
   *
   * <p>SEARCH-TIME BY DESIGN: it lets one cached index serve an entire nprobe sweep, so a
   * latency-at-recall curve needs no reindex per point. Without it a harness that varies nprobe
   * would silently reuse the value the index was built with and report one operating point
   * repeatedly.
   */
  private static final int NPROBE_OVERRIDE = Integer.getInteger("ivfaster.nprobe", 0);

  /**
   * Adaptive-nprobe quality margin: drop a selected cell whose exact distance is worse than {@code
   * d1 / margin}, where {@code d1} is the nearest cell's. {@code 1.0} disables it.
   *
   * <p>The dominant terms in query cost scale with DOCUMENTS SCANNED rather than with query count,
   * and a fixed nprobe spends the same document budget on every query. Queries differ: one landing
   * in a dense region has its neighbours spread over many cells, while one sitting near a centroid
   * has them concentrated in a few, where the tail cells contribute only bytes.
   *
   * <p>PROBE WIDE, PRUNE ON QUALITY. Raising nprobe and then dropping cells that are far relative
   * to the nearest keeps the cells that matter present while leaving the rest unscanned, which a
   * smaller fixed nprobe cannot separate.
   *
   * <p>FREE to evaluate: {@link #rerankCells} already computes an exact distance to every candidate
   * cell in order to pick the top {@code probe}, so this is arithmetic on values in hand.
   *
   * <p>Applied as {@code keep cells with d <= d1 * margin} on the negated-dot distance, which is
   * negative, so a margin below 1 shrinks the bound's magnitude toward zero and admits cells
   * further than the nearest. It is therefore a RETENTION fraction: smaller keeps MORE. Set 1.0 to
   * disable the prune and scan the full {@code nprobe}.
   */
  private static final float NPROBE_MARGIN =
      Float.parseFloat(System.getProperty("ivfaster.nprobeMargin", "0.75"));

  /**
   * Rebind the mmap'd coarse and code segments to the GLOBAL (always-alive) scope at open, so the
   * per-load session-liveness check ({@code MemorySessionImpl.checkValidStateRaw}) folds away.
   * Every {@code LongVector.fromMemorySegment} in the coarse scan otherwise re-validates a segment
   * that, for a mapped directory slice, is never closed under the reader.
   *
   * <p>A flag, defaulting OFF, because {@code reinterpret} detaches the segment from its arena's
   * lifetime, so a use-after-close becomes a SIGSEGV in place of a thrown {@code
   * IllegalStateException}. Sound here only because these segments live exactly as long as the
   * reader that scans them. Requires native access; if unavailable the reinterpret throws and the
   * checked segment is used, so correctness never depends on it.
   */
  private static final boolean GLOBAL_SCOPE = Boolean.getBoolean("ivfaster.globalScope");

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput data;

  IVFasterVectorsReader(SegmentReadState state) throws IOException {
    final String metaName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    int versionMeta = -1;
    boolean success = false;
    IndexInput d = null;
    try {
      try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
        Throwable priorE = null;
        try {
          versionMeta =
              CodecUtil.checkIndexHeader(
                  meta,
                  META_CODEC_NAME,
                  VERSION_START,
                  VERSION_CURRENT,
                  state.segmentInfo.getId(),
                  state.segmentSuffix);
          readFields(meta, state);
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(meta, priorE);
        }
      }
      d = openDataInput(state, versionMeta, DATA_EXTENSION, DATA_CODEC_NAME, state.context);
      this.data = d;
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(d);
      }
    }
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String extension,
      String codecName,
      org.apache.lucene.store.IOContext context)
      throws IOException {
    final String name =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
    IndexInput in = state.directory.openInput(name, context);
    boolean success = false;
    try {
      final int versionData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta=" + versionMeta + ", " + extension + "=" + versionData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, SegmentReadState state) throws IOException {
    int fieldNumber;
    while ((fieldNumber = meta.readInt()) != -1) {
      final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
      if (fieldInfo == null) {
        throw new CorruptIndexException("invalid field number: " + fieldNumber, meta);
      }
      fields.put(fieldInfo.name, new FieldEntry(meta, fieldInfo));
    }
  }

  /** One field's metadata, read in exactly the order the writer emits it. */
  private static final class FieldEntry {
    final byte encodingId;
    final VectorSimilarityFunction similarity;
    final int dim;
    final int nlist;
    final int count;
    final long rotationSeed;
    final int nprobe;
    final int spillBits;
    final long centroidsOffset, centroidsLength;
    final long meanOffset, meanLength;
    final long codeTableOffset, codeTableLength;
    final long coarseOffset, coarseLength;
    final long graphOffset, graphLength;
    final long ordToSlotOffset, ordToSlotLength;

    /**
     * Raw FP32 vectors in ordinal order; {@code rawLength == 0} means "not stored, reconstruct from
     * the fine code".
     */
    final long rawOffset, rawLength;

    final DirectMonotonicReader.Meta postingOffsetsMeta;
    final long postingOffsetsDataStart;

    final HadamardRotation rotation;
    final FineQuantizer quantizer;
    final int codeBytes;
    final int recordLen;
    final int planeBytes;

    /**
     * Bytes of coarse code per vector: the thermometer planes concatenated, and the Hamming row
     * stride.
     */
    final int coarseBytes;

    /**
     * Record field offsets, derived from {@code codeBytes} once at construction and invariant for
     * the field's lifetime. {@code correctionBase} is the fixed part of the correction run's
     * offset, so a hot loop indexes correction {@code k} as {@code correctionBase + k *
     * Float.BYTES}. {@link CodeRecord} stays the single authority for the layout.
     */
    final int docIdOffset;

    final int primaryCellOffset;
    final int correctionBase;

    /**
     * Whether this field's fine tier wants the FLAT staging layout rather than {@code byte[][]}
     * rows.
     *
     * <p>Decided from whether the tier OVERRIDES {@code scoreBulkStrided}, rather than from an
     * encoding-id list: a tier that inherits the default would be handed a flat buffer only for the
     * default to copy it back out into rows, which is a second copy of the whole shortlist.
     */
    final boolean wantsStrided;

    /** Stride of the staged flat buffer: one whole record. */
    final int stagedStride;

    FieldEntry(ChecksumIndexInput meta, FieldInfo fieldInfo) throws IOException {
      encodingId = meta.readByte();
      similarity = VectorSimilarityFunction.values()[meta.readInt()];
      dim = meta.readVInt();
      nlist = meta.readVInt();
      count = meta.readVInt();
      rotationSeed = meta.readLong();
      nprobe = meta.readVInt();
      spillBits = meta.readVInt();
      // The grids the codes were packed on, verified rather than merely read.
      final float persistedCoarseClip = Float.intBitsToFloat(meta.readInt());
      final int persistedPlanes = meta.readVInt();
      if (persistedPlanes != Nitrox2.PLANES) {
        throw new CorruptIndexException(
            "coarse plane count mismatch: segment was written with "
                + persistedPlanes
                + " plane(s) but this build uses "
                + Nitrox2.PLANES
                + " (-Divfaster.coarseLevels="
                + Nitrox2.LEVELS
                + "). The plane count fixes the coarse code's length, so reading across it would"
                + " misinterpret every record, not merely mis-score one.",
            meta);
      }
      if (persistedCoarseClip != Nitrox2.CLIP_SIGMA) {
        throw new CorruptIndexException(
            "coarse grid mismatch: segment was written with clip "
                + persistedCoarseClip
                + " but this build uses "
                + Nitrox2.CLIP_SIGMA
                + ". The codes cannot be scored on a different grid than they were packed on.",
            meta);
      }
      centroidsOffset = meta.readVLong();
      centroidsLength = meta.readVLong();
      meanOffset = meta.readVLong();
      meanLength = meta.readVLong();
      codeTableOffset = meta.readVLong();
      codeTableLength = meta.readVLong();
      coarseOffset = meta.readVLong();
      coarseLength = meta.readVLong();
      graphOffset = meta.readVLong();
      graphLength = meta.readVLong();
      ordToSlotOffset = meta.readVLong();
      ordToSlotLength = meta.readVLong();
      rawOffset = meta.readVLong();
      rawLength = meta.readVLong();
      postingOffsetsDataStart = meta.readVLong();
      postingOffsetsMeta =
          nlist > 0
              ? DirectMonotonicReader.loadMeta(
                  meta, nlist + 1, IVFasterVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT)
              : null;

      quantizer = quantizerFor(encodingId);
      rotation = HadamardRotation.create(dim, rotationSeed);
      codeBytes = quantizer.codeBytes(dim);
      recordLen = CodeRecord.length(codeBytes);
      planeBytes = Nitrox2.planeBytes(dim);
      coarseBytes = Nitrox2.bytesPerVector(dim);
      docIdOffset = CodeRecord.docIdOffset(codeBytes);
      primaryCellOffset = CodeRecord.primaryCellOffset(codeBytes);
      correctionBase = CodeRecord.correctionBase(codeBytes);
      wantsStrided = quantizer.wantsStridedStaging();
      stagedStride = recordLen;
    }

    /**
     * Resolves the fine tier from the PERSISTED encoding id, never from an ambient system property,
     * so an index built under one configuration reads correctly under any other.
     */
    private static FineQuantizer quantizerFor(byte id) {
      if (id == Int8Quantizer.ENCODING_ID) {
        return new Int8Quantizer();
      }
      if (id == Fp32Quantizer.ENCODING_ID) {
        return new Fp32Quantizer();
      }
      throw new IllegalStateException("unknown fine encoding id: " + id);
    }
  }

  /** Per-field views into the data file, built lazily on first use and reused thereafter. */
  private static final class FieldViews {
    final RandomAccessInput centroids;
    final RandomAccessInput codeTable;
    final RandomAccessInput coarse;

    /**
     * Mapped view of the code table, for the per-slot doc-id read in the dedup.
     *
     * <p>The dedup reads one doc id per pooled slot, and reaching it through {@code
     * RandomAccessInput.readInt} goes via {@code VarHandleGuards} to {@code VarHandleSegmentAsInts}
     * to {@code ScopedMemoryAccess}, which re-checks segment liveness and bounds on EVERY int.
     * Reading the segment directly is one unaligned int load.
     */
    final java.lang.foreign.MemorySegment codeTableSeg;

    /**
     * Memory-mapped view of the coarse code section, resolved once at open rather than per query.
     *
     * <p>Resolved once because the section does not move. The coarse scan reads it through the
     * Vector API, which bounds-checks once per load rather than per int, so there is no per-int
     * guard here to remove; the per-int cost is in the dedup, see {@link #codeTableSeg}.
     *
     * <p>Null when the directory cannot supply a mapping for the WHOLE section, which for an {@code
     * MMapDirectory} means any index whose data file crosses a chunk boundary; see {@link
     * #coarseAccess} for what the scan falls back to then.
     */
    final java.lang.foreign.MemorySegment coarseSeg;

    /**
     * The coarse section as a source of per-run mappings, for when {@link #coarseSeg} is null.
     *
     * <p>WHY THIS EXISTS. {@code MemorySegmentAccessInput.segmentSliceOrNull} can only return a
     * segment for a range that lies within ONE mmap chunk, and {@code
     * MMapDirectory.DEFAULT_MAX_CHUNK_SIZE} is 16 GiB, so on any index whose data file exceeds that
     * the whole-section request fails and {@link #coarseSeg} is null. Asking per PROBED RUN instead
     * almost always succeeds: a run is one cell, {@code rows * coarseBytes}, which is some 150 KB
     * at ten million documents, so only a run that happens to straddle a chunk boundary misses —
     * one cell in the index, not all of them.
     *
     * <p>This matters because the difference is not a bounds check. With a segment the scan runs
     * {@link HammingKernel#bulkDistances} over the run; without one it reads each slot's code into
     * heap and calls the scalar {@code VectorUtil.xorBitCount}, measured at 3.4x the per-slot cost
     * on a ten-million-document index. A silent 3.4x that engages purely on index size is worse
     * than a slow path, since nothing in the recall or the API reports it.
     *
     * <p>Null only when the directory is not memory-mapped at all, where the scalar path is the
     * only path.
     */
    final org.apache.lucene.store.MemorySegmentAccessInput coarseAccess;

    /**
     * Cell {@code c}'s slot run is {@code [cellStart[c], cellStart[c + 1])}: the persisted posting
     * directory, decoded into heap at open.
     *
     * <p>In heap rather than read through {@code DirectMonotonicReader} per query for two reasons.
     * It is one int per cell, so it is small. And on a directory whose inputs are not natively
     * random-access, {@code randomAccessSlice} falls back to a stateful seek-then-read adapter that
     * is not safe under concurrent queries; the codec reads it once here instead.
     */
    final int[] cellStart;

    /** Raw FP32 vectors in ordinal order, or {@code null} when {@code rawLength == 0}. */
    final RandomAccessInput raw;

    final float[] mean;

    // ---- search-only state, built on the first query; see ensureSearchState ----

    private final FieldEntry entry;
    private final IndexInput data;
    private volatile boolean searchReady;
    float[][] centroidVectors;
    CentroidGraph graph;

    /**
     * The centroid tier, rebuilt over the persisted centroids on the first query.
     *
     * <p>Encoding the centroids is {@code O(nlist * dim)} once per field, which buys every cell
     * comparison a fine-tier score in place of a float dot.
     */
    CentroidCodes codes;

    /** The identity {@code 0..nlist-1}, the candidate list for ranking every cell at once. */
    int[] allCells;

    /** Vector ordinal -> primary code-table slot, in document order. Read once, at open. */
    final int[] ordToSlot;

    /**
     * Vector ordinal -> document id, in document order.
     *
     * <p>Derived from the slot map at open rather than persisted separately: one int read per
     * ordinal off an already-open table. {@code ordToDoc} is declared not to throw, so it cannot do
     * I/O when called.
     */
    final int[] ordToDoc;

    /**
     * Slot -> document id, dense over every slot including spilled copies. See the build at open.
     */
    final int[] slotDoc;

    /**
     * Opens every view for one field, and reads at open what both a query and a merge need; what
     * only a query needs waits for the first query, see {@link #ensureSearchState}.
     *
     * <p>Cell membership is resolved through the {@code postingOffsets} directory alone: slots are
     * in cell order, so cell {@code c} is the contiguous slot range {@code [postingOffsets.get(c),
     * get(c+1))} divided by {@code Integer.BYTES}. The slot ordinals are not stored, since the
     * range names them.
     *
     * <p>The raw section is optional: {@code rawLength == 0} leaves {@code getFloatVectorValues} on
     * its reconstruct path. When present it holds {@code count * dim} floats in ordinal order, so
     * {@code raw[ord]} IS the vector.
     *
     * <p>{@code ordToSlot} and {@code ordToDoc} are read once here, since {@code
     * getFloatVectorValues} would otherwise rebuild the mapping by scanning and sorting the whole
     * index on every call.
     *
     * <p>{@code slotDoc} is a dense column rather than an in-record read: the dedup reads one docId
     * per pooled slot, and in-record those are scattered cache lines the rerank does not reuse,
     * while a separate {@code int[]} at 4 B per slot is near-sequential over a small array. It is
     * rebuilt here rather than persisted, since it is one linear pass and no format change.
     *
     * <p>The centroid matrix, its {@link CentroidCodes} (given the field's configured fine
     * quantizer so centroid reranking is symmetric with document reranking) and the centroid graph
     * are query-only, and are what a merge would otherwise pay for once per source segment; see
     * {@link #centroids()}.
     */
    FieldViews(FieldEntry e, IndexInput data) throws IOException {
      centroids = data.randomAccessSlice(e.centroidsOffset, e.centroidsLength);
      codeTable = data.randomAccessSlice(e.codeTableOffset, e.codeTableLength);
      coarse = data.randomAccessSlice(e.coarseOffset, e.coarseLength);
      codeTableSeg = segmentOf(codeTable, e.codeTableLength);
      coarseSeg = segmentOf(coarse, e.coarseLength);
      coarseAccess =
          coarse instanceof org.apache.lucene.store.MemorySegmentAccessInput msai ? msai : null;
      raw = e.rawLength > 0 ? data.randomAccessSlice(e.rawOffset, e.rawLength) : null;
      cellStart = new int[e.nlist + 1];
      if (e.postingOffsetsMeta != null) {
        final DirectMonotonicReader postingOffsets =
            DirectMonotonicReader.getInstance(
                e.postingOffsetsMeta,
                data.randomAccessSlice(
                    e.postingOffsetsDataStart, data.length() - e.postingOffsetsDataStart));
        for (int c = 0; c <= e.nlist; c++) {
          cellStart[c] = (int) (postingOffsets.get(c) / Integer.BYTES);
        }
      }
      if (e.meanLength > 0) {
        mean = new float[e.dim];
        final RandomAccessInput m = data.randomAccessSlice(e.meanOffset, e.meanLength);
        for (int d = 0; d < e.dim; d++) {
          mean[d] = Float.intBitsToFloat(m.readInt((long) d * Float.BYTES));
        }
      } else {
        mean = null;
      }
      ordToSlot = new int[e.count];
      ordToDoc = new int[e.count];
      if (e.ordToSlotLength > 0) {
        final RandomAccessInput in = data.randomAccessSlice(e.ordToSlotOffset, e.ordToSlotLength);
        for (int i = 0; i < e.count; i++) {
          ordToSlot[i] = in.readInt((long) i * Integer.BYTES);
          ordToDoc[i] = codeTable.readInt((long) ordToSlot[i] * e.recordLen + e.docIdOffset);
        }
      }
      final long slotCount = e.codeTableLength / e.recordLen;
      slotDoc = new int[(int) slotCount];
      {
        final int docIdOff = e.docIdOffset;
        if (codeTableSeg != null) {
          for (int s = 0; s < slotCount; s++) {
            slotDoc[s] =
                codeTableSeg.get(
                    java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED,
                    (long) s * e.recordLen + docIdOff);
          }
        } else {
          for (int s = 0; s < slotCount; s++) {
            slotDoc[s] = codeTable.readInt((long) s * e.recordLen + docIdOff);
          }
        }
      }
      // The posting-list contract the filtered path reads a cell by; see CellPostings. Verified
      // here, where every slot is being read anyway, so a violation fails at open rather than as
      // a silently incomplete intersection.
      {
        for (int c = 0; c < e.nlist; c++) {
          final int from = cellStart[c];
          final int to = cellStart[c + 1];
          for (int s = from + 1; s < to; s++) {
            if (slotDoc[s] <= slotDoc[s - 1]) {
              throw new CorruptIndexException(
                  "cell " + c + " is not in ascending doc-id order at slot " + s, data);
            }
          }
        }
      }
      // Ordinal order is doc order: the sparse iterator and the exact filtered path both rely on
      // it, so an index-sorted segment written without the writer's canonical order is refused.
      for (int i = 1; i < e.count; i++) {
        if (ordToDoc[i] <= ordToDoc[i - 1]) {
          throw new CorruptIndexException(
              "vector ordinals are not in ascending doc-id order at ordinal " + i, data);
        }
      }
      this.entry = e;
      this.data = data;
    }

    /**
     * A REBASED mapping of one run of {@code rows} consecutive slots' coarse codes, for when {@link
     * #coarseSeg} is null; the run starts at offset 0. Null when no mapping covers the run.
     *
     * <p>ONLY FOR THE NO-WHOLE-SECTION CASE. Where {@link #coarseSeg} exists a caller must pass the
     * ABSOLUTE offset into it and not come here, because slicing would allocate a segment per
     * probed cell on a path that otherwise allocates nothing. See {@link #coarseAccess} for why the
     * case this serves exists at all.
     *
     * <p>One slice per RUN, not per slot, so once per probed cell — and only on indexes big enough
     * that the alternative is the scalar per-slot read this exists to avoid.
     */
    java.lang.foreign.MemorySegment coarseRun(int slotBase, int rows, int coarseBytes) {
      if (coarseAccess == null) {
        return null;
      }
      try {
        return coarseAccess.segmentSliceOrNull(
            (long) slotBase * coarseBytes, (long) rows * coarseBytes);
      } catch (IOException _) {
        return null;
      }
    }

    /**
     * Hamming distance from {@code qCode} to each of {@code rows} consecutive slots' coarse codes,
     * into {@code out[0..rows)}, through the kernel over a mapping of the run.
     *
     * <p>Returns false, having written nothing, when no mapping covers the run and the caller must
     * read the codes itself. Returns true for an EMPTY run, which also writes nothing, so true
     * means "the run is scored" rather than "out was touched". The read is left to the caller
     * rather than done here because callers do not share an input: a long-lived scorer reads
     * through its own clone of the data file, since {@code randomAccessSlice} on a directory that
     * is not natively random-access is a stateful seek-then-read adapter that two concurrent
     * readers cannot share.
     *
     * @return whether the run was scored
     */
    boolean coarseDistances(
        HammingKernel hamming, byte[] qCode, int slotBase, int rows, int coarseBytes, int[] out) {
      if (rows == 0) {
        return true;
      }
      if (coarseSeg != null) {
        // Offset into the whole section, so no per-cell slice is allocated.
        hamming.bulkDistances(
            qCode, coarseSeg, (long) slotBase * coarseBytes, coarseBytes, rows, out);
        return true;
      }
      final java.lang.foreign.MemorySegment run = coarseRun(slotBase, rows, coarseBytes);
      if (run == null) {
        return false;
      }
      hamming.bulkDistances(qCode, run, 0L, coarseBytes, rows, out);
      return true;
    }

    /**
     * The centroid matrix, read into heap on first use.
     *
     * <p>Small ({@code nlist x dim} floats) and needed by every query, but NOT by a merge that only
     * copies this segment's records, and a merge opens every source segment: at {@code nlist =
     * 8000, dim = 1024} the matrix, its codes and its graph are some 50 MB per segment, which
     * across fifty flush segments is more heap than the merge itself needs.
     */
    synchronized float[][] centroids() throws IOException {
      if (centroidVectors == null) {
        final FieldEntry e = entry;
        final float[][] m = new float[e.nlist][];
        for (int c = 0; c < e.nlist; c++) {
          m[c] = new float[e.dim];
          for (int d = 0; d < e.dim; d++) {
            m[c][d] = Float.intBitsToFloat(centroids.readInt(((long) c * e.dim + d) * Float.BYTES));
          }
        }
        centroidVectors = m;
      }
      return centroidVectors;
    }

    /**
     * Builds what only a query reads (the centroid matrix, its fine codes, the centroid graph) on
     * the first query, so that opening a segment to merge it costs its ordinal map alone.
     */
    void ensureSearchState() throws IOException {
      if (searchReady) {
        return;
      }
      synchronized (this) {
        if (searchReady) {
          return;
        }
        final FieldEntry e = entry;
        final float[][] m = centroids();
        allCells = new int[e.nlist];
        for (int c = 0; c < e.nlist; c++) {
          allCells[c] = c;
        }
        graph =
            e.graphLength > 0
                ? CentroidGraph.read(
                    data.randomAccessSlice(e.graphOffset, e.graphLength), e.dim, e.graphLength)
                : null;
        codes = new CentroidCodes(m, e.dim, e.similarity, e.quantizer);
        searchReady = true;
      }
    }
  }

  private final Map<String, FieldViews> views = new HashMap<>();

  private synchronized FieldViews viewsFor(String field, FieldEntry e) throws IOException {
    FieldViews v = views.get(field);
    if (v == null) {
      v = new FieldViews(e, data);
      views.put(field, v);
    }
    return v;
  }

  /**
   * Searches one field: rotate and quantize the query, select cells, coarse-scan them, rerank.
   *
   * <p>The query is normalized unconditionally, matching the writer, since every distance in this
   * codec assumes unit norm and an unnormalized query would be scored on a different footing than
   * the documents. It is then quantized ONCE into both tiers, the coarse code as one byte string
   * that the scan Hammings whole.
   *
   * <p>The scan reaches the shortlist by random access, which is its shape: a scattered set of
   * slots rather than a sequential walk.
   *
   * <p>A query filter is resolved before any cell is chosen and walked doc-at-a-time; see the class
   * javadoc and {@link #filteredScanAndRerank}. Live docs alone stay on the bulk scan.
   */
  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    final FieldEntry e = fields.get(field);
    if (e == null || e.nlist == 0 || e.count == 0) {
      return;
    }
    final FieldViews v = viewsFor(field, e);
    v.ensureSearchState();
    final int dim = e.dim;

    // 1. Normalize and rotate.
    final float[] query = ArrayUtil.copyOfSubArray(target, 0, dim);
    org.apache.lucene.util.VectorUtil.l2normalize(query);
    final float[] rotated = new float[dim];
    e.rotation.rotate(query, rotated);

    // 2. Quantize once into both tiers.
    final byte[] qCode = new byte[e.coarseBytes];
    Nitrox2.encode(rotated, dim, qCode, 0);
    final FineQuantizer.QueryState fine =
        e.quantizer.prepareQuery(rotated, dim, v.mean, e.similarity);

    // 3. Resolve the filter BEFORE selecting cells; see the class javadoc. A BitSet is a query
    // filter, which AbstractKnnVectorQuery has already materialized; anything else is live docs.
    final Bits accept = acceptDocs == null ? null : acceptDocs.bits();
    final int probe = Math.min(NPROBE_OVERRIDE > 0 ? NPROBE_OVERRIDE : e.nprobe, e.nlist);
    if (accept instanceof BitSet == false) {
      // 4 + 5. Coarse-scan the selected cells, then rerank the shortlist.
      final int[] selected = selectCells(e, v, rotated, qCode, probe, true);
      scanAndRerank(e, v, qCode, fine, selected, knnCollector, accept);
      return;
    }
    // Cardinality first: AcceptDocs forbids cost() after iterator().
    final int cost = acceptDocs.cost();
    if (cost <= exactFilterBound(e)) {
      exactFilterQueries.incrementAndGet();
      rerankFilteredDocs(e, v, fine, acceptDocs.iterator(), knnCollector);
      return;
    }
    // No quality prune on the initial selection: the walk below widens on its own terms.
    final int[] initial = selectCells(e, v, rotated, qCode, probe, false);
    filteredScanAndRerank(e, v, rotated, qCode, fine, initial, (BitSet) accept, knnCollector);
  }

  /**
   * Keeps the {@code cap} candidates with the smallest COARSE distance, in place, and returns
   * {@code cap}.
   *
   * <p>Selection by THRESHOLD over a histogram rather than by sorting: coarse distances are small
   * non-negative ints, bounded by {@code coarseBytes*8}, so the cut is found by counting. Order
   * within the kept set does not matter, since {@link #rerankCells} re-ranks it exactly and sorts
   * its own prefix.
   *
   * <p>Ties at the cut are admitted up to {@code cap} and no further, so the kept count is exact
   * rather than data-dependent.
   */
  private static int narrowByCoarse(int[] cand, int[] dist, int got, int cap) {
    int maxD = 0;
    for (int i = 0; i < got; i++) {
      if (dist[i] > maxD) {
        maxD = dist[i];
      }
    }
    final int[] hist = new int[maxD + 2];
    for (int i = 0; i < got; i++) {
      hist[dist[i]]++;
    }
    int below = 0;
    int thr = 0;
    while (thr <= maxD && below + hist[thr] <= cap) {
      below += hist[thr];
      thr++;
    }
    // Everything strictly below thr is kept; ties AT thr fill the remainder.
    int n = 0;
    int ties = cap - below;
    for (int i = 0; i < got && n < cap; i++) {
      final int d = dist[i];
      if (d < thr || (d == thr && ties-- > 0)) {
        cand[n] = cand[i];
        dist[n] = d;
        n++;
      }
    }
    return n;
  }

  /**
   * How many candidates the fine tier verifies, as a multiple of {@code probe}, and a floor.
   *
   * <p>The coarse code is a WEAK ranker, which is why an exact verify exists at all, so this stays
   * several times {@code probe}: it drops the hopeless tail of the visited set and leaves the cell
   * choice to the exact stage. Set {@code ivfaster.verifyMultiplier=0} to verify everything, which
   * is the reference for this trade.
   */
  private static final int VERIFY_MULTIPLIER = Integer.getInteger("ivfaster.verifyMultiplier", 2);

  private static final int VERIFY_MIN = Integer.getInteger("ivfaster.verifyMin", 64);

  /**
   * Chooses the {@code probe} nearest cells.
   *
   * <p>Normally a graph descent. The exact scan of the centroid matrix is the fallback for a field
   * with no graph ({@code nlist == 1}) and, under {@code ivfaster.flatSelect}, the reference
   * behaviour the descent is checked against. This is the SEARCH path only: index-time routing is
   * always the exact scan and never consults the graph.
   *
   * <p>The candidates are ordered by PARTIAL SELECTION over the first {@code probe}, since only
   * that prefix is ever read, by the adaptive-margin scan and the copy into {@code out}. A full
   * sort would order all {@code got} visited candidates, which is several times {@code ef} by
   * design. Selection rather than a heap because {@code prefix} is small relative to {@code got}.
   *
   * <p>Tie order is identical to a stable sort's: scanning upward and taking strictly-less keeps
   * the earliest among equals. Ties are real here because the fine tier quantizes, and a different
   * tie winner would silently probe a different cell.
   *
   * <p>On the graph path the descent runs on the coarse code and the survivors are RE-RANKED
   * exactly, which is what makes the selection as good as a full scan's at {@code ef} comparisons
   * rather than {@code nlist}. The candidate buffer is sized for the VISITED set rather than the
   * beam, since {@code search} returns every node it scored, several times {@code ef}, because the
   * beam's evictions are coarse-code decisions the exact rerank should overrule. It is capped at
   * {@code nlist} and reused per thread, with both arrays consumed here before {@code
   * scanAndRerank} touches the same scratch.
   *
   * <p>The visited set is then narrowed by the free coarse distance the descent already computed
   * for its beam. The coarse code is a WEAK ranker, so the prefix stays several times {@code
   * probe}: it drops the hopeless tail without letting coarse pick the cells.
   */
  private int[] selectCells(
      FieldEntry e, FieldViews v, float[] rotated, byte[] qCode, int probe, boolean applyMargin) {
    if (v.graph != null && FLAT_SELECT == false) {
      final int ef = Math.max(CentroidGraph.MIN_EF, probe * CentroidGraph.EF_MULTIPLIER);
      final ScanScratch scan = SCAN_SCRATCH.get();
      final int[] candidates = scan.candidates(e.nlist);
      final int[] candDist = scan.candDist();
      final int got = v.graph.search(qCode, ef, candidates, candDist);
      // Narrow by the free coarse distance before the fine verify; see the javadoc.
      final int verifyCap =
          VERIFY_MULTIPLIER <= 0
              ? got
              : Math.min(got, Math.max(VERIFY_MIN, probe * VERIFY_MULTIPLIER));
      final int kept = verifyCap < got ? narrowByCoarse(candidates, candDist, got, verifyCap) : got;
      return rerankCells(e, v, rotated, candidates, kept, probe, applyMargin);
    }
    flatSelects.incrementAndGet();
    // Exact scan: a max-heap of the best `probe` cells, keyed on distance.
    final float[] bestDist = new float[probe];
    final int[] bestCell = new int[probe];
    java.util.Arrays.fill(bestDist, Float.POSITIVE_INFINITY);
    java.util.Arrays.fill(bestCell, -1);
    int filled = 0;
    for (int c = 0; c < e.nlist; c++) {
      final float[] cent = v.centroidVectors[c];
      double dot = 0;
      for (int d = 0; d < e.dim; d++) {
        dot += (double) rotated[d] * cent[d];
      }
      // NO ||c||^2 TERM, and no per-similarity branch. Centroids are unit norm for every
      // similarity (see Clustering#normalize), so ||q - c||^2 = ||q||^2 + 1 - 2*dot and -dot
      // induces
      // the same order. Accumulating ||c||^2 here would recompute a per-centroid constant dim times
      // and double this loop. The unit-norm invariant is load-bearing: were a centroid's norm to
      // vary
      // per cell, -dot would rank partly by norm and partly by proximity.
      final float dist = (float) -dot;
      if (filled == probe && dist >= bestDist[probe - 1]) {
        continue;
      }
      int pos = filled < probe ? filled : probe - 1;
      while (pos > 0 && bestDist[pos - 1] > dist) {
        bestDist[pos] = bestDist[pos - 1];
        bestCell[pos] = bestCell[pos - 1];
        pos--;
      }
      bestDist[pos] = dist;
      bestCell[pos] = c;
      if (filled < probe) {
        filled++;
      }
    }
    final int[] out = new int[filled];
    System.arraycopy(bestCell, 0, out, 0, filled);
    return out;
  }

  /**
   * Exactly re-ranks the graph's candidate cells and keeps the nearest {@code probe}.
   *
   * <p>The verify half of the same cascade the router uses: a cheap code narrows the field and an
   * exact comparison orders what survives, which is what lets the descent be approximate while the
   * cell choice is not.
   *
   * <p>Ranked with the FINE TIER. A float dot here would be {@code dim} multiplies per candidate
   * plus the centroid's own norm recomputed per candidate for a value that is 1 under unit norm,
   * and {@link CentroidCodes} already holds every centroid's fine code and performs this comparison
   * for routing.
   *
   * <p>The engagement counter reports {@code got}, the graph's VISITED set, rather than {@code
   * probe}, since the descent's evictions are coarse-code decisions the exact rerank should
   * overrule. So the per-query centroid cost scales with the visited set, at one fine code of
   * bandwidth each.
   *
   * <p>The prefix is ordered by a ROTATE rather than a swap: a swap would move {@code order[i]} to
   * position {@code bestJ}, reordering the unsorted tail and changing which of two equal candidates
   * a later pass finds first, while shifting preserves the tail's relative order so the prefix
   * matches a stable sort exactly.
   *
   * <p>ADAPTIVE NPROBE trims the tail on quality. The distances are already exact and already
   * sorted, so it costs one compare per kept cell, and every cell dropped is a whole posting list
   * that is never loaded or scored; see {@link #NPROBE_MARGIN}. The candidates are nearest-first,
   * so the first cell that fails the bound ends the run, and cell 0 is always kept, since a query
   * must probe somewhere.
   *
   * <p>The bound is {@code d1 * margin} on the negated dot, where {@code d1 <= 0} and nearer is
   * MORE negative, so a margin below 1 shrinks the magnitude toward zero and admits cells further
   * than the nearest, making the margin a RETENTION fraction. Dividing instead would make the bound
   * more negative than {@code d1} and reject everything including ties.
   */
  private int[] rerankCells(
      FieldEntry e,
      FieldViews v,
      float[] rotated,
      int[] candidates,
      int got,
      int probe,
      boolean applyMargin) {
    final float[] dist = new float[got];
    final int[] order = new int[got];
    if (Boolean.getBoolean("ivfaster.reportEngagement")) {
      verifiedCentroids.addAndGet(got);
      verifyQueries.incrementAndGet();
    }
    v.codes.rankCandidates(rotated, candidates, got, dist);
    for (int i = 0; i < got; i++) {
      order[i] = i;
    }

    // Partial selection over the first `probe`, not a full sort; see the method javadoc.
    final int prefix = Math.min(probe, got);
    for (int i = 0; i < prefix; i++) {
      int bestJ = i;
      float bestD = dist[order[i]];
      for (int j = i + 1; j < got; j++) {
        final float d = dist[order[j]];
        if (d < bestD) {
          bestD = d;
          bestJ = j;
        }
      }
      if (bestJ != i) {
        // A rotate rather than a swap; see the javadoc.
        final int chosen = order[bestJ];
        System.arraycopy(order, i, order, i + 1, bestJ - i);
        order[i] = chosen;
      }
    }

    int keep = prefix;
    // ADAPTIVE NPROBE; see the javadoc and NPROBE_MARGIN. Off under a filter, whose walk decides
    // its own width from what the filter admits.
    if (applyMargin && NPROBE_MARGIN != 1.0f && keep > 1) {
      final float d1 = dist[order[0]];
      final float bound = d1 * NPROBE_MARGIN;
      int k = 1;
      while (k < keep && dist[order[k]] <= bound) {
        k++;
      }
      keep = k;
    }
    final int[] out = new int[keep];
    for (int i = 0; i < keep; i++) {
      out[i] = candidates[order[i]];
    }
    return out;
  }

  /**
   * Coarse-scans the selected cells and reranks the survivors.
   *
   * <p>The coarse distance is a small non-negative integer, the summed per-dimension level distance
   * bounded by {@code coarseBytes*8}, so selection is a COUNTING SORT rather than a heap: histogram
   * the distances, walk down to the threshold that admits about the right number, then compact. No
   * comparisons and no heap churn over the tens of thousands of candidates a probe set contains.
   *
   * <p>Under spill a document holds several slots, so this selects a POOL of slots that provably
   * contains enough distinct documents and dedups within it; see the class javadoc. Cell bounds
   * come from the heap directory decoded at open; see {@code FieldViews.cellStart}.
   *
   * <p>THE ADMITTED ARRAY IS SIZED FOR EVERY CANDIDATE, even under streaming admission. A smaller
   * array with a drop-when-full guard is incorrect: candidates keep counting toward the admission
   * target after the array fills, and the dropped ones are whichever arrived late rather than the
   * worst, which under-fills the shortlist. What streaming admission saves is the store and the
   * histogram increment for the majority that lose, not the array.
   *
   * <p>The admission threshold is in DISTANCE units and indexes {@code hist}, so it starts at the
   * largest possible distance, admitting everything, rather than at {@code MAX_VALUE}, which would
   * index out of bounds on the first tightening read. Its mode branch is hoisted OUT of the row
   * loop, since testing a static final flag per row does not reliably fold away, so each mode gets
   * a specialized body; same shape as {@code PanamaHammingKernel}'s native split.
   *
   * <p>THE FINAL CUT is distinct from that admission threshold: the threshold tightens during the
   * scan to decide what gets stored, while the cut is taken over the histogram afterwards. {@code
   * need} is relative to EVERY candidate scanned, known from the posting lengths before the scan,
   * rather than to the admitted subset, since the pruned count would shrink the shortlist below
   * target. The cut is valid under streaming admission because {@code hist} is exact for every
   * bucket at or below the admission threshold and the cut can never exceed it: the threshold is
   * defined as "the buckets strictly below satisfy the target", so the cumulative sum reaches
   * {@code need} at or before it.
   *
   * <p>The pool is then distance-ordered by counting sort, so the dedup keeps the BEST copy of each
   * document; see {@link #cutAndOrderPool}. The shortlist is staged and scored by {@link
   * #rerankAndCollect}.
   *
   * <p>{@code liveDocs} is the segment's live-doc bits or null, applied at the dedup. A query
   * filter never reaches this method; see {@link #filteredScanAndRerank}.
   */
  private void scanAndRerank(
      FieldEntry e,
      FieldViews v,
      byte[] qCode,
      FineQuantizer.QueryState fine,
      int[] selected,
      KnnCollector collector,
      Bits liveDocs)
      throws IOException {

    final HammingKernel hamming = HammingKernel.get();
    final int coarseBytes = e.coarseBytes;

    final int histLen = (coarseBytes << 3) + 2;
    final ScanScratch sc = SCAN_SCRATCH.get();
    final int[] hist = sc.hist(histLen);

    final int fanout = 1 + e.spillBits;
    final int poolTarget = BRUTE_N * fanout;

    // Cell bounds decoded once, and every run hinted; see resolveCells.
    final int nCells = resolveCells(e, v, selected, selected.length, sc, 0);
    final int[] cellBase = sc.cellBase;
    final int[] cellRows = sc.cellRows;
    int totalCandidates = 0;
    int maxRows = 0;
    for (int ci = 0; ci < nCells; ci++) {
      final int rows = cellRows[ci];
      totalCandidates += rows;
      if (rows > maxRows) {
        maxRows = rows;
      }
    }
    if (totalCandidates == 0) {
      return;
    }

    // Opt-in instrumentation; see COUNT_SCAN.
    if (COUNT_SCAN) {
      scannedDocs.addAndGet(totalCandidates);
      scanQueries.incrementAndGet();
      probedCells.addAndGet(nCells);
    }

    // Distinct-document diagnostic; see MEASURE_SPILL.
    if (MEASURE_SPILL) {
      final java.util.HashSet<Integer> uniq = new java.util.HashSet<>(totalCandidates * 2);
      final int docIdOffDiag = e.docIdOffset;
      // Reads the SAME hoisted bounds the scan will, so the measurement cannot drift from it.
      for (int ci = 0; ci < nCells; ci++) {
        final int base = cellBase[ci];
        final int rowsDiag = cellRows[ci];
        for (int r = 0; r < rowsDiag; r++) {
          final long off = (long) (base + r) * e.recordLen + docIdOffDiag;
          uniq.add(
              v.codeTableSeg != null
                  ? v.codeTableSeg.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, off)
                  : v.codeTable.readInt(off));
        }
      }
      uniqueDocs.addAndGet(uniq.size());
    }
    // Sized for EVERY candidate; see the javadoc.
    final long[] packed = sc.packed(totalCandidates);
    int m = 0;
    // Admission threshold, in DISTANCE units and an index into hist; see the javadoc.
    int thr = histLen - 1;
    int admitted = 0;

    // Read once per scan, not per slot: see FieldViews.coarseDistances.
    final byte[] coarseRec = sc.coarseRec(coarseBytes);
    int[] rowDist = sc.rowDist;
    int[] filterIdx = sc.filterIdx;

    // Sized ONCE from the widest cell, rather than re-tested per cell inside the loop.
    if (rowDist.length < maxRows) {
      rowDist = new int[ArrayUtil.oversize(maxRows, Integer.BYTES)];
      filterIdx = new int[rowDist.length];
      sc.rowDist = rowDist;
      sc.filterIdx = filterIdx;
    }

    for (int ci = 0; ci < nCells; ci++) {
      // Cell-order layout: this cell's slots are one contiguous run, so one sequential block.
      final int slotBase = cellBase[ci];
      final int rows = cellRows[ci];
      if (v.coarseDistances(hamming, qCode, slotBase, rows, coarseBytes, rowDist) == false) {
        // No mapping covers this run: fall back to per-row reads of the whole code.
        for (int r = 0; r < rows; r++) {
          v.coarse.readBytes((long) (slotBase + r) * coarseBytes, coarseRec, 0, coarseBytes);
          rowDist[r] = org.apache.lucene.util.VectorUtil.xorBitCount(qCode, coarseRec);
        }
      }
      // The mode branch is OUTSIDE the row loop; see the javadoc.
      if (STREAM_SELECT && SIMD_ADMIT) {
        // Vector admission, re-tightening thr once per sub-block; see HammingKernel.filterAtMost.
        for (int base = 0; base < rows; base += SIMD_ADMIT_BLOCK) {
          final int len = Math.min(SIMD_ADMIT_BLOCK, rows - base);
          final int k = hamming.filterAtMost(rowDist, base, len, thr, filterIdx);
          for (int j = 0; j < k; j++) {
            final int r = base + filterIdx[j];
            final int dist = rowDist[r];
            hist[dist]++;
            packed[m++] = ((long) dist << 32) | (slotBase + r);
            admitted++;
          }
          if (admitted > poolTarget) {
            while (thr > 0 && admitted - hist[thr] >= poolTarget) {
              admitted -= hist[thr];
              thr--;
            }
          }
        }
      } else if (STREAM_SELECT) {
        // Scalar streaming admission, unrolled by four; see STREAM_SELECT and the javadoc.
        int r = 0;
        for (final int upper = rows - 4; r <= upper; r += 4) {
          final int d0 = rowDist[r];
          final int d1 = rowDist[r + 1];
          final int d2 = rowDist[r + 2];
          final int d3 = rowDist[r + 3];
          final int t = thr;
          // Branch-free min of the four, then ONE branch, since an && chain would be four.
          if (Math.min(Math.min(d0, d1), Math.min(d2, d3)) > t) {
            continue;
          }
          if (d0 <= t) {
            hist[d0]++;
            packed[m++] = ((long) d0 << 32) | (slotBase + r);
            admitted++;
          }
          if (d1 <= t) {
            hist[d1]++;
            packed[m++] = ((long) d1 << 32) | (slotBase + r + 1);
            admitted++;
          }
          if (d2 <= t) {
            hist[d2]++;
            packed[m++] = ((long) d2 << 32) | (slotBase + r + 2);
            admitted++;
          }
          if (d3 <= t) {
            hist[d3]++;
            packed[m++] = ((long) d3 << 32) | (slotBase + r + 3);
            admitted++;
          }
          // Tighten once per GROUP rather than per admitted row; thr never rises.
          if (admitted > poolTarget) {
            while (thr > 0 && admitted - hist[thr] >= poolTarget) {
              admitted -= hist[thr];
              thr--;
            }
          }
        }
        for (; r < rows; r++) {
          final int dist = rowDist[r];
          if (dist > thr) {
            continue;
          }
          hist[dist]++;
          packed[m++] = ((long) dist << 32) | (slotBase + r);
          admitted++;
          if (admitted > poolTarget) {
            while (thr > 0 && admitted - hist[thr] >= poolTarget) {
              admitted -= hist[thr];
              thr--;
            }
          }
        }
      } else {
        for (int r = 0; r < rows; r++) {
          final int dist = rowDist[r];
          packed[m++] = ((long) dist << 32) | (slotBase + r);
          hist[dist]++;
        }
      }
    }
    if (m == 0) {
      return;
    }

    // The final cut over the histogram, distinct from the admission threshold; see the javadoc.
    final int need = Math.min(poolTarget, totalCandidates);
    final int poolN = cutAndOrderPool(sc, packed, m, hist, histLen, need);

    // Keep the first BRUTE_N distinct documents in distance order.
    final int candCap = Math.min(BRUTE_N, poolN);
    final int[] cands = sc.cands(candCap);
    final int n = keepDistinctInOrder(sc.ordered, poolN, candCap, cands, v.slotDoc, liveDocs, sc);
    if (n == 0) {
      return;
    }
    rerankAndCollect(e, v, fine, cands, n, collector);
  }

  /**
   * Decodes the slot bounds of {@code n} cells into the scratch's {@code cellBase}/{@code
   * cellRows}, appending after the {@code from} cells already there, and hints every new run.
   *
   * <p>Empty cells and negative ids are skipped, so the returned count is of cells with rows.
   *
   * <p>Every probed run is hinted before any of it is scanned, so cold faults overlap; see {@link
   * #PREFETCH}. Placed at the first point the ranges are known, which is what buys the overlap.
   *
   * @return the total cell count after appending
   */
  private static int resolveCells(
      FieldEntry e, FieldViews v, int[] cells, int n, ScanScratch sc, int from) throws IOException {
    final int[] cellBase = sc.cellBase(from + n);
    final int[] cellRows = sc.cellRows(from + n);
    int nCells = from;
    for (int i = 0; i < n; i++) {
      final int c = cells[i];
      if (c < 0) {
        continue;
      }
      final int start = v.cellStart[c];
      final int rows = v.cellStart[c + 1] - start;
      if (rows == 0) {
        continue;
      }
      cellBase[nCells] = start;
      cellRows[nCells] = rows;
      nCells++;
    }
    if (PREFETCH) {
      final int coarseBytes = e.coarseBytes;
      final int recordLen = e.recordLen;
      for (int ci = from; ci < nCells; ci++) {
        final long slotBase = cellBase[ci];
        final int rows = cellRows[ci];
        v.coarse.prefetch(slotBase * coarseBytes, (long) rows * coarseBytes);
        v.codeTable.prefetch(slotBase * recordLen, (long) rows * recordLen);
      }
    }
    return nCells;
  }

  /**
   * Cuts the admitted slots to the {@code need} nearest by coarse distance and leaves them
   * distance-ordered in the scratch's {@code ordered}.
   *
   * <p>The cut is taken over the histogram AFTER the scan, distinct from the admission threshold
   * that tightened during it; see {@link #scanAndRerank}. Everything strictly under the cut is
   * taken, then ties up to {@code need}, so rerank cost is fixed. The pool is then distance-ordered
   * by counting sort bounded by the cut, since every pooled slot is at or below it and higher
   * buckets are empty, so the dedup that follows keeps the BEST copy of a document.
   *
   * @return the pool size
   */
  private static int cutAndOrderPool(
      ScanScratch sc, long[] packed, int m, int[] hist, int histLen, int need) {
    int below = 0;
    int selThr = 0;
    while (selThr < histLen && below + hist[selThr] < need) {
      below += hist[selThr];
      selThr++;
    }
    final long thrKey = (long) selThr << 32;

    final long[] pool = sc.pool(need);
    int poolN = 0;
    for (int i = 0; i < m && poolN < need; i++) {
      if (packed[i] < thrKey) {
        pool[poolN++] = packed[i];
      }
    }
    for (int i = 0; i < m && poolN < need; i++) {
      if ((packed[i] >>> 32) == selThr) {
        pool[poolN++] = packed[i];
      }
    }

    final int prefixLen = selThr + 1;
    final int[] prefix = sc.prefix(prefixLen + 1);
    for (int i = 0; i < poolN; i++) {
      prefix[(int) (pool[i] >>> 32)]++;
    }
    int acc = 0;
    for (int d = 0; d < prefixLen; d++) {
      final int cnt = prefix[d];
      prefix[d] = acc;
      acc += cnt;
    }
    final long[] ordered = sc.ordered(poolN);
    for (int i = 0; i < poolN; i++) {
      ordered[prefix[(int) (pool[i] >>> 32)]++] = pool[i];
    }
    return poolN;
  }

  /**
   * Stages and fine-scores {@code n} slots, collecting every score, and reports {@code n} visited.
   *
   * <p>THE RERANK IS BULK, so the fine tier's per-query setup is shared across the shortlist. Two
   * staging layouts, chosen by what the tier's kernel consumes: the bytes read are identical and
   * only the destination differs. FLAT is one constant-stride buffer, which a kernel scores in one
   * call; ROWS is {@code byte[][]} for record-direct kernels, where staging flat would add a second
   * copy.
   *
   * <p>Staging and scoring are BLOCKED, so an early stop skips STAGING and not merely scoring,
   * since staging copies the records and dominates the arithmetic. Blocks stay large because the
   * rerank is one kernel call per block.
   *
   * <p>The visited count is the number of DISTINCT DOCUMENTS fine-scored, on every path. {@code
   * AbstractKnnVectorQuery} falls back to an exact search once visited reaches the filter's
   * cardinality plus one, so counting slots scanned or cells probed, which a selective filter
   * drives far past its cardinality, would turn every such query into a brute-force one.
   */
  private void rerankAndCollect(
      FieldEntry e,
      FieldViews v,
      FineQuantizer.QueryState fine,
      int[] cands,
      int n,
      KnnCollector collector)
      throws IOException {
    final ScanScratch sc = SCAN_SCRATCH.get();
    final float[][] corrections = sc.corrections(n);
    final int[] docIds = sc.docIds;
    final float[] scores = sc.scores;

    // Blocked stage-and-score; see the javadoc.
    final int block = EARLY_TERM ? Math.min(n, Math.max(64, EARLY_TERM_BLOCK)) : n;
    int scored = 0;
    for (int start = 0; start < n; start += block) {
      final int len = Math.min(block, n - start);
      if (e.wantsStrided) {
        final byte[] flat = sc.flat(len, e.stagedStride);
        for (int i = 0; i < len; i++) {
          final int base = i * e.stagedStride;
          v.codeTable.readBytes((long) cands[start + i] * e.recordLen, flat, base, e.recordLen);
          docIds[i] = CodeRecord.readIntLE(flat, base + e.docIdOffset);
          for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
            corrections[i][k] =
                Float.intBitsToFloat(
                    CodeRecord.readIntLE(flat, base + e.correctionBase + k * Float.BYTES));
          }
        }
        fine.scoreBulkStrided(
            flat, len, e.stagedStride, CodeRecord.codeOffset(), corrections, scores, null);
      } else {
        final byte[][] records = sc.records(len, e.recordLen);
        for (int i = 0; i < len; i++) {
          final long off = (long) cands[start + i] * e.recordLen;
          v.codeTable.readBytes(off, records[i], 0, e.recordLen);
          docIds[i] = CodeRecord.readIntLE(records[i], e.docIdOffset);
          for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
            corrections[i][k] =
                Float.intBitsToFloat(
                    CodeRecord.readIntLE(records[i], e.correctionBase + k * Float.BYTES));
          }
        }
        fine.scoreBulk(records, len, CodeRecord.codeOffset(), corrections, scores);
      }
      // Read BEFORE collecting, so it is the k-th best as of the previous block.
      final float bar = EARLY_TERM ? collector.minCompetitiveSimilarity() : 0f;
      float blockBest = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < len; i++) {
        if (EARLY_TERM && scores[i] > blockBest) {
          blockBest = scores[i];
        }
        collector.collect(docIds[i], scores[i]);
      }
      scored += len;
      // A heuristic, hence off by default; see EARLY_TERM and EARLY_TERM_SLACK.
      if (EARLY_TERM && blockBest < bar - EARLY_TERM_SLACK && start + len < n) {
        break;
      }
    }
    collector.incVisitedCount(scored);
  }

  /**
   * Keeps the first {@code keep} DISTINCT document ids from a distance-ordered pool of slots.
   *
   * <p>Open-addressed set over document ids, sized to the keep target rather than the pool, since
   * only kept documents are ever inserted; see {@link #insertDistinct}.
   *
   * <p>The table is GENERATION-STAMPED and reused per thread: each entry carries the query
   * generation it was written in, so emptiness is "wrong generation" rather than a sentinel, which
   * means no per-query fill and no id value reserved. It is re-zeroed on generation wraparound, and
   * the stamp is never 0, so the test also holds on a fresh or grown table. Occupied therefore
   * means "written in THIS query", and probing stops at the first entry from an earlier one.
   *
   * <p>The table size is computed in LONG and range-checked, as in {@link ScanScratch#flat}: {@code
   * keep} comes from {@code -Divfaster.bruteN}, so a large enough value would make {@code
   * highestOneBit(keep) << 2} overflow int to a negative number and surface as a {@code
   * NegativeArraySizeException} far from the cause.
   */
  private static int keepDistinctInOrder(
      long[] pool,
      int poolSize,
      int keep,
      int[] out,
      int[] slotDoc,
      Bits liveDocs,
      ScanScratch sc) {
    final int cap = dedupCapacity(keep);
    final int mask = cap - 1;
    final int[] keys = sc.dedupKeys(cap);
    final int[] gens = sc.dedupGens(cap);
    final int stamp = sc.nextDedupGen();
    int n = 0;
    for (int i = 0; i < poolSize && n < keep; i++) {
      final int slot = (int) pool[i];
      // The hottest scalar read on the query path; see FieldViews.slotDoc.
      final int docId = slotDoc[slot];
      if (liveDocs != null && liveDocs.get(docId) == false) {
        continue;
      }
      if (insertDistinct(docId, keys, gens, stamp, mask)) {
        out[n++] = slot;
      }
    }
    return n;
  }

  /**
   * Dedup table size for {@code keep} distinct documents: a power of two at least four times it.
   */
  private static int dedupCapacity(int keep) {
    final long capL = (long) Integer.highestOneBit(Math.max(16, keep)) << 2;
    if (capL <= 0 || capL > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException(
          "rerank shortlist too large to dedup: keep="
              + keep
              + " needs a dedup table of "
              + capL
              + " entries. Lower -Divfaster.bruteN.");
    }
    return (int) capL;
  }

  /**
   * Inserts {@code docId} into the generation-stamped open-addressed table; false if present.
   *
   * <p>Ids are dense and sequential, so they are scrambled before masking or they clump into
   * consecutive buckets.
   */
  private static boolean insertDistinct(int docId, int[] keys, int[] gens, int stamp, int mask) {
    int h = (docId * 0x9E3779B9) >>> 1 & mask;
    while (gens[h] == stamp) {
      if (keys[h] == docId) {
        return false;
      }
      h = (h + 1) & mask;
    }
    gens[h] = stamp;
    keys[h] = docId;
    return true;
  }

  /**
   * The filtered query path: walks the filter doc-at-a-time against the probed cells, widening the
   * probe until enough accepted documents are in hand, then reranks the shortlist.
   *
   * <p>Cells arrive in rounds from a {@link CellRanker}, nearest-first, each round twice the size
   * of the last. THE CELLS LEAD: each probed run is walked in slot order and the filter's bit set
   * is tested per document, the way {@code BitSetConjunctionDISI} consumes a bit set that is the
   * costlier clause of a conjunction. The filter is always a materialized bit set on this path,
   * since {@code AbstractKnnVectorQuery} builds it before the codec runs. Letting the filter lead
   * instead, advancing a disjunction of the cells to each accepted document, was measured and
   * rejected: a cell's members are scattered over the whole doc-id space, so nearly every such
   * advance is a heap update that finds nothing, and its cost scales with the filter's cardinality.
   * Walking the runs costs a few nanoseconds per row, and dynamic nprobe ties the rows probed to
   * the filter's cardinality, so a bit test per row is cheaper wherever the walk runs at all; where
   * a filter is sparse enough for the other order to win, the exact path is cheaper still.
   *
   * <p>A document reached through several probed cells is scored once per copy; the copies carry
   * the same code and the same distance, so the dedup at the cut keeps one, and the pool multiplier
   * the unfiltered path uses covers the duplicates.
   *
   * <p>The survivor's distance is one single-row Hamming; the bulk kernel over a whole run would
   * score rows the filter rejects. The bit test itself stays a plain {@code BitSet.get}: a
   * gather-and-compress filter kernel and a branchless scalar variant were both measured against it
   * on a 1M x 1024-d segment and were slower on the machine at hand (5% selectivity: 2.9 ms and 2.5
   * ms of CPU per query against 2.4 ms), since the branch predicts well at either end of the
   * density range and NEON scalarizes the gather.
   *
   * <p>Admission is the same streaming histogram threshold the unfiltered scan uses, carried across
   * rounds, so the pool never holds more than it needs. The round loop stops once {@link
   * #FILTERED_TARGET} distinct documents have been scored, when the ranker has served every cell,
   * or at {@link #FILTERED_MAX_PROBE}. Distinct documents are counted through the same stamped
   * table the dedup uses, and only until the target is reached.
   */
  private void filteredScanAndRerank(
      FieldEntry e,
      FieldViews v,
      float[] rotated,
      byte[] qCode,
      FineQuantizer.QueryState fine,
      int[] initial,
      BitSet bits,
      KnnCollector collector)
      throws IOException {
    final HammingKernel hamming = HammingKernel.get();
    final int coarseBytes = e.coarseBytes;
    final int histLen = (coarseBytes << 3) + 2;
    final ScanScratch sc = SCAN_SCRATCH.get();
    final int[] hist = sc.hist(histLen);
    final int fanout = 1 + e.spillBits;
    final int poolTarget = BRUTE_N * fanout;
    // Relative to what this query asked for, not to nlist; see filteredMaxProbe.
    final int maxProbe = filteredMaxProbe(e, initial.length);
    final CellRanker ranker = new CellRanker(e, v, rotated, initial, sc);
    final byte[] rec = sc.coarseRec(coarseBytes);

    // Distinct accepted documents, counted only up to the target; see the javadoc.
    final int dedupCap = dedupCapacity(FILTERED_TARGET);
    final int dedupMask = dedupCap - 1;
    final int[] keys = sc.dedupKeys(dedupCap);
    final int[] gens = sc.dedupGens(dedupCap);
    final int stamp = sc.nextDedupGen();

    int nCells = 0;
    int m = 0;
    int thr = histLen - 1;
    int admitted = 0;
    int distinct = 0;
    long survivors = 0;
    int probed = 0;
    int rounds = 0;
    int batch = Math.max(1, initial.length);
    final int[] slotDoc = v.slotDoc;
    while (probed < maxProbe) {
      final int want = Math.min(batch, maxProbe - probed);
      final int[] batchCells = sc.batchCells(want);
      final int n = ranker.next(want, batchCells);
      if (n == 0) {
        break;
      }
      probed += n;
      rounds++;
      final int first = nCells;
      nCells = resolveCells(e, v, batchCells, n, sc, nCells);
      final int[] cellBase = sc.cellBase;
      final int[] cellRows = sc.cellRows;
      int roundRows = 0;
      for (int ci = first; ci < nCells; ci++) {
        roundRows += cellRows[ci];
      }
      if (roundRows > 0) {
        // Sized for every slot this round could admit; see scanAndRerank on why not smaller.
        final long[] packed = sc.packedGrow(m + roundRows);
        for (int ci = first; ci < nCells; ci++) {
          final int base = cellBase[ci];
          final int end = base + cellRows[ci];
          // One mapping per cell, hoisted out of the slot loop. The whole-section mapping is used
          // in place with an absolute base so it allocates nothing; only its absence pays for a
          // per-run slice. Null for neither means the scan below reads each code into heap instead.
          final java.lang.foreign.MemorySegment run =
              v.coarseSeg != null ? v.coarseSeg : v.coarseRun(base, cellRows[ci], coarseBytes);
          final long runBase = v.coarseSeg != null ? (long) base * coarseBytes : 0L;
          // Cells lead: the run is walked in slot order and the bit set is tested per document.
          for (int slot = base; slot < end; slot++) {
            final int doc = slotDoc[slot];
            if (bits.get(doc) == false) {
              continue;
            }
            final int dist;
            if (run != null) {
              dist =
                  hamming.distance(
                      qCode, run, runBase + (long) (slot - base) * coarseBytes, coarseBytes);
            } else {
              v.coarse.readBytes((long) slot * coarseBytes, rec, 0, coarseBytes);
              dist = org.apache.lucene.util.VectorUtil.xorBitCount(qCode, rec);
            }
            survivors++;
            if (distinct < FILTERED_TARGET && insertDistinct(doc, keys, gens, stamp, dedupMask)) {
              distinct++;
            }
            if (dist > thr) {
              continue;
            }
            hist[dist]++;
            packed[m++] = ((long) dist << 32) | slot;
            admitted++;
            if (admitted > poolTarget) {
              while (thr > 0 && admitted - hist[thr] >= poolTarget) {
                admitted -= hist[thr];
                thr--;
              }
            }
          }
        }
      }
      if (distinct >= FILTERED_TARGET || ranker.exhausted()) {
        break;
      }
      batch = Math.min(batch << 1, maxProbe - probed);
    }

    if (COUNT_SCAN) {
      scannedDocs.addAndGet(survivors);
      scanQueries.incrementAndGet();
      probedCells.addAndGet(nCells);
      filteredRounds.addAndGet(rounds);
      filteredQueries.incrementAndGet();
    }
    if (m == 0) {
      return;
    }
    final int need = (int) Math.min(poolTarget, survivors);
    final int poolN = cutAndOrderPool(sc, sc.packed, m, hist, histLen, need);
    final int candCap = Math.min(BRUTE_N, poolN);
    final int[] cands = sc.cands(candCap);
    // Every pooled slot passed the filter, so only the dedup remains.
    final int n = keepDistinctInOrder(sc.ordered, poolN, candCap, cands, v.slotDoc, null, sc);
    if (n == 0) {
      return;
    }
    rerankAndCollect(e, v, fine, cands, n, collector);
  }

  /**
   * Serves cells nearest-first, without bound: the graph's selection, then every other cell.
   *
   * <p>The first {@code initial.length} cells are the descent's exact-reranked prefix, the same
   * cells an unfiltered query probes. Past them the ranker scores EVERY centroid once through the
   * fine tier ({@code O(nlist * dim)} int8 work, a fraction of one probed cell's scan) and serves
   * the rest in distance order, skipping cells already served. The descent's visited set cannot
   * supply these, since it holds only a few times {@code ef} nodes.
   *
   * <p>Served cells are stamped in a generation-marked array rather than a cleared one, so a query
   * pays nothing per cell it never reaches.
   */
  private static final class CellRanker {
    private final FieldEntry e;
    private final FieldViews v;
    private final float[] rotated;
    private final int[] initial;
    private final ScanScratch sc;
    private final int[] probedGen;
    private final int gen;
    private int served;
    private boolean ranked;
    private int allPos;

    CellRanker(FieldEntry e, FieldViews v, float[] rotated, int[] initial, ScanScratch sc) {
      this.e = e;
      this.v = v;
      this.rotated = rotated;
      this.initial = initial;
      this.sc = sc;
      this.probedGen = sc.probedGen(e.nlist);
      this.gen = sc.nextProbeGen();
    }

    /** Fills up to {@code want} unserved cells into {@code out}, nearest-first. */
    int next(int want, int[] out) {
      int n = 0;
      while (n < want && served < initial.length) {
        final int c = initial[served++];
        if (c < 0 || probedGen[c] == gen) {
          continue;
        }
        probedGen[c] = gen;
        out[n++] = c;
      }
      if (n < want) {
        if (ranked == false) {
          rankAll();
        }
        final long[] keys = sc.allKeys;
        while (n < want && allPos < e.nlist) {
          final int c = (int) keys[allPos++];
          if (probedGen[c] == gen) {
            continue;
          }
          probedGen[c] = gen;
          out[n++] = c;
        }
      }
      return n;
    }

    boolean exhausted() {
      return served >= initial.length && ranked && allPos >= e.nlist;
    }

    private void rankAll() {
      ranked = true;
      final int nlist = e.nlist;
      final float[] dist = sc.allDist(nlist);
      v.codes.rankCandidates(rotated, v.allCells, nlist, dist);
      final long[] keys = sc.allKeys(nlist);
      for (int c = 0; c < nlist; c++) {
        keys[c] = ((long) NumericUtils.floatToSortableInt(dist[c]) << 32) | c;
      }
      Arrays.sort(keys, 0, nlist);
    }
  }

  /**
   * The degenerate filter: every accepted document is fine-reranked, with no cell selection.
   *
   * <p>Ordinals are in doc order, verified at open, so each accepted document resolves to its
   * primary slot by one binary search over {@code ordToDoc}; a miss is a document without a vector
   * in this field. Deleted documents are already excluded by {@link AcceptDocs}.
   */
  private void rerankFilteredDocs(
      FieldEntry e,
      FieldViews v,
      FineQuantizer.QueryState fine,
      DocIdSetIterator filter,
      KnnCollector collector)
      throws IOException {
    final ScanScratch sc = SCAN_SCRATCH.get();
    final int[] ordToDoc = v.ordToDoc;
    final int[] ordToSlot = v.ordToSlot;
    int[] cands = sc.cands(64);
    int n = 0;
    for (int doc = filter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = filter.nextDoc()) {
      final int ord = Arrays.binarySearch(ordToDoc, doc);
      if (ord < 0) {
        continue;
      }
      if (n == cands.length) {
        cands = sc.growCands(n + 1);
      }
      cands[n++] = ordToSlot[ord];
    }
    if (n == 0) {
      return;
    }
    rerankAndCollect(e, v, fine, cands, n, collector);
  }

  /**
   * A memory-mapped view of a slice, or null when the directory cannot provide one.
   *
   * <p>Under {@link #GLOBAL_SCOPE} the segment is rebound to the global scope so per-load liveness
   * checks fold away. If native access is not granted the reinterpret throws and the CHECKED
   * segment is returned, where correctness is identical and only the check remains.
   */
  @SuppressWarnings("restricted") // MemorySegment.reinterpret under the opt-in GLOBAL_SCOPE flag
  private static java.lang.foreign.MemorySegment segmentOf(RandomAccessInput in, long length) {
    if (length == 0) {
      return null;
    }
    if (in instanceof org.apache.lucene.store.MemorySegmentAccessInput msai) {
      try {
        final java.lang.foreign.MemorySegment seg = msai.segmentSliceOrNull(0, length);
        if (seg != null && GLOBAL_SCOPE) {
          try {
            return seg.reinterpret(seg.byteSize());
          } catch (RuntimeException | Error _) {
            // Native access not granted; the CHECKED segment is still correct, only slower.
            return seg;
          }
        }
        return seg;
      } catch (IOException _) {
        return null;
      }
    }
    return null;
  }

  /**
   * A merge-time view of this segment: its centroids, and byte-level access to its records.
   *
   * <p>Exists so a merge can DONATE this segment's clustering to the merged one: seeding Lloyd from
   * these centroids, carrying documents' cell assignments, and copying their codes verbatim.
   *
   * @return a view, or {@code null} if the field is absent or empty
   */
  DonorView donorView(String field) throws IOException {
    final FieldEntry e = fields.get(field);
    if (e == null || e.count == 0 || e.nlist == 0) {
      return null;
    }
    return new DonorView(e, viewsFor(field, e));
  }

  /**
   * Byte-level access to one segment's clustering, for merging.
   *
   * <p>Everything here is indexed by VECTOR ORDINAL, the order {@link #getFloatVectorValues}
   * reports, and internally translated to the physical slot. That translation is why this class
   * exists rather than callers reading the code table directly: the table is in CELL order and
   * holds several slots per spilled document, so ordinal and slot are unrelated. Conflating them
   * silently pairs each document with another document's code, producing a fully-populated index
   * whose every score is wrong.
   */
  static final class DonorView {
    private final FieldEntry entry;
    private final FieldViews views;

    /** Physical primary slot of each vector ordinal, in ordinal order. */
    private final int[] slotByOrd;

    /** Cell each ordinal's primary slot lives in. */
    private final int[] cellByOrd;

    /**
     * The persisted ordinal map IS the primary-slot-by-ordinal table, in doc order, so the view
     * costs one int read per ordinal for the cell and no scan or sort of the code table.
     */
    private DonorView(FieldEntry entry, FieldViews views) throws IOException {
      this.entry = entry;
      this.views = views;
      this.scratchRecord = new byte[entry.recordLen];
      this.slotByOrd = views.ordToSlot;
      final int n = entry.count;
      this.cellByOrd = new int[n];
      for (int ord = 0; ord < n; ord++) {
        cellByOrd[ord] =
            views.codeTable.readInt(
                (long) slotByOrd[ord] * entry.recordLen + entry.primaryCellOffset);
      }
    }

    int count() {
      return slotByOrd.length;
    }

    int nlist() {
      return entry.nlist;
    }

    int dim() {
      return entry.dim;
    }

    byte encodingId() {
      return entry.encodingId;
    }

    long rotationSeed() {
      return entry.rotationSeed;
    }

    int recordLen() {
      return entry.recordLen;
    }

    int planeBytes() {
      return entry.planeBytes;
    }

    /** The cell this ordinal was assigned to, for carrying the assignment across the merge. */
    int cellOf(int ord) {
      return cellByOrd[ord];
    }

    /** TEST ONLY: the merged document id this ordinal resolves to, read off its primary slot. */
    int ordToDocForTest(int ord) throws IOException {
      final long off = (long) slotByOrd[ord] * entry.recordLen;
      return views.codeTable.readInt(off + entry.docIdOffset);
    }

    /** This segment's centroids, as the merged segment's Lloyd seed. */
    float[][] centroids() throws IOException {
      return views.centroids();
    }

    /**
     * The mean this segment's codes were packed against, for the merged segment to INHERIT.
     *
     * <p>A merge that recomputed the mean while copying codes verbatim would decode every copied
     * document against a grid it was never quantized to. Inheriting is what makes copying and
     * centring compatible.
     */
    float[] mean() {
      return views.mean;
    }

    /**
     * Copies one document's fine-code record, rewriting only its document id and primary cell.
     *
     * <p>VERBATIM. The code is a deterministic function of the rotated vector, and the rotation
     * depends only on {@code dim}, so a document moving between segments has the same code by
     * definition. Re-encoding would run the rounding decision again on a value that has already
     * lost precision, flipping near-threshold bits.
     */
    void copyRecord(int ord, int newDocId, int newPrimaryCell, byte[] dest) throws IOException {
      copyRecordAt(ord, newDocId, newPrimaryCell, dest, 0);
    }

    /** As {@link #copyRecord}, into {@code dest} at {@code destOff}. */
    void copyRecordAt(int ord, int newDocId, int newPrimaryCell, byte[] dest, int destOff)
        throws IOException {
      final long off = (long) slotByOrd[ord] * entry.recordLen;
      views.codeTable.readBytes(off, dest, destOff, entry.recordLen);
      CodeRecord.writeIntLE(dest, destOff + entry.docIdOffset, newDocId);
      CodeRecord.writeIntLE(dest, destOff + entry.primaryCellOffset, newPrimaryCell);
    }

    /**
     * Reconstructs one document's vector IN ROTATED SPACE, unit length, into {@code dest}.
     *
     * <p>Skips the rotation round trip that reading through {@link FloatVectorValues} forces. That
     * path returns vectors in the ORIGINAL space, so it runs {@code inverseRotate} (a full FWHT)
     * per document, and the writer's next act is to normalize and {@code rotate} straight back.
     *
     * <p>The cancellation is exact: the rotation {@code R} is orthogonal, so it preserves norms and
     * {@code R(R^T x / ||R^T x||) == x / ||x||} to float rounding. Normalizing the stored rotated
     * vector therefore gives the same result as the round trip with neither transform.
     *
     * <p>These floats are consumed in rotated space throughout: by the Lloyd mean, and by the exact
     * stage that ranks each routing shortlist. The coarse scan reads the copied coarse planes. So
     * rotated space is all a merge needs, and centroids live there too, which is what makes the
     * donor's seed directly comparable.
     */
    void rotatedVector(int ord, float[] dest) throws IOException {
      rotatedVector(ord, dest, scratchRecord, scratchCorrections);
    }

    /**
     * As {@link #rotatedVector(int, float[])}, but with CALLER-OWNED scratch, so the gather can run
     * one worker per range of ordinals.
     *
     * <p>Everything else this touches is shared read-only state, the persisted ordinal map and the
     * mmapped code table read at absolute offsets, so the per-call scratch is the only thing that
     * would keep this single-threaded. Use {@link #newRecordScratch()} to size the buffer.
     */
    void rotatedVector(int ord, float[] dest, byte[] record, float[] corrections)
        throws IOException {
      final int slot = views.ordToSlot[ord];
      final long off = (long) slot * entry.recordLen;
      views.codeTable.readBytes(off, record, 0, entry.recordLen);
      // Decoded by the quantizer that wrote the record, since the code layout belongs to the tier.
      for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
        corrections[k] =
            Float.intBitsToFloat(
                CodeRecord.readIntLE(record, entry.correctionBase + k * Float.BYTES));
      }
      entry.quantizer.decode(
          record, CodeRecord.codeOffset(), entry.dim, views.mean, corrections, dest);
      // Unit length, as writeField produces via l2normalize on the un-rotated form.
      org.apache.lucene.util.VectorUtil.l2normalize(dest, false);
    }

    /** A record buffer sized for this segment, for a worker calling the scratch-taking overload. */
    byte[] newRecordScratch() {
      return new byte[entry.recordLen];
    }

    private final float[] scratchCorrections = new float[CodeRecord.CORRECTIONS];

    /** Record scratch for the single-threaded {@link #rotatedVector(int, float[])} overload. */
    private final byte[] scratchRecord;

    /**
     * Whether {@link #rotatedVector} can serve this segment.
     *
     * <p>Every tier decodes its own code, so this holds whenever a quantizer was resolved.
     */
    boolean canReadRotated() {
      return entry.quantizer != null;
    }

    /** Copies one document's coarse code, for the same reason. */
    void copyCoarse(int ord, byte[] dest, int destOff) throws IOException {
      final long off = (long) slotByOrd[ord] * entry.coarseBytes;
      views.coarse.readBytes(off, dest, destOff, entry.coarseBytes);
    }
  }

  /** The default admission width of {@link IVFasterKnnQuery}: the coarse shortlist size. */
  static int bruteN() {
    return BRUTE_N;
  }

  /**
   * Opens a per-query view of one field for {@link IVFasterKnnQuery}: the query quantized into both
   * tiers, the cells selected for it, and scoring of one slot at a time.
   *
   * @param nprobe cells to select, or {@code 0} for the value persisted at write time
   * @return the session, or {@code null} when the field is absent or empty
   */
  CellSession openCellSession(String field, float[] target, int nprobe) throws IOException {
    final FieldEntry e = fields.get(field);
    if (e == null || e.nlist == 0 || e.count == 0) {
      return null;
    }
    final FieldViews v = viewsFor(field, e);
    v.ensureSearchState();
    final int dim = e.dim;
    final float[] query = ArrayUtil.copyOfSubArray(target, 0, dim);
    org.apache.lucene.util.VectorUtil.l2normalize(query);
    final float[] rotated = new float[dim];
    e.rotation.rotate(query, rotated);
    final byte[] qCode = new byte[e.coarseBytes];
    Nitrox2.encode(rotated, dim, qCode, 0);
    final FineQuantizer.QueryState fine =
        e.quantizer.prepareQuery(rotated, dim, v.mean, e.similarity);
    final int probe = Math.min(nprobe > 0 ? nprobe : e.nprobe, e.nlist);
    return new CellSession(e, v, rotated, qCode, fine, probe);
  }

  /**
   * One query's cells and codes, for a scorer that walks them doc-at-a-time.
   *
   * <p>Everything here is what {@link #search} computes up front and then consumes in its own scan;
   * a scorer consumes it one slot at a time instead, as the conjunction it sits in hands documents
   * over.
   */
  final class CellSession {
    private final FieldEntry e;
    private final FieldViews v;
    private final float[] rotated;
    private final byte[] qCode;
    private final FineQuantizer.QueryState fine;
    private final int probe;
    private int[] cells;

    /** Per selected cell: slot base, rows, and where its distances start in {@link #dist}. */
    private int[] cellBase;

    private int[] cellRows;
    private int[] distOffset;

    /** Coarse distance of every row of every selected cell, in cell order; see {@link #prepare}. */
    private int[] dist;

    private int threshold;
    private final HammingKernel hamming = HammingKernel.get();

    /**
     * The session's own clone of the data file, positioned by seek. A scorer is long-lived and
     * interleaves with other readers of the same segment, so it does not share the field's stateful
     * fallback slices.
     */
    private final IndexInput in;

    private final byte[] coarseRec;
    private final byte[][] record;
    private final byte[] flat;
    private final float[][] corrections = {new float[CodeRecord.CORRECTIONS]};
    private final float[] score = new float[1];

    private CellSession(
        FieldEntry e,
        FieldViews v,
        float[] rotated,
        byte[] qCode,
        FineQuantizer.QueryState fine,
        int probe) {
      this.e = e;
      this.v = v;
      this.rotated = rotated;
      this.qCode = qCode;
      this.fine = fine;
      this.probe = probe;
      this.in = data.clone();
      // Unconditional: whether the coarse scan needs it is now decided per RUN, not at open, so
      // there is no state here that can tell. One code's worth of bytes, once per session.
      this.coarseRec = new byte[e.coarseBytes];
      this.record = e.wantsStrided ? null : new byte[][] {new byte[e.recordLen]};
      this.flat = e.wantsStrided ? new byte[e.stagedStride] : null;
    }

    /** The configured probe width: {@code nprobe} clamped to the cell count. */
    int probe() {
      return probe;
    }

    int nlist() {
      return e.nlist;
    }

    /** Slots in the segment, every spill copy counted; {@code slots / nlist} is a cell's width. */
    long slots() {
      return e.codeTableLength / e.recordLen;
    }

    /** The cardinality below which reranking every accepted document is the cheaper plan. */
    int exactBound() {
      return exactFilterBound(e);
    }

    int filteredTarget() {
      return FILTERED_TARGET;
    }

    /** Slots per document: one plus the spill copies, the codec's pool multiplier. */
    int fanout() {
      return 1 + e.spillBits;
    }

    /** Relative to this session's configured probe; see {@link #filteredMaxProbe}. */
    int maxProbe() {
      return filteredMaxProbe(e, probe);
    }

    /** Documents with a vector in this field. */
    int count() {
      return e.count;
    }

    /**
     * Selects the {@code n} nearest cells, Hamming-scores every row of their runs, and fixes the
     * admission threshold at the coarse distance that admits {@code admitRows} rows (ties
     * included), so that admission is a pure function of the document.
     *
     * <p>The scan is the codec's own coarse scan over the probed cells, run eagerly. A scorer
     * cannot admit by a threshold that tightens as it goes, since a conjunction may skip through it
     * in any order and a document's match must not depend on the path taken to reach it. Bulk over
     * each run through the vector kernel, so the eager pass costs what the unfiltered scan pays for
     * the same cells.
     *
     * <p>Without the quality prune when {@code n} exceeds the configured probe, since a wider probe
     * was asked for on purpose.
     *
     * @return the number of selected cells
     */
    int prepare(int n, long admitRows) throws IOException {
      if (cells != null) {
        throw new IllegalStateException("already prepared");
      }
      final int want = Math.min(Math.max(1, n), e.nlist);
      cells = selectCells(e, v, rotated, qCode, want, want <= probe);
      cellBase = new int[cells.length];
      cellRows = new int[cells.length];
      distOffset = new int[cells.length];
      int total = 0;
      for (int i = 0; i < cells.length; i++) {
        final int c = cells[i];
        cellBase[i] = v.cellStart[c];
        cellRows[i] = v.cellStart[c + 1] - cellBase[i];
        distOffset[i] = total;
        total += cellRows[i];
      }
      dist = new int[total];
      final int histLen = (e.coarseBytes << 3) + 2;
      final int[] hist = new int[histLen];
      // Sized once from the widest cell, since the kernel writes from index 0 and the run is then
      // copied to its place in dist.
      int widest = 0;
      for (int i = 0; i < cells.length; i++) {
        widest = Math.max(widest, cellRows[i]);
      }
      final int[] rowDist =
          new int[org.apache.lucene.util.ArrayUtil.oversize(widest, Integer.BYTES)];
      for (int i = 0; i < cells.length; i++) {
        final int rows = cellRows[i];
        if (rows == 0) {
          continue;
        }
        final int off = distOffset[i];
        if (v.coarseDistances(hamming, qCode, cellBase[i], rows, e.coarseBytes, rowDist) == false) {
          // Through this session's OWN clone, not the field's shared slice; see the `in` field.
          in.seek(e.coarseOffset + (long) cellBase[i] * e.coarseBytes);
          for (int r = 0; r < rows; r++) {
            in.readBytes(coarseRec, 0, e.coarseBytes);
            rowDist[r] = org.apache.lucene.util.VectorUtil.xorBitCount(qCode, coarseRec);
          }
        }
        System.arraycopy(rowDist, 0, dist, off, rows);
        for (int r = 0; r < rows; r++) {
          hist[dist[off + r]]++;
        }
      }
      // The cut: the smallest distance whose cumulative count reaches admitRows.
      long below = 0;
      int thr = 0;
      while (thr < histLen - 1 && below + hist[thr] < admitRows) {
        below += hist[thr];
        thr++;
      }
      threshold = thr;
      return cells.length;
    }

    /** Whether the row of a prepared cell passes the fixed coarse cut. */
    boolean admitted(CellPostings cell) {
      return dist[cell.distOffset + cell.row()] <= threshold;
    }

    /**
     * Every document with a vector, in doc order, as a posting list whose "slot" is the ORDINAL;
     * resolve it through {@link #ordToSlot} before scoring. The exact plan for a filter narrow
     * enough that reranking everything it accepts beats walking cells.
     */
    CellPostings allDocs() {
      return new CellPostings(v.ordToDoc, 0, v.ordToDoc.length);
    }

    int ordToSlot(int ord) {
      return v.ordToSlot[ord];
    }

    /** The {@code i}-th prepared cell as a posting list, or null when it is empty. */
    CellPostings postings(int i) {
      if (cellRows[i] == 0) {
        return null;
      }
      final CellPostings cell = new CellPostings(v.slotDoc, cellBase[i], cellRows[i]);
      cell.distOffset = distOffset[i];
      return cell;
    }

    /** Fine score of one slot, on the collector's similarity scale. */
    float fineScore(int slot) throws IOException {
      in.seek(e.codeTableOffset + (long) slot * e.recordLen);
      if (flat != null) {
        in.readBytes(flat, 0, e.recordLen);
        for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
          corrections[0][k] =
              Float.intBitsToFloat(CodeRecord.readIntLE(flat, e.correctionBase + k * Float.BYTES));
        }
        fine.scoreBulkStrided(
            flat, 1, e.stagedStride, CodeRecord.codeOffset(), corrections, score, null);
      } else {
        in.readBytes(record[0], 0, e.recordLen);
        for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
          corrections[0][k] =
              Float.intBitsToFloat(
                  CodeRecord.readIntLE(record[0], e.correctionBase + k * Float.BYTES));
        }
        fine.scoreBulk(record, 1, CodeRecord.codeOffset(), corrections, score);
      }
      return score[0];
    }

    int coarseBytes() {
      return e.coarseBytes;
    }
  }

  @Override
  public void search(
      String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("ivfaster supports only float vectors");
  }

  @Override
  public void search(
      String field, short[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {
    throw new UnsupportedOperationException("ivfaster supports only float vectors");
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) {
    return null;
  }

  /**
   * Reconstructs approximate float vectors from the fine codes.
   *
   * <p>The codec does not store full-precision vectors, so this is lossy. It exists because merging
   * consumes it.
   */
  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FieldEntry e = fields.get(field);
    if (e == null) {
      return null;
    }
    return new ReconstructedFloatVectorValues(e, viewsFor(field, e));
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    return null;
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  /**
   * Closes the data input, first reporting the engagement counters when asked for.
   *
   * <p>Which cell-select path ran is worth reporting because a silent fall-back to the exact scan
   * is correct but costs {@code O(nlist)} per query in place of {@code O(ef)}, so "the graph was
   * used" has to be provable. Gated, since a codec should not write to stdout by default.
   */
  @Override
  public void close() throws IOException {
    if (Boolean.getBoolean("ivfaster.reportEngagement")) {
      final long q = Math.max(1, scanQueries.get());
      IvfDiag.outln(
          "[ivfaster] graphDescents="
              + CentroidGraph.descents.get()
              + " flatSelects="
              + flatSelects.get()
              + " queries="
              + scanQueries.get()
              + " docsScanned/query="
              + (scannedDocs.get() / q)
              + " cellsProbed/query="
              + (probedCells.get() / q)
              + " centroidsVerified/query="
              + (verifiedCentroids.get() / Math.max(1, verifyQueries.get()))
              + " filteredQueries="
              + filteredQueries.get()
              + " rounds/filteredQuery="
              + (filteredRounds.get() / Math.max(1, filteredQueries.get()))
              + " exactFilterQueries="
              + exactFilterQueries.get()
              + (MEASURE_SPILL
                  ? " uniqueDocs/query="
                      + (uniqueDocs.get() / q)
                      + " amplification="
                      + String.format(
                          java.util.Locale.ROOT,
                          "%.2fx",
                          (double) scannedDocs.get() / Math.max(1, uniqueDocs.get()))
                  : ""));
    }
    IOUtils.close(data);
  }

  /**
   * Per-thread scan buffers, reused across queries.
   *
   * <p>A query would otherwise allocate one array per probed candidate plus one buffer per
   * shortlist candidate, and the JVM zeroes every new array before the scan reads its first byte.
   *
   * <p>One instance per thread, grown on demand and never shrunk: a search thread's queries are
   * sequential, so there is no sharing to guard.
   */
  private static final ThreadLocal<ScanScratch> SCAN_SCRATCH =
      ThreadLocal.withInitial(ScanScratch::new);

  /** Reusable buffers for one thread's coarse scan and rerank. */
  private static final class ScanScratch {
    int[] hist = new int[0];
    int[] prefix = new int[0];
    long[] packed = new long[0];
    long[] pool = new long[0];
    long[] ordered = new long[0];
    int[] cands = new int[0];

    /** Graph-descent visited set + its coarse distances, sized to nlist; reused across queries. */
    int[] candidates = new int[0];

    int[] candDist = new int[0];
    int[] rowDist = new int[0];

    /**
     * Surviving local indices from the SIMD admission filter; sized in lockstep with {@link
     * #rowDist}.
     */
    int[] filterIdx = new int[0];

    /**
     * One coarse code, for the scan's fallback when {@link FieldViews#coarseDistances} reports that
     * no mapping covers a run. Reused rather than allocated per cell, since the read it feeds is
     * per SLOT.
     */
    byte[] coarseRec = new byte[0];

    byte[][] records = new byte[0][];
    byte[] flat = new byte[0];

    /** Record length {@link #flat} was sized for; a change in it invalidates the stride. */
    int flatRecordLen = -1;

    float[][] corrections = new float[0][];
    int[] docIds = new int[0];
    float[] scores = new float[0];
    int[] cellBase = new int[0];
    int[] cellRows = new int[0];
    int[] dedupKeys = new int[0];
    int[] dedupGens = new int[0];

    /** Query generation stamping {@link #dedupGens}; see keepDistinctInOrder. */
    int dedupGen;

    /** Cells served to the current filtered query, stamped by {@link #probeGen}; see CellRanker. */
    int[] probedGen = new int[0];

    int probeGen;

    /** Every cell's exact distance, and the same keyed for sorting; see CellRanker.rankAll. */
    float[] allDist = new float[0];

    long[] allKeys = new long[0];

    /** The cells of one filtered round. */
    int[] batchCells = new int[0];

    /**
     * The next dedup stamp. Re-zeroed on wraparound, and never 0, so a freshly grown table (all
     * zero) reads as empty.
     */
    int nextDedupGen() {
      if (++dedupGen == 0) {
        Arrays.fill(dedupGens, 0);
        dedupGen = 1;
      }
      return dedupGen;
    }

    /** As {@link #nextDedupGen}, for {@link #probedGen}. */
    int nextProbeGen() {
      if (++probeGen == 0) {
        Arrays.fill(probedGen, 0);
        probeGen = 1;
      }
      return probeGen;
    }

    int[] probedGen(int n) {
      if (probedGen.length < n) {
        probedGen = new int[n];
      }
      return probedGen;
    }

    float[] allDist(int n) {
      if (allDist.length < n) {
        allDist = new float[n];
      }
      return allDist;
    }

    long[] allKeys(int n) {
      if (allKeys.length < n) {
        allKeys = new long[n];
      }
      return allKeys;
    }

    int[] batchCells(int n) {
      if (batchCells.length < n) {
        batchCells = new int[ArrayUtil.oversize(n, Integer.BYTES)];
      }
      return batchCells;
    }

    /** Grows {@link #packed} PRESERVING its contents, for a scan that admits across rounds. */
    long[] packedGrow(int n) {
      if (packed.length < n) {
        packed = ArrayUtil.grow(packed, n);
      }
      return packed;
    }

    /** Grows {@link #cands} preserving its contents. */
    int[] growCands(int n) {
      if (cands.length < n) {
        cands = ArrayUtil.grow(cands, n);
      }
      return cands;
    }

    /**
     * The dedup table's keys, reused across queries.
     *
     * <p>Grown by REPLACEMENT, with the paired gens array replaced alongside it: a grown gens array
     * is all-zero, which reads as "no entry belongs to the current stamp", and is why the stamp is
     * never allowed to be 0.
     */
    int[] dedupKeys(int n) {
      if (dedupKeys.length < n) {
        dedupKeys = new int[n];
        dedupGens = new int[n];
      }
      return dedupKeys;
    }

    /** Companion generations for {@link #dedupKeys}; sized in lockstep by it. */
    int[] dedupGens(int n) {
      return dedupGens;
    }

    /**
     * Per-probed-cell slot base, decoded once per query; see resolveCells. Grown PRESERVING its
     * contents, since a filtered query appends a round at a time.
     */
    int[] cellBase(int n) {
      if (cellBase.length < n) {
        cellBase = ArrayUtil.grow(cellBase, n);
      }
      return cellBase;
    }

    /** Per-probed-cell row count, paired with {@link #cellBase}. */
    int[] cellRows(int n) {
      if (cellRows.length < n) {
        cellRows = ArrayUtil.grow(cellRows, n);
      }
      return cellRows;
    }

    /**
     * Grows to at least {@code n}, zeroing the prefix that the caller treats as histogram state.
     */
    int[] hist(int n) {
      if (hist.length < n) {
        hist = new int[ArrayUtil.oversize(n, Integer.BYTES)];
      } else {
        java.util.Arrays.fill(hist, 0, n, 0);
      }
      return hist;
    }

    int[] prefix(int n) {
      if (prefix.length < n) {
        prefix = new int[ArrayUtil.oversize(n, Integer.BYTES)];
      } else {
        java.util.Arrays.fill(prefix, 0, n, 0);
      }
      return prefix;
    }

    long[] packed(int n) {
      if (packed.length < n) {
        packed = new long[ArrayUtil.oversize(n, Long.BYTES)];
      }
      return packed;
    }

    long[] pool(int n) {
      if (pool.length < n) {
        pool = new long[ArrayUtil.oversize(n, Long.BYTES)];
      }
      return pool;
    }

    long[] ordered(int n) {
      if (ordered.length < n) {
        ordered = new long[ArrayUtil.oversize(n, Long.BYTES)];
      }
      return ordered;
    }

    int[] cands(int n) {
      if (cands.length < n) {
        cands = new int[ArrayUtil.oversize(n, Integer.BYTES)];
      }
      return cands;
    }

    /** Exactly {@code n} bytes, since {@code VectorUtil.xorBitCount} requires equal lengths. */
    byte[] coarseRec(int n) {
      if (coarseRec.length != n) {
        coarseRec = new byte[n];
      }
      return coarseRec;
    }

    /**
     * Graph-descent visited buffer, sized to nlist and reused across queries. {@code graph.search}
     * bounds every read by its returned count, so a buffer larger than nlist is harmless. Paired
     * with {@link #candDist} and consumed entirely within selectCells, before scanAndRerank touches
     * this scratch.
     */
    int[] candidates(int n) {
      if (candidates.length < n) {
        candidates = new int[ArrayUtil.oversize(n, Integer.BYTES)];
        candDist = new int[candidates.length];
      }
      return candidates;
    }

    /** Companion coarse distances for {@link #candidates}; sized in lockstep by it. */
    int[] candDist() {
      return candDist;
    }

    /** Record rows for the rerank: {@code n} buffers of {@code recordLen} bytes, allocated once. */
    byte[][] records(int n, int recordLen) {
      if (records.length < n || (records.length > 0 && records[0].length != recordLen)) {
        records = new byte[ArrayUtil.oversize(n, Integer.BYTES)][];
        for (int i = 0; i < records.length; i++) {
          records[i] = new byte[recordLen];
        }
        sizeCommon(records.length);
      }
      return records;
    }

    /**
     * FLAT staging for the strided rerank: {@code n} records of {@code recordLen} in one buffer.
     *
     * <p>The alternative to {@link #records}: a query uses one or the other, so whichever the tier
     * does not want stays at length 0.
     *
     * <p>Oversized in RECORDS, so the stride stays exact: growing in bytes could leave a partial
     * trailing record and make {@code flat.length / recordLen} disagree with the usable capacity.
     * The product is computed in LONG and checked, because {@code bruteN} is a system property and
     * a large enough value would overflow {@code cap * recordLen} in int arithmetic, allocating a
     * SHORT buffer that throws nothing here and corrupts the staging loop's offsets far from the
     * cause.
     */
    byte[] flat(int n, int recordLen) {
      final long need = (long) n * recordLen;
      if (flat.length < need || flatRecordLen != recordLen) {
        final int cap = ArrayUtil.oversize(n, Integer.BYTES);
        final long bytes = (long) cap * recordLen;
        if (bytes > ArrayUtil.MAX_ARRAY_LENGTH) {
          throw new IllegalStateException(
              "rerank shortlist too large to stage: "
                  + cap
                  + " records of "
                  + recordLen
                  + " B exceeds the maximum array length; lower ivfaster.bruteN");
        }
        flat = new byte[(int) bytes];
        flatRecordLen = recordLen;
        sizeCommon(cap);
      }
      return flat;
    }

    /**
     * Grows the per-candidate side tables that BOTH staging layouts need.
     *
     * <p>Shared because they are indexed by candidate rather than by layout; sizing them in each
     * grow path separately is how one layout ends up with a {@code scores} array sized for the
     * other.
     */
    private void sizeCommon(int cap) {
      if (corrections.length < cap) {
        corrections = new float[cap][];
        for (int i = 0; i < cap; i++) {
          corrections[i] = new float[CodeRecord.CORRECTIONS];
        }
        docIds = new int[cap];
        scores = new float[cap];
      }
    }

    /** Per-candidate correction floats, sized independently of which staging layout is in use. */
    float[][] corrections(int n) {
      sizeCommon(ArrayUtil.oversize(n, Integer.BYTES));
      return corrections;
    }
  }

  /** Counts queries that selected cells by exact scan rather than by graph descent. */
  static final java.util.concurrent.atomic.AtomicLong flatSelects =
      new java.util.concurrent.atomic.AtomicLong();

  static final java.util.concurrent.atomic.AtomicLong verifiedCentroids =
      new java.util.concurrent.atomic.AtomicLong();
  static final java.util.concurrent.atomic.AtomicLong verifyQueries =
      new java.util.concurrent.atomic.AtomicLong();

  /** Documents coarse-scanned, summed over queries; with {@link #scanQueries} gives docs/query. */
  static final java.util.concurrent.atomic.AtomicLong scannedDocs =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * -Divfaster.earlyTerm=true: stage and score the shortlist in BLOCKS, stopping when a whole block
   * yields nothing competitive.
   *
   * <p>OFF BY DEFAULT because it is a HEURISTIC rather than a bound. Candidates arrive in
   * coarse-distance order and coarse distance does not bound the fine score; if it did, the fine
   * tier would be unnecessary. An exhausted block is therefore evidence that the coarse ordering
   * has stopped delivering rather than proof that no later candidate can win, so enabling it must
   * come with a recall measurement.
   *
   * <p>Blocking is what makes it worth anything: staging dominates scoring and runs first, so a
   * stop test after a monolithic staging pass could only skip the arithmetic.
   */
  private static final boolean EARLY_TERM = Boolean.getBoolean("ivfaster.earlyTerm");

  /**
   * Candidates staged and scored per block under {@link #EARLY_TERM}; floored at 64 at the call
   * site.
   */
  private static final int EARLY_TERM_BLOCK = Integer.getInteger("ivfaster.earlyTermBlock", 128);

  /**
   * Slack for the early-exit test, ABSOLUTE on the collector's [0,1] similarity scale.
   *
   * <p>0 (the default) stops when a block produced nothing competitive. NEGATIVE values fire
   * EARLIER, since the block's best then has to clear {@code bar - slack}, trading recall for
   * latency; positive values are more conservative. Absolute rather than relative because the
   * scores are already a bounded similarity, where a ratio would mean different things at different
   * bars.
   */
  private static final float EARLY_TERM_SLACK =
      Float.parseFloat(System.getProperty("ivfaster.earlyTermSlack", "0"));

  /**
   * -Divfaster.measureSpill=true: count DISTINCT documents among the scanned slots. Diagnostic
   * only.
   */
  private static final boolean MEASURE_SPILL = Boolean.getBoolean("ivfaster.measureSpill");

  /**
   * -Divfaster.countScan=true: maintain the docs/query and cells/query counters.
   *
   * <p>OFF by default because they are three atomic read-modify-writes per query on the hot path.
   * Any diagnostic that READS them turns them on, so the numbers are either maintained and reported
   * or neither.
   */
  private static final boolean COUNT_SCAN =
      Boolean.getBoolean("ivfaster.countScan")
          || Boolean.getBoolean("ivfaster.reportEngagement")
          || MEASURE_SPILL;

  /**
   * Distinct documents among the scanned slots, summed over queries; see the MEASURE_SPILL block.
   */
  static final java.util.concurrent.atomic.AtomicLong uniqueDocs =
      new java.util.concurrent.atomic.AtomicLong();

  /** Queries that ran a coarse scan. */
  static final java.util.concurrent.atomic.AtomicLong scanQueries =
      new java.util.concurrent.atomic.AtomicLong();

  /** Cells actually probed, summed over queries: nprobe is a cap, not necessarily the count. */
  static final java.util.concurrent.atomic.AtomicLong probedCells =
      new java.util.concurrent.atomic.AtomicLong();

  /** Filtered queries that walked cells, and the rounds they took; see filteredScanAndRerank. */
  static final java.util.concurrent.atomic.AtomicLong filteredQueries =
      new java.util.concurrent.atomic.AtomicLong();

  static final java.util.concurrent.atomic.AtomicLong filteredRounds =
      new java.util.concurrent.atomic.AtomicLong();

  /** Filtered queries whose accepted set was reranked whole; see EXACT_FILTER_COST. */
  static final java.util.concurrent.atomic.AtomicLong exactFilterQueries =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Approximate float vectors decoded from the fine codes, in DOCUMENT order.
   *
   * <p>The code table is in cell order and, under spill, holds several slots per document, so a
   * document's vector is served from the persisted document-ordered map of primary slots: reported
   * once, and from its primary cell's copy.
   */
  private static final class ReconstructedFloatVectorValues extends FloatVectorValues {
    private final FieldEntry entry;
    private final FieldViews views;
    private final float[] value;
    private final byte[] record;

    ReconstructedFloatVectorValues(FieldEntry entry, FieldViews views) {
      this.entry = entry;
      this.views = views;
      this.value = new float[entry.dim];
      this.rotatedValue = new float[entry.dim];
      this.record = new byte[entry.recordLen];
    }

    @Override
    public int dimension() {
      return entry.dim;
    }

    @Override
    public int size() {
      return views.ordToSlot.length;
    }

    /**
     * One document's vector, in the ORIGINAL space.
     *
     * <p>EXACT when {@code keepFullPrecision} stored the original vectors: that section is
     * ordinal-indexed, so {@code ord * dim * 4} is the document's start, and there is no rotation
     * to undo and no normalize, since the writer stored what the caller handed in verbatim. Merge
     * reads this too, which is why the section exists.
     *
     * <p>Otherwise the code is DECODED BY THE QUANTIZER THAT WROTE IT, rather than by assuming a
     * layout here. An inlined int8-shaped reconstruction (offset byte {@code d}, times {@code
     * corrections[0]}) would be right for the default tier and meaningless for any tier whose byte
     * {@code d} is not coordinate {@code d}, producing finite plausible floats that throw nothing
     * and surface only as wrong recall.
     */
    @Override
    public float[] vectorValue(int ord) throws IOException {
      if (views.raw != null) {
        final long base = (long) ord * entry.dim * Float.BYTES;
        for (int d = 0; d < entry.dim; d++) {
          value[d] = Float.intBitsToFloat(views.raw.readInt(base + (long) d * Float.BYTES));
        }
        return value;
      }
      final int slot = views.ordToSlot[ord];
      final long off = (long) slot * entry.recordLen;
      views.codeTable.readBytes(off, record, 0, entry.recordLen);
      for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
        corrections[k] =
            Float.intBitsToFloat(
                CodeRecord.readIntLE(record, entry.correctionBase + k * Float.BYTES));
      }
      entry.quantizer.decode(
          record, CodeRecord.codeOffset(), entry.dim, views.mean, corrections, rotatedValue);
      // Lossy reconstruction, so renormalize; the norm is not recoverable from the code.
      org.apache.lucene.util.VectorUtil.l2normalize(rotatedValue, false);
      // Back to the original space, which is what merging needs.
      entry.rotation.inverseRotate(rotatedValue, value);
      return value;
    }

    /** Per-instance scratch, so a scan over every ordinal allocates nothing per vector. */
    private final float[] rotatedValue;

    private final float[] corrections = new float[CodeRecord.CORRECTIONS];

    @Override
    public int ordToDoc(int ord) {
      return views.ordToDoc[ord];
    }

    @Override
    public FloatVectorValues copy() {
      // The shared state is the persisted map and the mmapped table, so this is fresh scratch.
      return new ReconstructedFloatVectorValues(entry, views);
    }

    @Override
    public org.apache.lucene.index.KnnVectorValues.DocIndexIterator iterator() {
      // Doc ids are not contiguous, so the iterator maps through ordToDoc.
      return createSparseIterator();
    }

    /**
     * The scorer {@code AbstractKnnVectorQuery}'s EXACT search runs on: a filter no wider than
     * {@code k}, or a fallback from the approximate search. Scores the stored vector, exact when
     * full precision was kept and the fine-code reconstruction otherwise, with the field's
     * similarity in the original space.
     */
    @Override
    public org.apache.lucene.search.VectorScorer scorer(float[] target) throws IOException {
      final ReconstructedFloatVectorValues values = (ReconstructedFloatVectorValues) copy();
      final org.apache.lucene.index.KnnVectorValues.DocIndexIterator it = values.iterator();
      final VectorSimilarityFunction similarity = entry.similarity;
      return new org.apache.lucene.search.VectorScorer() {
        @Override
        public float score() throws IOException {
          return similarity.compare(target, values.vectorValue(it.index()));
        }

        @Override
        public DocIdSetIterator iterator() {
          return it;
        }
      };
    }
  }
}
