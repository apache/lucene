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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes the ivfaster index: clusters the field's vectors, then emits the data sections and the
 * per-field metadata record.
 *
 * <p>See {@link IVFasterVectorsFormat} for the architecture and the in-RAM scope limit. The field's
 * float vectors are buffered in heap because the Lloyd mean reads them, as does the exact stage
 * that ranks each routing shortlist. The coarse scan that opens every routing pass reads the packed
 * 2-bit planes.
 *
 * @lucene.experimental
 */
final class IVFasterVectorsWriter extends KnnVectorsWriter {

  private final int nlist;
  private final int nprobe;
  private final int spillBits;
  private final float soarLambda;
  private final int lloydIters;

  /** The fine (rerank) tier, or {@code null} for exact FP32 rerank. */
  private final IVFasterVectorsFormat.FineTier fineTier;

  /**
   * Store the original FP32 vectors as an inert, search-unused section (merged by concatenation).
   */
  private final boolean keepFullPrecision;

  private final IndexOutput meta;
  private final IndexOutput data;
  private final Map<String, BufferedField> fields = new HashMap<>();
  private final List<BufferedField> ordered = new ArrayList<>();
  private boolean finished;

  /**
   * Whether a merge copies non-donor coarse planes verbatim from their source segment (default) or
   * re-encodes them from the fine-code reconstruction. A kill switch, so the copy's recall effect
   * is measurable against one binary; see the gate in {@code mergeOneField}.
   */
  private static final boolean COARSE_COPY =
      Boolean.parseBoolean(System.getProperty("ivfaster.coarseCopy", "true"));

  /** Diagnostic: per-stage wall time for the build, so a merge's cost can be attributed. */
  private static final boolean TRACE = Boolean.getBoolean("ivfaster.buildTrace");

  private static long traceStart() {
    return TRACE ? System.nanoTime() : 0L;
  }

  private static long traceStage(String what, int n, long t0) {
    if (TRACE) {
      IvfDiag.err(
          "[ivfaster-stage] %-16s n=%-9d %.3f s%n", what, n, (System.nanoTime() - t0) / 1e9);
      return System.nanoTime();
    }
    return 0L;
  }

  IVFasterVectorsWriter(
      SegmentWriteState state,
      int nlist,
      int nprobe,
      int spillBits,
      float soarLambda,
      int lloydIters,
      IVFasterVectorsFormat.FineTier fineTier,
      boolean keepFullPrecision)
      throws IOException {
    this.nlist = nlist;
    this.nprobe = nprobe;
    this.spillBits = spillBits;
    this.soarLambda = soarLambda;
    this.lloydIters = lloydIters;
    this.fineTier = fineTier;
    this.keepFullPrecision = keepFullPrecision;

    IndexOutput m = null;
    IndexOutput d = null;
    boolean success = false;
    try {
      m =
          state.directory.createOutput(
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name, state.segmentSuffix, META_EXTENSION),
              state.context);
      CodecUtil.writeIndexHeader(
          m, META_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      d =
          state.directory.createOutput(
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name, state.segmentSuffix, DATA_EXTENSION),
              state.context);
      CodecUtil.writeIndexHeader(
          d, DATA_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      this.meta = m;
      this.data = d;
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(m, d);
      }
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "ivfaster supports only FLOAT32 vectors, got " + fieldInfo.getVectorEncoding());
    }
    BufferedField f = new BufferedField(fieldInfo);
    fields.put(fieldInfo.name, f);
    ordered.add(f);
    return f;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (BufferedField f : ordered) {
      f.applySort(sortMap);
      // Flush: original-space vectors and no source segment, so every plane is encoded fresh.
      writeField(
          f.fieldInfo,
          f.vectors,
          f.docIds,
          f.size,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);
    }
  }

  /**
   * Merges one field, seeding the merged clustering from an incoming segment.
   *
   * <h2>Donor selection</h2>
   *
   * <p>The largest incoming segment by LIVE DOCUMENT COUNT donates its centroids, and the others'
   * are discarded. Ranking is on doc count rather than centroid count because every segment holding
   * at least {@code nlist} documents persists exactly {@code nlist} centroids, so centroid count
   * ties across all of them and the choice would degenerate to reader order.
   *
   * <p>Two elections run, and the FINE one is stricter. A verbatim fine-record copy needs the
   * matching fine encoding and a clustering no coarser than ours. A COARSE plane needs only a
   * matching rotation, which is a function of {@code dim} alone: the plane is a pure function of
   * the rotated vector and the coarse grid, with no mean and no fine encoding in it, and the reader
   * validated that grid at open, so every same-dim same-rotation segment donates its own plane
   * whoever wins the fine election.
   *
   * <h2>Gather</h2>
   *
   * <p>Vectors are needed for the Lloyd mean regardless; the donor buys keeping their cell, so no
   * re-routing, and copying rather than re-encoding their codes.
   *
   * <p>TWO PASSES, because reading the vectors is the expensive half and the half that
   * parallelizes. Pass 1 records only WHERE each live vector is, an ordinal-map lookup with no
   * decode; pass 2 splits that list over cores for the decode (int8 reconstruct, plus an inverse
   * FWHT for non-donors). Only the iterator walk is inherently sequential. Each worker takes its
   * OWN {@link FloatVectorValues} per reader via {@code copy()}, the standard Lucene contract, so
   * the shared state is the persisted ordinal map and the mmapped table read at absolute offsets.
   *
   * <p>The ordinal correspondence both tiers rely on: {@code DonorView} orders by ascending docId
   * and so does {@code getFloatVectorValues}, so {@code it.index()} is the source ordinal for
   * either.
   *
   * <p>THE DONOR'S VECTORS ARE GATHERED ALREADY-ROTATED, which skips a full FWHT round trip per
   * document. Reading through {@link FloatVectorValues} returns ORIGINAL-space vectors, so it
   * inverseRotates every document, and {@code writeField}'s first act is to normalize and rotate
   * straight back. The cancellation is exact, since R is orthogonal and {@code R(R^T x / ||R^T x||)
   * == x / ||x||}. These floats feed the Lloyd mean and the exact stage that ranks each routing
   * shortlist; the coarse scan reads the copied planes. Centroids are in rotated space, so rotated
   * space is what a merge wants.
   */
  @Override
  public IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "ivfaster supports only FLOAT32 vectors, got " + fieldInfo.getVectorEncoding());
    }
    final int dim = fieldInfo.getVectorDimension();

    // ---- DONOR SELECTION ----
    IVFasterVectorsReader.DonorView donor = null;
    int donorIndex = -1;
    int donorDocs = -1;
    // A donor whose clustering is much coarser than ours is not worth seeding from.
    final int minDonorCells = Math.max(1, nlist / 2);
    // A fine record can be copied verbatim only from a source in OUR configured fine encoding.
    final byte fineEncodingId = fineQuantizer().encodingId();
    // COARSE PLANE SOURCES, indexed by reader; broader than the fine-donor election.
    final IVFasterVectorsReader.DonorView[] coarseViews =
        new IVFasterVectorsReader.DonorView[mergeState.knnVectorsReaders.length];
    for (int r = 0; r < mergeState.knnVectorsReaders.length; r++) {
      final var reader = mergeState.knnVectorsReaders[r];
      if (reader == null) {
        continue;
      }
      if (reader.unwrapReaderForField(fieldInfo.name) instanceof IVFasterVectorsReader ivf) {
        final IVFasterVectorsReader.DonorView view = ivf.donorView(fieldInfo.name);
        if (view == null || view.dim() != dim) {
          continue;
        }
        // Coarse-compatible iff the rotation matches; see the javadoc.
        if (view.rotationSeed() != rotationSeed(dim)) {
          continue;
        }
        // Gates ONLY the coarse-source registration; see COARSE_COPY.
        if (COARSE_COPY) {
          coarseViews[r] = view;
        }
        // The stricter fine-donor election; see the javadoc.
        if (view.nlist() >= minDonorCells
            && view.encodingId() == fineEncodingId
            && view.count() > donorDocs) {
          donorDocs = view.count();
          donor = view;
          donorIndex = r;
        }
      }
    }

    // ---- GATHER ---- (two passes; see the javadoc)
    final IntArrayList srcReader = new IntArrayList();
    final IntArrayList srcOrd = new IntArrayList();
    final IntArrayList docs = new IntArrayList();
    // Carried cell per gathered document, or -1 to route it.
    final IntArrayList carried = new IntArrayList();
    // Donor ordinal per gathered document, or -1; the key to copying its FINE record verbatim.
    final IntArrayList donorOrd = new IntArrayList();
    // COARSE source reader index per gathered document, or -1 to encode, and the ordinal in it.
    final IntArrayList coarseSrc = new IntArrayList();
    final IntArrayList coarseOrd = new IntArrayList();

    final boolean donorRotated = donor != null && donor.canReadRotated();

    long t = traceStart();
    // Per-reader values for the parallel pass to copy() from; null where a reader has no vectors.
    final FloatVectorValues[] readerValues =
        new FloatVectorValues[mergeState.knnVectorsReaders.length];
    for (int r = 0; r < mergeState.knnVectorsReaders.length; r++) {
      final var reader = mergeState.knnVectorsReaders[r];
      if (reader == null) {
        continue;
      }
      final FloatVectorValues values = reader.getFloatVectorValues(fieldInfo.name);
      if (values == null) {
        continue;
      }
      readerValues[r] = values;
      final boolean isDonor = r == donorIndex;
      final IVFasterVectorsReader.DonorView coarseView = coarseViews[r];
      final org.apache.lucene.index.MergeState.DocMap docMap = mergeState.docMaps[r];
      final KnnVectorValues.DocIndexIterator it = values.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        final int newDoc = docMap.get(doc);
        if (newDoc == -1) {
          continue; // deleted
        }
        final int ord = it.index();
        srcReader.add(r);
        srcOrd.add(ord);
        docs.add(newDoc);
        carried.add(isDonor ? donor.cellOf(ord) : -1);
        donorOrd.add(isDonor ? ord : -1);
        coarseSrc.add(coarseView != null ? r : -1);
        coarseOrd.add(coarseView != null ? ord : -1);
      }
    }
    final int gatheredCount = docs.size();
    t = traceStage("gatherIndex", gatheredCount, t);

    // Pass 2: read the vectors, one worker per range; see the javadoc.
    final float[][] gathered = new float[gatheredCount][];
    final int[] srcReaderA = srcReader.toArray();
    final int[] srcOrdA = srcOrd.toArray();
    final int nReaders = readerValues.length;
    // Effectively final, so the worker body can capture them.
    final IVFasterVectorsReader.DonorView donorFinal = donor;
    final int donorIndexFinal = donorIndex;
    Parallel.overRange(
        gatheredCount,
        (from, to) -> {
          // Lazily, because a worker's range usually spans one or two source segments.
          final FloatVectorValues[] local = new FloatVectorValues[nReaders];
          final byte[] donorRecord = donorRotated ? donorFinal.newRecordScratch() : null;
          final float[] donorCorrections = new float[CodeRecord.CORRECTIONS];
          for (int i = from; i < to; i++) {
            final int r = srcReaderA[i];
            final int ord = srcOrdA[i];
            if (donorRotated && r == donorIndexFinal) {
              final float[] v = new float[dim];
              donorFinal.rotatedVector(ord, v, donorRecord, donorCorrections);
              gathered[i] = v;
              continue;
            }
            FloatVectorValues lv = local[r];
            if (lv == null) {
              lv = local[r] = readerValues[r].copy();
            }
            gathered[i] = ArrayUtil.copyOfSubArray(lv.vectorValue(ord), 0, dim);
          }
        });

    if (TRACE) {
      IvfDiag.err(
          "[ivfaster-stage] merge readers=%d donorIndex=%d donorDocs=%d donorRotated=%b gathered=%d%n",
          mergeState.knnVectorsReaders.length, donorIndex, donorDocs, donorRotated, gatheredCount);
    }
    t = traceStage("gatherRead", gatheredCount, t);

    // Which gathered vectors are ALREADY rotated, so writeField does not rotate them twice.
    final boolean[] preRotated = new boolean[gatheredCount];
    if (donorRotated) {
      final int[] ords = donorOrd.toArray();
      for (int i = 0; i < preRotated.length; i++) {
        preRotated[i] = ords[i] >= 0;
      }
    }

    writeField(
        fieldInfo,
        gathered,
        docs.toArray(),
        gatheredCount,
        donor == null ? null : donor.centroids(),
        donor,
        carried.toArray(),
        donorOrd.toArray(),
        donor == null ? null : donor.mean(),
        preRotated,
        coarseViews,
        coarseSrc.toArray(),
        coarseOrd.toArray());
    // No deferred phase: this writer emits everything for the field inline.
    return null;
  }

  /**
   * Clusters and writes one field.
   *
   * @param seed centroids to warm-start clustering from, or {@code null} to train fresh ones
   * @param donor the segment donating those centroids, or {@code null}; its documents' codes are
   *     copied verbatim instead of being re-encoded
   * @param carried per-document donor cell, or -1 to route; {@code null} when there is no donor
   * @param donorOrd per-document donor vector ordinal, or -1; {@code null} when there is no donor
   * @param donorMean the donor's persisted mean, INHERITED rather than recomputed so that copied
   *     codes stay on the grid they were packed against; {@code null} on a fresh segment
   * @param preRotated per-document flag marking vectors ALREADY in rotated, unit-length space
   *     (donor documents read through {@code DonorView.rotatedVector}); {@code null} means none
   *     are. Rotating a rotated vector yields a plausible vector in the wrong space and raises
   *     nothing, so this is correctness-critical.
   * @param coarseViews coarse plane sources indexed by {@code coarseSrc}, or {@code null}; broader
   *     than {@code donor} because the coarse plane is encoding-independent, so any same-dim,
   *     same-rotation segment donates it
   * @param coarseSrc per-document index into {@code coarseViews}, or -1 to encode from the rotated
   *     vector; {@code null} encodes everything
   *     <h2>Pipeline</h2>
   *     <p>Normalize and rotate, cluster, then emit the sections. The rotation is the same in every
   *     segment, derived from {@code dim} alone, so codes and centroids from different segments are
   *     directly comparable at merge. The rotate pass is parallel over documents: {@link
   *     HadamardRotation#rotate} is stateless, reading only final fields and writing only its
   *     {@code out} argument, so one instance is safely shared across workers.
   *     <p>NORMALIZED UNCONDITIONALLY, for every similarity. Once every vector is unit length,
   *     squared Euclidean is an affine function of the dot product, so all four similarities rank
   *     identically and every distance in this codec reduces to one dot: no per-similarity branch
   *     in any inner loop, and no norms to store or recompute. The rotation is orthogonal, so it
   *     preserves the unit length it is given. Centroids hold the same invariant, which {@code
   *     Clustering#normalize} maintains for every similarity, so the reduction covers cell
   *     selection as well as document scoring.
   *     <p>ADOPTING A DONOR MEANS ADOPTING ITS CELL COUNT. A carried assignment is meaningful only
   *     because seed centroid {@code c} IS donor cell {@code c}, so the seed fixes the cell space
   *     and clustering into a different number of cells would leave carried ids pointing outside
   *     it. The configured {@code nlist} governs fresh segments, merges inherit, and the donor gate
   *     keeps the inherited value within a factor of two. Donor documents therefore start from
   *     their carried cell, and only the other segments' documents are routed from scratch; the
   *     Reaper corrects any donor document the refined centroids moved away from.
   *     <p>Coarse planes are packed ONCE for the whole build: clustering routes from them, and the
   *     coarse section is written straight out of the same buffer, so there is exactly one
   *     derivation of every document's code.
   *     <h2>Section invariants</h2>
   *     <p>THE DOCUMENT MEAN MUST BE SEGMENT-INDEPENDENT, because merge copies codes VERBATIM: the
   *     donor's codes were packed against the donor's mean, so decoding them against a freshly
   *     averaged one would score copied documents on a grid they were never quantized to, silently
   *     and compounding per merge. A merge INHERITS the donor's mean as it inherits the cell count,
   *     and a fresh segment computes it once. It is computed before the code table, since encode
   *     derives its grid from the mean and must derive it exactly as the query side will.
   *     <p>SLOTS ARE GROUPED BY CELL unconditionally, whether or not the field spills, because
   *     every fast path depends on a probed cell's records being one contiguous run. Under spill a
   *     document occupies one slot per chosen cell, and its record is emitted once per slot,
   *     byte-identical across the copies, with the primary-cell field naming the PRIMARY cell in
   *     every copy, which is how the reader tells a primary from a spill. That per-slot emission of
   *     both the code table and the coarse planes is the dominant term in index size; see {@link
   *     IVFasterVectorsFormat#DEFAULT_SPILL_BITS}.
   *     <p>THE POSTING DIRECTORY IS OFFSETS ONLY. Cell {@code c} is the contiguous slot range
   *     {@code [postingOffsets[c], postingOffsets[c+1])}, so the slot ordinals themselves are the
   *     ascending integers that range already names. Offsets are BYTE offsets ({@code slot *
   *     Integer.BYTES}) and the reader divides back, so that scale is part of the on-disk contract.
   *     <p>The document-ordered SLOT MAP is persisted because the writer already knows it: the code
   *     table is in CELL order and holds several slots per document under spill, so a reader given
   *     only the table would have to scan and sort to recover it, inside {@code
   *     getFloatVectorValues}, which Lucene calls once per query.
   *     <p>The RAW FP32 section is inert: read only by {@code getFloatVectorValues}, and so by
   *     merge, which gathers every document through it. It is stored in VECTOR-ORDINAL order, so
   *     {@code getFloatVectorValues(ord)} maps straight to {@code raw[ord]}, and it holds the
   *     caller's exact input, since the normalize and rotate step copies into scratch. Zero-length
   *     when off, which the reader reads as "reconstruct from the fine code".
   * @param coarseOrd per-document ordinal within its coarse source, or -1; {@code null} encodes
   *     everything
   */
  private void writeField(
      FieldInfo fieldInfo,
      float[][] vectors,
      int[] docIds,
      int count,
      float[][] seed,
      IVFasterVectorsReader.DonorView donor,
      int[] carried,
      int[] donorOrd,
      float[] donorMean,
      boolean[] preRotated,
      IVFasterVectorsReader.DonorView[] coarseViews,
      int[] coarseSrc,
      int[] coarseOrd)
      throws IOException {

    final int dim = fieldInfo.getVectorDimension();
    final VectorSimilarityFunction sim = fieldInfo.getVectorSimilarityFunction();
    final FineQuantizer quantizer = fineQuantizer();

    if (count == 0) {
      writeEmptyField(fieldInfo, dim, sim, quantizer);
      return;
    }

    // 1. Normalize and rotate, in parallel over documents.
    long t = traceStart();
    final HadamardRotation rotation = HadamardRotation.create(dim, rotationSeed(dim));
    final float[][] rotated = new float[count][];
    Parallel.overRange(
        count,
        (from, to) -> {
          final float[] v = new float[dim];
          for (int i = from; i < to; i++) {
            if (preRotated != null && preRotated[i]) {
              // Already rotated and unit length; rotating again would change space silently.
              rotated[i] = vectors[i];
              continue;
            }
            System.arraycopy(vectors[i], 0, v, 0, dim);
            VectorUtil.l2normalize(v);
            rotated[i] = new float[dim];
            // Reads `v` and writes only rotated[i], which this worker owns.
            rotation.rotate(v, rotated[i]);
          }
        });

    // 2. Cluster: exhaustive routing, Lloyd iterations, the Reaper. A seed fixes the cell space.
    final int nlistActual = seed != null ? seed.length : Math.min(nlist, Math.max(1, count));
    final int[] seedAssignment = seed != null ? carried : null;
    t = traceStage("rotate", count, t);
    final DocPlanes planes =
        DocPlanes.encode(rotated, count, dim, coarseViews, coarseSrc, coarseOrd);
    t = traceStage("docPlanes", count, t);
    final Clustering.Result cl =
        Clustering.cluster(
            rotated,
            count,
            dim,
            nlistActual,
            lloydIters,
            sim,
            seed,
            seedAssignment,
            spillBits,
            soarLambda,
            planes);
    t = traceStage("cluster", nlistActual, t);

    // Coarse-retention audit; see Clustering.EXACT_PLACEMENT_AUDIT.
    if (Clustering.EXACT_PLACEMENT_AUDIT && count > 0) {
      IvfDiag.err(
          "[ivfaster] field=%s count=%d nlist=%d primariesMisplaced=%d (%.4f)%n",
          fieldInfo.name,
          count,
          nlistActual,
          cl.primariesMoved,
          (double) cl.primariesMoved / count);
    }

    // 3. Spill fan-out came from clustering, as the Reaper's other output.
    final int[][] cellsPerDoc = cl.cells;
    int totalSlots = 0;
    for (int i = 0; i < count; i++) {
      totalSlots += cellsPerDoc[i].length;
    }

    // 4. Cell-order layout; see the javadoc.
    final int[] cellStart = new int[nlistActual + 1];
    for (int i = 0; i < count; i++) {
      for (int c : cellsPerDoc[i]) {
        cellStart[c + 1]++;
      }
    }
    for (int c = 0; c < nlistActual; c++) {
      cellStart[c + 1] += cellStart[c];
    }
    final int[] slotDoc = new int[totalSlots];
    {
      final int[] cursor = new int[nlistActual];
      for (int i = 0; i < count; i++) {
        for (int c : cellsPerDoc[i]) {
          slotDoc[cellStart[c] + cursor[c]++] = i;
        }
      }
    }

    // ---- sections ----
    final int codeBytes = quantizer.codeBytes(dim);
    final int recordLen = CodeRecord.length(codeBytes);

    // S1. centroid float matrix
    final long centroidsOffset = data.getFilePointer();
    for (int c = 0; c < nlistActual; c++) {
      for (int d = 0; d < dim; d++) {
        data.writeInt(Float.floatToIntBits(cl.centroids[c][d]));
      }
    }
    final long centroidsLength = data.getFilePointer() - centroidsOffset;

    // S2. Document mean, when the fine tier centres its codes; inherited at merge, see the javadoc.
    final float[] docMean;
    if (quantizer.needsMean() == false) {
      docMean = null;
    } else if (donorMean != null) {
      docMean = donorMean;
    } else {
      docMean = new float[dim];
      for (int i = 0; i < count; i++) {
        for (int d = 0; d < dim; d++) {
          docMean[d] += rotated[i][d];
        }
      }
      for (int d = 0; d < dim; d++) {
        docMean[d] /= count;
      }
    }

    final long meanOffset = data.getFilePointer();
    long meanLength = 0;
    if (docMean != null) {
      for (int d = 0; d < dim; d++) {
        data.writeInt(Float.floatToIntBits(docMean[d]));
      }
      meanLength = data.getFilePointer() - meanOffset;
    }

    // S3. code table, in cell order: [code][docId][primaryCell][4 correction floats]
    final long codeTableOffset = data.getFilePointer();
    {
      // Donor records are COPIED, everyone else's encoded; see DonorView.copyRecord.
      final byte[][] records = new byte[count][];
      Parallel.overRange(
          count,
          (from, to) -> {
            final byte[] code = new byte[codeBytes];
            final float[] corrections = new float[4];
            // Centred for ENCODING only, so this must never write back into `rotated`.
            final float[] centred = docMean == null ? null : new float[dim];
            for (int i = from; i < to; i++) {
              records[i] = new byte[recordLen];
              if (donorOrd != null && donorOrd[i] >= 0) {
                donor.copyRecord(donorOrd[i], docIds[i], cellsPerDoc[i][0], records[i]);
                continue;
              }
              float[] toEncode = rotated[i];
              if (docMean != null) {
                for (int d = 0; d < dim; d++) {
                  centred[d] = rotated[i][d] - docMean[d];
                }
                toEncode = centred;
              }
              quantizer.encode(toEncode, dim, docMean, code, corrections);
              System.arraycopy(code, 0, records[i], CodeRecord.codeOffset(), codeBytes);
              CodeRecord.writeIntLE(records[i], CodeRecord.docIdOffset(codeBytes), docIds[i]);
              CodeRecord.writeIntLE(
                  records[i], CodeRecord.primaryCellOffset(codeBytes), cellsPerDoc[i][0]);
              for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
                CodeRecord.writeIntLE(
                    records[i],
                    CodeRecord.correctionOffset(codeBytes, k),
                    Float.floatToIntBits(corrections[k]));
              }
            }
          });
      for (int s = 0; s < totalSlots; s++) {
        final int i = slotDoc[s];
        // Every copy names the PRIMARY cell; see the javadoc.
        CodeRecord.writeIntLE(
            records[i], CodeRecord.primaryCellOffset(codeBytes), cellsPerDoc[i][0]);
        data.writeBytes(records[i], 0, recordLen);
      }
    }
    final long codeTableLength = data.getFilePointer() - codeTableOffset;
    t = traceStage("codeTable", totalSlots, t);

    // S4. Coarse codes, one record per slot, in slot order; a sequential copy of the packed
    // payload.
    final byte[] planeBuf = planes.buffer();
    final int coarseBytes = Nitrox2.bytesPerVector(dim);
    final long coarseOffset = data.getFilePointer();
    for (int s = 0; s < totalSlots; s++) {
      data.writeBytes(planeBuf, planes.offset(slotDoc[s]), coarseBytes);
    }
    final long coarseLength = data.getFilePointer() - coarseOffset;
    t = traceStage("coarseSection", totalSlots, t);

    // S5. Per-cell slot directory, as BYTE offsets; see the javadoc. Computed here, emitted at S9.
    final long[] postingOffsets = new long[nlistActual + 1];
    for (int c = 0; c <= nlistActual; c++) {
      postingOffsets[c] = (long) cellStart[c] * Integer.BYTES;
    }

    // S6. Centroid graph, built from the FINAL centroids, so after clustering. Zero length means
    // the reader selects cells by exact scan.
    final long graphOffset = data.getFilePointer();
    long graphLength = 0;
    if (nlistActual > 1) {
      // Coarse-only: graph construction never scores the fine tier.
      final CentroidCodes graphCodes = new CentroidCodes(cl.centroids, dim, sim, null);
      CentroidGraph.build(graphCodes, dim).write(data);
      graphLength = data.getFilePointer() - graphOffset;
    }
    t = traceStage("centroidGraph", nlistActual, t);

    // S7. Document-ordered slot map: per vector ordinal, the slot holding its PRIMARY copy.
    final long ordToSlotOffset = data.getFilePointer();
    {
      final int[] primarySlot = new int[count];
      // Cell by cell, so a slot's cell is known without searching.
      for (int c = 0; c < nlistActual; c++) {
        for (int s = cellStart[c]; s < cellStart[c + 1]; s++) {
          final int i = slotDoc[s];
          if (cellsPerDoc[i][0] == c) {
            primarySlot[i] = s;
          }
        }
      }
      for (int i = 0; i < count; i++) {
        data.writeInt(primarySlot[i]);
      }
    }
    final long ordToSlotLength = data.getFilePointer() - ordToSlotOffset;

    // S8. Full-precision vectors, in VECTOR-ORDINAL order; see the javadoc.
    final long rawOffset = data.getFilePointer();
    if (keepFullPrecision) {
      for (int i = 0; i < count; i++) {
        final float[] vec = vectors[i];
        for (int d = 0; d < dim; d++) {
          data.writeInt(Float.floatToIntBits(vec[d]));
        }
      }
    }
    final long rawLength = data.getFilePointer() - rawOffset;

    // S9. posting offset directory, the S5 offsets, written last
    final long offsetsDataStart = data.getFilePointer();

    final Sections sec = new Sections();
    sec.centroidsOffset = centroidsOffset;
    sec.centroidsLength = centroidsLength;
    sec.meanOffset = meanOffset;
    sec.meanLength = meanLength;
    sec.codeTableOffset = codeTableOffset;
    sec.codeTableLength = codeTableLength;
    sec.coarseOffset = coarseOffset;
    sec.coarseLength = coarseLength;
    sec.graphOffset = graphOffset;
    sec.graphLength = graphLength;
    sec.ordToSlotOffset = ordToSlotOffset;
    sec.ordToSlotLength = ordToSlotLength;
    sec.rawOffset = rawOffset;
    sec.rawLength = rawLength;
    sec.offsetsDataStart = offsetsDataStart;
    sec.postingOffsets = postingOffsets;
    writeMeta(fieldInfo, dim, sim, quantizer, nlistActual, count, sec);
    traceStage("tailSections", count, t);
  }

  /** A field with no vectors still needs a meta record, so the reader can report zero results. */
  private void writeEmptyField(
      FieldInfo fieldInfo, int dim, VectorSimilarityFunction sim, FineQuantizer quantizer)
      throws IOException {
    final long here = data.getFilePointer();
    final Sections sec = new Sections();
    // Every section is empty at the current position, so the reader reports no results.
    sec.centroidsOffset = here;
    sec.meanOffset = here;
    sec.codeTableOffset = here;
    sec.coarseOffset = here;
    sec.graphOffset = here;
    sec.ordToSlotOffset = here;
    sec.rawOffset = here;
    sec.offsetsDataStart = here;
    writeMeta(fieldInfo, dim, sim, quantizer, 0, 0, sec);
  }

  /**
   * The data file's section table for one field.
   *
   * <p>The sections are seven interchangeable {@code long} offset and length pairs, so a positional
   * argument list would still compile after a section is dropped or reordered. This holder assigns
   * every field by name, so a mismatch cannot survive.
   */
  private static final class Sections {
    long centroidsOffset, centroidsLength;
    long meanOffset, meanLength;
    long codeTableOffset, codeTableLength;
    long coarseOffset, coarseLength;
    long graphOffset, graphLength;
    long ordToSlotOffset, ordToSlotLength;

    /** Raw FP32 vectors in ordinal order; {@code rawLength == 0} means "not stored". */
    long rawOffset, rawLength;

    long offsetsDataStart;
    long[] postingOffsets = {0};
  }

  /**
   * Writes one field's metadata record, in the order the reader reads it.
   *
   * <p>BOTH COARSE GRID PARAMETERS ARE PERSISTED. They are compile-time constants, so nothing
   * varies them at runtime today, but they fix the grid the codes were PACKED on:
   *
   * <ul>
   *   <li>the grid HALF-WIDTH, since a reader assuming a different one would quantize queries into
   *       buckets that were never encoded, which raises no error and only shifts recall;
   *   <li>the PLANE COUNT, which sets how many bytes a coarse code occupies, so a reader assuming a
   *       different one would misread every record boundary.
   * </ul>
   *
   * <p>Persisting them turns a later edit to either constant into a loud mismatch at open.
   *
   * <p>The raw FP32 offset and length are written unconditionally, so the on-disk shape does not
   * depend on {@code keepFullPrecision}; the flag only controls whether the length is nonzero, and
   * {@code rawLength == 0} tells the reader to reconstruct from the fine code.
   */
  private void writeMeta(
      FieldInfo fieldInfo,
      int dim,
      VectorSimilarityFunction sim,
      FineQuantizer quantizer,
      int nlistActual,
      int count,
      Sections sec)
      throws IOException {

    meta.writeInt(fieldInfo.number);
    meta.writeByte(quantizer.encodingId());
    meta.writeInt(sim.ordinal());
    meta.writeVInt(dim);
    meta.writeVInt(nlistActual);
    meta.writeVInt(count);
    meta.writeLong(rotationSeed(dim));
    meta.writeVInt(nprobe);
    meta.writeVInt(spillBits);
    meta.writeInt(Float.floatToIntBits(Nitrox2.CLIP_SIGMA));
    meta.writeVInt(Nitrox2.PLANES);
    meta.writeVLong(sec.centroidsOffset);
    meta.writeVLong(sec.centroidsLength);
    meta.writeVLong(sec.meanOffset);
    meta.writeVLong(sec.meanLength);
    meta.writeVLong(sec.codeTableOffset);
    meta.writeVLong(sec.codeTableLength);
    meta.writeVLong(sec.coarseOffset);
    meta.writeVLong(sec.coarseLength);
    meta.writeVLong(sec.graphOffset);
    meta.writeVLong(sec.graphLength);
    meta.writeVLong(sec.ordToSlotOffset);
    meta.writeVLong(sec.ordToSlotLength);
    meta.writeVLong(sec.rawOffset);
    meta.writeVLong(sec.rawLength);
    meta.writeVLong(sec.offsetsDataStart);

    if (nlistActual > 0) {
      DirectMonotonicWriter offsets =
          DirectMonotonicWriter.getInstance(
              meta, data, nlistActual + 1, IVFasterVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
      for (int c = 0; c <= nlistActual; c++) {
        offsets.add(sec.postingOffsets[c]);
      }
      offsets.finish();
    }
  }

  /**
   * The fine tier to encode with, from the format's configured {@link
   * IVFasterVectorsFormat.FineTier}.
   *
   * <p>A WRITE-TIME choice: it determines every code byte. The reader dispatches on the persisted
   * encoding id and never consults the enum, so a segment is self-describing.
   *
   * <p>A {@code null} fine tier means EXACT FP32 rerank: the fine code is the rotated float vector,
   * scored by a plain float dot through the same rerank machinery as the quantized tiers, with a
   * {@code 4*dim} code.
   */
  FineQuantizer fineQuantizer() {
    if (fineTier == null) {
      return new Fp32Quantizer();
    }
    return switch (fineTier) {
      case INT8 -> new Int8Quantizer();
    };
  }

  /**
   * The rotation seed, a function of {@code dim} alone.
   *
   * <p>Every segment of a given dimension therefore rotates identically, which is what lets one
   * segment's centroids seed another's clustering at merge. A per-segment seed would make codes
   * from different segments incomparable.
   */
  static long rotationSeed(int dim) {
    return 0x9E3779B97F4A7C15L ^ dim;
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    meta.writeInt(-1);
    CodecUtil.writeFooter(meta);
    CodecUtil.writeFooter(data);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, data);
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (BufferedField f : ordered) {
      total += f.ramBytesUsed();
    }
    return total;
  }

  /**
   * Buffers a field's vectors until flush.
   *
   * <p>Parallel arrays rather than a list of objects, so a buffered document costs its vector plus
   * two array slots.
   */
  private static final class BufferedField extends KnnFieldVectorsWriter<float[]> {

    private final FieldInfo fieldInfo;
    private final int dim;
    float[][] vectors = new float[16][];
    int[] docIds = new int[16];
    int size;
    private int lastDocId = -1;

    BufferedField(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
    }

    @Override
    public void addValue(int docID, float[] value) throws IOException {
      if (docID == lastDocId) {
        throw new IllegalArgumentException(
            "field \""
                + fieldInfo.name
                + "\" appears more than once in document "
                + docID
                + "; only one value per field per document is supported");
      }
      if (size == vectors.length) {
        vectors = ArrayUtil.grow(vectors, size + 1);
        docIds = ArrayUtil.growExact(docIds, vectors.length);
      }
      // Copy: the caller may reuse its array.
      vectors[size] = ArrayUtil.copyOfSubArray(value, 0, dim);
      docIds[size] = docID;
      size++;
      lastDocId = docID;
    }

    @Override
    public float[] copyValue(float[] value) {
      return ArrayUtil.copyOfSubArray(value, 0, dim);
    }

    /** Rewrites doc ids through an index sort, keeping them ascending. */
    void applySort(Sorter.DocMap sortMap) {
      if (sortMap == null) {
        return;
      }
      for (int i = 0; i < size; i++) {
        docIds[i] = sortMap.oldToNew(docIds[i]);
      }
      // Cell order makes vector order irrelevant, but the doc ids must be the NEW ones.
    }

    @Override
    public long ramBytesUsed() {
      if (size == 0) {
        return 0;
      }
      return RamUsageEstimator.NUM_BYTES_OBJECT_REF * (long) vectors.length
          + (long) size * (RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) dim * Float.BYTES)
          + (long) docIds.length * Integer.BYTES;
    }
  }
}
