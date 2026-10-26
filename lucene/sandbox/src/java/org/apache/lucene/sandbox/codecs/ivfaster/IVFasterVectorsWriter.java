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
import org.apache.lucene.index.DocIDMerger;
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
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes the ivfaster index: clusters the field's vectors, then emits the data sections and the
 * per-field metadata record.
 *
 * <p>See {@link IVFasterVectorsFormat} for the architecture. THE CORPUS IS NEVER HELD IN HEAP: a
 * flush drains its buffered vectors into a staged temp file, a merge stages its sources' records
 * into one, and clustering streams that file through per-thread cursors while keeping a few dozen
 * bytes per document; see {@link StagedVectors} and {@link Clustering}.
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

  /** Where the staging temp files live; see {@link StagedVectors}. */
  private final org.apache.lucene.store.Directory directory;

  private final String segmentName;
  private final org.apache.lucene.store.IOContext context;
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
    this.directory = state.directory;
    this.segmentName = state.segmentInfo.name;
    this.context = state.context;

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
      writeFlushedField(f);
      f.release();
    }
  }

  /**
   * Stages a flushed field's buffered vectors, in doc order, and writes the field from the staged
   * records.
   *
   * <p>The buffer is the one copy of the corpus this writer ever holds in heap, and IndexWriter
   * bounded it. Each chunk's floats are dropped as soon as the chunk is staged, so the buffer
   * drains as the temp file fills, and clustering then runs over the file; see {@link
   * StagedVectors}.
   *
   * <p>An index sort is the one case the buffer is not already in doc order; the permutation is
   * applied by reading through {@code order} rather than by reordering the buffer.
   */
  private void writeFlushedField(BufferedField f) throws IOException {
    final FieldInfo fieldInfo = f.fieldInfo;
    final int dim = fieldInfo.getVectorDimension();
    final VectorSimilarityFunction sim = fieldInfo.getVectorSimilarityFunction();
    final FineQuantizer quantizer = fineQuantizer();
    final int count = f.size;
    if (count == 0) {
      writeEmptyField(fieldInfo, dim, sim, quantizer);
      return;
    }
    long t = traceStart();
    final int[] order = isAscending(f.docIds, count) ? null : docOrder(f.docIds, count);
    final HadamardRotation rotation = HadamardRotation.create(dim, rotationSeed(dim));
    // The document mean, when the tier centres: one pass before any code is derived from it.
    final float[] docMean =
        quantizer.needsMean() ? meanOfRotated(f.vectors, count, dim, rotation) : null;
    final StagedVectors.Builder b =
        StagedVectors.begin(
            directory, segmentName, context, dim, quantizer, docMean, keepFullPrecision);
    final StagedVectors staged;
    try {
      final int chunkOrds = Math.min(count, StagedVectors.CHUNK_ORDS);
      final byte[] chunk = b.chunk(chunkOrds);
      final float[] raw = b.rawChunk(chunkOrds);
      final int stride = b.stride;
      final float[][] vectors = f.vectors;
      final int[] docIds = f.docIds;
      for (int start = 0; start < count; start += chunkOrds) {
        final int n = Math.min(chunkOrds, count - start);
        final int base = start;
        Parallel.overRange(
            n,
            (lo, hi) -> {
              final float[] unit = new float[dim];
              final float[] rot = new float[dim];
              final StagedVectors.Builder.EncodeScratch sc = b.encodeScratch();
              for (int j = lo; j < hi; j++) {
                final int i = order == null ? base + j : order[base + j];
                final float[] v = vectors[i];
                // Normalized for every similarity, then rotated; see the writeField javadoc.
                System.arraycopy(v, 0, unit, 0, dim);
                VectorUtil.l2normalize(unit);
                rotation.rotate(unit, rot);
                b.encodeInto(rot, docIds[i], chunk, j * stride, sc);
                if (raw != null) {
                  // The caller's exact input, for the inert full-precision section.
                  System.arraycopy(v, 0, raw, j * dim, dim);
                }
              }
            });
        b.writeChunk(chunk, raw, n);
        // Drain the buffer behind the chunk just staged.
        for (int j = 0; j < n; j++) {
          vectors[order == null ? base + j : order[base + j]] = null;
        }
      }
      staged = b.finish();
    } catch (Throwable e) {
      b.abort();
      throw e;
    }
    traceStage("stage", count, t);
    try (staged) {
      writeField(fieldInfo, staged, null, null, docMean);
    }
  }

  /** The mean of the normalized, rotated vectors; only a centring fine tier needs it. */
  private static float[] meanOfRotated(
      float[][] vectors, int count, int dim, HadamardRotation rotation) throws IOException {
    final double[] acc = new double[dim];
    Parallel.overRange(
        count,
        (lo, hi) -> {
          final double[] local = new double[dim];
          final float[] unit = new float[dim];
          final float[] rot = new float[dim];
          for (int i = lo; i < hi; i++) {
            System.arraycopy(vectors[i], 0, unit, 0, dim);
            VectorUtil.l2normalize(unit);
            rotation.rotate(unit, rot);
            for (int d = 0; d < dim; d++) {
              local[d] += rot[d];
            }
          }
          synchronized (acc) {
            for (int d = 0; d < dim; d++) {
              acc[d] += local[d];
            }
          }
        });
    final float[] mean = new float[dim];
    for (int d = 0; d < dim; d++) {
      mean[d] = (float) (acc[d] / count);
    }
    return mean;
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
   * <h2>Staging</h2>
   *
   * <p>The merged corpus is staged to a temp file in one walk of a {@link DocIDMerger}, which
   * yields every live document in ascending merged doc id, index sort included. Each chunk of that
   * walk is encoded in parallel: a donor document's fine record and coarse planes are copied
   * verbatim, another ivfaster segment's document is decoded straight from its rotated int8 record
   * (no FWHT round trip) and re-encoded, with its coarse planes copied verbatim, and any other
   * source is read through {@link FloatVectorValues}, normalized and rotated. What stays in heap
   * per document is its carried cell; the vectors live in the temp file, and clustering streams
   * them from there. See {@link StagedVectors}.
   *
   * <p>The ordinal correspondence both tiers rely on: {@code DonorView} orders by ascending docId
   * and so does {@code getFloatVectorValues}, so {@code it.index()} is the source ordinal for
   * either.
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

    // ---- STAGING ---- (one DocIDMerger walk, encoded in chunks; see the javadoc)
    final FineQuantizer quantizer = fineQuantizer();
    // Stateless and a function of dim alone, so one instance serves every worker.
    final HadamardRotation rotation = HadamardRotation.create(dim, rotationSeed(dim));
    final float[] donorMean = donor == null ? null : donor.mean();
    final float[] docMean;
    if (quantizer.needsMean() == false) {
      docMean = null;
    } else if (donorMean != null) {
      // Inherited, so copied codes stay on the grid they were packed against; see writeField.
      docMean = donorMean;
    } else {
      docMean = mergedMean(fieldInfo, mergeState, dim, rotation);
    }

    long t = traceStart();
    final FloatVectorValues[] readerValues =
        new FloatVectorValues[mergeState.knnVectorsReaders.length];
    final List<MergeSub> subs = new ArrayList<>();
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
      subs.add(new MergeSub(mergeState.docMaps[r], values.iterator(), r));
    }
    final DocIDMerger<MergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);

    final IntArrayList carried = new IntArrayList();
    final StagedVectors.Builder b =
        StagedVectors.begin(
            directory, segmentName, context, dim, quantizer, docMean, keepFullPrecision);
    final StagedVectors staged;
    final IVFasterVectorsReader.DonorView donorFinal = donor;
    final int donorIndexFinal = donorIndex;
    try {
      final int chunkOrds = StagedVectors.CHUNK_ORDS;
      final byte[] chunk = b.chunk(chunkOrds);
      final float[] raw = b.rawChunk(chunkOrds);
      final int stride = b.stride;
      final int[] chunkReader = new int[chunkOrds];
      final int[] chunkOrd = new int[chunkOrds];
      final int[] chunkDoc = new int[chunkOrds];
      final int nReaders = readerValues.length;
      boolean exhausted = false;
      while (exhausted == false) {
        int n = 0;
        while (n < chunkOrds) {
          final MergeSub sub = merger.next();
          if (sub == null) {
            // The merger must not be asked again once it has answered null.
            exhausted = true;
            break;
          }
          chunkReader[n] = sub.reader;
          chunkOrd[n] = sub.iterator.index();
          chunkDoc[n] = sub.mappedDocID;
          n++;
        }
        if (n == 0) {
          break;
        }
        Parallel.overRange(
            n,
            (lo, hi) -> {
              // Lazily, because a worker's range usually spans one or two source segments.
              final FloatVectorValues[] local = new FloatVectorValues[nReaders];
              final byte[][] recordScratch = new byte[nReaders][];
              final float[] corrections = new float[CodeRecord.CORRECTIONS];
              final float[] unit = new float[dim];
              final float[] rot = new float[dim];
              final StagedVectors.Builder.EncodeScratch sc = b.encodeScratch();
              for (int j = lo; j < hi; j++) {
                final int r = chunkReader[j];
                final int ord = chunkOrd[j];
                final int doc = chunkDoc[j];
                final int off = j * stride;
                final IVFasterVectorsReader.DonorView view = coarseViews[r];
                if (r == donorIndexFinal) {
                  // Fine and coarse VERBATIM; see DonorView.copyRecord.
                  b.copyInto(donorFinal, ord, doc, chunk, off);
                } else if (view != null && view.canReadRotated()) {
                  // Rotated space straight from the record, then the same encode as a flush;
                  // the coarse planes are copied, not re-derived; see DocPlanes.
                  if (recordScratch[r] == null) {
                    recordScratch[r] = view.newRecordScratch();
                  }
                  view.rotatedVector(ord, rot, recordScratch[r], corrections);
                  b.encodeInto(rot, doc, chunk, off, sc);
                  b.copyCoarseInto(view, ord, chunk, off);
                } else {
                  FloatVectorValues lv = local[r];
                  if (lv == null) {
                    lv = local[r] = readerValues[r].copy();
                  }
                  System.arraycopy(lv.vectorValue(ord), 0, unit, 0, dim);
                  VectorUtil.l2normalize(unit);
                  rotation.rotate(unit, rot);
                  b.encodeInto(rot, doc, chunk, off, sc);
                }
                if (raw != null) {
                  // Exact when the source kept precision, its reconstruction otherwise.
                  FloatVectorValues lv = local[r];
                  if (lv == null) {
                    lv = local[r] = readerValues[r].copy();
                  }
                  System.arraycopy(lv.vectorValue(ord), 0, raw, j * dim, dim);
                }
              }
            });
        b.writeChunk(chunk, raw, n);
        for (int j = 0; j < n; j++) {
          carried.add(chunkReader[j] == donorIndex ? donor.cellOf(chunkOrd[j]) : -1);
        }
      }
      staged = b.finish();
    } catch (Throwable e) {
      b.abort();
      throw e;
    }
    if (TRACE) {
      IvfDiag.err(
          "[ivfaster-stage] merge readers=%d donorIndex=%d donorDocs=%d staged=%d%n",
          mergeState.knnVectorsReaders.length, donorIndex, donorDocs, staged.count());
    }
    traceStage("stage", staged.count(), t);
    try (staged) {
      writeField(
          fieldInfo,
          staged,
          donor == null ? null : donor.centroids(),
          donor == null ? null : carried.toArray(),
          docMean);
    }
    // No deferred phase: this writer emits everything for the field inline.
    return null;
  }

  /** One source segment's live vectors, for the {@link DocIDMerger} walk. */
  private static final class MergeSub extends DocIDMerger.Sub {
    final KnnVectorValues.DocIndexIterator iterator;
    final int reader;

    MergeSub(MergeState.DocMap docMap, KnnVectorValues.DocIndexIterator iterator, int reader) {
      super(docMap);
      this.iterator = iterator;
      this.reader = reader;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  /**
   * The mean of every source's normalized, rotated vectors, for a centring fine tier with no donor
   * to inherit one from. A separate sequential pass, since encoding needs the mean before the first
   * record; only an opt-in tier pays it.
   */
  private static float[] mergedMean(
      FieldInfo fieldInfo, MergeState mergeState, int dim, HadamardRotation rotation)
      throws IOException {
    final double[] acc = new double[dim];
    long n = 0;
    final float[] unit = new float[dim];
    final float[] rot = new float[dim];
    for (int r = 0; r < mergeState.knnVectorsReaders.length; r++) {
      final var reader = mergeState.knnVectorsReaders[r];
      if (reader == null) {
        continue;
      }
      final FloatVectorValues values = reader.getFloatVectorValues(fieldInfo.name);
      if (values == null) {
        continue;
      }
      final MergeState.DocMap docMap = mergeState.docMaps[r];
      final KnnVectorValues.DocIndexIterator it = values.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        if (docMap.get(doc) == -1) {
          continue;
        }
        System.arraycopy(values.vectorValue(it.index()), 0, unit, 0, dim);
        VectorUtil.l2normalize(unit);
        rotation.rotate(unit, rot);
        for (int d = 0; d < dim; d++) {
          acc[d] += rot[d];
        }
        n++;
      }
    }
    final float[] mean = new float[dim];
    for (int d = 0; d < dim; d++) {
      mean[d] = (float) (acc[d] / Math.max(1, n));
    }
    return mean;
  }

  /**
   * Clusters and writes one field from its staged records.
   *
   * @param staged the field's documents, in ascending doc order, holding every code the sections
   *     need; see {@link StagedVectors}
   * @param seed centroids to warm-start clustering from, or {@code null} to train fresh ones
   * @param carried per-document donor cell, or -1 to route; {@code null} when there is no donor
   * @param docMean the mean the codes were centred on, or {@code null} for a tier that does not
   *     centre. INHERITED from the donor at merge rather than recomputed, so that copied codes stay
   *     on the grid they were packed against; see below
   *     <h2>Pipeline</h2>
   *     <p>Cluster, then emit the sections by gathering records out of the staged file in cell
   *     order. Every vector was normalized and rotated once, as it was staged: the rotation is the
   *     same in every segment, derived from {@code dim} alone, so codes and centroids from
   *     different segments are directly comparable at merge, and once every vector is unit length,
   *     squared Euclidean is an affine function of the dot product, so all four similarities rank
   *     identically and every distance in this codec reduces to one dot. Centroids hold the same
   *     invariant, which {@code Clustering#normalize} maintains for every similarity.
   *     <p>NOTHING PROPORTIONAL TO {@code count * dim} IS IN HEAP. Clustering streams the staged
   *     file through cursors and keeps a few dozen bytes per document; emission holds the slot map
   *     and copies one record at a time. See {@link StagedVectors} for the budget.
   *     <p>ADOPTING A DONOR MEANS ADOPTING ITS CELL COUNT. A carried assignment is meaningful only
   *     because seed centroid {@code c} IS donor cell {@code c}, so the seed fixes the cell space
   *     and clustering into a different number of cells would leave carried ids pointing outside
   *     it. The configured {@code nlist} governs fresh segments, merges inherit, and the donor gate
   *     keeps the inherited value within a factor of two. Donor documents therefore start from
   *     their carried cell, and only the other segments' documents are routed from scratch; the
   *     Reaper corrects any donor document the refined centroids moved away from.
   *     <p>Coarse planes are packed ONCE for the whole build, into the staged record: clustering
   *     routes from them, and the coarse section is copied out of the same bytes, so there is
   *     exactly one derivation of every document's code.
   *     <h2>Section invariants</h2>
   *     <p>THE DOCUMENT MEAN MUST BE SEGMENT-INDEPENDENT, because merge copies codes VERBATIM: the
   *     donor's codes were packed against the donor's mean, so decoding them against a freshly
   *     averaged one would score copied documents on a grid they were never quantized to, silently
   *     and compounding per merge. A merge INHERITS the donor's mean as it inherits the cell count,
   *     and a fresh segment computes it once, before any code is derived.
   *     <p>SLOTS ARE GROUPED BY CELL unconditionally, whether or not the field spills, because
   *     every fast path depends on a probed cell's records being one contiguous run. Under spill a
   *     document occupies one slot per chosen cell, and its record is emitted once per slot,
   *     byte-identical across the copies, with the primary-cell field naming the PRIMARY cell in
   *     every copy, which is how the reader tells a primary from a spill. That per-slot emission of
   *     both the code table and the coarse planes is the dominant term in index size; see {@link
   *     IVFasterVectorsFormat#DEFAULT_SPILL_BITS}.
   *     <p>WITHIN A CELL, SLOTS ARE IN ASCENDING DOC-ID ORDER, so a cell is a posting list: the
   *     reader walks the probed cells as a disjunction of sorted iterators and intersects them with
   *     a filter's {@code DocIdSetIterator} before any document is scored. Staged ordinals ascend
   *     by doc id (verified as they are staged) and the slot fill walks ordinals upward, so the
   *     order holds by construction and is checked here.
   *     <p>THE POSTING DIRECTORY IS OFFSETS ONLY. Cell {@code c} is the contiguous slot range
   *     {@code [postingOffsets[c], postingOffsets[c+1])}, so the slot ordinals themselves are the
   *     ascending integers that range already names. Offsets are BYTE offsets ({@code slot *
   *     Integer.BYTES}) and the reader divides back, so that scale is part of the on-disk contract.
   *     <p>The document-ordered SLOT MAP is persisted because the writer already knows it: the code
   *     table is in CELL order and holds several slots per document under spill, so a reader given
   *     only the table would have to scan and sort to recover it, inside {@code
   *     getFloatVectorValues}, which Lucene calls once per query.
   *     <p>The RAW FP32 section is inert: read only by {@code getFloatVectorValues}, and so by
   *     merge. It is stored in VECTOR-ORDINAL order, so {@code getFloatVectorValues(ord)} maps
   *     straight to {@code raw[ord]}, and it holds the caller's exact input, staged beside the
   *     codes and copied here by concatenation. Zero-length when off, which the reader reads as
   *     "reconstruct from the fine code".
   */
  private void writeField(
      FieldInfo fieldInfo, StagedVectors staged, float[][] seed, int[] carried, float[] docMean)
      throws IOException {

    final int dim = fieldInfo.getVectorDimension();
    final VectorSimilarityFunction sim = fieldInfo.getVectorSimilarityFunction();
    final FineQuantizer quantizer = staged.quantizer();
    final int count = staged.count();
    if (count == 0) {
      writeEmptyField(fieldInfo, dim, sim, quantizer);
      return;
    }

    // 1. Cluster: exhaustive routing, Lloyd iterations, the Reaper. A seed fixes the cell space.
    long t = traceStart();
    final int nlistActual = seed != null ? seed.length : Math.min(nlist, Math.max(1, count));
    final Clustering.Result cl =
        Clustering.cluster(
            staged, nlistActual, lloydIters, sim, seed, carried, spillBits, soarLambda);
    t = traceStage("cluster", nlistActual, t);
    if (TRACE && Clustering.EXACT_PLACEMENT_AUDIT) {
      IvfDiag.err(
          "[ivfaster-audit] field=%s docs=%d nlist=%d primariesMoved=%d (%.3f%%)%n",
          fieldInfo.name,
          count,
          nlistActual,
          cl.primariesMoved,
          (double) cl.primariesMoved / count);
    }

    // 2. Spill fan-out came from clustering, as the Reaper's other output.
    final int cellStride = cl.cellStride;
    final int[] cells = cl.cells;
    int totalSlots = 0;
    for (int i = 0; i < count; i++) {
      totalSlots += cl.cellCount(i);
    }

    // 3. Cell-order layout; see the javadoc.
    final int[] cellStart = new int[nlistActual + 1];
    for (int i = 0; i < count; i++) {
      final int n = cl.cellCount(i);
      for (int k = 0; k < n; k++) {
        cellStart[cells[i * cellStride + k] + 1]++;
      }
    }
    for (int c = 0; c < nlistActual; c++) {
      cellStart[c + 1] += cellStart[c];
    }
    final int[] slotDoc = new int[totalSlots];
    {
      final int[] cursor = new int[nlistActual];
      for (int i = 0; i < count; i++) {
        final int n = cl.cellCount(i);
        for (int k = 0; k < n; k++) {
          final int c = cells[i * cellStride + k];
          slotDoc[cellStart[c] + cursor[c]++] = i;
        }
      }
    }
    // WITHIN A CELL, SLOTS ASCEND BY DOC ID; see the javadoc. Ordinal order is doc order, so the
    // check is on ordinals. Enforced rather than assumed, since the reader treats a cell as a
    // posting list.
    for (int c = 0; c < nlistActual; c++) {
      for (int s = cellStart[c] + 1; s < cellStart[c + 1]; s++) {
        if (slotDoc[s] <= slotDoc[s - 1]) {
          throw new IllegalStateException(
              "cell " + c + " is not in ascending doc-id order at slot " + s);
        }
      }
    }

    // ---- sections ----
    final int codeBytes = quantizer.codeBytes(dim);
    final int recordLen = staged.recordLen();
    final int coarseBytes = staged.coarseBytes();

    // S1. centroid float matrix
    final long centroidsOffset = data.getFilePointer();
    for (int c = 0; c < nlistActual; c++) {
      for (int d = 0; d < dim; d++) {
        data.writeInt(Float.floatToIntBits(cl.centroids[c][d]));
      }
    }
    final long centroidsLength = data.getFilePointer() - centroidsOffset;

    // S2. Document mean, when the fine tier centres its codes; see the javadoc.
    final long meanOffset = data.getFilePointer();
    long meanLength = 0;
    if (docMean != null) {
      for (int d = 0; d < dim; d++) {
        data.writeInt(Float.floatToIntBits(docMean[d]));
      }
      meanLength = data.getFilePointer() - meanOffset;
    }

    // S3. code table, in cell order: [code][docId][primaryCell][4 correction floats]. Each record
    // is gathered from the staged file by ordinal, with the next block hinted ahead of the copy.
    final long codeTableOffset = data.getFilePointer();
    {
      final byte[] rec = new byte[recordLen];
      final int primaryOff = CodeRecord.primaryCellOffset(codeBytes);
      for (int s = 0; s < totalSlots; s++) {
        if ((s & (GATHER_AHEAD - 1)) == 0) {
          final int end = Math.min(totalSlots, s + GATHER_AHEAD);
          for (int p = s; p < end; p++) {
            staged.prefetch(slotDoc[p]);
          }
        }
        final int i = slotDoc[s];
        staged.copyRecord(i, rec);
        // Every copy names the PRIMARY cell; see the javadoc.
        CodeRecord.writeIntLE(rec, primaryOff, cells[i * cellStride]);
        data.writeBytes(rec, 0, recordLen);
      }
    }
    final long codeTableLength = data.getFilePointer() - codeTableOffset;
    t = traceStage("codeTable", totalSlots, t);

    // S4. Coarse codes, one record per slot, in slot order, gathered the same way.
    final long coarseOffset = data.getFilePointer();
    {
      final byte[] planes = new byte[coarseBytes];
      for (int s = 0; s < totalSlots; s++) {
        if ((s & (GATHER_AHEAD - 1)) == 0) {
          final int end = Math.min(totalSlots, s + GATHER_AHEAD);
          for (int p = s; p < end; p++) {
            staged.prefetch(slotDoc[p]);
          }
        }
        staged.copyCoarse(slotDoc[s], planes);
        data.writeBytes(planes, 0, coarseBytes);
      }
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
          if (cells[i * cellStride] == c) {
            primarySlot[i] = s;
          }
        }
      }
      for (int i = 0; i < count; i++) {
        data.writeInt(primarySlot[i]);
      }
    }
    final long ordToSlotLength = data.getFilePointer() - ordToSlotOffset;

    // S8. Full-precision vectors, in VECTOR-ORDINAL order, by concatenation; see the javadoc.
    final long rawOffset = data.getFilePointer();
    if (staged.rawLength() > 0) {
      data.copyBytes(staged.rawInput(), staged.rawLength());
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

  /**
   * Records hinted ahead of the emission gather, a power of two. The gather reads staged records in
   * cell order, which is a permutation of the file, so each block's ordinals are hinted before any
   * of them is copied and the faults of a cold temp file overlap.
   */
  private static final int GATHER_AHEAD = 256;

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

  private static boolean isAscending(int[] docIds, int count) {
    for (int i = 1; i < count; i++) {
      if (docIds[i] <= docIds[i - 1]) {
        return false;
      }
    }
    return true;
  }

  /** Vector indices in ascending doc-id order. */
  private static int[] docOrder(int[] docIds, int count) {
    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      order[i] = i;
    }
    new IntroSorter() {
      int pivot;

      @Override
      protected void swap(int i, int j) {
        final int t = order[i];
        order[i] = order[j];
        order[j] = t;
      }

      @Override
      protected void setPivot(int i) {
        pivot = docIds[order[i]];
      }

      @Override
      protected int comparePivot(int j) {
        return Integer.compare(pivot, docIds[order[j]]);
      }
    }.sort(0, count);
    return order;
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

    /**
     * Rewrites doc ids through an index sort.
     *
     * <p>The ids are no longer ascending afterwards; {@code writeField} restores ascending order
     * before anything depends on it, since both the ordinal map and the per-cell slot order are
     * contracts on doc order.
     */
    void applySort(Sorter.DocMap sortMap) {
      if (sortMap == null) {
        return;
      }
      for (int i = 0; i < size; i++) {
        docIds[i] = sortMap.oldToNew(docIds[i]);
      }
    }

    /**
     * The buffer's retained size, which is the writer's whole heap footprint for this field: flush
     * stages the buffer to a temp file and drains it as it goes, so there is no transient peak
     * beyond it for IndexWriter's RAM accounting to miss.
     */
    @Override
    public long ramBytesUsed() {
      if (size == 0) {
        return 0;
      }
      return RamUsageEstimator.shallowSizeOf(vectors)
          + (long) size
              * RamUsageEstimator.alignObjectSize(
                  RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) dim * Float.BYTES)
          + RamUsageEstimator.sizeOf(docIds);
    }

    /** Drops the buffer once the field is written, so {@link #ramBytesUsed} reads zero. */
    void release() {
      vectors = null;
      docIds = null;
      size = 0;
    }
  }
}
