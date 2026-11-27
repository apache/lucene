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

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.PAGE_BYTES;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_COUNT;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.VERSION_CURRENT;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.IntUnaryOperator;
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
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FieldMetadata;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes SegmentIVF fields, in the layout {@link SegmentIVFVectorsFormat} defines.
 *
 * <p>A flush buffers each field's vectors on heap; a merge reads them from the source segments,
 * copying the fine codes of compatible SegmentIVF sources unchanged. Either way every vector is
 * normalized and rotated ({@link HadamardRotation}), staged to a temporary file ({@link
 * StagedVectors}), and clustered from the plan {@link Clustering} makes for a new segment or a
 * merge. The writer then lays the slots out cell by cell and writes each section in order.
 *
 * <p><b>Terms.</b> The writer's code uses these throughout:
 *
 * <ul>
 *   <li>An <b>ordinal</b> numbers a field's vectors 0 to {@code count - 1} in doc id order. Fine
 *       codes and staged rows are stored by ordinal.
 *   <li>A <b>cell</b> is one cluster: a centroid and the vectors assigned to it.
 *   <li>A <b>slot</b> is one copy of a vector in one cell. Each vector has a <b>primary</b> slot in
 *       its nearest cell and, with {@code spillBits > 0}, <b>spill</b> slots in further cells, so a
 *       search probing any of them finds it. Slots are numbered cell by cell ({@link SlotLayout}),
 *       and coarse codes and slot documents are stored by slot.
 *   <li>A <b>staged row</b> is a vector's encoded form in the temporary file: its fine code, doc id
 *       and centroid code ({@link StagedVectors}).
 * </ul>
 *
 * <p><b>Writing one field</b> ({@link #writeField}) runs these steps:
 *
 * <ol>
 *   <li>Stage every vector, in ordinal order: normalize, rotate, encode.
 *   <li>Cluster the staged vectors into {@code nlist} cells, warm-started from the plan's seed when
 *       there is one, and assign each vector its primary and spill cells.
 *   <li>Publish the clustering so later flushes and merges can warm-start from it.
 *   <li>Lay out the slots cell by cell, then write the data sections in file order, recording where
 *       each starts in the metadata.
 * </ol>
 */
final class SegmentIVFVectorsWriter extends KnnVectorsWriter {
  private final SegmentWriteState state;
  private final SegmentIVFVectorsFormat format;

  /** Fields added since the last flush, each buffering its vectors on heap. */
  private final List<BufferedField> fields = new ArrayList<>();

  /**
   * Fields this format cannot index ({@link #unsupported}). Each still has vectors in its {@link
   * FieldInfo}, so each is written as an empty entry, as a merge writes it, and reads back as empty
   * values rather than none.
   */
  private final List<FieldInfo> rejected = new ArrayList<>();

  /** One {@link FieldMetadata} entry per field, then -1 ({@link #finish}). */
  private IndexOutput meta;

  /** Every field's data sections, back to back. */
  private IndexOutput data;

  SegmentIVFVectorsWriter(SegmentWriteState state, SegmentIVFVectorsFormat format)
      throws IOException {
    this.state = state;
    this.format = format;
    byte[] id = state.segmentInfo.getId();
    try {
      meta = createOutput(META_EXTENSION);
      data = createOutput(DATA_EXTENSION);
      CodecUtil.writeIndexHeader(meta, META_CODEC_NAME, VERSION_CURRENT, id, state.segmentSuffix);
      CodecUtil.writeIndexHeader(data, DATA_CODEC_NAME, VERSION_CURRENT, id, state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, meta, data);
      throw t;
    }
  }

  private IndexOutput createOutput(String extension) throws IOException {
    String name =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
    return state.directory.createOutput(name, state.context);
  }

  /** A new temporary staging file for a field of {@code dim} dimensions ({@link StagedVectors}). */
  private StagedVectors stage(int dim, int readers) throws IOException {
    FineCodec fine = FineCodec.of(format.fineTier, dim);
    CoarseCodec coarse = CoarseCodec.of(format.coarseTier, dim);
    return new StagedVectors(
        state, fine, coarse, readers, new Clustering.Parallel(format.buildThreads));
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo info) {
    String unsupported = unsupported(info);
    if (unsupported != null) {
      // IndexingChain treats a failing addField as tragic, closing the IndexWriter; failing each
      // value instead rejects only the documents that carry the field.
      rejected.add(info);
      return new RejectingField(unsupported);
    }
    BufferedField field = new BufferedField(info);
    fields.add(field);
    return field;
  }

  // ---- Flush.

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (BufferedField field : fields) {
      flushField(field, sortMap);
      field.size = 0;
    }
    for (FieldInfo info : rejected) {
      writeEmptyField(info, List.of());
    }
  }

  /**
   * Flushes one field: stages its buffered vectors in doc id order, then clusters and writes it.
   * The vectors were buffered in the order documents arrived, which differs from doc id order when
   * the index is sorted.
   */
  private void flushField(BufferedField field, Sorter.DocMap sortMap) throws IOException {
    int count = field.size;
    float[][] vectors = field.vectors;
    // Ordinals follow doc ids, after index sorting. Each entry packs (doc << 32 | buffered index),
    // so sorting the longs orders by doc and still remembers where each vector was buffered.
    long[] order = new long[count];
    for (int i = 0; i < count; i++) {
      int doc = sortMap == null ? field.docIds[i] : sortMap.oldToNew(field.docIds[i]);
      order[i] = (long) doc << 32 | i;
    }
    Arrays.sort(order);
    try (StagedVectors staged = stage(field.info.getVectorDimension(), 0)) {
      // Stage a chunk at a time, in ordinal order: ordinal ord is buffered vector (int) order[ord].
      for (int start = 0; start < count; ) {
        int n = Math.min(StagedVectors.CHUNK_ORDS, count - start);
        staged.add(
            start,
            n,
            ord -> (int) (order[ord] >>> 32),
            (ord, _) -> vectors[(int) order[ord]],
            true);
        // Release each buffered vector once staged.
        for (int ord = start; ord < start + n; ord++) {
          vectors[(int) order[ord]] = null;
        }
        start += n;
      }
      // A flush warm-starts from the clustering of a segment this process already wrote, when
      // there is one (Clustering.planNewSegment); otherwise it clusters from scratch.
      int nlist = format.nlist(count);
      Clustering.Plan plan;
      if (nlist > 1) {
        plan =
            Clustering.planNewSegment(
                state.segmentInfo.dir, state.segmentInfo.name, field.info, nlist);
      } else {
        // A single cell has nothing to seed, so skip the seed lookup and the directory listing it
        // prunes by, which a tiny flush would otherwise pay for. Remembered segments that have
        // since left the directory are pruned by the next clustered flush, and are capped by
        // HotStart.MAX_SEEDS and MAX_BYTES meanwhile.
        plan = Clustering.Plan.cold(state.segmentInfo.name);
      }
      if (state.infoStream.isEnabled("SIVF")) {
        state.infoStream.message(
            "SIVF",
            "flush field=" + field.info.name + " docs=" + count + " source=" + plan.donor());
      }
      writeField(field.info, staged, plan, List.of(), () -> {});
    }
  }

  // ---- Merge.

  /**
   * One source segment's vectors, for {@link DocIDMerger}, which interleaves the sources into
   * merged doc id order. {@code mappedDocID} is the merged doc id; {@code iterator.index()} is the
   * vector's ordinal in its source.
   */
  // Core: KnnVectorsWriter.FloatVectorValuesSub, package-private and without a source index.
  private static final class MergeSub extends DocIDMerger.Sub {
    final KnnVectorValues.DocIndexIterator iterator;

    /** Which source segment, an index into the merge's readers. */
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
   * Merges a field. Source segments written by a compatible SegmentIVF configuration copy their
   * fine codes instead of re-encoding vectors, and offer their clustering as a warm start; other
   * sources are read as plain float vectors.
   *
   * <p>A source is compatible when SegmentIVF wrote it with the same fine tier: its fine codes are
   * then the bytes this segment would write, since every index rotates with the same seed ({@link
   * HadamardRotation#SEED}) and a fine code depends only on the vector and its tier's frozen scale
   * ({@link FineCodec}). The merge writes its field before returning, so it returns no deferred
   * work.
   */
  @Override
  public IORunnable mergeOneField(FieldInfo info, MergeState mergeState) throws IOException {
    if (unsupported(info) != null) {
      // Sources of another format may hold such a field, for example after addIndexes or a change
      // of per-field format. Its vectors are not read: re-encoding them would score the wrong
      // geometry, and a byte or float16 source cannot supply floats.
      writeEmptyField(info, List.of());
      return null;
    }
    int dim = info.getVectorDimension();
    int readers = mergeState.knnVectorsReaders.length;
    // Per source r, null or 0 where it does not apply:
    //   compatible[r]  its SegmentIVF field, when its fine codes can be copied
    //   remembered[r]  the clustering it published when written, a candidate warm start
    //   liveVectors[r] how many of its vectors survive deletions, passed along with its seed
    //   values[r]      its vectors as Lucene exposes them, for sources that are re-encoded
    IVFFieldReader[] compatible = new IVFFieldReader[readers];
    Clustering.HotStart.Seed[] remembered = new Clustering.HotStart.Seed[readers];
    int[] liveVectors = new int[readers];
    FloatVectorValues[] values = new FloatVectorValues[readers];
    List<MergeSub> subs = new ArrayList<>();
    // The ids of the SegmentIVF segments this merge replaces, whose remembered clusterings are then
    // forgotten.
    List<String> merged = new ArrayList<>();
    // Plain sources are read on the encoding threads when every one of them can be copied: each
    // thread then reads through its own copy of the values. One source that cannot be copied
    // makes the whole merge read on this thread.
    boolean parallel = true;
    for (int r = 0; r < readers; r++) {
      var reader = mergeState.knnVectorsReaders[r];
      // As core's merge does: a source without the field is not asked for it, since not every
      // reader answers null for a field it lacks (a sorting view, a format without per-field
      // dispatch).
      FieldInfo sourceInfo =
          mergeState.fieldInfos[r] == null ? null : mergeState.fieldInfos[r].fieldInfo(info.name);
      if (reader == null || sourceInfo == null || sourceInfo.hasVectorValues() == false) {
        continue;
      }
      if (SegmentIVFVectorsReader.unwrapReaderForField(reader, info.name)
          instanceof SegmentIVFVectorsReader sivf) {
        // The segment leaves the index with this merge, and so does its remembered clustering.
        merged.add(sivf.segmentId());
        if (sivf.field(info.name) instanceof IVFFieldReader field
            && field.fineTier == format.fineTier) {
          compatible[r] = field;
          remembered[r] =
              Clustering.HotStart.snapshot(state.segmentInfo.dir, sivf.segmentId(), info);
          liveVectors[r] = liveVectors(field.values(), mergeState.liveDocs[r]);
        }
      }
      values[r] = reader.getFloatVectorValues(info.name);
      if (values[r] == null) {
        continue;
      }
      subs.add(new MergeSub(mergeState.docMaps[r], values[r].iterator(), r));
      if (compatible[r] == null && parallel) {
        try {
          values[r].copy();
        } catch (UnsupportedOperationException _) {
          // For example a sorting view: read it on the merge thread instead.
          parallel = false;
        }
      }
    }
    // The planner sees every staged vector's source and ordinal, and from them picks or combines
    // the sources' clusterings into the merged segment's warm start.
    Clustering.MergePlanner planner =
        new Clustering.MergePlanner(
            state.segmentInfo.name, format.nlist(), compatible, remembered, liveVectors);
    try (StagedVectors staged = stage(dim, readers)) {
      // No source may have values: a field whose every document SegmentIVF rejected still has
      // vectors in its FieldInfo. A merger of no subs cannot be stepped, and there is nothing to
      // stage; the field is written empty.
      if (subs.isEmpty() == false) {
        DocIDMerger<MergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        stageMerged(merger, mergeState, staged, compatible, values, parallel, planner);
      }
      writeField(info, staged, planner.plan(), merged, mergeState::checkAborted);
    }
    return null;
  }

  // Core: IncrementalHnswGraphMerger#countLiveVectors, private to lucene-core.
  /** How many of {@code values}' documents survive the merge. */
  private static int liveVectors(FloatVectorValues values, Bits liveDocs) throws IOException {
    if (liveDocs == null) {
      return values.size();
    }
    int live = 0;
    KnnVectorValues.DocIndexIterator iterator = values.iterator();
    for (int doc = iterator.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = iterator.nextDoc()) {
      if (liveDocs.get(doc)) {
        live++;
      }
    }
    return live;
  }

  /**
   * Stages every merged vector in merged doc order, recording each for the merge's plan. Each pass
   * of the loop takes up to {@link StagedVectors#CHUNK_ORDS} vectors from {@code merger}, notes
   * where each came from, and stages them as one chunk; a short chunk is the last.
   */
  private static void stageMerged(
      DocIDMerger<MergeSub> merger,
      MergeState mergeState,
      StagedVectors staged,
      IVFFieldReader[] compatible,
      FloatVectorValues[] values,
      boolean parallel,
      Clustering.MergePlanner planner)
      throws IOException {
    int max = StagedVectors.CHUNK_ORDS;
    // For the j-th vector of the current chunk: its source segment, its ordinal there, and its
    // merged doc id.
    int[] sourceOf = new int[max];
    int[] ordOf = new int[max];
    int[] docOf = new int[max];
    // A compatible source copies its fine code straight into the chunk; others supply a vector.
    StagedVectors.Rows rows =
        (j, local) -> {
          int r = sourceOf[j];
          if (compatible[r] != null) {
            if (local[r] == null) {
              local[r] = compatible[r].values();
            }
            ((IVFVectorValues) local[r]).copyRow(ordOf[j], docOf[j], staged, j * staged.stride);
            return null;
          }
          if (local[r] == null) {
            local[r] = parallel ? values[r].copy() : values[r];
          }
          return local[r].vectorValue(ordOf[j]);
        };
    int n;
    do {
      mergeState.checkAborted();
      n = 0;
      MergeSub sub;
      while (n < max && (sub = merger.next()) != null) {
        sourceOf[n] = sub.reader;
        ordOf[n] = sub.iterator.index();
        docOf[n] = sub.mappedDocID;
        n++;
      }
      staged.add(0, n, j -> docOf[j], rows, parallel);
      planner.record(sourceOf, ordOf, n);
    } while (n == max);
  }

  // ---- Writing a field.

  /**
   * Writes field {@code info} without vectors: its header, then every section starting, and ending,
   * here. A merge whose vectors were all deleted still replaces its sources, so the clusterings of
   * the segments it {@code merged} are forgotten.
   */
  private void writeEmptyField(FieldInfo info, List<String> merged) throws IOException {
    Clustering.HotStart.forget(state.segmentInfo.dir, state.segmentInfo.name, info, merged);
    FieldMetadata.writeHeader(meta, info, format, 0, 0);
    for (int s = 0; s < SECTION_COUNT; s++) {
      meta.writeVLong(data.getFilePointer());
    }
  }

  /**
   * Clusters the staged vectors, starting from {@code plan}, and writes the field; {@code merged}
   * names the segments a merge replaces, and {@code checkAborted} throws once the merge is aborted.
   *
   * <p>The metadata gets the field's header, then the start of each data section as it is written,
   * in the order of {@link SegmentIVFVectorsFormat}'s {@code SECTION_*} constants. A section ends
   * where the next starts, so an empty section is two equal starts.
   */
  private void writeField(
      FieldInfo info,
      StagedVectors staged,
      Clustering.Plan plan,
      List<String> merged,
      IORunnable checkAborted)
      throws IOException {
    RandomAccessInput rows = staged.finish();
    int count = staged.count;
    // A clustered segment trains the configured cell count, so any clustered segment of this
    // configuration can seed any other; one with too few vectors per cell is written as a single
    // cell instead (SegmentIVFVectorsFormat.MIN_VECTORS_PER_CELL).
    int nlist = format.nlist(count);
    if (nlist == 1 && plan.seed() != null) {
      // A merge plans with the configured cell count before it knows how many vectors survive. Its
      // seed has the wrong number of cells for a single cell, so start cold.
      plan = Clustering.Plan.cold(state.segmentInfo.name);
    }
    if (nlist == 0) {
      writeEmptyField(info, merged);
      return;
    }
    FieldMetadata.writeHeader(meta, info, format, nlist, count);
    Clustering.Result clustering =
        Clustering.cluster(staged, nlist, plan, format.spillBits, checkAborted);
    if (state.infoStream.isEnabled("SIVF")) {
      // Passes and the last pass's moved fraction show whether clustering converged; the donor
      // tells a cold start (which may hit the cap on a large segment) from a seeded one.
      state.infoStream.message(
          "SIVF",
          String.format(
              Locale.ROOT,
              "cluster field=%s vectors=%d nlist=%d passes=%d movedFraction=%.4f hitCap=%b"
                  + " source=%s",
              info.name,
              count,
              nlist,
              clustering.passes(),
              clustering.movedFraction(),
              clustering.hitCap(),
              plan.donor()));
    }
    // Remember this clustering for later flushes and merges to warm-start from, and forget the
    // clusterings of the segments this merge replaces. A single cell is not remembered: a donor
    // must have the configured cell count (HotStart.donor), so it could never seed anything.
    if (nlist > 1) {
      Clustering.HotStart.publish(
          state.segmentInfo.dir,
          state.segmentInfo.name,
          StringHelper.idToString(state.segmentInfo.getId()),
          plan.lineage(),
          info,
          clustering,
          count,
          merged);
    } else {
      Clustering.HotStart.forget(state.segmentInfo.dir, state.segmentInfo.name, info, merged);
    }
    SlotLayout slots = new SlotLayout(clustering, count, nlist);

    // SECTION_CENTROIDS: nlist rotated unit centroids, as floats.
    meta.writeVLong(data.getFilePointer());
    writeCentroids(clustering.centroids());

    // SECTION_FINE: one fine code per vector, by ordinal. Page-aligned, so a page-sized record is
    // one page read; collects each ordinal's doc id on the way, for SECTION_SLOT_DOCS.
    data.alignFilePointer(PAGE_BYTES);
    meta.writeVLong(data.getFilePointer());
    int[] docs = new int[count];
    writeFineCodes(staged, rows, docs);

    // SECTION_COARSE: one coarse code per slot, cell by cell, each relative to its cell's centroid.
    meta.writeVLong(data.getFilePointer());
    writeAnchoredCoarse(staged, clustering, slots);

    // SECTION_GRAPH: a neighbour graph over the centroids, to find a query's nearest cells
    // without scoring all of them. A single cell needs none, so the section is empty.
    meta.writeVLong(data.getFilePointer());
    if (nlist > 1) {
      CentroidCodes centroidCodes = new CentroidCodes(clustering.centroids(), staged.coarse);
      CentroidGraph.build(centroidCodes, staged.parallel).write(data);
    }

    // SECTION_ORD_TO_SLOT: each ordinal's primary slot, which locates its cell.
    meta.writeVLong(data.getFilePointer());
    for (int slot : slots.primarySlot) {
      data.writeInt(slot);
    }

    // SECTION_SLOT_DOCS: each slot's doc id, so a scan maps the slots it scores to documents.
    meta.writeVLong(data.getFilePointer());
    for (int slot = 0; slot < slots.slotRow.length; slot++) {
      data.writeInt(docs[slots.slotRow[slot]]);
    }

    // SECTION_CELL_OFFSETS: where each cell's slots start, in bytes of SECTION_SLOT_DOCS, plus the
    // end; DirectMonotonic, whose own metadata follows in the metadata file.
    meta.writeVLong(data.getFilePointer());
    DirectMonotonicWriter cellOffsets =
        DirectMonotonicWriter.getInstance(meta, data, nlist + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
    for (int start : slots.cellStart) {
      cellOffsets.add((long) start * Integer.BYTES);
    }
    cellOffsets.finish();
  }

  /**
   * Where each vector's copies go: slots are numbered cell by cell, and within a cell in ordinal
   * order.
   *
   * <p>For example, with three vectors where vector 0 is in cell 1, vector 1 in cell 0 with a spill
   * copy in cell 1, and vector 2 in cell 0: cell 0 holds slots 0 and 1 (vectors 1 and 2), and cell
   * 1 holds slots 2 and 3 (vectors 0 and 1). Then {@code cellStart = [0, 2, 4]}, {@code slotRow =
   * [1, 2, 0, 1]} and {@code primarySlot = [2, 0, 1]}.
   */
  private static final class SlotLayout {
    /** The first slot of each cell, plus the total slot count at {@code nlist}. */
    final int[] cellStart;

    /** The staged ordinal of each slot's vector. */
    final int[] slotRow;

    /** Each vector's slot in its primary cell. */
    final int[] primarySlot;

    SlotLayout(Clustering.Result clustering, int count, int nlist) {
      // Count each cell's slots, then turn the counts into starts: a counting sort of the slots by
      // cell. cell(i, 0) is vector i's primary cell, cell(i, k > 0) its spill cells.
      cellStart = new int[nlist + 1];
      for (int i = 0; i < count; i++) {
        for (int k = clustering.cellCount(i) - 1; k >= 0; k--) {
          cellStart[clustering.cell(i, k) + 1]++;
        }
      }
      for (int c = 0; c < nlist; c++) {
        cellStart[c + 1] += cellStart[c];
      }
      slotRow = new int[cellStart[nlist]];
      primarySlot = new int[count];
      // Hand out each cell's slots in turn. Vectors are visited in ordinal order, so within a cell
      // slots stay in ordinal order.
      int[] next = ArrayUtil.copyOfSubArray(cellStart, 0, nlist);
      for (int i = 0; i < count; i++) {
        for (int k = 0, cells = clustering.cellCount(i); k < cells; k++) {
          int slot = next[clustering.cell(i, k)]++;
          slotRow[slot] = i;
          if (k == 0) {
            primarySlot[i] = slot;
          }
        }
      }
    }
  }

  /** Writes each centroid's floats, little-endian, centroid by centroid. */
  private void writeCentroids(float[][] centroids) throws IOException {
    for (float[] centroid : centroids) {
      for (float value : centroid) {
        data.writeInt(Float.floatToIntBits(value));
      }
    }
  }

  /**
   * Writes each vector's fine code in ordinal order, collecting its doc id, which the staged row
   * carries next to the code.
   */
  private void writeFineCodes(StagedVectors staged, RandomAccessInput rows, int[] docs)
      throws IOException {
    FineCodec fine = staged.fine;
    byte[] row = new byte[Math.max(fine.recordBytes, staged.queryOffset)];
    for (int ord = 0; ord < staged.count; ord++) {
      rows.readBytes((long) ord * staged.stride, row, 0, staged.queryOffset);
      docs[ord] = (int) BitUtil.VH_LE_INT.get(row, staged.docOffset);
      // Zero the padding, which the staged doc id occupies.
      Arrays.fill(row, fine.codeBytes, fine.recordBytes, (byte) 0);
      data.writeBytes(row, 0, fine.recordBytes);
    }
  }

  /**
   * Writes each slot's code relative to its cell's centroid, so a spill copy is coded against the
   * cell it is scanned in, as each cell's region of whole blocks ({@link CoarseCodec#regionBytes}),
   * then the field's {@link CoarseCodec#parameters}, the means of every slot's shares. Codes are
   * derived from the staged fine codes.
   *
   * <p><b>Why spill slots are real copies.</b> Spill copies are most of the coarse section's
   * growth, and the coarse section is what a search keeps resident (fine codes are read off disk).
   * A spill slot could instead hold a reference to its vector's primary slot, scored from the
   * primary code against the primary cell's centroid. Searches with spill references were 10x
   * slower.
   *
   * <p>The slowdown is the block layout: a block stores byte {@code j} of {@link
   * Kernels#BLOCK_ROWS} rows together, so one row spans {@code codeBytes} cache lines. A primary
   * row shares those lines with the rest of its block, but a reference reads them all for a single
   * row. References also lose an estimate: a copy coded against the spill cell's own centroid gives
   * a different estimate than the primary, and the scan keeps the nearer one, while references
   * score every copy of a vector the same. A cheaper reference would need row-major codes, which
   * the block-table scan of primary rows cannot use.
   */
  private void writeAnchoredCoarse(
      StagedVectors staged, Clustering.Result clustering, SlotLayout slots) throws IOException {
    CoarseCodec coarse = staged.coarse;
    float[][] centroids = clustering.centroids();
    int[] cellStart = slots.cellStart;
    int[] slotRow = slots.slotRow;
    int codeBytes = coarse.codeBytes;
    // Slots are encoded a chunk at a time, in two phases per chunk: encode every slot of the chunk
    // in parallel into codes, then, on this thread, pack the codes into blocks and write them.
    int chunk = StagedVectors.CHUNK_ORDS;
    byte[] codes = new byte[chunk * codeBytes];
    // Each slot's encode also reports its shares of the field's parameters; the field keeps their
    // means over every slot.
    int parameters = coarse.parameters();
    float[] shares = new float[chunk * parameters];
    double[] sums = new double[parameters];
    // A cell's codes gather into a block of blockRows rows, written whole when full or when the
    // cell ends; a block may span chunks.
    int blockRows = coarse.blockRows();
    byte[] pending = new byte[blockRows * codeBytes];
    byte[] block = new byte[blockRows * codeBytes];
    int pendingRows = 0;
    int pendingCell = -1;
    for (int chunkStart = 0; chunkStart < slotRow.length; chunkStart += chunk) {
      int base = chunkStart;
      int n = Math.min(chunk, slotRow.length - chunkStart);
      // Phase 1: encode. Each worker takes a range of the chunk's slots, finds the cell of its
      // first, and walks forward: slots are in cell order, so the cell only ever advances.
      staged.parallel.run(
          n,
          (from, to) -> {
            StagedVectors.Cursor cursor = staged.cursor();
            int cell = cellOfSlot(cellStart, base + from);
            for (int i = from; i < to; i++) {
              while (cellStart[cell + 1] <= base + i) {
                cell++;
              }
              cursor.load(slotRow[base + i]);
              coarse.encode(
                  cursor.vector(), centroids[cell], codes, i * codeBytes, shares, i * parameters);
            }
          });
      // Phase 2: pack. A block holds one cell's codes only, so a block is written when it fills or
      // its cell ends; the last block of a cell may be partly empty.
      int cell = cellOfSlot(cellStart, base);
      for (int i = 0; i < n; i++) {
        while (cellStart[cell + 1] <= base + i) {
          cell++;
        }
        if (pendingRows > 0 && (pendingCell != cell || pendingRows == blockRows)) {
          coarse.writeBlock(pending, pendingRows, block);
          data.writeBytes(block, 0, block.length);
          pendingRows = 0;
        }
        pendingCell = cell;
        System.arraycopy(codes, i * codeBytes, pending, pendingRows * codeBytes, codeBytes);
        pendingRows++;
        for (int k = 0; k < parameters; k++) {
          sums[k] += shares[i * parameters + k];
        }
      }
    }
    // The last cell's last block.
    if (pendingRows > 0) {
      coarse.writeBlock(pending, pendingRows, block);
      data.writeBytes(block, 0, block.length);
    }
    // The field's parameters follow its codes, where IVFFieldReader.open reads them.
    for (int k = 0; k < parameters; k++) {
      float mean = slotRow.length == 0 ? 0f : (float) (sums[k] / slotRow.length);
      data.writeInt(Float.floatToIntBits(mean));
    }
  }

  /**
   * The cell holding {@code slot}: the last cell whose start is at most {@code slot}, by binary
   * search over {@code cellStart}. An empty cell starts where its successor does, so the last such
   * cell is never empty.
   */
  static int cellOfSlot(int[] cellStart, int slot) {
    int lo = 0;
    int hi = cellStart.length - 2;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (cellStart[mid] <= slot) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /** Ends both files: -1 after the last field's metadata, then each file's checksum footer. */
  @Override
  public void finish() throws IOException {
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
    for (BufferedField field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  /**
   * Why SegmentIVF cannot index field {@code info}, or null if it can. Every code is of the unit
   * vector, so the format keeps a vector's direction but not its length: only similarities that
   * ignore length, or that require unit vectors, score as they would without it.
   */
  static String unsupported(FieldInfo info) {
    if (info.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return "SegmentIVF supports only FLOAT32 vectors, but field \""
          + info.name
          + "\" is "
          + info.getVectorEncoding();
    }
    VectorSimilarityFunction similarity = info.getVectorSimilarityFunction();
    if (similarity != VectorSimilarityFunction.COSINE
        && similarity != VectorSimilarityFunction.DOT_PRODUCT) {
      return "SegmentIVF supports only COSINE and DOT_PRODUCT similarity, but field \""
          + info.name
          + "\" uses "
          + similarity;
    }
    return null;
  }

  /** A field SegmentIVF cannot index: adding any value fails, rejecting the document. */
  private static final class RejectingField extends KnnFieldVectorsWriter<Object> {
    private final String message;

    RejectingField(String message) {
      this.message = message;
    }

    @Override
    public void addValue(int docID, Object value) {
      throw new IllegalArgumentException(message);
    }

    @Override
    public Object copyValue(Object value) {
      throw new UnsupportedOperationException(message);
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  /**
   * A field's vectors buffered on heap until flush, in the order documents were added, which is doc
   * id order unless the index is sorted.
   */
  private static final class BufferedField extends KnnFieldVectorsWriter<float[]> {
    final FieldInfo info;

    /** The buffered vectors, copied on add, since callers may reuse the array they pass. */
    float[][] vectors = new float[16][];

    /** Each buffered vector's doc id, parallel to {@link #vectors}. */
    int[] docIds = new int[16];

    int size;

    BufferedField(FieldInfo info) {
      this.info = info;
    }

    @Override
    public void addValue(int docID, float[] value) {
      if (size > 0 && docIds[size - 1] == docID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + info.name
                + "\" appears more than once in this document (only one value is allowed per"
                + " field)");
      }
      if (size == vectors.length) {
        vectors = ArrayUtil.grow(vectors, size + 1);
        docIds = ArrayUtil.growExact(docIds, vectors.length);
      }
      vectors[size] = copyValue(value);
      docIds[size] = docID;
      size++;
    }

    @Override
    public float[] copyValue(float[] value) {
      return ArrayUtil.copyOfSubArray(value, 0, info.getVectorDimension());
    }

    @Override
    public long ramBytesUsed() {
      if (size == 0) {
        return 0;
      }
      long vector = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 4L * info.getVectorDimension();
      long arrays = RamUsageEstimator.shallowSizeOf(vectors) + RamUsageEstimator.sizeOf(docIds);
      return arrays + size * RamUsageEstimator.alignObjectSize(vector);
    }
  }

  /**
   * A field's vectors, encoded once into a temporary file that clustering reads on every pass, so a
   * flush or merge never holds all of its vectors on heap.
   *
   * <p>Rows are staged in ordinal order, each a vector's fine code, its doc id, and its coarse
   * query code, which routing anchors to the centroids:
   *
   * <pre>
   *   [fine code: fine.codeBytes][doc id: int][centroid code: coarse.centroidBytes]
   * </pre>
   *
   * Clustering decodes the fine code back to a vector, so routing sees the same quantized vectors
   * that searches will.
   */
  static final class StagedVectors implements Closeable {
    /** Rows are encoded and written this many at a time. */
    static final int CHUNK_ORDS = 16_384;

    /**
     * Supplies the {@code index}-th vector of a chunk, or null when it wrote the row into {@link
     * #chunk} itself. {@code local} holds per-thread copies of the source values.
     */
    interface Rows {
      float[] vector(int index, FloatVectorValues[] local) throws IOException;
    }

    final FineCodec fine;
    final CoarseCodec coarse;
    final int dim;

    /** Where a staged row's doc id starts: right after its fine code. */
    final int docOffset;

    /** Where a staged row's centroid code starts: right after its doc id. */
    final int queryOffset;

    /** Bytes of one staged row. */
    final int stride;

    /** The number of source readers of a merge, sizing {@link Rows}'s per-thread copies. */
    private final int readers;

    /** This writer's cap on build threads, for clustering and encoding these vectors. */
    final Clustering.Parallel parallel;

    private final HadamardRotation rotation;
    private final SegmentWriteState state;
    private final String fileName;
    private IndexOutput out;
    private IndexInput input;

    /**
     * The chunk being encoded, {@code CHUNK_ORDS} rows at most. Allocated by the first {@link #add}
     * and reused; {@link Rows} may write rows into it directly, as a merge does when it copies a
     * compatible source's fine code.
     */
    byte[] chunk;

    /** Rows staged so far. */
    int count;

    StagedVectors(
        SegmentWriteState state,
        FineCodec fine,
        CoarseCodec coarse,
        int readers,
        Clustering.Parallel parallel)
        throws IOException {
      this.state = state;
      this.fine = fine;
      this.coarse = coarse;
      this.readers = readers;
      this.parallel = parallel;
      dim = fine.dim;
      docOffset = fine.codeBytes;
      queryOffset = docOffset + Integer.BYTES;
      stride = queryOffset + coarse.centroidBytes;
      rotation = HadamardRotation.forDimension(dim);
      out = state.directory.createTempOutput(state.segmentInfo.name, "ivfstage", state.context);
      fileName = out.getName();
    }

    /**
     * Stages rows {@code base} to {@code base + n - 1}: each vector is normalized, rotated and
     * encoded, with {@code docs} giving its doc id.
     */
    void add(int base, int n, IntUnaryOperator docs, Rows source, boolean parallel)
        throws IOException {
      if (chunk == null) {
        chunk = new byte[n * stride];
      }
      // Each worker encodes a range of the chunk into its rows of chunk. A row the source wrote
      // itself (vector == null) already holds its fine code, doc id and centroid code.
      Clustering.Parallel.RangeTask encode =
          (from, to) -> {
            FloatVectorValues[] local = new FloatVectorValues[readers];
            float[] unit = new float[dim];
            float[] rotated = new float[dim];
            for (int j = from; j < to; j++) {
              float[] vector = source.vector(base + j, local);
              if (vector == null) {
                continue;
              }
              int at = j * stride;
              System.arraycopy(vector, 0, unit, 0, dim);
              // A zero vector stays zero.
              VectorUtil.l2normalize(unit, false);
              rotation.rotate(unit, rotated);
              fine.encode(rotated, chunk, at);
              BitUtil.VH_LE_INT.set(chunk, at + docOffset, docs.applyAsInt(base + j));
              coarse.encodeCentroid(rotated, chunk, at + queryOffset);
            }
          };
      if (parallel) {
        this.parallel.run(n, encode);
      } else {
        encode.run(0, n);
      }
      out.writeBytes(chunk, 0, n * stride);
      count += n;
    }

    /** Ends staging and returns the staged rows for reading. */
    RandomAccessInput finish() throws IOException {
      out.close();
      out = null;
      input = state.directory.openInput(fileName, state.context);
      return input.randomAccessSlice(0, input.length());
    }

    /** A reader of staged rows, for one thread; valid after {@link #finish}. */
    Cursor cursor() throws IOException {
      return new Cursor();
    }

    final class Cursor {
      private final RandomAccessInput in = input.clone().randomAccessSlice(0, input.length());
      private final byte[] row = new byte[stride];
      private final float[] vector = new float[dim];
      private boolean decoded;

      private Cursor() throws IOException {}

      void load(int ord) throws IOException {
        in.readBytes((long) ord * stride, row, 0, stride);
        decoded = false;
      }

      /** The loaded row's vector, decoded from its fine code and normalized. */
      float[] vector() {
        if (decoded == false) {
          fine.decode(row, 0, vector);
          VectorUtil.l2normalize(vector, false);
          decoded = true;
        }
        return vector;
      }

      /**
       * Copies the loaded row's centroid code, for comparison with centroid codes, into {@code
       * dest}.
       */
      void prepareRouting(byte[] dest) {
        System.arraycopy(row, queryOffset, dest, 0, coarse.centroidBytes);
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(
          out, input, () -> IOUtils.deleteFilesIgnoringExceptions(state.directory, fileName));
    }
  }

  /**
   * A randomized Hadamard rotation: random signs, a random permutation, then a normalized
   * Walsh-Hadamard transform over the leading {@code P} components, {@code P} the largest power of
   * two at most {@code dim}. When {@code dim} is not a power of two, a second transform, with signs
   * of its own, follows over the trailing {@code P} components. The two spans overlap and cover
   * every component, so each output component mixes every input, as for a power of two.
   *
   * <p>The rotation is orthogonal, so it preserves dot products and distances, while spreading each
   * vector's energy evenly over its dimensions. That makes fixed per-dimension quantization
   * thresholds work for every dimension ({@link FineCodec.Int8}). Transforming each power-of-two
   * block of {@code dim} on its own, as an earlier version did, left a vector whose energy sat in a
   * small block (an outlier raw dimension that the permutation sent there; for an odd {@code dim},
   * a block of one) concentrated, and its components clipped.
   *
   * <p>The writer rotates every vector before any tier sees it, whichever fine tier the field uses:
   * an even spread of energy helps every per-dimension quantizer.
   */
  record HadamardRotation(float[] signs, int[] permutation, float[] trailingSigns) {

    /**
     * The one seed every index rotates with, whatever its fine tier, so codes from any two segments
     * of a dimension are comparable and merges can copy them. It is part of the on-disk format:
     * changing it changes every code, so it needs a new {@code VERSION_START}. It is deliberately
     * not {@link Clustering#SEED}, which only seeds clustering and may change freely.
     */
    static final long SEED = 42L;

    /** The rotation of every {@code dim}-dimensional field. */
    static HadamardRotation forDimension(int dim) {
      return create(dim, SEED);
    }

    static HadamardRotation create(int dim, long seed) {
      Random random = new Random(seed);
      float[] signs = new float[dim];
      for (int i = 0; i < dim; i++) {
        signs[i] = random.nextBoolean() ? 1f : -1f;
      }
      // Fisher-Yates shuffle.
      int[] permutation = new int[dim];
      for (int i = 0; i < dim; i++) {
        permutation[i] = i;
      }
      for (int i = dim - 1; i > 0; i--) {
        int j = random.nextInt(i + 1);
        int swap = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = swap;
      }
      // Drawn after the first pass's, so a power-of-two dimension's rotation is what it was.
      float[] trailingSigns = null;
      int span = Integer.highestOneBit(dim);
      if (span != dim) {
        trailingSigns = new float[span];
        for (int i = 0; i < span; i++) {
          trailingSigns[i] = random.nextBoolean() ? 1f : -1f;
        }
      }
      return new HadamardRotation(signs, permutation, trailingSigns);
    }

    /** Rotates {@code in} into {@code out}. */
    void rotate(float[] in, float[] out) {
      int dim = permutation.length;
      for (int i = 0; i < dim; i++) {
        int from = permutation[i];
        out[i] = signs[from] * in[from];
      }
      int span = Integer.highestOneBit(dim);
      transform(out, 0, span);
      if (trailingSigns != null) {
        int start = dim - span;
        for (int i = 0; i < span; i++) {
          out[start + i] *= trailingSigns[i];
        }
        transform(out, start, span);
      }
    }

    /** Undoes {@link #rotate}: each normalized transform is its own inverse, applied in reverse. */
    void inverseRotate(float[] in, float[] out) {
      int dim = permutation.length;
      int span = Integer.highestOneBit(dim);
      float[] transformed = in.clone();
      if (trailingSigns != null) {
        int start = dim - span;
        transform(transformed, start, span);
        for (int i = 0; i < span; i++) {
          transformed[start + i] *= trailingSigns[i];
        }
      }
      transform(transformed, 0, span);
      for (int i = 0; i < dim; i++) {
        int to = permutation[i];
        out[to] = signs[to] * transformed[i];
      }
    }

    /**
     * In-place normalized fast Walsh-Hadamard transform of the {@code length} components from
     * {@code start}; {@code length} is a power of two.
     */
    private static void transform(float[] values, int start, int length) {
      int end = start + length;
      for (int half = 1; half < length; half <<= 1) {
        for (int pair = start; pair < end; pair += half << 1) {
          for (int p = pair; p < pair + half; p++) {
            float x = values[p];
            float y = values[p + half];
            values[p] = x + y;
            values[p + half] = x - y;
          }
        }
      }
      float norm = (float) (1.0 / Math.sqrt(length));
      for (int i = start; i < end; i++) {
        values[i] *= norm;
      }
    }
  }
}
