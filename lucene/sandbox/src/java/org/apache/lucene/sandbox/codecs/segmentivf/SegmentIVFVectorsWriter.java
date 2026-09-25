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
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.VERSION_CURRENT;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.lucene.sandbox.codecs.segmentivf.Centroids.CentroidCodes;
import org.apache.lucene.sandbox.codecs.segmentivf.Centroids.CentroidGraph;
import org.apache.lucene.sandbox.codecs.segmentivf.Clustering.HotStart;
import org.apache.lucene.sandbox.codecs.segmentivf.Clustering.Parallel;
import org.apache.lucene.sandbox.codecs.segmentivf.Clustering.WarmState;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsReader.Field;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.CodeRecord;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.FineCodec;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.HadamardRotation;
import org.apache.lucene.sandbox.codecs.segmentivf.Tiers.Nitrox2;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Builds SegmentIVF fields by rotating, staging, clustering, spilling, and encoding their vectors.
 *
 * <p>Staging keeps the large build data sequential and reusable during clustering, while compatible
 * merges copy encoded rows and warm-start from existing centroid state.
 */
final class SegmentIVFVectorsWriter extends KnnVectorsWriter {
  private static final int GATHER_AHEAD = 256;

  private final SegmentWriteState state;
  private final SegmentIVFVectorsFormat format;
  private IndexOutput meta, data;
  private final List<BufferedField> fields = new ArrayList<>();

  SegmentIVFVectorsWriter(SegmentWriteState state, SegmentIVFVectorsFormat format)
      throws IOException {
    this.state = state;
    this.format = format;
    byte[] id = state.segmentInfo.getId();
    try {
      meta = create(META_EXTENSION);
      data = create(DATA_EXTENSION);
      CodecUtil.writeIndexHeader(meta, META_CODEC_NAME, VERSION_CURRENT, id, state.segmentSuffix);
      CodecUtil.writeIndexHeader(data, DATA_CODEC_NAME, VERSION_CURRENT, id, state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, meta, data);
      throw t;
    }
  }

  private IndexOutput create(String ext) throws IOException {
    String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext);
    return state.directory.createOutput(name, state.context);
  }

  static long rotationSeed(int dim) {
    return 0x9E3779B97F4A7C15L ^ dim;
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo info) {
    if (info.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("SegmentIVF supports only FLOAT32 vectors");
    }
    fields.add(new BufferedField(info));
    return fields.getLast();
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (BufferedField field : fields) {
      int count = field.size, dim = field.info.getVectorDimension();
      float[][] vectors = field.vectors;
      var si = state.segmentInfo;
      long[] keys = new long[count];
      for (int i = 0; i < count; i++) {
        int doc = sortMap == null ? field.docIds[i] : sortMap.oldToNew(field.docIds[i]);
        keys[i] = (long) doc << 32 | i;
      }
      Arrays.sort(keys);
      try (var staged = new StagedVectors(state, new FineCodec(format.fineTier, dim), 0)) {
        for (int start = 0, n; start < count; start += n) {
          n = Math.min(StagedVectors.CHUNK_ORDS, count - start);
          staged.add(start, n, k -> (int) (keys[k] >>> 32), (k, _) -> vectors[(int) keys[k]], true);
          for (int k = start; k < start + n; k++) vectors[(int) keys[k]] = null;
        }
        HotStart.Seed hs = HotStart.seed(si.dir, si.name, field.info, format.nlist);
        if (state.infoStream.isEnabled("SIVF")) {
          String src = hs == null ? "cold" : "segment=" + hs.segment() + " vectors=" + hs.vectors();
          String message = "flush field=" + field.info.name + " docs=" + count + " source=" + src;
          state.infoStream.message("SIVF", message);
        }
        String lineage = hs == null ? si.name : hs.lineage();
        writeField(field.info, staged, hs == null ? null : hs.centroids(), null, lineage);
      }
      field.size = 0;
    }
  }

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

  @Override
  public IORunnable mergeOneField(FieldInfo info, MergeState mergeState) throws IOException {
    int dim = info.getVectorDimension(), readers = mergeState.knnVectorsReaders.length;
    Field[] views = new Field[readers];
    HotStart.Seed[] snapshots = new HotStart.Seed[readers];
    HotStart.Source[] sources = new HotStart.Source[readers];
    FloatVectorValues[] vals = new FloatVectorValues[readers];
    List<MergeSub> subs = new ArrayList<>();
    int at = 0;
    boolean copyable = true;
    for (int r = 0; r < readers; r++) {
      var reader = mergeState.knnVectorsReaders[r];
      if (reader == null) continue;
      if (reader.unwrapReaderForField(info.name) instanceof SegmentIVFVectorsReader sivf
          && sivf.field(info.name) instanceof Field view
          && view.rotationSeed == rotationSeed(dim)
          && view.fineTier == format.fineTier) {
        views[r] = view;
        snapshots[r] = HotStart.snapshot(state.segmentInfo.dir, sivf.segment(), info);
        String lineage = snapshots[r] == null ? null : snapshots[r].lineage();
        sources[r] = new HotStart.Source(view.centroids, null, lineage, view.count);
      }
      vals[r] = reader.getFloatVectorValues(info.name);
      if (vals[r] == null) continue;
      subs.add(new MergeSub(mergeState.docMaps[r], vals[r].iterator(), r));
      if (views[r] != null || copyable == false) continue;
      try {
        vals[r].copy();
      } catch (UnsupportedOperationException _) {
        copyable = false; // e.g. a sorting view: stage on the merge thread instead
      }
    }
    DocIDMerger<MergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
    int donor = HotStart.donor(sources, format.nlist);
    Field from = donor < 0 ? null : views[donor];
    int[] cells = new int[0], cell2 = new int[0];
    // Rows of same-lineage readers carry their cells into the donor's clustering; their live
    // primary-cell populations weight the seed centroids, exactly as for a flush.
    int[][] primary = new int[readers][], secondary = new int[readers][];
    for (int r = 0; from != null && r < readers; r++) {
      if (HotStart.sameLineage(sources[r], sources[donor]) == false) continue;
      sources[r] =
          new HotStart.Source(
              views[r].centroids, new int[from.nlist], sources[r].lineage(), views[r].count);
      if (snapshots[r] == null) continue;
      int[] assignment = snapshots[r].assignment();
      if (assignment != null && assignment.length == views[r].count) primary[r] = assignment;
      secondary[r] = snapshots[r].cell2();
    }
    boolean parallel = copyable;
    try (var staged = new StagedVectors(state, new FineCodec(format.fineTier, dim), readers)) {
      int max = StagedVectors.CHUNK_ORDS;
      int[] srcs = new int[max], ords = new int[max], docs = new int[max];
      StagedVectors.Rows rows =
          (j, local) -> {
            int r = srcs[j];
            if (views[r] != null) {
              if (local[r] == null) local[r] = views[r].new Values();
              ((Field.Values) local[r]).copyRow(ords[j], docs[j], staged.chunk, j * staged.stride);
              return null;
            }
            if (local[r] == null) local[r] = parallel ? vals[r].copy() : vals[r];
            return local[r].vectorValue(ords[j]);
          };
      for (int n = max; n == max; ) {
        mergeState.checkAborted();
        n = 0;
        for (MergeSub sub; n < max && (sub = merger.next()) != null; n++) {
          srcs[n] = sub.reader;
          ords[n] = sub.iterator.index();
          docs[n] = sub.mappedDocID;
        }
        staged.add(0, n, j -> docs[j], rows, parallel);
        if (from == null) continue;
        cells = ArrayUtil.grow(cells, at + n);
        cell2 = ArrayUtil.grow(cell2, at + n);
        for (int j = 0; j < n; j++, at++) {
          int r = srcs[j], ord = ords[j];
          int[] members = sources[r] == null ? null : sources[r].members();
          if (members == null) {
            cells[at] = cell2[at] = -1;
            continue;
          }
          int[] first = primary[r], second = secondary[r];
          cells[at] = first != null && ord < first.length ? first[ord] : views[r].cellOf(ord);
          cell2[at] = second != null && ord < second.length ? second[ord] : -1;
          members[cells[at]]++;
        }
      }
      float[][] seed = from == null ? null : HotStart.weightedCentroids(sources, donor);
      // Carried rows get NaN (recompute) distances, others MAX_VALUE; seedless warm is dropped.
      float[] d = new float[at];
      for (int i = 0; i < at; i++) d[i] = cells[i] < 0 ? Float.MAX_VALUE : Float.NaN;
      var warm =
          new WarmState(
              ArrayUtil.copyOfSubArray(cells, 0, at),
              ArrayUtil.copyOfSubArray(cell2, 0, at),
              d,
              d.clone());
      String lineage = from == null ? null : sources[donor].lineage();
      writeField(info, staged, seed, warm, lineage == null ? state.segmentInfo.name : lineage);
    }
    return null;
  }

  private void writeField(
      FieldInfo info, StagedVectors staged, float[][] seed, WarmState warm, String lineage)
      throws IOException {
    RandomAccessInput rows = staged.finish();
    int dim = info.getVectorDimension(), count = staged.count;
    if (seed == null) warm = null;
    // Every non-empty segment trains the configured cell count, leaving cells empty when it has
    // fewer vectors, so any segment of this configuration can seed any other.
    int nlist = count == 0 ? 0 : format.nlist;
    meta.writeInt(info.number);
    meta.writeByte((byte) format.fineTier.ordinal());
    meta.writeVInt(dim);
    meta.writeVInt(nlist);
    meta.writeVInt(count);
    meta.writeLong(rotationSeed(dim));
    meta.writeVInt(format.nprobe);
    meta.writeVInt(format.spillBits);
    if (nlist == 0) {
      for (int s = 0; s < 7; s++) meta.writeVLong(data.getFilePointer());
      return;
    }
    Clustering.Result cl = Clustering.cluster(staged, nlist, seed, warm, format.spillBits);
    HotStart.publish(state.segmentInfo.dir, state.segmentInfo.name, lineage, info, cl, count);
    int[] cellStart = new int[nlist + 1];
    for (int i = 0; i < count; i++) {
      for (int k = cl.cellCount(i) - 1; k >= 0; k--) cellStart[cl.cell(i, k) + 1]++;
    }
    for (int c = 0; c < nlist; c++) cellStart[c + 1] += cellStart[c];
    int[] slotRow = new int[cellStart[nlist]], slotDoc = new int[slotRow.length];
    int[] primarySlot = new int[count], next = ArrayUtil.copyOfSubArray(cellStart, 0, nlist);
    for (int i = 0; i < count; i++) {
      for (int k = 0, n = cl.cellCount(i); k < n; k++) {
        int slot = next[cl.cell(i, k)]++;
        slotRow[slot] = i;
        if (k == 0) primarySlot[i] = slot;
      }
    }
    meta.writeVLong(data.getFilePointer());
    for (float[] centroid : cl.centroids()) {
      for (float v : centroid) data.writeInt(Float.floatToIntBits(v));
    }
    byte[] row = new byte[staged.recordLen];
    int stride = staged.stride, docIdOffset = staged.fine.codeBytes;
    int primaryOffset = CodeRecord.primaryCellOffset(docIdOffset);
    for (int pass = 0; pass < 2; pass++) {
      int from = pass * staged.recordLen, len = pass == 0 ? staged.recordLen : staged.coarseBytes;
      meta.writeVLong(data.getFilePointer());
      for (int slot = 0; slot < slotRow.length; slot++) {
        if ((slot & (GATHER_AHEAD - 1)) == 0) {
          int end = Math.min(slotRow.length, slot + GATHER_AHEAD);
          for (int p = slot; p < end; p++) rows.prefetch((long) slotRow[p] * stride, stride);
        }
        rows.readBytes((long) slotRow[slot] * stride + from, row, 0, len);
        if (pass == 0) {
          BitUtil.VH_LE_INT.set(row, primaryOffset, cl.cell(slotRow[slot], 0));
          slotDoc[slot] = (int) BitUtil.VH_LE_INT.get(row, docIdOffset);
        }
        data.writeBytes(row, 0, len);
      }
    }
    meta.writeVLong(data.getFilePointer());
    if (nlist > 1) {
      CentroidGraph.build(new CentroidCodes(cl.centroids(), dim, null), dim).write(data);
    }
    meta.writeVLong(data.getFilePointer());
    for (int slot : primarySlot) data.writeInt(slot);
    meta.writeVLong(data.getFilePointer());
    for (int doc : slotDoc) data.writeInt(doc);
    meta.writeVLong(data.getFilePointer());
    var w = DirectMonotonicWriter.getInstance(meta, data, nlist + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
    for (int start : cellStart) w.add((long) start * Integer.BYTES);
    w.finish();
  }

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
    return fields.stream().mapToLong(BufferedField::ramBytesUsed).sum();
  }

  private static final class BufferedField extends KnnFieldVectorsWriter<float[]> {
    final FieldInfo info;
    float[][] vectors = new float[16][];
    int[] docIds = new int[16];
    int size;

    BufferedField(FieldInfo info) {
      this.info = info;
    }

    @Override
    public void addValue(int docID, float[] value) {
      if (size > 0 && docIds[size - 1] == docID) {
        throw new IllegalArgumentException(
            "field \"" + info.name + "\" appears more than once in document " + docID);
      }
      if (size == vectors.length) {
        vectors = ArrayUtil.grow(vectors, size + 1);
        docIds = ArrayUtil.growExact(docIds, vectors.length);
      }
      vectors[size] = copyValue(value);
      docIds[size++] = docID;
    }

    @Override
    public float[] copyValue(float[] value) {
      return ArrayUtil.copyOfSubArray(value, 0, info.getVectorDimension());
    }

    @Override
    public long ramBytesUsed() {
      if (size == 0) return 0;
      long vector = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 4L * info.getVectorDimension();
      long arrays = RamUsageEstimator.shallowSizeOf(vectors) + RamUsageEstimator.sizeOf(docIds);
      return arrays + size * RamUsageEstimator.alignObjectSize(vector);
    }
  }

  /** Disk-backed build rows, reused across clustering passes without keeping vectors on heap. */
  static final class StagedVectors implements Closeable {
    static final int CHUNK_ORDS = 16_384;

    interface Rows {
      float[] vector(int index, FloatVectorValues[] local) throws IOException;
    }

    final FineCodec fine;
    final int dim, recordLen, coarseBytes, stride, readers;
    private final HadamardRotation rotation;
    private final SegmentWriteState state;
    private final String name;
    private byte[] chunk;
    private IndexOutput out;
    private IndexInput input;
    int count;

    StagedVectors(SegmentWriteState state, FineCodec fine, int readers) throws IOException {
      this.state = state;
      this.fine = fine;
      this.readers = readers;
      dim = fine.dim;
      recordLen = CodeRecord.length(fine.codeBytes);
      coarseBytes = Nitrox2.bytesPerVector(dim);
      stride = recordLen + coarseBytes;
      rotation = HadamardRotation.create(dim, rotationSeed(dim));
      out = state.directory.createTempOutput(state.segmentInfo.name, "ivfstage", state.context);
      name = out.getName();
    }

    void add(int base, int n, IntUnaryOperator docs, Rows source, boolean parallel)
        throws IOException {
      if (chunk == null) chunk = new byte[n * stride];
      int docIdOffset = fine.codeBytes, primaryOffset = CodeRecord.primaryCellOffset(docIdOffset);
      Parallel.RangeTask encode =
          (lo, hi) -> {
            FloatVectorValues[] local = new FloatVectorValues[readers];
            float[] unit = new float[dim], rotated = new float[dim];
            for (int j = lo; j < hi; j++) {
              float[] vector = source.vector(base + j, local);
              if (vector == null) continue;
              int at = j * stride;
              System.arraycopy(vector, 0, unit, 0, dim);
              VectorUtil.l2normalize(unit);
              rotation.rotate(unit, rotated);
              fine.encode(rotated, chunk, at);
              BitUtil.VH_LE_INT.set(chunk, at + docIdOffset, docs.applyAsInt(base + j));
              BitUtil.VH_LE_INT.set(chunk, at + primaryOffset, 0);
              Nitrox2.encode(rotated, dim, chunk, at + recordLen);
            }
          };
      if (parallel) Parallel.overRange(n, encode);
      else encode.run(0, n);
      out.writeBytes(chunk, 0, n * stride);
      count += n;
    }

    RandomAccessInput finish() throws IOException {
      out.close();
      out = null;
      input = state.directory.openInput(name, state.context);
      return input.randomAccessSlice(0, input.length());
    }

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

      float[] vector() {
        if (decoded == false) {
          fine.decode(row, 0, vector);
          VectorUtil.l2normalize(vector, false);
          decoded = true;
        }
        return vector;
      }

      void coarseInto(byte[] dest) {
        System.arraycopy(row, recordLen, dest, 0, coarseBytes);
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(out, input, () -> IOUtils.deleteFilesIgnoringExceptions(state.directory, name));
    }
  }
}
