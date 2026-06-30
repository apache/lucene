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

package org.apache.lucene.codecs.dedup;

import static org.apache.lucene.codecs.dedup.DedupFlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Writes a {@link DedupFlatVectorsFormat} segment.
 *
 * <p><b>Indexing (flush)</b>: per-field calls accumulate vectors in heap, with a per-pool dedup
 * hash table ({@link FlushPool}). {@code flush()} writes contiguous unique-vector bytes per pool,
 * then per-field {@code docOrd → vecOrd} maps.
 *
 * <p><b>Merge</b>: per-field calls iterate source segments via {@link DocIDMerger}. A per-pool
 * {@link MergePool} hash table maps the 64-bit hash of each candidate vector to a {@code
 * (KnnVectorValues, srcOrd)} pair so that hash-collisions can be byte-verified by re-reading the
 * source via mmap. No temp files are written; pool data is materialised into {@code .dvc} at {@link
 * #finish()} time by re-reading each unique vector from its source segment (sources stay open until
 * merge ends).
 *
 * <p>When a source reader is itself a {@link DedupFlatVectorsReader}, the merge takes a fast path
 * (Level A): each source vec-ord is interned <em>once</em> per (mergedPool, sourceReader) pair and
 * cached in a sparse {@code srcVecOrd → mergedVecOrd} array, so the per-doc loop becomes two int
 * reads with no hash + no byte-verify. Lazy materialisation ensures source uniques whose docs are
 * all deleted are never copied into the merged pool.
 *
 * @lucene.experimental
 */
public final class DedupFlatVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta;
  private final IndexOutput vectorData;

  // Pools used during flush — keyed by (dim, encoding); insertion-order preserved as pool id.
  private final Map<PoolKey, FlushPool> flushPools = new LinkedHashMap<>();
  private final List<FlushFieldWriter<?>> flushFields = new ArrayList<>();

  // Pools used during merge.
  private final Map<PoolKey, MergePool> mergePools = new LinkedHashMap<>();
  private final List<MergeFieldState> mergeFields = new ArrayList<>();

  private boolean finished;
  private boolean usedForMerge;

  DedupFlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer) throws IOException {
    super(scorer);
    this.segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, DedupFlatVectorsFormat.META_EXTENSION);
    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            DedupFlatVectorsFormat.VECTOR_DATA_EXTENSION);

    boolean success = false;
    IndexOutput m = null, v = null;
    try {
      m = state.directory.createOutput(metaFileName, state.context);
      v = state.directory.createOutput(vectorDataFileName, state.context);
      CodecUtil.writeIndexHeader(
          m,
          DedupFlatVectorsFormat.META_CODEC_NAME,
          DedupFlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          v,
          DedupFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          DedupFlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      this.meta = m;
      this.vectorData = v;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(m, v);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Indexing path (flush)
  // ---------------------------------------------------------------------------

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    PoolKey key = new PoolKey(fieldInfo.getVectorDimension(), fieldInfo.getVectorEncoding());
    FlushPool pool = flushPools.computeIfAbsent(key, FlushPool::new);
    FlushFieldWriter<?> w =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> new FlushFieldWriter.ByteImpl(fieldInfo, pool);
          case FLOAT32 -> new FlushFieldWriter.FloatImpl(fieldInfo, pool);
        };
    flushFields.add(w);
    return w;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    if (usedForMerge) {
      throw new IllegalStateException(
          "DedupFlatVectorsWriter cannot be used for both flush and merge in the same instance");
    }
    // 1. Write each pool's contiguous unique-vector bytes to .dvc.
    long[] poolOffsets = new long[flushPools.size()];
    long[] poolLengths = new long[flushPools.size()];
    assignPoolIds(flushPools.values());
    int p = 0;
    for (FlushPool pool : flushPools.values()) {
      long start = alignOutput(vectorData, pool.key.encoding);
      pool.writeBytes(vectorData);
      poolOffsets[p] = start;
      poolLengths[p] = vectorData.getFilePointer() - start;
      p++;
    }
    // 2. Pool metadata.
    writePoolsMeta(flushPools.values(), poolOffsets, poolLengths);
    // 3. Per-field DISI + map + meta record.
    for (FlushFieldWriter<?> field : flushFields) {
      writeFlushField(field, maxDoc, sortMap);
      field.finish();
    }
  }

  private void assignPoolIds(Iterable<? extends Pool> pools) {
    int n = 0;
    for (Pool pool : pools) {
      pool.poolId = n++;
    }
  }

  private void writePoolsMeta(
      Iterable<? extends Pool> orderedPools, long[] poolOffsets, long[] poolLengths)
      throws IOException {
    int numPools = poolOffsets.length;
    meta.writeVInt(numPools);
    int p = 0;
    for (Pool pool : orderedPools) {
      meta.writeVInt(pool.key.dim);
      meta.writeByte((byte) pool.key.encoding.ordinal());
      meta.writeVLong(poolOffsets[p]);
      meta.writeVLong(poolLengths[p]);
      meta.writeVInt(pool.uniqueCount());
      p++;
    }
  }

  private void writeFlushField(FlushFieldWriter<?> field, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    int cardinality = field.docsWithField.cardinality();
    DocsWithFieldSet docsToWrite = field.docsWithField;
    int[] mapValues = new int[cardinality];
    if (sortMap == null || cardinality == 0) {
      for (int i = 0; i < cardinality; i++) {
        mapValues[i] = field.docOrdToVecOrd.get(i);
      }
    } else {
      int[] old2New = new int[cardinality];
      int[] new2Old = new int[cardinality];
      DocsWithFieldSet sortedSet = new DocsWithFieldSet();
      KnnVectorsWriter.mapOldOrdToNewOrd(field.docsWithField, sortMap, old2New, new2Old, sortedSet);
      for (int newOrd = 0; newOrd < cardinality; newOrd++) {
        mapValues[newOrd] = field.docOrdToVecOrd.get(new2Old[newOrd]);
      }
      docsToWrite = sortedSet;
    }
    writeFieldRecord(field.fieldInfo, field.pool, maxDoc, docsToWrite, mapValues);
  }

  /**
   * Emit one field's DISI + packed map to {@code .dvc} and one field meta record to {@code .dvm}.
   * Meta-record format (matching read order in {@link DedupFlatVectorsReader.FieldEntry}):
   *
   * <pre>
   *   [int]   fieldNumber
   *   [byte]  similarityOrdinal
   *   [vint]  poolId
   *   [int]   cardinality
   *   ... OrdToDocDISIReaderConfiguration meta (writes inline) ...
   *   [vlong] mapOffset
   *   [vlong] mapLength
   *   [byte]  bitsPerVecOrd
   * </pre>
   */
  private void writeFieldRecord(
      FieldInfo fieldInfo,
      Pool pool,
      int maxDoc,
      DocsWithFieldSet docsWithField,
      int[] docOrdToVecOrd)
      throws IOException {
    int cardinality = docsWithField.cardinality();
    meta.writeInt(fieldInfo.number);
    meta.writeByte((byte) fieldInfo.getVectorSimilarityFunction().ordinal());
    meta.writeVInt(pool.poolId);
    meta.writeInt(cardinality);
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, cardinality, maxDoc, docsWithField);
    long mapOffset = vectorData.getFilePointer();
    int bitsPerVecOrd;
    if (cardinality == 0) {
      bitsPerVecOrd = 1;
    } else {
      int maxVecOrd = Math.max(0, pool.uniqueCount() - 1);
      bitsPerVecOrd = DirectWriter.bitsRequired(Math.max(1, maxVecOrd));
      DirectWriter writer = DirectWriter.getInstance(vectorData, cardinality, bitsPerVecOrd);
      for (int v : docOrdToVecOrd) {
        writer.add(v);
      }
      writer.finish();
    }
    long mapLength = vectorData.getFilePointer() - mapOffset;
    meta.writeVLong(mapOffset);
    meta.writeVLong(mapLength);
    meta.writeByte((byte) bitsPerVecOrd);
  }

  // ---------------------------------------------------------------------------
  // Merge path
  // ---------------------------------------------------------------------------

  @Override
  public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    if (!flushPools.isEmpty()) {
      throw new IllegalStateException(
          "DedupFlatVectorsWriter cannot be used for both flush and merge in the same instance");
    }
    usedForMerge = true;
    PoolKey key = new PoolKey(fieldInfo.getVectorDimension(), fieldInfo.getVectorEncoding());
    MergePool pool = mergePools.computeIfAbsent(key, MergePool::new);
    MergeFieldState fieldState = new MergeFieldState(fieldInfo, pool);
    mergeFields.add(fieldState);

    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> mergeByteField(fieldInfo, mergeState, fieldState);
      case FLOAT32 -> mergeFloatField(fieldInfo, mergeState, fieldState);
    }
  }

  private void mergeFloatField(
      FieldInfo fieldInfo, MergeState mergeState, MergeFieldState fieldState) throws IOException {
    List<FloatSub> subs = new ArrayList<>();
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      if (mergeState.knnVectorsReaders[i] == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name) == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name).hasVectorValues() == false) {
        continue;
      }
      FloatVectorValues vals = mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
      if (vals == null) continue;
      subs.add(new FloatSub(mergeState.docMaps[i], vals, i));
    }
    DocIDMerger<FloatSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
    int dim = fieldInfo.getVectorDimension();
    byte[] candidateBytes = new byte[dim * Float.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(candidateBytes).order(ByteOrder.LITTLE_ENDIAN);
    // Level-A fast path setup: when a source reader is itself a dedup format reader, attach the
    // source's docOrd→vecOrd table and a lazily-filled srcVecOrd→mergedVecOrd map (shared per
    // (mergedPool, sourceReader)). The doc loop below interns each source vec-ord on first
    // encounter only — uniques whose docs are all deleted are never interned (correctness),
    // and uniques shared by many surviving docs are interned exactly once (perf win).
    for (FloatSub sub : subs) {
      if (sub.values instanceof OffHeapDedupFloatVectorValues dedup) {
        sub.srcDocOrdToVecOrd = dedup.docOrdToVecOrd;
        sub.srcPoolView = dedup.poolView();
        sub.srcVecToMerged =
            fieldState.pool.srcVecToMergedMap(sub.readerIdx, sub.srcPoolView.size());
      }
    }
    for (FloatSub sub = merger.next(); sub != null; sub = merger.next()) {
      int srcOrd = sub.iterator.index();
      int vecOrd;
      if (sub.srcVecToMerged != null) {
        int srcVecOrd = sub.srcDocOrdToVecOrd[srcOrd];
        vecOrd = sub.srcVecToMerged[srcVecOrd];
        if (vecOrd < 0) {
          // First surviving doc to reference this src vec-ord — read bytes, hash, intern.
          float[] floats = ((FloatVectorValues) sub.srcPoolView).vectorValue(srcVecOrd);
          bb.position(0).asFloatBuffer().put(floats);
          vecOrd = fieldState.pool.intern(candidateBytes, sub.srcPoolView, srcVecOrd);
          sub.srcVecToMerged[srcVecOrd] = vecOrd;
        }
      } else {
        float[] v = sub.values.vectorValue(srcOrd);
        bb.position(0).asFloatBuffer().put(v);
        // Pass the field-specific values directly so that future byte-verify lookups (which can
        // race across fields sharing this pool) read from the correct field-local ord-space.
        vecOrd = fieldState.pool.intern(candidateBytes, sub.values, srcOrd);
      }
      fieldState.docOrdToVecOrd.add(vecOrd);
      fieldState.docsWithField.add(sub.mappedDocID);
    }
  }

  private void mergeByteField(
      FieldInfo fieldInfo, MergeState mergeState, MergeFieldState fieldState) throws IOException {
    List<ByteSub> subs = new ArrayList<>();
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      if (mergeState.knnVectorsReaders[i] == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name) == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name).hasVectorValues() == false) {
        continue;
      }
      ByteVectorValues vals = mergeState.knnVectorsReaders[i].getByteVectorValues(fieldInfo.name);
      if (vals == null) continue;
      subs.add(new ByteSub(mergeState.docMaps[i], vals, i));
    }
    DocIDMerger<ByteSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
    int dim = fieldInfo.getVectorDimension();
    byte[] candidateBytes = new byte[dim];
    // Level-A fast path: see mergeFloatField.
    for (ByteSub sub : subs) {
      if (sub.values instanceof OffHeapDedupByteVectorValues dedup) {
        sub.srcDocOrdToVecOrd = dedup.docOrdToVecOrd;
        sub.srcPoolView = dedup.poolView();
        sub.srcVecToMerged =
            fieldState.pool.srcVecToMergedMap(sub.readerIdx, sub.srcPoolView.size());
      }
    }
    for (ByteSub sub = merger.next(); sub != null; sub = merger.next()) {
      int srcOrd = sub.iterator.index();
      int vecOrd;
      if (sub.srcVecToMerged != null) {
        int srcVecOrd = sub.srcDocOrdToVecOrd[srcOrd];
        vecOrd = sub.srcVecToMerged[srcVecOrd];
        if (vecOrd < 0) {
          byte[] bytes = ((ByteVectorValues) sub.srcPoolView).vectorValue(srcVecOrd);
          System.arraycopy(bytes, 0, candidateBytes, 0, dim);
          vecOrd = fieldState.pool.intern(candidateBytes, sub.srcPoolView, srcVecOrd);
          sub.srcVecToMerged[srcVecOrd] = vecOrd;
        }
      } else {
        byte[] v = sub.values.vectorValue(srcOrd);
        System.arraycopy(v, 0, candidateBytes, 0, dim);
        vecOrd = fieldState.pool.intern(candidateBytes, sub.values, srcOrd);
      }
      fieldState.docOrdToVecOrd.add(vecOrd);
      fieldState.docsWithField.add(sub.mappedDocID);
    }
  }

  // ---------------------------------------------------------------------------
  // Common: finish (writes metadata; for merge also materialises pool bytes)
  // ---------------------------------------------------------------------------

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (usedForMerge) {
      finishForMerge();
    }
    if (meta != null) {
      meta.writeInt(-1); // sentinel
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
    }
  }

  private void finishForMerge() throws IOException {
    int numPools = mergePools.size();
    long[] poolOffsets = new long[numPools];
    long[] poolLengths = new long[numPools];
    assignPoolIds(mergePools.values());
    int p = 0;
    for (MergePool pool : mergePools.values()) {
      long start = alignOutput(vectorData, pool.key.encoding);
      pool.writeBytes(vectorData);
      poolOffsets[p] = start;
      poolLengths[p] = vectorData.getFilePointer() - start;
      p++;
    }
    writePoolsMeta(mergePools.values(), poolOffsets, poolLengths);
    int maxDoc = segmentWriteState.segmentInfo.maxDoc();
    for (MergeFieldState fs : mergeFields) {
      int cardinality = fs.docsWithField.cardinality();
      int[] mapValues = new int[cardinality];
      for (int i = 0; i < cardinality; i++) {
        mapValues[i] = fs.docOrdToVecOrd.get(i);
      }
      writeFieldRecord(fs.fieldInfo, fs.pool, maxDoc, fs.docsWithField, mapValues);
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle / accounting
  // ---------------------------------------------------------------------------

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    // Note: each FlushFieldWriter.ramBytesUsed() already includes its pool's RAM (so wrappers
    // that only see per-field RAM stay accurate). Sum fields directly; do not add pools again.
    for (FlushFieldWriter<?> field : flushFields) {
      total += field.ramBytesUsed();
    }
    for (MergePool pool : mergePools.values()) {
      total += pool.ramBytesUsed();
    }
    for (MergeFieldState fs : mergeFields) {
      total += fs.ramBytesUsed();
    }
    return total;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData);
  }

  private static long alignOutput(IndexOutput output, VectorEncoding encoding) throws IOException {
    return output.alignFilePointer(
        switch (encoding) {
          case BYTE -> Float.BYTES;
          case FLOAT32 -> 64;
        });
  }

  // ---------------------------------------------------------------------------
  // Pool / field types
  // ---------------------------------------------------------------------------

  static record PoolKey(int dim, VectorEncoding encoding) {}

  abstract static class Pool {
    final PoolKey key;
    int poolId = -1;

    Pool(PoolKey key) {
      this.key = key;
    }

    abstract int uniqueCount();

    abstract long ramBytesUsed();
  }

  /** Pool used for in-flush dedup: all unique vector bytes live in heap. */
  static final class FlushPool extends Pool {
    private final List<byte[]> vectors = new ArrayList<>();

    /**
     * Parallel list of float[] views for FLOAT32 pools, indexed by vec-ord. Populated only when
     * {@link #internFloats} is used (i.e. for the FLOAT32 encoding); stays empty for BYTE.
     *
     * <p>This avoids the per-call {@code new float[dim]} allocation that would otherwise happen in
     * {@link FlushFieldWriter.FloatImpl#getVectors()} during HNSW build at flush. With it, the HNSW
     * builder can score against the cached float[] directly — same shape as {@link
     * org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter}'s in-heap list, with no per-call
     * materialisation.
     */
    private final List<float[]> vectorFloats = new ArrayList<>();

    private final HashIndex hashIndex = new HashIndex();
    private final int byteSize;

    FlushPool(PoolKey key) {
      super(key);
      this.byteSize = key.dim * key.encoding.byteSize;
    }

    int intern(byte[] bytesCandidate) {
      assert bytesCandidate.length == byteSize;
      long h = DedupHash.hash(bytesCandidate, byteSize);
      // Open-addressed probe; on hit byte-verify against stored vector.
      for (int slot = hashIndex.probeStart(h); ; slot = hashIndex.probeNext(slot)) {
        long stored = hashIndex.hashAt(slot);
        if (stored == HashIndex.EMPTY) {
          int newOrd = vectors.size();
          byte[] copy = ArrayUtil.copyOfSubArray(bytesCandidate, 0, byteSize);
          vectors.add(copy);
          hashIndex.put(slot, h, newOrd);
          return newOrd;
        }
        if (stored == h) {
          int candidateOrd = hashIndex.ordAt(slot);
          if (Arrays.equals(bytesCandidate, vectors.get(candidateOrd))) {
            return candidateOrd;
          }
          // hash collision (different bytes, same 64-bit hash) — keep probing
        }
      }
    }

    /**
     * Intern a FLOAT32 vector. {@code floatsCandidate} is the source float[] (typically the
     * caller's reused buffer); {@code bytesCandidate} is its IEEE-754 little-endian byte form (also
     * a reused scratch buffer). On miss, both a {@link ArrayUtil#copyOfSubArray} of the bytes and a
     * {@link ArrayUtil#copyOfSubArray} of the floats are stored. On hit (by hash then
     * byte-equality), no allocation occurs.
     *
     * <p>Storing both byte[] and float[] uses ~2× memory per unique vector vs byte-only, but
     * eliminates the per-scoring-call float[] allocation in HNSW build — a net memory win for any
     * non-trivial workload because GC churn dominates at HNSW scale.
     */
    int internFloats(float[] floatsCandidate, byte[] bytesCandidate) {
      assert bytesCandidate.length == byteSize;
      assert floatsCandidate.length * Float.BYTES == byteSize;
      long h = DedupHash.hash(bytesCandidate, byteSize);
      for (int slot = hashIndex.probeStart(h); ; slot = hashIndex.probeNext(slot)) {
        long stored = hashIndex.hashAt(slot);
        if (stored == HashIndex.EMPTY) {
          int newOrd = vectors.size();
          vectors.add(ArrayUtil.copyOfSubArray(bytesCandidate, 0, byteSize));
          vectorFloats.add(ArrayUtil.copyOfSubArray(floatsCandidate, 0, floatsCandidate.length));
          hashIndex.put(slot, h, newOrd);
          return newOrd;
        }
        if (stored == h) {
          int candidateOrd = hashIndex.ordAt(slot);
          // float-equality is identical to byte-equality modulo NaN/0.0 (which can never reach
          // here because they would yield different bytes → different hash → different slot).
          if (Arrays.equals(floatsCandidate, vectorFloats.get(candidateOrd))) {
            return candidateOrd;
          }
          // hash collision; keep probing
        }
      }
    }

    /** Float view by vec-ord; only valid for FLOAT32 pools. */
    float[] vectorFloatsAt(int vecOrd) {
      return vectorFloats.get(vecOrd);
    }

    @Override
    int uniqueCount() {
      return vectors.size();
    }

    void writeBytes(IndexOutput out) throws IOException {
      for (byte[] v : vectors) {
        out.writeBytes(v, 0, v.length);
      }
    }

    @Override
    long ramBytesUsed() {
      long size = RamUsageEstimator.shallowSizeOfInstance(FlushPool.class);
      size += hashIndex.ramBytesUsed();
      size +=
          (long) vectors.size()
                  * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                      + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
              + (long) vectors.size() * byteSize;
      // Float view (FLOAT32 only) — same number of entries × byteSize (4B per float × dim).
      if (!vectorFloats.isEmpty()) {
        size +=
            (long) vectorFloats.size()
                    * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                        + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
                + (long) vectorFloats.size() * byteSize;
      }
      return size;
    }
  }

  /**
   * Pool used during merge: holds source-vector references (no full bytes). Pool bytes are
   * materialised into {@code .dvc} by re-reading sources at {@link #writeBytes} (called from {@link
   * #finishForMerge}).
   *
   * <p>A {@link MergePool} is shared across all fields with matching {@code (dim, encoding)} — the
   * cross-field dedup that's the format's reason for existing. To verify a hash hit we may need to
   * re-read bytes for a vector that was originally interned from a <em>different</em> field on the
   * same source segment. Each pool entry therefore stores a direct reference to the field-specific
   * {@link KnnVectorValues} that produced it, not a (readerIdx, fieldName) pair — the readerIdx
   * alone is insufficient because the field-local ord-space differs across fields.
   *
   * <p><b>Level-A optimisation</b>: when a source reader is itself a {@link
   * DedupFlatVectorsReader}, its pool already guarantees byte-distinct vec-ords. {@link
   * #srcVecToMergedMap} returns a sparse {@code srcVecOrd → mergedVecOrd} array (cached per source
   * reader) that the merge doc loop lazily fills on first encounter of each surviving vec-ord.
   * Multiple target fields sharing this merged pool reuse the same cached array.
   *
   * <p>Not thread-safe — single-threaded per merge.
   */
  static final class MergePool extends Pool {
    private final HashIndex hashIndex = new HashIndex();

    /**
     * For each vecOrd: the field-specific {@link KnnVectorValues} the vector came from. Calling
     * {@code refValues[ord].vectorValue(refSourceOrd[ord])} returns the original bytes (in the
     * reader's reused buffer — copy before retaining).
     */
    private KnnVectorValues[] refValues = new KnnVectorValues[16];

    /** For each vecOrd: field-local ord within {@code refValues[ord]}. */
    private int[] refSourceOrd = new int[16];

    private int unique = 0;
    private final int byteSize;

    /**
     * Cache of {@code (sourceReaderIdx → srcVecOrd → mergedVecOrd)} mappings populated by {@link
     * #srcVecToMergedMap}. Lets multiple target fields sharing this merged pool reuse the same
     * source-uniqueness work for one source reader. Keyed only by {@code readerIdx} because all
     * fields with this {@link PoolKey} on a given source dedup reader share the source's pool.
     */
    private final Map<Integer, int[]> srcInternCache = new HashMap<>();

    MergePool(PoolKey key) {
      super(key);
      this.byteSize = key.dim * key.encoding.byteSize;
    }

    int intern(byte[] bytesCandidate, KnnVectorValues srcValues, int srcOrd) throws IOException {
      assert bytesCandidate.length == byteSize;
      long h = DedupHash.hash(bytesCandidate, byteSize);
      for (int slot = hashIndex.probeStart(h); ; slot = hashIndex.probeNext(slot)) {
        long stored = hashIndex.hashAt(slot);
        if (stored == HashIndex.EMPTY) {
          int newOrd = unique++;
          if (newOrd == refValues.length) {
            refValues = ArrayUtil.grow(refValues, newOrd + 1);
            refSourceOrd = ArrayUtil.grow(refSourceOrd, newOrd + 1);
          }
          refValues[newOrd] = srcValues;
          refSourceOrd[newOrd] = srcOrd;
          hashIndex.put(slot, h, newOrd);
          return newOrd;
        }
        if (stored == h) {
          int candidateOrd = hashIndex.ordAt(slot);
          byte[] reference = readSourceBytes(refValues[candidateOrd], refSourceOrd[candidateOrd]);
          if (Arrays.equals(bytesCandidate, reference)) {
            return candidateOrd;
          }
          // genuine hash collision; keep probing
        }
      }
    }

    /**
     * Level-A fast path: source reader is itself a dedup format reader, so its pool's vec-ords are
     * already byte-distinct (guaranteed by the source's own intern()). Returns a dense {@code
     * srcVecOrd → mergedVecOrd} array of size {@code srcUniqueCount}, filled with {@code -1} on
     * first call. The caller fills entries lazily on first encounter of each surviving src vec-ord
     * — so source uniques whose docs are all deleted never get interned (correctness) and uniques
     * referenced by multiple surviving docs are interned exactly once (perf win).
     *
     * <p>The array is cached per {@code readerIdx}: subsequent fields on this merged pool from the
     * same source reader share the same array (cumulative lazy fill across fields).
     */
    int[] srcVecToMergedMap(int readerIdx, int srcUniqueCount) {
      int[] cached = srcInternCache.get(readerIdx);
      if (cached != null) {
        assert cached.length == srcUniqueCount
            : "src pool size mismatch for readerIdx="
                + readerIdx
                + ": cached="
                + cached.length
                + " new="
                + srcUniqueCount;
        return cached;
      }
      int[] map = new int[srcUniqueCount];
      Arrays.fill(map, -1);
      srcInternCache.put(readerIdx, map);
      return map;
    }

    private byte[] readSourceBytes(KnnVectorValues vals, int sourceOrd) throws IOException {
      byte[] out = new byte[byteSize];
      switch (key.encoding) {
        case BYTE -> {
          byte[] v = ((ByteVectorValues) vals).vectorValue(sourceOrd);
          System.arraycopy(v, 0, out, 0, byteSize);
        }
        case FLOAT32 -> {
          float[] v = ((FloatVectorValues) vals).vectorValue(sourceOrd);
          ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(v);
        }
      }
      return out;
    }

    void writeBytes(IndexOutput out) throws IOException {
      // Re-read each unique vector from its source and emit contiguously.
      // Sources stay open for the duration of the parent merge.
      for (int vecOrd = 0; vecOrd < unique; vecOrd++) {
        byte[] bytes = readSourceBytes(refValues[vecOrd], refSourceOrd[vecOrd]);
        out.writeBytes(bytes, 0, byteSize);
      }
    }

    @Override
    int uniqueCount() {
      return unique;
    }

    @Override
    long ramBytesUsed() {
      long size = RamUsageEstimator.shallowSizeOfInstance(MergePool.class);
      size += hashIndex.ramBytesUsed();
      // refValues holds object references only; the KnnVectorValues are owned by the caller
      // (one per source-reader × field), so we account for the array slots only.
      size += (long) refValues.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      size += RamUsageEstimator.sizeOf(refSourceOrd);
      // srcInternCache: per source reader, an int[] of size source-pool-uniqueCount.
      for (int[] arr : srcInternCache.values()) {
        size += RamUsageEstimator.sizeOf(arr);
      }
      return size;
    }
  }

  abstract static class FlushFieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW =
        RamUsageEstimator.shallowSizeOfInstance(FlushFieldWriter.class);
    final FieldInfo fieldInfo;
    final FlushPool pool;
    final DocsWithFieldSet docsWithField = new DocsWithFieldSet();

    /** Per-doc-ord (insertion order) → vec-ord into the pool. */
    final IntList docOrdToVecOrd = new IntList();

    private boolean finished;
    private int lastDocID = -1;

    FlushFieldWriter(FieldInfo fieldInfo, FlushPool pool) {
      this.fieldInfo = fieldInfo;
      this.pool = pool;
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return docsWithField;
    }

    @Override
    public void finish() throws IOException {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public long ramBytesUsed() {
      // Include the pool's full RAM here so wrappers (like Lucene99HnswVectorsWriter) that only
      // consult per-field RAM still see the dedup storage. Multiple fields sharing a pool will
      // over-report; this is conservative and triggers earlier flush, which is safe.
      return SHALLOW
          + docsWithField.ramBytesUsed()
          + docOrdToVecOrd.ramBytesUsed()
          + pool.ramBytesUsed();
    }

    /** Add and intern. Subclasses serialise the vector to bytes for hashing. */
    void registerDocVecOrd(int docID, int vecOrd) {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      docOrdToVecOrd.add(vecOrd);
      lastDocID = docID;
    }

    static final class FloatImpl extends FlushFieldWriter<float[]> {
      private final byte[] scratch;
      private final ByteBuffer scratchBuffer;

      FloatImpl(FieldInfo fieldInfo, FlushPool pool) {
        super(fieldInfo, pool);
        this.scratch = new byte[fieldInfo.getVectorDimension() * Float.BYTES];
        this.scratchBuffer = ByteBuffer.wrap(scratch).order(ByteOrder.LITTLE_ENDIAN);
      }

      @Override
      public float[] copyValue(float[] value) {
        return ArrayUtil.copyOfSubArray(value, 0, fieldInfo.getVectorDimension());
      }

      @Override
      public void addValue(int docID, float[] vectorValue) throws IOException {
        if (isFinished()) {
          throw new IllegalStateException("already finished, cannot add more values");
        }
        scratchBuffer.position(0).asFloatBuffer().put(vectorValue);
        int vecOrd = pool.internFloats(vectorValue, scratch);
        registerDocVecOrd(docID, vecOrd);
      }

      @Override
      public List<float[]> getVectors() {
        // Live view backed by the pool's cached float[] list. Each get() does ZERO allocation
        // and returns the canonical float[] for the unique vector — critical for HNSW build
        // performance, since the default heap-based scorer calls vectorValue() per score.
        return new AbstractList<>() {
          @Override
          public float[] get(int index) {
            return pool.vectorFloatsAt(docOrdToVecOrd.get(index));
          }

          @Override
          public int size() {
            return docOrdToVecOrd.size();
          }
        };
      }

      @Override
      public KnnVectorValues asKnnVectorValues(VectorEncoding encoding, int dim)
          throws IOException {
        return FloatVectorValues.fromFloats(getVectors(), dim);
      }
    }

    static final class ByteImpl extends FlushFieldWriter<byte[]> {
      ByteImpl(FieldInfo fieldInfo, FlushPool pool) {
        super(fieldInfo, pool);
      }

      @Override
      public byte[] copyValue(byte[] value) {
        return ArrayUtil.copyOfSubArray(value, 0, fieldInfo.getVectorDimension());
      }

      @Override
      public void addValue(int docID, byte[] vectorValue) throws IOException {
        if (isFinished()) {
          throw new IllegalStateException("already finished, cannot add more values");
        }
        int vecOrd = pool.intern(vectorValue);
        registerDocVecOrd(docID, vecOrd);
      }

      @Override
      public List<byte[]> getVectors() {
        return new AbstractList<>() {
          @Override
          public byte[] get(int index) {
            return pool.vectors.get(docOrdToVecOrd.get(index));
          }

          @Override
          public int size() {
            return docOrdToVecOrd.size();
          }
        };
      }

      @Override
      public KnnVectorValues asKnnVectorValues(VectorEncoding encoding, int dim)
          throws IOException {
        return ByteVectorValues.fromBytes(getVectors(), dim);
      }
    }
  }

  static final class MergeFieldState {
    final FieldInfo fieldInfo;
    final MergePool pool;
    final DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    final IntList docOrdToVecOrd = new IntList();

    MergeFieldState(FieldInfo fieldInfo, MergePool pool) {
      this.fieldInfo = fieldInfo;
      this.pool = pool;
    }

    long ramBytesUsed() {
      return RamUsageEstimator.shallowSizeOfInstance(MergeFieldState.class)
          + docsWithField.ramBytesUsed()
          + docOrdToVecOrd.ramBytesUsed();
    }
  }

  // ---------------------------------------------------------------------------
  // Custom Sub types — same shape as KnnVectorsWriter.{Float,Byte}VectorValuesSub but with source
  // identity tracked. We can't reach those package-private types from here.
  // ---------------------------------------------------------------------------

  private static final class FloatSub extends DocIDMerger.Sub {
    final FloatVectorValues values;
    final KnnVectorValues.DocIndexIterator iterator;
    final int readerIdx;

    /**
     * Level-A fast-path state, populated only when {@code values instanceof
     * OffHeapDedupFloatVectorValues}: the source's dense {@code docOrd → vecOrd} table, a
     * vec-ord-keyed view of the source pool (for lazy byte reads on first encounter), and the
     * lazily-filled {@code srcVecOrd → mergedVecOrd} map shared via {@link
     * MergePool#srcVecToMergedMap}. All three are null (non-dedup source) or all non-null (dedup
     * source).
     */
    int[] srcDocOrdToVecOrd;

    KnnVectorValues srcPoolView;
    int[] srcVecToMerged;

    FloatSub(MergeState.DocMap docMap, FloatVectorValues values, int readerIdx) {
      super(docMap);
      this.values = values;
      this.iterator = values.iterator();
      this.readerIdx = readerIdx;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  private static final class ByteSub extends DocIDMerger.Sub {
    final ByteVectorValues values;
    final KnnVectorValues.DocIndexIterator iterator;
    final int readerIdx;

    /** See {@link FloatSub#srcVecToMerged}. */
    int[] srcDocOrdToVecOrd;

    KnnVectorValues srcPoolView;
    int[] srcVecToMerged;

    ByteSub(MergeState.DocMap docMap, ByteVectorValues values, int readerIdx) {
      super(docMap);
      this.values = values;
      this.iterator = values.iterator();
      this.readerIdx = readerIdx;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  /**
   * Minimal int list backed by an {@code int[]} grown via {@link ArrayUtil#grow(int[], int)}.
   * Avoids autoboxing of {@code Integer}.
   */
  static final class IntList {
    private int[] data = new int[8];
    private int size = 0;

    void add(int v) {
      if (size == data.length) {
        data = ArrayUtil.grow(data, size + 1);
      }
      data[size++] = v;
    }

    int get(int idx) {
      return data[idx];
    }

    int size() {
      return size;
    }

    long ramBytesUsed() {
      return RamUsageEstimator.shallowSizeOfInstance(IntList.class)
          + RamUsageEstimator.sizeOf(data);
    }
  }
}
