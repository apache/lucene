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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.FileOpenHint;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectReader;

/**
 * Reads a {@link DedupFlatVectorsFormat} segment.
 *
 * <p>Layout: {@code .dvc} holds pool vector bytes (contiguous per pool) followed by per-field DISI
 * + packed {@code docOrd → vecOrd} maps. {@code .dvm} holds pool/field metadata.
 *
 * @lucene.experimental
 */
public final class DedupFlatVectorsReader extends FlatVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatVectorsReader.class);

  private final FlatVectorsScorer vectorScorer;
  private final FlatVectorsScorer translatingScorer;
  private final FieldInfos fieldInfos;
  private final IndexInput vectorData;
  private final IOContext dataContext;

  private final IntObjectHashMap<FieldEntry> fields = new IntObjectHashMap<>();

  public DedupFlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer)
      throws IOException {
    this(state, scorer, DataAccessHint.RANDOM);
  }

  public DedupFlatVectorsReader(
      SegmentReadState state, FlatVectorsScorer scorer, DataAccessHint accessHint)
      throws IOException {
    this.vectorScorer = scorer;
    this.translatingScorer = new DedupTranslatingScorer(scorer);
    this.fieldInfos = state.fieldInfos;

    int versionMeta;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, DedupFlatVectorsFormat.META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                DedupFlatVectorsFormat.META_CODEC_NAME,
                DedupFlatVectorsFormat.VERSION_START,
                DedupFlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readMetaBody(meta, state.fieldInfos);
      } catch (Throwable e) {
        priorE = e;
        throw e;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }

    FileOpenHint[] hints =
        Stream.of(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, accessHint)
            .filter(Objects::nonNull)
            .toArray(FileOpenHint[]::new);
    dataContext = state.context.withHints(hints);
    boolean success = false;
    IndexInput data = null;
    try {
      data = openDataInput(state, versionMeta, dataContext);
      this.vectorData = data;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(data);
      }
    }
  }

  private void readMetaBody(ChecksumIndexInput meta, FieldInfos fieldInfos) throws IOException {
    int numPools = meta.readVInt();
    PoolEntry[] poolsLocal = new PoolEntry[numPools];
    for (int p = 0; p < numPools; p++) {
      int dim = meta.readVInt();
      int encOrd = meta.readByte();
      VectorEncoding encoding = VectorEncoding.values()[encOrd];
      long offset = meta.readVLong();
      long length = meta.readVLong();
      int uniqueCount = meta.readVInt();
      poolsLocal[p] = new PoolEntry(p, dim, encoding, offset, length, uniqueCount);
    }
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      int simOrd = meta.readByte();
      VectorSimilarityFunction sim = VectorSimilarityFunction.values()[simOrd];
      int poolId = meta.readVInt();
      if (poolId < 0 || poolId >= poolsLocal.length) {
        throw new CorruptIndexException(
            "Invalid poolId " + poolId + " (numPools=" + poolsLocal.length + ")", meta);
      }
      int cardinality = meta.readInt();
      OrdToDocDISIReaderConfiguration ordToDoc =
          OrdToDocDISIReaderConfiguration.fromStoredMeta(meta, cardinality);
      long mapOffset = meta.readVLong();
      long mapLength = meta.readVLong();
      int bitsPerVecOrd = meta.readByte();
      FieldEntry entry =
          new FieldEntry(
              info,
              sim,
              poolId,
              cardinality,
              mapOffset,
              mapLength,
              bitsPerVecOrd,
              ordToDoc,
              poolsLocal[poolId]);
      fields.put(info.number, entry);
    }
  }

  private static IndexInput openDataInput(
      SegmentReadState state, int versionMeta, IOContext context) throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            DedupFlatVectorsFormat.VECTOR_DATA_EXTENSION);
    IndexInput in = state.directory.openInput(fileName, context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              DedupFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
              DedupFlatVectorsFormat.VERSION_START,
              DedupFlatVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + DedupFlatVectorsFormat.VECTOR_DATA_CODEC_NAME
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private FieldEntry getFieldEntry(String fieldName, VectorEncoding expected) {
    FieldInfo info = fieldInfos.fieldInfo(fieldName);
    if (info == null) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" not found");
    }
    FieldEntry entry = fields.get(info.number);
    if (entry == null) {
      throw new IllegalArgumentException(
          "field=\"" + fieldName + "\" not encoded by DedupFlatVectorsFormat");
    }
    if (entry.pool.encoding != expected) {
      throw new IllegalArgumentException(
          "field=\""
              + fieldName
              + "\" encoded as "
              + entry.pool.encoding
              + " but expected "
              + expected);
    }
    return entry;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.FLOAT32);
    return OffHeapDedupFloatVectorValues.create(entry, vectorData, vectorScorer);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.BYTE);
    return OffHeapDedupByteVectorValues.create(entry, vectorData, vectorScorer);
  }

  @Override
  public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
    // Used by Lucene99HnswVectorsWriter at merge time: it asks for a scorer over our flat
    // values, then builds a RandomVectorScorerSupplier from it. The translating wrapper
    // detects our OffHeapDedup* values, extracts the pool view, builds an off-heap SIMD
    // supplier over the pool, and translates docOrd → vecOrd on every scoring call.
    return translatingScorer;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.FLOAT32);
    if (entry.cardinality == 0) {
      return null;
    }
    OffHeapDedupFloatVectorValues fieldView =
        OffHeapDedupFloatVectorValues.create(entry, vectorData, vectorScorer);
    KnnVectorValues poolView = fieldView.poolView();
    RandomVectorScorer poolScorer =
        vectorScorer.getRandomVectorScorer(entry.similarity, poolView, target);
    int[] docOrdToVecOrd = entry.getOrLoadDocOrdToVecOrd(vectorData);
    return DedupTranslatingScorer.wrapScorer(poolScorer, fieldView, docOrdToVecOrd);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.BYTE);
    if (entry.cardinality == 0) {
      return null;
    }
    OffHeapDedupByteVectorValues fieldView =
        OffHeapDedupByteVectorValues.create(entry, vectorData, vectorScorer);
    KnnVectorValues poolView = fieldView.poolView();
    RandomVectorScorer poolScorer =
        vectorScorer.getRandomVectorScorer(entry.similarity, poolView, target);
    int[] docOrdToVecOrd = entry.getOrLoadDocOrdToVecOrd(vectorData);
    return DedupTranslatingScorer.wrapScorer(poolScorer, fieldView, docOrdToVecOrd);
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    FieldEntry entry = fields.get(fieldInfo.number);
    if (entry == null) {
      return Map.of();
    }
    return Map.of(
        DedupFlatVectorsFormat.VECTOR_DATA_EXTENSION, entry.pool.length + entry.mapLength);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
  }

  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    vectorData.updateIOContext(dataContext.withHints(DataAccessHint.SEQUENTIAL));
    return this;
  }

  @Override
  public void finishMerge() throws IOException {
    vectorData.updateIOContext(dataContext);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + fields.ramBytesUsed();
  }

  /** Test-only: look up a field entry by name. Package-private. */
  FieldEntry getFieldEntryForTesting(String fieldName) {
    FieldInfo info = fieldInfos.fieldInfo(fieldName);
    if (info == null) {
      throw new IllegalArgumentException("no such field " + fieldName);
    }
    return fields.get(info.number);
  }

  /** Per-pool runtime state. */
  static final class PoolEntry {
    final int poolId;
    final int dim;
    final VectorEncoding encoding;
    final long offset;
    final long length;
    final int uniqueCount;
    final int byteSize;

    PoolEntry(
        int poolId, int dim, VectorEncoding encoding, long offset, long length, int uniqueCount) {
      this.poolId = poolId;
      this.dim = dim;
      this.encoding = encoding;
      this.offset = offset;
      this.length = length;
      this.uniqueCount = uniqueCount;
      this.byteSize = dim * encoding.byteSize;
    }
  }

  /** Per-field runtime state. */
  static final class FieldEntry {
    final FieldInfo info;
    final VectorSimilarityFunction similarity;
    final int poolId;
    final int cardinality;
    final long mapOffset;
    final long mapLength;
    final int bitsPerVecOrd;
    final OrdToDocDISIReaderConfiguration ordToDoc;
    final PoolEntry pool;

    /**
     * Lazily-materialised dense {@code docOrd → vecOrd} array. Loaded on first scorer build so the
     * hot HNSW build/search loop translates with a single primitive int[] read instead of
     * dispatching through {@link DirectReader}'s {@link LongValues}. Memory footprint: 4 ×
     * cardinality bytes per field (e.g. ~4 MB for 1M docs).
     */
    private volatile int[] docOrdToVecOrdArray;

    FieldEntry(
        FieldInfo info,
        VectorSimilarityFunction similarity,
        int poolId,
        int cardinality,
        long mapOffset,
        long mapLength,
        int bitsPerVecOrd,
        OrdToDocDISIReaderConfiguration ordToDoc,
        PoolEntry pool) {
      this.info = info;
      this.similarity = similarity;
      this.poolId = poolId;
      this.cardinality = cardinality;
      this.mapOffset = mapOffset;
      this.mapLength = mapLength;
      this.bitsPerVecOrd = bitsPerVecOrd;
      this.ordToDoc = ordToDoc;
      this.pool = pool;
    }

    int[] getOrLoadDocOrdToVecOrd(IndexInput vectorData) throws IOException {
      int[] cached = docOrdToVecOrdArray;
      if (cached != null) {
        return cached;
      }
      synchronized (this) {
        cached = docOrdToVecOrdArray;
        if (cached != null) {
          return cached;
        }
        cached = loadDocOrdToVecOrd(vectorData);
        docOrdToVecOrdArray = cached;
        return cached;
      }
    }

    private int[] loadDocOrdToVecOrd(IndexInput vectorData) throws IOException {
      int[] arr = new int[cardinality];
      if (cardinality == 0) {
        return arr;
      }
      RandomAccessInput mapInput = vectorData.randomAccessSlice(mapOffset, mapLength);
      LongValues map = DirectReader.getInstance(mapInput, bitsPerVecOrd);
      for (int i = 0; i < cardinality; i++) {
        arr[i] = (int) map.get(i);
      }
      return arr;
    }
  }
}
