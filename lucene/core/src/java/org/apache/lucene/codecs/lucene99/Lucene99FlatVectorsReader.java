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

package org.apache.lucene.codecs.lucene99;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reads vectors from the index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatVectorsReader extends FlatVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatVectorsFormat.class);

  private final IntObjectHashMap<FieldEntry> fields;
  private final FlatVectorsScorer vectorScorer;
  private final IndexInput vectorData;
  private final FieldInfos fieldInfos;
  private final IOContext dataContext;
  private final Directory directory;
  private final String vectorDataFN;
  // the reader that owns the mapping merges read
  private final Lucene99FlatVectorsReader original;
  private IndexInput mergeVectorData;
  // merge instances handed out and not yet finished
  private int mergeInstances;

  public Lucene99FlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer)
      throws IOException {
    this.fields = new IntObjectHashMap<>();
    int versionMeta = readMetadata(state);
    this.vectorScorer = scorer;
    this.fieldInfos = state.fieldInfos;
    boolean success = false;
    this.directory = state.directory;
    this.original = this;
    // how these are read is up to whoever wraps this format
    dataContext = state.context.union(FileTypeHint.DATA, FileDataHint.KNN_VECTORS);
    this.vectorDataFN =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION);
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              vectorDataFN,
              Lucene99FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
              dataContext);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  /** Reads the same fields as {@code reader}, through the mapping a merge opened for itself. */
  private Lucene99FlatVectorsReader(Lucene99FlatVectorsReader reader, IndexInput vectorData) {
    this.fields = reader.fields;
    this.vectorScorer = reader.vectorScorer;
    this.fieldInfos = reader.fieldInfos;
    this.dataContext = reader.dataContext;
    this.directory = reader.directory;
    this.vectorDataFN = reader.vectorDataFN;
    this.original = reader.original;
    this.vectorData = vectorData;
  }

  private int readMetadata(SegmentReadState state) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene99FlatVectorsFormat.META_CODEC_NAME,
                Lucene99FlatVectorsFormat.VERSION_START,
                Lucene99FlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }
    return versionMeta;
  }

  private static IndexInput openDataInput(
      SegmentReadState state, int versionMeta, String fileName, String codecName, IOContext context)
      throws IOException {
    IndexInput in = state.directory.openInput(fileName, context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene99FlatVectorsFormat.VERSION_START,
              Lucene99FlatVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
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

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = FieldEntry.create(meta, info);
      fields.put(info.number, fieldEntry);
    }
  }

  @Override
  public long ramBytesUsed() {
    return Lucene99FlatVectorsReader.SHALLOW_SIZE + fields.ramBytesUsed();
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    final FieldEntry entry = getFieldEntryOrThrow(fieldInfo.name);
    return Map.of(Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION, entry.vectorDataLength());
  }

  @Override
  public int getVectorCount(FieldInfo fieldInfo) {
    return getFieldEntryOrThrow(fieldInfo.name).size();
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
  }

  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    IndexInput data = original.mergeVectorData();
    boolean success = false;
    try {
      FlatVectorsReader mergeInstance = new Lucene99FlatVectorsReader(this, data.clone());
      success = true;
      return mergeInstance;
    } finally {
      if (success == false) {
        original.releaseMergeVectorData();
      }
    }
  }

  /**
   * The vectors as a merge reads them, front to back and once. Advice belongs to a mapping, so a
   * merge maps the file again. Mapped on the first merge, released by {@link #finishMerge()}.
   */
  private synchronized IndexInput mergeVectorData() throws IOException {
    assert original == this;
    if (mergeVectorData == null) {
      if (dataContext.context() == IOContext.Context.MERGE) {
        // opened by a merge to begin with, so it already reads the file front to back
        mergeVectorData = vectorData;
      } else {
        try {
          mergeVectorData =
              directory.openInput(
                  vectorDataFN,
                  IOContext.merge()
                      .withHints(
                          FileTypeHint.DATA,
                          FileDataHint.KNN_VECTORS,
                          DataAccessHint.SEQUENTIAL,
                          NoReuseHint.INSTANCE));
        } catch (@SuppressWarnings("unused") FileNotFoundException | NoSuchFileException e) {
          // an open reader outlives its files, so fall back to the mapping it already holds
          mergeVectorData = vectorData;
        }
      }
    }
    mergeInstances++;
    return mergeVectorData;
  }

  private FieldEntry getFieldEntryOrThrow(String field) {
    final FieldInfo info = fieldInfos.fieldInfo(field);
    final FieldEntry entry;
    if (info == null || (entry = fields.get(info.number)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    return entry;
  }

  private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
    final FieldEntry fieldEntry = getFieldEntryOrThrow(field);
    if (fieldEntry.vectorEncoding != expectedEncoding) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.vectorEncoding
              + " expected: "
              + expectedEncoding);
    }
    return fieldEntry;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
    return OffHeapFloatVectorValues.load(
        fieldEntry.similarityFunction,
        vectorScorer,
        fieldEntry.ordToDoc,
        fieldEntry.vectorEncoding,
        fieldEntry.dimension,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        vectorData);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.BYTE);
    return OffHeapByteVectorValues.load(
        fieldEntry.similarityFunction,
        vectorScorer,
        fieldEntry.ordToDoc,
        fieldEntry.vectorEncoding,
        fieldEntry.dimension,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        vectorData);
  }

  @Override
  public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
    return vectorScorer;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
    return vectorScorer.getRandomVectorScorer(
        fieldEntry.similarityFunction,
        OffHeapFloatVectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData),
        target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.BYTE);
    return vectorScorer.getRandomVectorScorer(
        fieldEntry.similarityFunction,
        OffHeapByteVectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData),
        target);
  }

  /**
   * Closes the mapping a merge used, once no merge instance holds it. A later merge maps the file
   * again.
   */
  @Override
  public void finishMerge() throws IOException {
    original.releaseMergeVectorData();
  }

  private synchronized void releaseMergeVectorData() throws IOException {
    assert original == this;
    if (--mergeInstances > 0) {
      return;
    }
    if (mergeVectorData != null && mergeVectorData != vectorData) {
      mergeVectorData.close();
    }
    mergeVectorData = null;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, mergeVectorDataToClose());
  }

  /** The mapping a merge opened, read under the lock that guards it, closed outside it. */
  private synchronized IndexInput mergeVectorDataToClose() {
    return original == this && mergeVectorData != vectorData ? mergeVectorData : null;
  }

  private record FieldEntry(
      VectorSimilarityFunction similarityFunction,
      VectorEncoding vectorEncoding,
      long vectorDataOffset,
      long vectorDataLength,
      int dimension,
      int size,
      OrdToDocDISIReaderConfiguration ordToDoc,
      FieldInfo info) {

    FieldEntry {
      if (similarityFunction != info.getVectorSimilarityFunction()) {
        throw new IllegalStateException(
            "Inconsistent vector similarity function for field=\""
                + info.name
                + "\"; "
                + similarityFunction
                + " != "
                + info.getVectorSimilarityFunction());
      }
      int infoVectorDimension = info.getVectorDimension();
      if (infoVectorDimension != dimension) {
        throw new IllegalStateException(
            "Inconsistent vector dimension for field=\""
                + info.name
                + "\"; "
                + infoVectorDimension
                + " != "
                + dimension);
      }

      int byteSize =
          switch (info.getVectorEncoding()) {
            case BYTE -> Byte.BYTES;
            case FLOAT32 -> Float.BYTES;
          };
      long vectorBytes = Math.multiplyExact((long) infoVectorDimension, byteSize);
      long numBytes = Math.multiplyExact(vectorBytes, size);
      if (numBytes != vectorDataLength) {
        throw new IllegalStateException(
            "Vector data length "
                + vectorDataLength
                + " not matching size="
                + size
                + " * dim="
                + dimension
                + " * byteSize="
                + byteSize
                + " = "
                + numBytes);
      }
    }

    static FieldEntry create(IndexInput input, FieldInfo info) throws IOException {
      final VectorEncoding vectorEncoding = readVectorEncoding(input);
      final VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
      final var vectorDataOffset = input.readVLong();
      final var vectorDataLength = input.readVLong();
      final var dimension = input.readVInt();
      final var size = input.readInt();
      final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
      return new FieldEntry(
          similarityFunction,
          vectorEncoding,
          vectorDataOffset,
          vectorDataLength,
          dimension,
          size,
          ordToDoc,
          info);
    }
  }
}
