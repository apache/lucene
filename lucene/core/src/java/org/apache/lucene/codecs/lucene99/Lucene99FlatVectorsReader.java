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
import java.util.stream.Stream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloat16VectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
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
  // on a merge instance: whether it gave the mapping back, guarded by the original's lock
  private boolean finished;

  public Lucene99FlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer)
      throws IOException {
    this.fields = new IntObjectHashMap<>();
    int versionMeta = readMetadata(state);
    this.vectorScorer = scorer;
    this.fieldInfos = state.fieldInfos;
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
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
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
      return in;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, in);
      throw t;
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
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    CodecUtil.checksumEntireFile(vectorData, merge);
  }

  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    if (mergeNeedsItsOwnMapping() == false) {
      return this;
    }
    IndexInput data = original.mergeVectorData();
    boolean success = false;
    try {
      FlatVectorsReader mergeInstance = new Lucene99FlatVectorsReader(this, data.clone());
      success = true;
      return mergeInstance;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(original.release());
      }
    }
  }

  /**
   * Whether a merge has to map the file again: only when searches read it at random. Otherwise the
   * mapping searches use carries no advice a merge reading front to back would suffer from, and a
   * reader a merge opened already reads the file the way a merge does.
   */
  private boolean mergeNeedsItsOwnMapping() {
    return dataContext.context() != IOContext.Context.MERGE
        && dataContext.hints().contains(DataAccessHint.RANDOM);
  }

  /**
   * The vectors as a merge reads them, front to back. Advice belongs to a mapping, so a merge maps
   * the file again. Mapped on the first merge, released by {@link #finishMerge()}.
   */
  private synchronized IndexInput mergeVectorData() throws IOException {
    assert original == this;
    if (mergeVectorData == null) {
      try {
        mergeVectorData = directory.openInput(vectorDataFN, mergeContext());
      } catch (FileNotFoundException | NoSuchFileException _) {
        // an open reader outlives its files, so fall back to the mapping it already holds
        mergeVectorData = vectorData;
      }
    }
    mergeInstances++;
    return mergeVectorData;
  }

  /** The caller's context as a merge reading the file front to back: only the access changes. */
  private IOContext mergeContext() {
    return IOContext.merge()
        .withHints(
            Stream.concat(
                    dataContext.hints().stream()
                        .filter(hint -> hint instanceof DataAccessHint == false),
                    Stream.of(DataAccessHint.SEQUENTIAL))
                .toArray(IOContext.FileOpenHint[]::new));
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
  public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT16);
    return OffHeapFloat16VectorValues.load(
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

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, short[] target) throws IOException {

    final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT16);
    return vectorScorer.getRandomVectorScorer(
        fieldEntry.similarityFunction,
        OffHeapFloat16VectorValues.load(
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
   * again. Only a merge instance holds the mapping, and it gives it back once: finishing the reader
   * it came from, or finishing it again, releases nothing.
   */
  @Override
  public void finishMerge() throws IOException {
    if (original != this) {
      IOUtils.close(original.releaseMergeVectorData(this));
    }
  }

  /** Gives back the hold of {@code mergeInstance}, once; returns the mapping to close, if any. */
  private synchronized IndexInput releaseMergeVectorData(Lucene99FlatVectorsReader mergeInstance) {
    assert original == this && mergeInstance.original == this;
    if (mergeInstance.finished) {
      return null;
    }
    mergeInstance.finished = true;
    return release();
  }

  /**
   * Gives back one hold on the mapping. Once none is left, returns it for the caller to close
   * outside the lock.
   */
  private synchronized IndexInput release() {
    assert original == this && mergeInstances > 0;
    if (--mergeInstances > 0) {
      return null;
    }
    IndexInput toClose = mergeVectorData == vectorData ? null : mergeVectorData;
    mergeVectorData = null;
    return toClose;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, takeMergeVectorData());
  }

  /**
   * The mapping a merge opened, taken under the lock that guards it so that a merge finishing later
   * does not close it again, and closed outside it.
   */
  private synchronized IndexInput takeMergeVectorData() {
    if (original != this || mergeVectorData == vectorData) {
      return null;
    }
    IndexInput toClose = mergeVectorData;
    mergeVectorData = null;
    return toClose;
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
            case FLOAT16 -> Short.BYTES;
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
