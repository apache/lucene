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
package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.writeCorrections;
import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.writeQueryRecord;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.Mode;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Writes quantized vector values and metadata to index segments in the format for Lucene 10.4.
 *
 * @lucene.experimental
 */
public class Lucene104ScalarQuantizedVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene104ScalarQuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private final IndexOutput meta, vectorData;
  private final ScalarEncoding encoding;
  private final Mode mode;
  private final int version;
  private final FlatVectorsWriter rawVectorDelegate;
  private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
  private boolean finished;

  /** Sole constructor */
  public Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      Mode mode,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
    this.vectorScorer = vectorsScorer;
    this.encoding = encoding;
    this.mode = mode;
    this.version =
        mode == Mode.CENTERED
            ? Lucene104ScalarQuantizedVectorsFormat.VERSION_START
            : Lucene104ScalarQuantizedVectorsFormat.VERSION_DATA_BLIND;
    this.segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene104ScalarQuantizedVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene104ScalarQuantizedVectorsFormat.META_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding().isFloatingPoint() && mode == Mode.DATA_BLIND_WITHOUT_FLOATS) {
      // Data-blind mode without floats: keep vectors in memory only and never write
      // full-precision float vectors.
      FlatFieldVectorsWriter<?> storage =
          switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> new InMemoryFieldWriter<float[]>(fieldInfo, Float.BYTES);
            case FLOAT16 -> new InMemoryFieldWriter<short[]>(fieldInfo, Short.BYTES);
            case BYTE -> throw new IllegalStateException("Byte Vectors aren't supported");
          };
      FieldWriter<?> fieldWriter = FieldWriter.create(fieldInfo, storage, false);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    FlatFieldVectorsWriter<?> storage = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().isFloatingPoint()) {
      FieldWriter<?> fieldWriter = FieldWriter.create(fieldInfo, storage, mode == Mode.CENTERED);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return storage;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter<?> field : fields) {
      final float[] clusterCenter = field.computeCentroid();
      if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
        segmentWriteState.infoStream.message(
            QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + field.getVectors().size());
      }
      OptimizedScalarQuantizer quantizer =
          new OptimizedScalarQuantizer(field.fieldInfo.getVectorSimilarityFunction());
      if (sortMap == null) {
        writeField(field, clusterCenter, maxDoc, quantizer);
      } else {
        writeSortingField(field, clusterCenter, maxDoc, sortMap, quantizer);
      }
      field.finish();
    }
  }

  private void writeField(
      FieldWriter<?> fieldData,
      float[] clusterCenter,
      int maxDoc,
      OptimizedScalarQuantizer quantizer)
      throws IOException {
    // write vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    writeVectors(fieldData, clusterCenter, quantizer);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    float centroidDp =
        !fieldData.getVectors().isEmpty() ? VectorUtil.dotProduct(clusterCenter, clusterCenter) : 0;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        clusterCenter,
        centroidDp,
        fieldData.getDocsWithFieldSet());
  }

  private void writeVectors(
      FieldWriter<?> fieldData, float[] clusterCenter, OptimizedScalarQuantizer scalarQuantizer)
      throws IOException {
    byte[] scratch =
        new byte[encoding.getDiscreteDimensions(fieldData.fieldInfo.getVectorDimension())];
    byte[] vector =
        switch (encoding) {
          case UNSIGNED_BYTE, SEVEN_BIT -> scratch;
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };
    for (int i = 0; i < fieldData.getVectors().size(); i++) {
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(
              fieldData.floatVectorValue(i), scratch, encoding.getBits(), clusterCenter);
      packIndexRecord(encoding, scratch, vector);
      vectorData.writeBytes(vector, vector.length);
      writeCorrections(vectorData, corrections);
    }
  }

  private void writeSortingField(
      FieldWriter<?> fieldData,
      float[] clusterCenter,
      int maxDoc,
      Sorter.DocMap sortMap,
      OptimizedScalarQuantizer scalarQuantizer)
      throws IOException {
    final int[] ordMap =
        new int[fieldData.getDocsWithFieldSet().cardinality()]; // new ord to old ord

    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    mapOldOrdToNewOrd(fieldData.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);

    // write vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    writeSortedVectors(fieldData, clusterCenter, ordMap, scalarQuantizer);
    long quantizedVectorLength = vectorData.getFilePointer() - vectorDataOffset;

    float centroidDp = VectorUtil.dotProduct(clusterCenter, clusterCenter);
    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        quantizedVectorLength,
        clusterCenter,
        centroidDp,
        newDocsWithField);
  }

  private void writeSortedVectors(
      FieldWriter<?> fieldData,
      float[] clusterCenter,
      int[] ordMap,
      OptimizedScalarQuantizer scalarQuantizer)
      throws IOException {
    byte[] scratch =
        new byte[encoding.getDiscreteDimensions(fieldData.fieldInfo.getVectorDimension())];
    byte[] vector =
        switch (encoding) {
          case UNSIGNED_BYTE, SEVEN_BIT -> scratch;
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };
    for (int ordinal : ordMap) {
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(
              fieldData.floatVectorValue(ordinal), scratch, encoding.getBits(), clusterCenter);
      packIndexRecord(encoding, scratch, vector);
      vectorData.writeBytes(vector, vector.length);
      writeCorrections(vectorData, corrections);
    }
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      float[] clusterCenter,
      float centroidDp,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVInt(field.getVectorDimension());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    int count = docsWithField.cardinality();
    meta.writeVInt(count);
    if (count > 0) {
      meta.writeVInt(encoding.getWireNumber());
      if (version == Lucene104ScalarQuantizedVectorsFormat.VERSION_START) {
        final ByteBuffer buffer =
            ByteBuffer.allocate(field.getVectorDimension() * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(clusterCenter);
        meta.writeBytes(buffer.array(), buffer.array().length);
        meta.writeInt(Float.floatToIntBits(centroidDp));
      } else {
        // Data-blind (version 1): the centroid and centroidDP are omitted; a zero centroid is
        // substituted at read time. The mode is written explicitly since both data-blind variants
        // share this version.
        meta.writeByte(mode.wireNumber());
      }
    }
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, count, maxDoc, docsWithField);
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    rawVectorDelegate.finish();
    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
    }
  }

  /**
   * The merged float vectors of the field, as the quantizer sees them: inflated from fp16 if need
   * be, and normalized for COSINE.
   */
  private FloatVectorValues mergedFloatVectorValues(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    FloatVectorValues vectorValues =
        fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT16
            ? new Float16AsFloatVectorValues(
                MergedVectorValues.mergeFloat16VectorValues(fieldInfo, mergeState))
            : MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
    if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
      vectorValues = new NormalizedFloatVectorValues(vectorValues);
    }
    return vectorValues;
  }

  private QuantizedByteVectorValues mergedQuantizedVectorValues(
      FieldInfo fieldInfo, MergeState mergeState, float[] centroid) throws IOException {
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    return new QuantizedFloatVectorValues(
        mergedFloatVectorValues(fieldInfo, mergeState), quantizer, encoding, centroid);
  }

  /**
   * Returns a view that quantizes a single segment's float vectors against {@code centroid} using
   * this writer's encoding, without consulting any quantized bytes the segment may already store.
   */
  private QuantizedFloatVectorValues quantizeFromFloats(
      KnnVectorsReader reader, FieldInfo fieldInfo, float[] centroid) throws IOException {
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    FloatVectorValues vectorValues =
        fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT16
            ? new Float16AsFloatVectorValues(reader.getFloat16VectorValues(fieldInfo.name))
            : reader.getFloatVectorValues(fieldInfo.name);
    if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
      vectorValues = new NormalizedFloatVectorValues(vectorValues);
    }
    return new QuantizedFloatVectorValues(vectorValues, quantizer, encoding, centroid);
  }

  @Override
  public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    mergeOneFlatVectorField(fieldInfo, mergeState, _ -> false);
  }

  /**
   * {@inheritDoc}
   *
   * <p>HNSW merges call this overload rather than {@link #mergeOneFlatVectorField(FieldInfo,
   * MergeState)}, so subclasses that customize merging must override this method too.
   *
   * <p>Only {@link Mode#CENTERED} prepares data. The data-blind modes return {@code null} and use
   * the reader fallback for the graph build.
   */
  @Override
  public MergeScorerData mergeOneFlatVectorFieldForMergeScorer(
      FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
      throws IOException {
    return mergeOneFlatVectorField(fieldInfo, mergeState, needsMergeScorer);
  }

  private MergeScorerData mergeOneFlatVectorField(
      FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
      throws IOException {
    if (fieldInfo.getVectorEncoding().isFloatingPoint() == false) {
      rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
      return null;
    }
    return switch (mode) {
      case CENTERED -> {
        failIfFloatVectorsMissing(fieldInfo, mergeState);
        yield mergeOneFlatVectorFieldCentered(fieldInfo, mergeState, needsMergeScorer);
      }
      case DATA_BLIND_WITH_FLOATS -> {
        // The output segment stores full-precision floats, so every contributing segment must
        // provide them; the quantized merge then re-quantizes them against the zero centroid.
        failIfFloatVectorsMissing(fieldInfo, mergeState);
        rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
        mergeOneFlatVectorFieldDataBlind(fieldInfo, mergeState);
        yield null;
      }
      case DATA_BLIND_WITHOUT_FLOATS -> {
        mergeOneFlatVectorFieldDataBlind(fieldInfo, mergeState);
        yield null;
      }
    };
  }

  /**
   * Fails the merge when the output mode stores full-precision float vectors ({@code mode} is
   * {@link Mode#CENTERED} or {@link Mode#DATA_BLIND_WITH_FLOATS}) but a contributing segment cannot
   * supply them, e.g. a segment written in {@link Mode#DATA_BLIND_WITHOUT_FLOATS}. Such segments
   * can only offer dequantized (already-quantized) values in place of true full-precision vectors;
   * this fails loudly rather than silently degrading quality.
   */
  private void failIfFloatVectorsMissing(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
      if (reader == null || hasRawVectorValues(reader, fieldInfo)) {
        continue;
      }
      // Tolerate segments that have no vectors for the field at all; they contribute nothing.
      FloatVectorValues values = floatingPointVectorValues(reader, fieldInfo);
      if (values == null || values.size() == 0) {
        continue;
      }
      throw new IllegalStateException(
          "Cannot merge field \""
              + fieldInfo.name
              + "\" from a segment without full-precision float vectors into "
              + mode
              + " mode, which stores float vectors. The contributing segment was likely written"
              + " in DATA_BLIND_WITHOUT_FLOATS mode and its vectors cannot be carried over.");
    }
  }

  private MergeScorerData mergeOneFlatVectorFieldCentered(
      FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
      throws IOException {
    // Don't need access to the random vectors, we can just use the merged
    rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    // Only asymmetric encodings have query-side records, and only FLOAT32 fields score graphs with
    // them. FLOAT16 fields use fp16 vectors. The predicate sees vectorCount before deletions, so it
    // can request data even when the merged field is too small to build a graph.
    boolean prepareQueryData =
        encoding.isAsymmetric()
            && fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32
            && needsMergeScorer.test(vectorCount);
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    DocsWithFieldSet docsWithField;
    MergeScorerData mergeScorerData = null;
    if (prepareQueryData) {
      docsWithField = new DocsWithFieldSet();
      mergeScorerData =
          writeVectorAndQueryData(fieldInfo, mergeState, mergedCentroid, docsWithField);
    } else {
      QuantizedByteVectorValues quantizedVectorValues =
          mergedQuantizedVectorValues(fieldInfo, mergeState, mergedCentroid);
      docsWithField = writeVectorData(vectorData, quantizedVectorValues);
    }
    try {
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      float centroidDp =
          docsWithField.cardinality() > 0
              ? VectorUtil.dotProduct(mergedCentroid, mergedCentroid)
              : 0;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          mergedCentroid,
          centroidDp,
          docsWithField);
      return mergeScorerData;
    } catch (Throwable t) {
      // The handle was never returned, so nobody else can release its records.
      IOUtils.closeWhileSuppressingExceptions(t, mergeScorerData);
      throw t;
    }
  }

  /**
   * The quantized query vectors staged for the merge scorer. A graph build reads them by ordinal as
   * it walks, so they are read at random.
   */
  private IOContext queryDataContext() {
    return segmentWriteState.context.withHints(
        FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);
  }

  /**
   * Writes merged index-side and query-side records in one pass. Index-side records go to the
   * quantized vector data file, while query-side records go to a temporary file owned by the
   * returned handle.
   *
   * <p>{@link OptimizedScalarQuantizer#multiScalarQuantize} centers each vector once and quantizes
   * it at both bit widths.
   *
   * @param docsWithField filled with the documents that were written
   */
  private MergeScorerData writeVectorAndQueryData(
      FieldInfo fieldInfo, MergeState mergeState, float[] centroid, DocsWithFieldSet docsWithField)
      throws IOException {
    assert encoding.isAsymmetric();
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    FloatVectorValues vectorValues = mergedFloatVectorValues(fieldInfo, mergeState);
    int discretizedDims = encoding.getDiscreteDimensions(vectorValues.dimension());
    byte[] indexQuantized = new byte[discretizedDims];
    byte[] queryQuantized = new byte[discretizedDims];
    byte[] indexPacked =
        switch (encoding) {
          case UNSIGNED_BYTE, SEVEN_BIT -> indexQuantized;
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(discretizedDims)];
        };
    byte[] queryPacked = new byte[encoding.getQueryPackedLength(discretizedDims)];
    byte[] bits = new byte[] {encoding.getBits(), encoding.getQueryBits()};
    byte[][] destinations = new byte[][] {indexQuantized, queryQuantized};
    String queryDataName = null;
    try (IndexOutput queryData =
        segmentWriteState.directory.createTempOutput(
            segmentWriteState.segmentInfo.name, "queries", queryDataContext())) {
      queryDataName = queryData.getName();
      KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
      for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
        OptimizedScalarQuantizer.QuantizationResult[] corrections =
            quantizer.multiScalarQuantize(
                vectorValues.vectorValue(iterator.index()), destinations, bits, centroid);
        // the index side, packed as QuantizedFloatVectorValues packs it
        packIndexRecord(encoding, indexQuantized, indexPacked);
        vectorData.writeBytes(indexPacked, indexPacked.length);
        writeCorrections(vectorData, corrections[0]);
        writeQueryRecord(queryData, queryQuantized, queryPacked, corrections[1]);
        docsWithField.add(docV);
      }
      CodecUtil.writeFooter(queryData);
    } catch (Throwable t) {
      if (queryDataName != null) {
        IOUtils.deleteFilesSuppressingExceptions(t, segmentWriteState.directory, queryDataName);
      }
      throw t;
    }
    return new PreparedQueryData(
        segmentWriteState.directory, queryDataContext(), fieldInfo, vectorScorer, queryDataName);
  }

  /** Owns one field's temporary query-side records until its graph scorer takes them over. */
  private static final class PreparedQueryData implements MergeScorerData {
    private final Directory directory;
    private final IOContext context;
    private final FieldInfo fieldInfo;
    private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
    private final String fileName;
    private boolean spent;

    PreparedQueryData(
        Directory directory,
        IOContext context,
        FieldInfo fieldInfo,
        Lucene104ScalarQuantizedVectorScorer vectorScorer,
        String fileName) {
      this.directory = directory;
      this.context = context;
      this.fieldInfo = fieldInfo;
      this.vectorScorer = vectorScorer;
      this.fileName = fileName;
    }

    @Override
    public CloseableRandomVectorScorerSupplier scorerSupplier(FlatVectorsReader mergedReader)
        throws IOException {
      if (spent) {
        throw new IllegalStateException("already consumed or closed: " + fileName);
      }
      spent = true;
      boolean handedOver = false;
      try {
        QuantizedByteVectorValues indexVectors =
            mergedReader.unwrapReaderForField(fieldInfo.name)
                    instanceof Lucene104ScalarQuantizedVectorsReader quantizedReader
                ? quantizedReader.getQuantizedVectorValues(fieldInfo.name)
                : null;
        if (indexVectors == null) {
          // not this format's reader, even after unwrapping
          return null;
        }
        handedOver = true;
        return Lucene104ScalarQuantizedVectorsReader.mergeScorerSupplier(
            fieldInfo, indexVectors, vectorScorer, directory, context, fileName);
      } finally {
        if (handedOver == false) {
          IOUtils.deleteFilesIgnoringExceptions(directory, fileName);
        }
      }
    }

    @Override
    public void close() {
      if (spent == false) {
        spent = true;
        IOUtils.deleteFilesIgnoringExceptions(directory, fileName);
      }
    }
  }

  private void mergeOneFlatVectorFieldDataBlind(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    float[] zeroCentroid = new float[fieldInfo.getVectorDimension()];
    // Build one merged view where, per contributing segment, either its existing quantized bytes
    // are passed through or its float vectors are quantized fresh. Inputs already quantized to
    // {@code encoding} against a zero centroid (data-blind segments) are copied directly; they are
    // never dequantized and re-quantized, which would only add loss. Segments with raw floats are
    // quantized fresh, as their stored bytes live in a different (centered) quantization space.
    List<QuantizedByteVectorValuesSub> subs = new ArrayList<>();
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
      if (reader == null) {
        continue;
      }
      QuantizedByteVectorValues values;
      if (hasRawVectorValues(reader, fieldInfo)) {
        // Segment stored full-precision floats; quantize them against the zero centroid.
        values = quantizeFromFloats(reader, fieldInfo, zeroCentroid);
      } else {
        QuantizedByteVectorValues qvv = getQuantizedVectorValues(reader, fieldInfo.name);
        if (qvv == null || qvv.size() == 0) {
          continue;
        }
        if (qvv.getScalarEncoding() != encoding) {
          // Re-quantization from raw floats would be required, which is not possible when raw
          // floats were never written.
          throw new IllegalStateException(
              "Cannot merge field \""
                  + fieldInfo.name
                  + "\" from data-blind segment with encoding "
                  + qvv.getScalarEncoding()
                  + " into data-blind format with encoding "
                  + encoding
                  + ": re-quantization requires raw float vectors");
        }
        Mode sourceMode = getMode(reader, fieldInfo.name);
        if (sourceMode != null && sourceMode != Mode.CENTERED) {
          // Quantized-only segment whose bytes already match the output format (encoding and zero
          // centroid): copy them directly.
          values = qvv;
        } else {
          // Bytes were produced against a (possibly unknown) non-zero centroid, so they cannot be
          // passed through into the zero-centroid output; re-quantize from floats.
          values = quantizeFromFloats(reader, fieldInfo, zeroCentroid);
        }
      }
      subs.add(new QuantizedByteVectorValuesSub(mergeState.docMaps[i], values));
    }
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    MergedQuantizedByteVectorValues mergedQBVV =
        MergedQuantizedByteVectorValues.merge(mergeState, zeroCentroid, encoding, subs);
    DocsWithFieldSet docsWithField = writeVectorData(vectorData, mergedQBVV);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    // centroidDp is 0 (zero centroid); the data-blind metadata omits it and the centroid.
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        null,
        0f,
        docsWithField);
  }

  static DocsWithFieldSet writeVectorData(
      IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    KnnVectorValues.DocIndexIterator iterator = quantizedByteVectorValues.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue(iterator.index());
      output.writeBytes(binaryValue, binaryValue.length);
      writeCorrections(output, quantizedByteVectorValues.getCorrectiveTerms(iterator.index()));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  /**
   * Packs a quantized index-side record from {@code scratch} into {@code dest}. {@link
   * ScalarEncoding#UNSIGNED_BYTE} and {@link ScalarEncoding#SEVEN_BIT} write the quantized bytes as
   * they are, so for them this is a copy, and callers that pass the same array for both save it.
   */
  private static void packIndexRecord(ScalarEncoding encoding, byte[] scratch, byte[] dest) {
    switch (encoding) {
      case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, dest);
      case SINGLE_BIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.packAsBinary(scratch, dest);
      case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(scratch, dest);
      case UNSIGNED_BYTE, SEVEN_BIT -> {
        if (dest != scratch) {
          System.arraycopy(scratch, 0, dest, 0, scratch.length);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    // Query-side records are deliberately not released here: this writer is closed before the
    // caller can open the reader a merge scorer needs, so every handle it handed out is pending.
    IOUtils.close(meta, vectorData, rawVectorDelegate);
  }

  /**
   * Unwraps a merge-time reader down to the flat vectors reader. The per-field wrapper unwraps to
   * the HNSW reader, which must additionally be unwrapped to reach the flat reader.
   */
  private static KnnVectorsReader unwrapToFlatReader(
      KnnVectorsReader vectorsReader, String fieldName) {
    vectorsReader = vectorsReader.unwrapReaderForField(fieldName);
    if (vectorsReader instanceof Lucene99HnswVectorsReader hnswReader) {
      vectorsReader = hnswReader.getFlatVectorsReader().unwrapReaderForField(fieldName);
    }
    return vectorsReader;
  }

  static float[] getCentroid(KnnVectorsReader vectorsReader, String fieldName) {
    vectorsReader = unwrapToFlatReader(vectorsReader, fieldName);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getCentroid(fieldName);
    }
    return null;
  }

  /** Returns the mode the source segment was written with, or null for foreign readers. */
  static Mode getMode(KnnVectorsReader vectorsReader, String fieldName) {
    vectorsReader = unwrapToFlatReader(vectorsReader, fieldName);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getMode(fieldName);
    }
    return null;
  }

  static QuantizedByteVectorValues getQuantizedVectorValues(
      KnnVectorsReader vectorsReader, String fieldName) throws IOException {
    vectorsReader = unwrapToFlatReader(vectorsReader, fieldName);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getQuantizedVectorValues(fieldName);
    }
    return null;
  }

  /**
   * Returns whether the segment stores full-precision vectors for this field, or false when the
   * field is absent or byte-encoded. Data-blind segments report {@code false} since only quantized
   * bytes were written.
   */
  private static boolean hasRawVectorValues(KnnVectorsReader vectorsReader, FieldInfo fieldInfo)
      throws IOException {
    vectorsReader = unwrapToFlatReader(vectorsReader, fieldInfo.name);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return switch (fieldInfo.getVectorEncoding()) {
        case FLOAT32 -> reader.hasRawFloatVectors(fieldInfo.name);
        case FLOAT16 -> reader.hasRawFloat16Vectors(fieldInfo.name);
        case BYTE -> false;
      };
    }
    // Foreign format: assume full-precision vectors are available when the reader serves them.
    return switch (fieldInfo.getVectorEncoding()) {
      case FLOAT32 -> {
        FloatVectorValues values = vectorsReader.getFloatVectorValues(fieldInfo.name);
        yield values != null && values.size() > 0;
      }
      case FLOAT16 -> {
        Float16VectorValues values = vectorsReader.getFloat16VectorValues(fieldInfo.name);
        yield values != null && values.size() > 0;
      }
      case BYTE -> false;
    };
  }

  /**
   * Returns the reader's floating-point vectors viewed as fp32, inflating fp16 on read, or null
   * when the field is absent from this reader or is byte-encoded.
   */
  private static FloatVectorValues floatingPointVectorValues(
      KnnVectorsReader reader, FieldInfo fieldInfo) throws IOException {
    return switch (fieldInfo.getVectorEncoding()) {
      case FLOAT32 -> reader.getFloatVectorValues(fieldInfo.name);
      case FLOAT16 -> {
        Float16VectorValues f16 = reader.getFloat16VectorValues(fieldInfo.name);
        yield f16 == null ? null : new Float16AsFloatVectorValues(f16);
      }
      case BYTE -> null;
    };
  }

  static int mergeAndRecalculateCentroids(
      MergeState mergeState, FieldInfo fieldInfo, float[] mergedCentroid) throws IOException {
    boolean recalculate = false;
    int totalVectorCount = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null) {
        continue;
      }
      KnnVectorValues values = floatingPointVectorValues(knnVectorsReader, fieldInfo);
      if (values == null) {
        continue;
      }
      int vectorCount = values.size();
      if (vectorCount == 0) {
        continue;
      }
      float[] centroid = getCentroid(knnVectorsReader, fieldInfo.name);
      totalVectorCount += vectorCount;
      // If there aren't centroids, or previously clustered with more than one cluster
      // or if there are deleted docs, we must recalculate the centroid. A data-blind segment
      // stores no centroid (its vectors were quantized against zero); it can't be combined with
      // the others, so recompute from the vectors.
      Mode mode = getMode(knnVectorsReader, fieldInfo.name);
      if (centroid == null
          || (mode != null && mode != Mode.CENTERED)
          || mergeState.liveDocs[i] != null) {
        recalculate = true;
        break;
      }
      for (int j = 0; j < centroid.length; j++) {
        mergedCentroid[j] += centroid[j] * vectorCount;
      }
    }
    if (totalVectorCount == 0) {
      return 0;
    } else if (recalculate) {
      return calculateCentroid(mergeState, fieldInfo, mergedCentroid);
    } else {
      for (int j = 0; j < mergedCentroid.length; j++) {
        mergedCentroid[j] = mergedCentroid[j] / totalVectorCount;
      }
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        VectorUtil.l2normalize(mergedCentroid);
      }
      return totalVectorCount;
    }
  }

  static int calculateCentroid(MergeState mergeState, FieldInfo fieldInfo, float[] centroid)
      throws IOException {
    assert fieldInfo.getVectorEncoding().isFloatingPoint();
    // clear out the centroid
    Arrays.fill(centroid, 0);
    int count = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null) continue;
      count += accumulateCentroid(knnVectorsReader, fieldInfo, centroid);
    }
    if (count == 0) {
      return count;
    }
    for (int i = 0; i < centroid.length; i++) {
      centroid[i] /= count;
    }
    if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
      VectorUtil.l2normalize(centroid);
    }
    return count;
  }

  private static int accumulateCentroid(
      KnnVectorsReader reader, FieldInfo fieldInfo, float[] centroid) throws IOException {
    FloatVectorValues vectorValues = floatingPointVectorValues(reader, fieldInfo);
    if (vectorValues == null) {
      return 0;
    }
    int count = 0;
    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
      count++;
      float[] vector = vectorValues.vectorValue(iterator.index());
      for (int j = 0; j < vector.length; j++) {
        centroid[j] += vector[j];
      }
    }
    return count;
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    // The rawVectorDelegate tracks all vector data for byte and float fields that were
    // written through it. For data-blind fields (held only in memory), it accounts for
    // nothing, so those are tracked via field.ramBytesUsed() instead.
    total += rawVectorDelegate.ramBytesUsed();
    for (FieldWriter<?> field : fields) {
      if (mode != Mode.DATA_BLIND_WITHOUT_FLOATS) {
        // quantizationOverheadBytesUsed() intentionally excludes flatFieldVectorsWriter
        // because rawVectorDelegate.ramBytesUsed() already accounts for all flat vector
        // data at the writer level. Calling field.ramBytesUsed() here would double-count.
        total += field.quantizationOverheadBytesUsed();
      } else {
        total += field.ramBytesUsed();
      }
    }
    return total;
  }

  abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    protected final FieldInfo fieldInfo;
    private boolean finished;
    protected final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;
    private final float[] dimensionSums;
    private final FloatArrayList magnitudes = new FloatArrayList();
    protected final int dim;

    protected final boolean enableCentering;

    FieldWriter(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<T> flatFieldVectorsWriter,
        boolean enableCentering) {
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.enableCentering = enableCentering;
      this.dim = fieldInfo.getVectorDimension();
      this.dimensionSums = enableCentering ? new float[dim] : null;
    }

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<?> flatFieldVectorsWriter,
        boolean enableCentering) {
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> throw new UnsupportedOperationException("Byte Vectors aren't supported");
        case FLOAT32 ->
            new Float32FieldWriter(
                fieldInfo,
                (FlatFieldVectorsWriter<float[]>) flatFieldVectorsWriter,
                enableCentering);
        case FLOAT16 ->
            new Float16FieldWriter(
                fieldInfo,
                (FlatFieldVectorsWriter<short[]>) flatFieldVectorsWriter,
                enableCentering);
      };
    }

    @Override
    public List<T> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public T copyValue(T vectorValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      if (finished) {
        return;
      }
      if (flatFieldVectorsWriter.isFinished() == false) {
        // In-memory writers are not flushed through the raw delegate, so finish them here.
        flatFieldVectorsWriter.finish();
      }
      assert flatFieldVectorsWriter.isFinished();
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && flatFieldVectorsWriter.isFinished();
    }

    /**
     * The ordinal's stored vector as fp32, ready for quantization. Scaled to unit length for
     * COSINE. DOT_PRODUCT vectors are expected to already be unit length.
     */
    abstract float[] floatVectorValue(int ord);

    /**
     * Adds {@code vector} to the centroid sums, unit-scaled when COSINE, and caches its magnitude
     * for {@link #scaleToUnitLength}. In data-blind mode the centroid sums are not accumulated.
     */
    protected final void accumulate(float[] vector) {
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        float dp = VectorUtil.dotProduct(vector, vector);
        float divisor = (float) Math.sqrt(dp);
        magnitudes.add(divisor);
        if (enableCentering) {
          for (int i = 0; i < vector.length; i++) {
            dimensionSums[i] += (vector[i] / divisor);
          }
        }
      } else if (enableCentering) {
        for (int i = 0; i < vector.length; i++) {
          dimensionSums[i] += vector[i];
        }
      }
    }

    /**
     * Returns the mean of the accumulated vectors, unit-length for COSINE, used as the quantization
     * centroid. All zeroes when no vectors were added, or when centering is disabled (data-blind
     * mode).
     */
    protected final float[] computeCentroid() {
      if (enableCentering == false) {
        return new float[dim];
      }
      float[] centroid = new float[dim];
      int vectorCount = getVectors().size();
      if (vectorCount > 0) {
        for (int i = 0; i < dim; i++) {
          centroid[i] = dimensionSums[i] / vectorCount;
        }
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
          VectorUtil.l2normalize(centroid);
        }
      }
      return centroid;
    }

    /**
     * Writes {@code src} into {@code dst} scaled to unit length, using the magnitude cached for
     * {@code ord}. The two arrays may be the same.
     */
    protected final void scaleToUnitLength(float[] src, float[] dst, int ord) {
      float magnitude = magnitudes.get(ord);
      for (int i = 0; i < src.length; i++) {
        dst[i] = src[i] / magnitude;
      }
    }

    /**
     * Returns the RAM usage of quantization-specific state only (magnitudes, dimensionSums, shallow
     * object overhead). The underlying flat vector data is tracked separately by the
     * rawVectorDelegate at the writer level to avoid double-counting.
     */
    long quantizationOverheadBytesUsed() {
      long size = SHALLOW_SIZE;
      size += magnitudes.ramBytesUsed();
      if (dimensionSums != null) {
        size += RamUsageEstimator.sizeOf(dimensionSums);
      }
      return size;
    }

    @Override
    public long ramBytesUsed() {
      long size = quantizationOverheadBytesUsed();
      size += flatFieldVectorsWriter.ramBytesUsed();
      return size;
    }
  }

  private static class Float32FieldWriter extends FieldWriter<float[]> {
    private final float[] normalized;

    Float32FieldWriter(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter,
        boolean enableCentering) {
      super(fieldInfo, flatFieldVectorsWriter, enableCentering);
      this.normalized = new float[dim];
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      accumulate(vectorValue);
    }

    @Override
    float[] floatVectorValue(int ord) {
      float[] vector = flatFieldVectorsWriter.getVectors().get(ord);
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        scaleToUnitLength(vector, normalized, ord);
        return normalized;
      }
      return vector;
    }

    @Override
    long quantizationOverheadBytesUsed() {
      return super.quantizationOverheadBytesUsed() + RamUsageEstimator.sizeOf(normalized);
    }
  }

  private static class Float16FieldWriter extends FieldWriter<short[]> {
    private final float[] inflated;

    Float16FieldWriter(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<short[]> flatFieldVectorsWriter,
        boolean enableCentering) {
      super(fieldInfo, flatFieldVectorsWriter, enableCentering);
      this.inflated = new float[dim];
    }

    @Override
    public void addValue(int docID, short[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      inflate(vectorValue);
      accumulate(inflated);
    }

    @Override
    float[] floatVectorValue(int ord) {
      inflate(flatFieldVectorsWriter.getVectors().get(ord));
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        scaleToUnitLength(inflated, inflated, ord);
      }
      return inflated;
    }

    /** Inflates an fp16 vector into {@link #inflated}. */
    private void inflate(short[] vectorValue) {
      for (int i = 0; i < vectorValue.length; i++) {
        inflated[i] = Float.float16ToFloat(vectorValue[i]);
      }
    }

    @Override
    long quantizationOverheadBytesUsed() {
      return super.quantizationOverheadBytesUsed() + RamUsageEstimator.sizeOf(inflated);
    }
  }

  /**
   * In-memory storage for full-precision vectors used in data-blind mode; nothing is written to
   * disk. fp16 vectors are kept as fp16 so HNSW wrappers can read them back as {@link
   * Float16VectorValues}.
   */
  private static class InMemoryFieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(InMemoryFieldWriter.class);
    private final FieldInfo fieldInfo;
    private final List<T> vectors = new ArrayList<>();
    private final DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    private final int bytesPerElement;
    private boolean finished;
    private int lastDocID = -1;

    InMemoryFieldWriter(FieldInfo fieldInfo, int bytesPerElement) {
      this.fieldInfo = fieldInfo;
      this.bytesPerElement = bytesPerElement;
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
      if (finished) {
        throw new IllegalStateException("already finished, cannot add more values");
      }
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      vectors.add(copyValue(vectorValue));
      docsWithField.add(docID);
      lastDocID = docID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T copyValue(T vectorValue) {
      int dim = fieldInfo.getVectorDimension();
      return (T)
          (vectorValue instanceof float[] f
              ? ArrayUtil.copyOfSubArray(f, 0, dim)
              : ArrayUtil.copyOfSubArray((short[]) vectorValue, 0, dim));
    }

    @Override
    public List<T> getVectors() {
      return vectors;
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return docsWithField;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      if (vectors.isEmpty()) {
        return size;
      }
      return size
          + docsWithField.ramBytesUsed()
          + (long) vectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) vectors.size() * fieldInfo.getVectorDimension() * bytesPerElement;
    }
  }

  static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
    private OptimizedScalarQuantizer.QuantizationResult corrections;
    private final byte[] quantized;
    private final byte[] packed;
    private final float[] centroid;
    private final float centroidDP;
    private final FloatVectorValues values;
    private final OptimizedScalarQuantizer quantizer;
    private final ScalarEncoding encoding;

    private int lastOrd = -1;

    QuantizedFloatVectorValues(
        FloatVectorValues delegate,
        OptimizedScalarQuantizer quantizer,
        ScalarEncoding encoding,
        float[] centroid) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.encoding = encoding;
      this.quantized = new byte[encoding.getDiscreteDimensions(delegate.dimension())];
      this.packed =
          switch (encoding) {
            case UNSIGNED_BYTE, SEVEN_BIT -> this.quantized;
            case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
                new byte[encoding.getDocPackedLength(quantized.length)];
          };
      this.centroid = centroid;
      this.centroidDP = VectorUtil.dotProduct(centroid, centroid);
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) {
      if (ord != lastOrd) {
        throw new IllegalStateException(
            "attempt to retrieve corrective terms for different ord "
                + ord
                + " than the quantization was done for: "
                + lastOrd);
      }
      return corrections;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      if (ord != lastOrd) {
        quantize(ord);
        lastOrd = ord;
      }
      return packed;
    }

    @Override
    public int dimension() {
      return values.dimension();
    }

    @Override
    public OptimizedScalarQuantizer getQuantizer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScalarEncoding getScalarEncoding() {
      return encoding;
    }

    @Override
    public float[] getCentroid() throws IOException {
      return centroid;
    }

    @Override
    public float getCentroidDP() {
      return centroidDP;
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(short[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public QuantizedByteVectorValues copy() throws IOException {
      return new QuantizedFloatVectorValues(values.copy(), quantizer, encoding, centroid);
    }

    private void quantize(int ord) throws IOException {
      corrections =
          quantizer.scalarQuantize(
              values.vectorValue(ord), quantized, encoding.getBits(), centroid);
      packIndexRecord(encoding, quantized, packed);
    }

    @Override
    public DocIndexIterator iterator() {
      return values.iterator();
    }

    @Override
    public int ordToDoc(int ord) {
      return values.ordToDoc(ord);
    }
  }

  private static final class QuantizedByteVectorValuesSub extends DocIDMerger.Sub {
    final QuantizedByteVectorValues values;
    final KnnVectorValues.DocIndexIterator iterator;

    QuantizedByteVectorValuesSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
      super(docMap);
      this.values = values;
      this.iterator = values.iterator();
      assert iterator.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  /** Merged view of {@link QuantizedByteVectorValues} from multiple segments. */
  static final class MergedQuantizedByteVectorValues extends QuantizedByteVectorValues {
    private final List<QuantizedByteVectorValuesSub> subs;
    private final DocIDMerger<QuantizedByteVectorValuesSub> docIdMerger;
    private final int size;
    private final float[] centroid;
    private final float centroidDP;
    private final ScalarEncoding scalarEncoding;
    private int docId = -1;
    private int lastOrd = -1;
    private QuantizedByteVectorValuesSub current;

    private MergedQuantizedByteVectorValues(
        List<QuantizedByteVectorValuesSub> subs,
        MergeState mergeState,
        float[] centroid,
        ScalarEncoding scalarEncoding)
        throws IOException {
      this.subs = subs;
      this.docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
      int totalSize = 0;
      for (QuantizedByteVectorValuesSub sub : subs) {
        totalSize += sub.values.size();
      }
      this.size = totalSize;
      this.centroid = centroid;
      this.centroidDP = VectorUtil.dotProduct(centroid, centroid);
      this.scalarEncoding = scalarEncoding;
    }

    /**
     * Merges the pre-built per-segment {@link QuantizedByteVectorValuesSub}s in doc order. Each sub
     * contributes either a segment's stored quantized bytes (passed through untouched) or freshly
     * quantized values; the caller decides which per segment.
     */
    static MergedQuantizedByteVectorValues merge(
        MergeState mergeState,
        float[] centroid,
        ScalarEncoding encoding,
        List<QuantizedByteVectorValuesSub> subs)
        throws IOException {
      return new MergedQuantizedByteVectorValues(subs, mergeState, centroid, encoding);
    }

    @Override
    public DocIndexIterator iterator() {
      return new DocIndexIterator() {
        private int index = -1;

        @Override
        public int docID() {
          return docId;
        }

        @Override
        public int index() {
          return index;
        }

        @Override
        public int nextDoc() throws IOException {
          current = docIdMerger.next();
          if (current == null) {
            docId = NO_MORE_DOCS;
            index = NO_MORE_DOCS;
          } else {
            docId = current.mappedDocID;
            ++lastOrd;
            ++index;
          }
          return docId;
        }

        @Override
        public int advance(int target) {
          throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
          return size;
        }
      };
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      if (ord != lastOrd) {
        throw new IllegalStateException(
            "only supports forward iteration: ord=" + ord + ", lastOrd=" + lastOrd);
      }
      return current.values.vectorValue(current.iterator.index());
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord)
        throws IOException {
      if (ord != lastOrd) {
        throw new IllegalStateException(
            "only supports forward iteration: ord=" + ord + ", lastOrd=" + lastOrd);
      }
      return current.values.getCorrectiveTerms(current.iterator.index());
    }

    @Override
    public int dimension() {
      return subs.isEmpty() ? 0 : subs.get(0).values.dimension();
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScalarEncoding getScalarEncoding() {
      return scalarEncoding;
    }

    @Override
    public float[] getCentroid() {
      return centroid;
    }

    @Override
    public float getCentroidDP() {
      return centroidDP;
    }

    @Override
    public OptimizedScalarQuantizer getQuantizer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public QuantizedByteVectorValues copy() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Exposes a {@link Float16VectorValues} as {@link FloatVectorValues}, inflating fp16 to fp32 on
   * read, so the merge path can reuse the fp32 quantization classes.
   */
  static final class Float16AsFloatVectorValues extends FloatVectorValues {
    private final Float16VectorValues values;
    private final float[] floatVector;

    Float16AsFloatVectorValues(Float16VectorValues values) {
      this.values = values;
      this.floatVector = new float[values.dimension()];
    }

    @Override
    public int dimension() {
      return values.dimension();
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return values.ordToDoc(ord);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      short[] v = values.vectorValue(ord);
      for (int i = 0; i < v.length; i++) {
        floatVector[i] = Float.float16ToFloat(v[i]);
      }
      return floatVector;
    }

    @Override
    public DocIndexIterator iterator() {
      return values.iterator();
    }

    @Override
    public Float16AsFloatVectorValues copy() throws IOException {
      return new Float16AsFloatVectorValues(values.copy());
    }
  }
}
