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
import java.util.concurrent.Callable;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.document.column.VectorValuesCursor;
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
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
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
  private final FlatVectorsWriter rawVectorDelegate;
  private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;
  private boolean finished;

  /** Creates a writer that quantizes merged vectors on the merging thread. */
  public Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    this(state, encoding, rawVectorDelegate, vectorsScorer, 1, null);
  }

  /**
   * Creates a writer that quantizes merged vectors on {@code numMergeWorkers} tasks, as the HNSW
   * graph merge does.
   *
   * @param numMergeWorkers tasks that quantize the vectors of a merged field; 1 quantizes them on
   *     the merging thread
   * @param mergeExec the executor for those tasks, or null to use the merge's {@link
   *     MergeState#intraMergeTaskExecutor}
   */
  public Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer,
      int numMergeWorkers,
      TaskExecutor mergeExec)
      throws IOException {
    super(vectorsScorer);
    this.numMergeWorkers = numMergeWorkers;
    this.mergeExec = mergeExec;
    this.vectorScorer = vectorsScorer;
    this.encoding = encoding;
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
      vectorData =
          state.directory.createOutput(
              vectorDataFileName, state.context.union(FileTypeHint.DATA, FileDataHint.KNN_VECTORS));

      CodecUtil.writeIndexHeader(
          meta,
          Lucene104ScalarQuantizedVectorsFormat.META_CODEC_NAME,
          Lucene104ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene104ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().isFloatingPoint()) {
      FieldWriter<?> fieldWriter = FieldWriter.create(fieldInfo, rawVectorDelegate);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawVectorDelegate;
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
      final ByteBuffer buffer =
          ByteBuffer.allocate(field.getVectorDimension() * Float.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN);
      buffer.asFloatBuffer().put(clusterCenter);
      meta.writeBytes(buffer.array(), buffer.array().length);
      meta.writeInt(Float.floatToIntBits(centroidDp));
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
    // Don't need access to the random vectors, we can just use the merged
    rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
    if (fieldInfo.getVectorEncoding().isFloatingPoint() == false) {
      return null;
    }
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount =
        mergeAndRecalculateCentroids(
            mergeState, fieldInfo, mergedCentroid, mergeExecutor(mergeState), numMergeWorkers);
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    // Only asymmetric encodings have query-side records, and both FLOAT16 and FLOAT32 fields
    // score graphs with them.
    boolean prepareQueryData =
        encoding.isAsymmetric()
            && fieldInfo.getVectorEncoding().isFloatingPoint()
            && needsMergeScorer.test(vectorCount);
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    ParallelMergeQuantizer parallelQuantizer =
        parallelMergeQuantizer(fieldInfo, mergeState, vectorCount);
    DocsWithFieldSet docsWithField;
    MergeScorerData mergeScorerData = null;
    if (prepareQueryData) {
      docsWithField = new DocsWithFieldSet();
      mergeScorerData =
          writeVectorAndQueryData(
              fieldInfo, mergeState, mergedCentroid, docsWithField, parallelQuantizer);
    } else if (parallelQuantizer != null) {
      docsWithField = new DocsWithFieldSet();
      parallelQuantizer.writeRecords(
          vectorData,
          null,
          new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction()),
          encoding,
          mergedCentroid,
          docsWithField,
          QUANTIZE_BLOCK_SIZE);
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
      FieldInfo fieldInfo,
      MergeState mergeState,
      float[] centroid,
      DocsWithFieldSet docsWithField,
      ParallelMergeQuantizer parallelQuantizer)
      throws IOException {
    assert encoding.isAsymmetric();
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    String queryDataName = null;
    try (IndexOutput queryData =
        segmentWriteState.directory.createTempOutput(
            segmentWriteState.segmentInfo.name, "queries", queryDataContext())) {
      queryDataName = queryData.getName();
      if (parallelQuantizer != null) {
        parallelQuantizer.writeRecords(
            vectorData,
            queryData,
            quantizer,
            encoding,
            centroid,
            docsWithField,
            QUANTIZE_BLOCK_SIZE);
      } else {
        writeVectorAndQueryRecords(
            vectorData,
            queryData,
            mergedFloatVectorValues(fieldInfo, mergeState),
            quantizer,
            encoding,
            centroid,
            docsWithField);
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

  /**
   * Vectors in one block of a parallel merge quantization. Most of the memory this costs is the
   * encoded records of one block: 5.5 MB at 1024 dimensions with 1-bit codes and 4-bit queries.
   */
  static final int QUANTIZE_BLOCK_SIZE = 8192;

  /**
   * The executor that computes the centroid and quantizes a merged field in parallel, or null to do
   * both on the merging thread. As for the HNSW graph merge, this is the format's executor if it
   * has one and the merge's own otherwise, and only with more than one merge worker.
   */
  private TaskExecutor mergeExecutor(MergeState mergeState) {
    if (numMergeWorkers <= 1) {
      return null;
    }
    if (mergeExec != null) {
      return mergeExec;
    }
    return mergeState.intraMergeTaskExecutor == null
        ? null
        : new TaskExecutor(mergeState.intraMergeTaskExecutor);
  }

  /**
   * Returns the parallel quantizer of a merged field, or null to quantize it serially. A field of
   * one block or less is quantized on the merging thread.
   */
  private ParallelMergeQuantizer parallelMergeQuantizer(
      FieldInfo fieldInfo, MergeState mergeState, int vectorCount) throws IOException {
    TaskExecutor executor = mergeExecutor(mergeState);
    if (executor == null || vectorCount <= QUANTIZE_BLOCK_SIZE) {
      return null;
    }
    // the segments that MergedVectorValues merges, viewed as mergedFloatVectorValues views them
    List<FloatVectorValues> segments = new ArrayList<>();
    List<MergeState.DocMap> docMaps = new ArrayList<>();
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
      if (reader == null
          || MergedVectorValues.hasVectorValues(mergeState.fieldInfos[i], fieldInfo.name)
              == false) {
        continue;
      }
      FloatVectorValues values = floatingPointVectorValues(reader, fieldInfo);
      if (values != null) {
        segments.add(
            fieldInfo.getVectorSimilarityFunction() == COSINE
                ? new NormalizedFloatVectorValues(values)
                : values);
        docMaps.add(mergeState.docMaps[i]);
      }
    }
    return segments.isEmpty()
        ? null
        : ParallelMergeQuantizer.create(
            executor, numMergeWorkers, segments, docMaps, mergeState.needsIndexSort);
  }

  /**
   * Quantizes a vector and writes its records: the index-side record and, if there is a query side,
   * the query-side record. The serial and the parallel paths both use it, so they write the same
   * bytes. Not thread-safe.
   */
  static final class RecordEncoder {
    private final OptimizedScalarQuantizer quantizer;
    private final ScalarEncoding encoding;
    private final float[] centroid;
    private final byte[] indexQuantized;
    private final byte[] indexPacked;
    private final byte[] queryQuantized;
    private final byte[] queryPacked;
    private final byte[][] destinations;
    private final byte[] bits;

    RecordEncoder(
        OptimizedScalarQuantizer quantizer, ScalarEncoding encoding, float[] centroid, int dim) {
      this.quantizer = quantizer;
      this.encoding = encoding;
      this.centroid = centroid;
      int discretizedDims = encoding.getDiscreteDimensions(dim);
      indexQuantized = new byte[discretizedDims];
      indexPacked =
          switch (encoding) {
            case UNSIGNED_BYTE, SEVEN_BIT -> indexQuantized;
            case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
                new byte[encoding.getDocPackedLength(discretizedDims)];
          };
      queryQuantized = new byte[discretizedDims];
      queryPacked = new byte[encoding.getQueryPackedLength(discretizedDims)];
      destinations = new byte[][] {indexQuantized, queryQuantized};
      bits = new byte[] {encoding.getBits(), encoding.getQueryBits()};
    }

    int indexRecordLength() {
      return indexPacked.length + CORRECTIONS_LENGTH;
    }

    int queryRecordLength() {
      return queryPacked.length + CORRECTIONS_LENGTH;
    }

    /**
     * Writes the records of {@code vector}, which the quantizer centers in place. Writes no query
     * record if {@code query} is null.
     */
    void encode(float[] vector, DataOutput index, DataOutput query) throws IOException {
      OptimizedScalarQuantizer.QuantizationResult indexCorrections;
      if (query == null) {
        indexCorrections =
            quantizer.scalarQuantize(vector, indexQuantized, encoding.getBits(), centroid);
      } else {
        // centers the vector once and quantizes it at both bit widths
        OptimizedScalarQuantizer.QuantizationResult[] corrections =
            quantizer.multiScalarQuantize(vector, destinations, bits, centroid);
        indexCorrections = corrections[0];
        writeQueryRecord(query, queryQuantized, queryPacked, corrections[1]);
      }
      // the index side, packed as QuantizedFloatVectorValues packs it
      packIndexRecord(encoding, indexQuantized, indexPacked);
      index.writeBytes(indexPacked, indexPacked.length);
      writeCorrections(index, indexCorrections);
    }
  }

  /** Bytes of the corrective terms that follow each record. */
  private static final int CORRECTIONS_LENGTH = 3 * Float.BYTES + Integer.BYTES;

  /**
   * Writes the index-side record of each vector to {@code vectorData} and its query-side record to
   * {@code queryData}.
   */
  static void writeVectorAndQueryRecords(
      IndexOutput vectorData,
      IndexOutput queryData,
      FloatVectorValues values,
      OptimizedScalarQuantizer quantizer,
      ScalarEncoding encoding,
      float[] centroid,
      DocsWithFieldSet docsWithField)
      throws IOException {
    RecordEncoder encoder = new RecordEncoder(quantizer, encoding, centroid, values.dimension());
    KnnVectorValues.DocIndexIterator iterator = values.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      encoder.encode(values.vectorValue(iterator.index()), vectorData, queryData);
      docsWithField.add(docV);
    }
  }

  /**
   * Quantizes the vectors of a merged field on several workers, and writes the same records as the
   * serial path. The merged view of the vectors can only be read forward by one thread, so the
   * workers read the source segments instead, each through its own copies.
   *
   * <p>For each block, the merging thread walks the merged documents and records the segment and
   * ordinal of each vector, the workers each read, quantize and encode a range of the block, and
   * the merging thread writes the block with one call per file. Writes stay on the merging thread,
   * because {@code OneMergeProgress#pauseNanos} expects the merge's rate limiter to pause that
   * thread only.
   */
  static final class ParallelMergeQuantizer {
    private final TaskExecutor executor;
    private final DocIDMerger<SegmentVectors> merger;
    private final int dim;
    // [worker][segment]
    private final FloatVectorValues[][] copies;
    private boolean exhausted;

    private ParallelMergeQuantizer(
        TaskExecutor executor,
        DocIDMerger<SegmentVectors> merger,
        int dim,
        FloatVectorValues[][] copies) {
      this.executor = executor;
      this.merger = merger;
      this.dim = dim;
      this.copies = copies;
    }

    /**
     * Returns a quantizer that reads {@code segmentVectors}, whose documents {@code docMaps} map
     * into the merged segment, on {@code workers} tasks.
     */
    static ParallelMergeQuantizer create(
        TaskExecutor executor,
        int workers,
        List<FloatVectorValues> segmentVectors,
        List<MergeState.DocMap> docMaps,
        boolean indexIsSorted)
        throws IOException {
      List<SegmentVectors> segments = new ArrayList<>(segmentVectors.size());
      FloatVectorValues[][] copies = new FloatVectorValues[workers][segmentVectors.size()];
      for (int s = 0; s < segmentVectors.size(); s++) {
        FloatVectorValues values = segmentVectors.get(s);
        segments.add(new SegmentVectors(docMaps.get(s), s, values));
        for (int w = 0; w < workers; w++) {
          copies[w][s] = values.copy();
        }
      }
      return new ParallelMergeQuantizer(
          executor,
          DocIDMerger.of(segments, indexIsSorted),
          segmentVectors.get(0).dimension(),
          copies);
    }

    /**
     * Writes the index-side records to {@code vectorData} and, if {@code queryData} is not null,
     * the query-side records to it, in blocks of {@code blockSize} vectors.
     */
    void writeRecords(
        IndexOutput vectorData,
        IndexOutput queryData,
        OptimizedScalarQuantizer quantizer,
        ScalarEncoding encoding,
        float[] centroid,
        DocsWithFieldSet docsWithField,
        int blockSize)
        throws IOException {
      int workers = copies.length;
      RecordEncoder[] encoders = new RecordEncoder[workers];
      for (int w = 0; w < workers; w++) {
        encoders[w] = new RecordEncoder(quantizer, encoding, centroid, dim);
      }
      float[][] vectors = new float[workers][dim];
      int indexLength = encoders[0].indexRecordLength();
      int queryLength = queryData == null ? 0 : encoders[0].queryRecordLength();
      byte[] indexRecords = new byte[blockSize * indexLength];
      byte[] queryRecords = new byte[blockSize * queryLength];
      Block block = new Block(blockSize);
      List<Callable<Void>> tasks = new ArrayList<>(workers);
      for (fill(block); block.count > 0; fill(block)) {
        tasks.clear();
        int rangeSize = (block.count + workers - 1) / workers;
        for (int w = 0; w * rangeSize < block.count; w++) {
          int worker = w;
          int from = w * rangeSize;
          int to = Math.min(block.count, from + rangeSize);
          tasks.add(
              () -> {
                DataOutput index =
                    new ByteArrayDataOutput(
                        indexRecords, from * indexLength, (to - from) * indexLength);
                DataOutput query =
                    queryData == null
                        ? null
                        : new ByteArrayDataOutput(
                            queryRecords, from * queryLength, (to - from) * queryLength);
                float[] vector = vectors[worker];
                for (int i = from; i < to; i++) {
                  // a copy, because the quantizer centers its input in place
                  float[] v = copies[worker][block.segments[i]].vectorValue(block.ords[i]);
                  System.arraycopy(v, 0, vector, 0, dim);
                  encoders[worker].encode(vector, index, query);
                }
                return null;
              });
        }
        executor.invokeAll(tasks);
        vectorData.writeBytes(indexRecords, block.count * indexLength);
        if (queryData != null) {
          queryData.writeBytes(queryRecords, block.count * queryLength);
        }
        for (int i = 0; i < block.count; i++) {
          docsWithField.add(block.docs[i]);
        }
      }
    }

    /** Fills {@code block} with the next merged documents; it has none when all were walked. */
    private void fill(Block block) throws IOException {
      block.count = 0;
      while (exhausted == false && block.count < block.docs.length) {
        SegmentVectors sub = merger.next();
        if (sub == null) {
          // DocIDMerger must not be called again once exhausted
          exhausted = true;
        } else {
          block.docs[block.count] = sub.mappedDocID;
          block.segments[block.count] = sub.segment;
          block.ords[block.count] = sub.iterator.index();
          block.count++;
        }
      }
    }
  }

  /** One source segment's vectors, as {@link DocIDMerger} merges them. */
  private static final class SegmentVectors extends DocIDMerger.Sub {
    final int segment;
    final KnnVectorValues.DocIndexIterator iterator;

    SegmentVectors(MergeState.DocMap docMap, int segment, FloatVectorValues values) {
      super(docMap);
      this.segment = segment;
      this.iterator = values.iterator();
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  /** The merged documents of a block, with the segment and the ordinal of each one's vector. */
  private static final class Block {
    final int[] docs;
    final int[] segments;
    final int[] ords;
    int count;

    Block(int size) {
      docs = new int[size];
      segments = new int[size];
      ords = new int[size];
    }
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

  static float[] getCentroid(KnnVectorsReader vectorsReader, String fieldName) {
    vectorsReader = vectorsReader.unwrapReaderForField(fieldName);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getCentroid(fieldName);
    }
    return null;
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

  /**
   * Computes the centroid of the merged vectors into {@code mergedCentroid} and returns their
   * count. Recomputing it reads every vector, on up to {@code workers} tasks when {@code executor}
   * is not null.
   */
  static int mergeAndRecalculateCentroids(
      MergeState mergeState,
      FieldInfo fieldInfo,
      float[] mergedCentroid,
      TaskExecutor executor,
      int workers)
      throws IOException {
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
      // or if there are deleted docs, we must recalculate the centroid
      if (centroid == null || mergeState.liveDocs[i] != null) {
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
      return calculateCentroid(mergeState, fieldInfo, mergedCentroid, executor, workers);
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

  /**
   * Live vectors that one task sums when a merge recomputes the centroid. Fixed, so that the
   * centroid does not depend on the number of workers.
   */
  static final int CENTROID_CHUNK_SIZE = 1024;

  static int calculateCentroid(
      MergeState mergeState,
      FieldInfo fieldInfo,
      float[] centroid,
      TaskExecutor executor,
      int workers)
      throws IOException {
    assert fieldInfo.getVectorEncoding().isFloatingPoint();
    double[] sums = new double[centroid.length];
    int count = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
      FloatVectorValues values =
          reader == null ? null : floatingPointVectorValues(reader, fieldInfo);
      if (values != null) {
        count += sumVectors(values, mergeState.liveDocs[i], sums, executor, workers);
      }
    }
    for (int i = 0; i < centroid.length; i++) {
      centroid[i] = count == 0 ? 0 : (float) (sums[i] / count);
    }
    if (count > 0 && fieldInfo.getVectorSimilarityFunction() == COSINE) {
      VectorUtil.l2normalize(centroid);
    }
    return count;
  }

  /**
   * Adds the live vectors of {@code values} to {@code sums} and returns how many there are. The
   * merging thread splits them in chunks of {@link #CENTROID_CHUNK_SIZE}, up to {@code workers}
   * tasks read and sum one chunk each, and the merging thread adds the chunk sums in order. So the
   * result does not depend on the number of workers.
   */
  static int sumVectors(
      FloatVectorValues values, Bits liveDocs, double[] sums, TaskExecutor executor, int workers)
      throws IOException {
    int tasks =
        executor == null
            ? 1
            : Math.clamp(Math.ceilDiv(values.size(), CENTROID_CHUNK_SIZE), 1, workers);
    int[][] ords = new int[tasks][CENTROID_CHUNK_SIZE];
    int[] sizes = new int[tasks];
    double[][] chunkSums = new double[tasks][sums.length];
    List<Callable<Void>> calls = new ArrayList<>(tasks);
    for (int t = 0; t < tasks; t++) {
      int task = t;
      // one task runs on the merging thread, so it reads the values themselves
      FloatVectorValues vectors = tasks == 1 ? values : values.copy();
      calls.add(
          () -> {
            Arrays.fill(chunkSums[task], 0);
            for (int i = 0; i < sizes[task]; i++) {
              float[] vector = vectors.vectorValue(ords[task][i]);
              for (int j = 0; j < vector.length; j++) {
                chunkSums[task][j] += vector[j];
              }
            }
            return null;
          });
    }
    TaskExecutor taskExecutor = executor == null ? new TaskExecutor(Runnable::run) : executor;
    int count = 0;
    KnnVectorValues.DocIndexIterator iterator = values.iterator();
    int doc = iterator.nextDoc();
    while (doc != NO_MORE_DOCS) {
      int chunks = 0;
      for (; chunks < tasks && doc != NO_MORE_DOCS; chunks++) {
        int size = 0;
        for (; size < CENTROID_CHUNK_SIZE && doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
          if (liveDocs == null || liveDocs.get(doc)) {
            ords[chunks][size++] = iterator.index();
          }
        }
        sizes[chunks] = size;
      }
      taskExecutor.invokeAll(calls.subList(0, chunks));
      for (int t = 0; t < chunks; t++) {
        for (int j = 0; j < sums.length; j++) {
          sums[j] += chunkSums[t][j];
        }
        count += sizes[t];
      }
    }
    return count;
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    // The rawVectorDelegate tracks all vector data for both byte and float32 fields.
    // For byte vector fields (which bypass our FieldWriter), this is the only accounting.
    // For float32 fields, this covers the flat vector data; our FieldWriter adds the
    // quantization-specific overhead (magnitudes, dimensionSums) on top.
    total += rawVectorDelegate.ramBytesUsed();
    for (FieldWriter<?> field : fields) {
      // quantizationOverheadBytesUsed() intentionally excludes flatFieldVectorsWriter
      // because rawVectorDelegate.ramBytesUsed() already accounts for all flat vector
      // data at the writer level. Calling field.ramBytesUsed() here would double-count.
      total += field.quantizationOverheadBytesUsed();
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

    FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<T> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dim = fieldInfo.getVectorDimension();
      this.dimensionSums = new float[dim];
    }

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(
        FieldInfo fieldInfo, FlatFieldVectorsWriter<?> flatFieldVectorsWriter) {
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> throw new UnsupportedOperationException("Byte Vectors aren't supported");
        case FLOAT32 ->
            new Float32FieldWriter(
                fieldInfo, (FlatFieldVectorsWriter<float[]>) flatFieldVectorsWriter);
        case FLOAT16 ->
            new Float16FieldWriter(
                fieldInfo, (FlatFieldVectorsWriter<short[]>) flatFieldVectorsWriter);
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
    public void addValue(int docID, T vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      accumulate(vectorValue);
    }

    /**
     * Adds the batch to the flat writer, then accumulates the new vectors. The flat writer records
     * nothing if the cursor throws, so neither does this writer.
     */
    @Override
    public void addDenseValues(int firstDocID, VectorValuesCursor<T> values) throws IOException {
      final int firstOrd = getVectors().size();
      flatFieldVectorsWriter.addDenseValues(firstDocID, values);
      final List<T> vectors = getVectors();
      for (int ord = firstOrd; ord < vectors.size(); ord++) {
        accumulate(vectors.get(ord));
      }
    }

    /** Adds a stored vector to the centroid sums and caches its magnitude when COSINE. */
    abstract void accumulate(T vector);

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      if (finished) {
        return;
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
     * for {@link #scaleToUnitLength}.
     */
    protected final void accumulateFloats(float[] vector) {
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        float dp = VectorUtil.dotProduct(vector, vector);
        float divisor = (float) Math.sqrt(dp);
        magnitudes.add(divisor);
        for (int i = 0; i < vector.length; i++) {
          dimensionSums[i] += (vector[i] / divisor);
        }
      } else {
        for (int i = 0; i < vector.length; i++) {
          dimensionSums[i] += vector[i];
        }
      }
    }

    /**
     * Returns the mean of the accumulated vectors, unit-length for COSINE, used as the quantization
     * centroid. All zeroes when no vectors were added.
     */
    protected final float[] computeCentroid() {
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
      size += RamUsageEstimator.sizeOf(dimensionSums);
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
        FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      super(fieldInfo, flatFieldVectorsWriter);
      this.normalized = new float[dim];
    }

    @Override
    void accumulate(float[] vector) {
      accumulateFloats(vector);
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
        FieldInfo fieldInfo, FlatFieldVectorsWriter<short[]> flatFieldVectorsWriter) {
      super(fieldInfo, flatFieldVectorsWriter);
      this.inflated = new float[dim];
    }

    @Override
    void accumulate(short[] vector) {
      inflate(vector);
      accumulateFloats(inflated);
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
