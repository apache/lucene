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
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
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
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, vectorData;
  private final ScalarEncoding encoding;
  private final FlatVectorsWriter rawVectorDelegate;
  private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
  private boolean finished;

  /** Sole constructor */
  public Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
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
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

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
      IOUtils.closeWhileHandlingException(this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      @SuppressWarnings("unchecked")
      FieldWriter fieldWriter =
          new FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) rawVectorDelegate);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawVectorDelegate;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : fields) {
      // after raw vectors are written, normalize vectors for clustering and quantization
      if (VectorSimilarityFunction.COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
        field.normalizeVectors();
      }
      final float[] clusterCenter;
      int vectorCount = field.flatFieldVectorsWriter.getVectors().size();
      clusterCenter = new float[field.dimensionSums.length];
      if (vectorCount > 0) {
        for (int i = 0; i < field.dimensionSums.length; i++) {
          clusterCenter[i] = field.dimensionSums[i] / vectorCount;
        }
        if (VectorSimilarityFunction.COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
          VectorUtil.l2normalize(clusterCenter);
        }
      }
      if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
        segmentWriteState.infoStream.message(
            QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
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
      FieldWriter fieldData, float[] clusterCenter, int maxDoc, OptimizedScalarQuantizer quantizer)
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
      FieldWriter fieldData, float[] clusterCenter, OptimizedScalarQuantizer scalarQuantizer)
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
      float[] v = fieldData.getVectors().get(i);
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(v, scratch, encoding.getBits(), clusterCenter);
      packIndexRecord(encoding, scratch, vector);
      vectorData.writeBytes(vector, vector.length);
      writeCorrections(vectorData, corrections);
    }
  }

  private void writeSortingField(
      FieldWriter fieldData,
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
      FieldWriter fieldData,
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
      float[] v = fieldData.getVectors().get(ordinal);
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(v, scratch, encoding.getBits(), clusterCenter);
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

  /** The merged float vectors of the field, as the quantizer sees them: normalized for COSINE. */
  private FloatVectorValues mergedFloatVectorValues(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    FloatVectorValues vectorValues =
        MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
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
    mergeOneFlatVectorField(fieldInfo, mergeState, vectorCount -> false);
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
    if (!fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      return null;
    }
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    // Only asymmetric encodings have query-side records. The predicate sees vectorCount before
    // deletions, so it can request data even when the merged field is too small to build a graph.
    boolean prepareQueryData = encoding.isAsymmetric() && needsMergeScorer.test(vectorCount);
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
        IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, queryDataName);
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
            mergedReader instanceof Lucene104ScalarQuantizedVectorsReader quantizedReader
                ? quantizedReader.getQuantizedVectorValues(fieldInfo.name)
                : null;
        if (indexVectors == null) {
          // not this format's reader
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
    if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
      vectorsReader = candidateReader.getFieldReader(fieldName);
    }
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getCentroid(fieldName);
    }
    return null;
  }

  static int mergeAndRecalculateCentroids(
      MergeState mergeState, FieldInfo fieldInfo, float[] mergedCentroid) throws IOException {
    boolean recalculate = false;
    int totalVectorCount = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null
          || knnVectorsReader.getFloatVectorValues(fieldInfo.name) == null) {
        continue;
      }
      float[] centroid = getCentroid(knnVectorsReader, fieldInfo.name);
      int vectorCount = knnVectorsReader.getFloatVectorValues(fieldInfo.name).size();
      if (vectorCount == 0) {
        continue;
      }
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
    assert fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32);
    // clear out the centroid
    Arrays.fill(centroid, 0);
    int count = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null) continue;
      FloatVectorValues vectorValues =
          mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
      if (vectorValues == null) {
        continue;
      }
      KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ++count;
        float[] vector = vectorValues.vectorValue(iterator.index());
        for (int j = 0; j < vector.length; j++) {
          centroid[j] += vector[j];
        }
      }
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

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    // The rawVectorDelegate tracks all vector data for both byte and float32 fields.
    // For byte vector fields (which bypass our FieldWriter), this is the only accounting.
    // For float32 fields, this covers the flat vector data; our FieldWriter adds the
    // quantization-specific overhead (magnitudes, dimensionSums) on top.
    total += rawVectorDelegate.ramBytesUsed();
    for (FieldWriter field : fields) {
      // quantizationOverheadBytesUsed() intentionally excludes flatFieldVectorsWriter
      // because rawVectorDelegate.ramBytesUsed() already accounts for all flat vector
      // data at the writer level. Calling field.ramBytesUsed() here would double-count.
      total += field.quantizationOverheadBytesUsed();
    }
    return total;
  }

  static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;
    private final FloatArrayList magnitudes = new FloatArrayList();

    FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dimensionSums = new float[fieldInfo.getVectorDimension()];
    }

    @Override
    public List<float[]> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    public void normalizeVectors() {
      for (int i = 0; i < flatFieldVectorsWriter.getVectors().size(); i++) {
        float[] vector = flatFieldVectorsWriter.getVectors().get(i);
        float magnitude = magnitudes.get(i);
        for (int j = 0; j < vector.length; j++) {
          vector[j] /= magnitude;
        }
      }
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
      assert flatFieldVectorsWriter.isFinished();
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        float dp = VectorUtil.dotProduct(vectorValue, vectorValue);
        float divisor = (float) Math.sqrt(dp);
        magnitudes.add(divisor);
        for (int i = 0; i < vectorValue.length; i++) {
          dimensionSums[i] += (vectorValue[i] / divisor);
        }
      } else {
        for (int i = 0; i < vectorValue.length; i++) {
          dimensionSums[i] += vectorValue[i];
        }
      }
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      throw new UnsupportedOperationException();
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
}
