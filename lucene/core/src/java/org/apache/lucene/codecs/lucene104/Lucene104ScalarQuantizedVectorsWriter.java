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
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10 */
public class Lucene104ScalarQuantizedVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene104ScalarQuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, vectorData;
  private final ScalarEncoding encoding;
  private final FlatVectorsWriter rawVectorDelegate;
  private final Lucene104ScalarQuantizedVectorScorer vectorsScorer;
  private boolean finished;

  /**
   * Sole constructor
   *
   * @param vectorsScorer the scorer to use for scoring vectors
   */
  protected Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
    this.encoding = encoding;
    this.vectorsScorer = vectorsScorer;
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
      IOUtils.closeWhileSuppressingExceptions(t, this);
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
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    for (int i = 0; i < fieldData.getVectors().size(); i++) {
      float[] v = fieldData.getVectors().get(i);
      // XXX must pack PACKED_NIBBLE
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(v, vector, encoding.getBits(), clusterCenter);
      vectorData.writeBytes(vector, vector.length);
      vectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
      vectorData.writeInt(corrections.quantizedComponentSum());
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
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    for (int ordinal : ordMap) {
      float[] v = fieldData.getVectors().get(ordinal);
      // XXX must pack PACKED_NIBBLE
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(v, vector, encoding.getBits(), clusterCenter);
      vectorData.writeBytes(vector, vector.length);
      vectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
      vectorData.writeInt(corrections.quantizedComponentSum());
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

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (!fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
      return;
    }

    final float[] centroid;
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
    // Don't need access to the random vectors, we can just use the merged
    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    centroid = mergedCentroid;
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    FloatVectorValues floatVectorValues =
        MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
    if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
      floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
    }
    QuantizedFloatVectorValues quantizedVectorValues =
        new QuantizedFloatVectorValues(
            floatVectorValues,
            new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction()),
            encoding,
            centroid);
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    DocsWithFieldSet docsWithField = writeVectorData(vectorData, quantizedVectorValues);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    float centroidDp =
        docsWithField.cardinality() > 0 ? VectorUtil.dotProduct(centroid, centroid) : 0;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        centroid,
        centroidDp,
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
      OptimizedScalarQuantizer.QuantizationResult corrections =
          quantizedByteVectorValues.getCorrectiveTerms(iterator.index());
      output.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
      output.writeInt(Float.floatToIntBits(corrections.upperInterval()));
      output.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
      output.writeInt(corrections.quantizedComponentSum());
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (!fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    final float[] centroid;
    final float cDotC;
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);

    // Don't need access to the random vectors, we can just use the merged
    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    centroid = mergedCentroid;
    cDotC = vectorCount > 0 ? VectorUtil.dotProduct(centroid, centroid) : 0;
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    return mergeOneFieldToIndex(segmentWriteState, fieldInfo, mergeState, centroid, cDotC);
  }

  private CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      SegmentWriteState segmentWriteState,
      FieldInfo fieldInfo,
      MergeState mergeState,
      float[] centroid,
      float cDotC)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempQuantizedVectorData = null;
    IndexInput quantizedDataInput = null;
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    try {
      tempQuantizedVectorData =
          segmentWriteState.directory.createTempOutput(
              vectorData.getName(), "temp", segmentWriteState.context);
      final String tempQuantizedVectorName = tempQuantizedVectorData.getName();
      FloatVectorValues floatVectorValues =
          MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
      }
      DocsWithFieldSet docsWithField =
          writeVectorData(
              tempQuantizedVectorData,
              new QuantizedFloatVectorValues(floatVectorValues, quantizer, encoding, centroid));
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      quantizedDataInput =
          segmentWriteState.directory.openInput(tempQuantizedVectorName, segmentWriteState.context);
      vectorData.copyBytes(
          quantizedDataInput, quantizedDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(quantizedDataInput);
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          centroid,
          cDotC,
          docsWithField);

      final IndexInput finalQuantizedDataInput = quantizedDataInput;
      tempQuantizedVectorData = null;
      quantizedDataInput = null;

      OffHeapScalarQuantizedVectorValues vectorValues =
          new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
              fieldInfo.getVectorDimension(),
              docsWithField.cardinality(),
              centroid,
              cDotC,
              quantizer,
              encoding,
              fieldInfo.getVectorSimilarityFunction(),
              vectorsScorer,
              finalQuantizedDataInput);
      RandomVectorScorerSupplier scorerSupplier =
          vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(), vectorValues);
      return new QuantizedCloseableRandomVectorScorerSupplier(
          scorerSupplier,
          vectorValues,
          () -> {
            IOUtils.close(finalQuantizedDataInput);
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory, tempQuantizedVectorName);
          });
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, tempQuantizedVectorData, quantizedDataInput);
      if (tempQuantizedVectorData != null) {
        IOUtils.deleteFilesSuppressingExceptions(
            t, segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
      throw t;
    }
  }

  @Override
  public void close() throws IOException {
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
    if (recalculate) {
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
    for (FieldWriter field : fields) {
      // the field tracks the delegate field usage
      total += field.ramBytesUsed();
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

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      size += flatFieldVectorsWriter.ramBytesUsed();
      size += magnitudes.ramBytesUsed();
      return size;
    }
  }

  static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
    private OptimizedScalarQuantizer.QuantizationResult corrections;
    private final byte[] quantized;
    private final float[] centroid;
    private final FloatVectorValues values;
    private final OptimizedScalarQuantizer quantizer;
    private final ScalarEncoding encoding;

    private int lastOrd = -1;

    QuantizedFloatVectorValues(
        FloatVectorValues delegate, OptimizedScalarQuantizer quantizer, ScalarEncoding encoding, float[] centroid) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.encoding = encoding;
      this.quantized = new byte[delegate.dimension()];
      this.centroid = centroid;
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
      return quantized;
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
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getCentroid() throws IOException {
      return centroid;
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
      // XXX pack PACKED_NIBBLE, maybe???
      corrections =
          quantizer.scalarQuantize(values.vectorValue(ord), quantized, encoding.getBits(), centroid);
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

  static class QuantizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {
    private final RandomVectorScorerSupplier supplier;
    private final KnnVectorValues vectorValues;
    private final Closeable onClose;

    QuantizedCloseableRandomVectorScorerSupplier(
        RandomVectorScorerSupplier supplier, KnnVectorValues vectorValues, Closeable onClose) {
      this.supplier = supplier;
      this.onClose = onClose;
      this.vectorValues = vectorValues;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return supplier.scorer();
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return supplier.copy();
    }

    @Override
    public void close() throws IOException {
      onClose.close();
    }

    @Override
    public int totalVectorCount() {
      return vectorValues.size();
    }
  }

  static final class NormalizedFloatVectorValues extends FloatVectorValues {
    private final FloatVectorValues values;
    private final float[] normalizedVector;

    NormalizedFloatVectorValues(FloatVectorValues values) {
      this.values = values;
      this.normalizedVector = new float[values.dimension()];
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
      System.arraycopy(values.vectorValue(ord), 0, normalizedVector, 0, normalizedVector.length);
      VectorUtil.l2normalize(normalizedVector);
      return normalizedVector;
    }

    @Override
    public DocIndexIterator iterator() {
      return values.iterator();
    }

    @Override
    public NormalizedFloatVectorValues copy() throws IOException {
      return new NormalizedFloatVectorValues(values.copy());
    }
  }
}
