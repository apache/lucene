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
package org.apache.lucene.codecs.lucene912;

import static org.apache.lucene.codecs.lucene912.Lucene912BinaryQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.BinaryQuantizer;

public class Lucene912BinaryQuantizedVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene912BinaryQuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, binarizedVectorData;
  private final FlatVectorsWriter rawVectorDelegate;
  private final BinaryFlatVectorsScorer vectorsScorer;
  private final int numberOfVectorsPerCluster;
  private boolean finished;

  /**
   * Sole constructor
   *
   * @param vectorsScorer the scorer to use for scoring vectors
   */
  protected Lucene912BinaryQuantizedVectorsWriter(
      BinaryFlatVectorsScorer vectorsScorer,
      int numberOfVectorsPerCluster,
      FlatVectorsWriter rawVectorDelegate,
      SegmentWriteState state)
      throws IOException {
    super(vectorsScorer);
    this.vectorsScorer = vectorsScorer;
    this.numberOfVectorsPerCluster = numberOfVectorsPerCluster;
    this.segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene912BinaryQuantizedVectorsFormat.META_EXTENSION);

    String binarizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      binarizedVectorData =
          state.directory.createOutput(binarizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene912BinaryQuantizedVectorsFormat.META_CODEC_NAME,
          Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          binarizedVectorData,
          Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      @SuppressWarnings("unchecked")
      FieldWriter fieldWriter =
          new FieldWriter(
              fieldInfo,
              segmentWriteState.infoStream,
              (FlatFieldVectorsWriter<float[]>) rawVectorDelegate);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawVectorDelegate;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : fields) {
      // TODO handle more than one centroid
      BinaryQuantizer quantizer = field.createQuantizer();
      if (sortMap == null) {
        writeField(field, (byte) 0, false, maxDoc, quantizer);
      } else {
        writeSortingField(field, (byte) 0, false, maxDoc, sortMap, quantizer);
      }
      field.finish();
    }
  }

  private void writeField(
      FieldWriter fieldData,
      byte clusterId,
      boolean multipleClusters,
      int maxDoc,
      BinaryQuantizer quantizer)
      throws IOException {
    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeBinarizedVectors(fieldData, clusterId, multipleClusters, quantizer);
    long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        new float[][] {fieldData.dimensionSums},
        fieldData.getDocsWithFieldSet());
  }

  private void writeBinarizedVectors(
      FieldWriter fieldData,
      byte clusterId,
      boolean multipleClusters,
      BinaryQuantizer scalarQuantizer)
      throws IOException {
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 7) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?
    for (float[] v : fieldData.getVectors()) {
      float[] corrections =
          scalarQuantizer.quantizeForIndex(
              v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      binarizedVectorData.writeBytes(vector, vector.length);
      if (multipleClusters) {
        binarizedVectorData.writeByte(clusterId);
      }
      correctionsBuffer.putFloat(corrections[0]);
      correctionsBuffer.putFloat(corrections[1]);
      binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
      correctionsBuffer.rewind();
    }
  }

  private void writeSortingField(
      FieldWriter fieldData,
      byte clusterId,
      boolean multipleClusters,
      int maxDoc,
      Sorter.DocMap sortMap,
      BinaryQuantizer scalarQuantizer)
      throws IOException {
    final int[] ordMap =
        new int[fieldData.getDocsWithFieldSet().cardinality()]; // new ord to old ord

    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    mapOldOrdToNewOrd(fieldData.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);

    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeSortedBinarizedVectors(fieldData, clusterId, multipleClusters, ordMap, scalarQuantizer);
    long quantizedVectorLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        quantizedVectorLength,
        new float[][] {fieldData.dimensionSums},
        newDocsWithField);
  }

  private void writeSortedBinarizedVectors(
      FieldWriter fieldData,
      byte clusterId,
      boolean multipleClusters,
      int[] ordMap,
      BinaryQuantizer scalarQuantizer)
      throws IOException {
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 7) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?
    for (int ordinal : ordMap) {
      float[] v = fieldData.getVectors().get(ordinal);
      float[] corrections =
          scalarQuantizer.quantizeForIndex(
              v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      binarizedVectorData.writeBytes(vector, vector.length);
      if (multipleClusters) {
        binarizedVectorData.writeByte(clusterId);
      }
      correctionsBuffer.putFloat(corrections[0]);
      correctionsBuffer.putFloat(corrections[1]);
      binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
      correctionsBuffer.rewind();
    }
  }

  // TODO do we want to use `LongValues` to store vectorOrd -> centroidOrd mapping?
  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      float[][] clusterCenters,
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
      meta.writeVInt(clusterCenters.length);
      final ByteBuffer buffer =
          ByteBuffer.allocate(field.getVectorDimension() * Float.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN);
      for (float[] clusterCenter : clusterCenters) {
        buffer.asFloatBuffer().put(clusterCenter);
        meta.writeBytes(buffer.array(), buffer.array().length);
      }
    }
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, binarizedVectorData, count, maxDoc, docsWithField);
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
    if (binarizedVectorData != null) {
      CodecUtil.writeFooter(binarizedVectorData);
    }
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      float[][] centroids = mergeAndRecalculateCentroids(mergeState, fieldInfo);
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              new BinaryQuantizer(centroids[0]),
              fieldInfo.getVectorSimilarityFunction());
      long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(binarizedVectorData, binarizedVectorValues, false);
      long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          centroids,
          docsWithField);
    }
  }

  static DocsWithFieldSet writeBinarizedVectorData(
      IndexOutput output,
      BinarizedByteVectorValues binarizedByteVectorValues,
      boolean moreThanOneCluster)
      throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = binarizedByteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = binarizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = binarizedByteVectorValues.vectorValue();
      output.writeBytes(binaryValue, binaryValue.length);
      if (moreThanOneCluster) {
        output.writeByte(binarizedByteVectorValues.clusterId());
      }
      output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getDistanceToCentroid()));
      output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getMagnitude()));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
      float[][] centroids = mergeAndRecalculateCentroids(mergeState, fieldInfo);
      return mergeOneFieldToIndex(segmentWriteState, fieldInfo, mergeState, centroids);
    }
    return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
  }

  private CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      SegmentWriteState segmentWriteState,
      FieldInfo fieldInfo,
      MergeState mergeState,
      float[][] centroids)
      throws IOException {
    // TODO support more than one centroid
    assert centroids.length == 1;
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempQuantizedVectorData =
        segmentWriteState.directory.createTempOutput(
            binarizedVectorData.getName(), "temp", segmentWriteState.context);
    IndexOutput tempScoreQuantizedVectorData =
        segmentWriteState.directory.createTempOutput(
            binarizedVectorData.getName(), "score_temp", segmentWriteState.context);
    IndexInput binarizedDataInput = null;
    IndexInput binarizedScoreDataInput = null;
    boolean success = false;
    try {
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              new BinaryQuantizer(centroids[0]),
              fieldInfo.getVectorSimilarityFunction());
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(tempQuantizedVectorData, binarizedVectorValues, false);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      binarizedDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      binarizedVectorData.copyBytes(
          binarizedDataInput, binarizedDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = binarizedDataInput.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(binarizedDataInput);
      // TODO When we support more than one centroid, we will likely have to write num_centroid
      // quantized values
      // this way we have the query vector encoded for all centroids
      BinarizedForQueryFloatVectorValues binarizedForQueryVectorValues =
          new BinarizedForQueryFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              new BinaryQuantizer(centroids[0]),
              fieldInfo.getVectorSimilarityFunction());
      writeBinarizedVectorData(tempScoreQuantizedVectorData, binarizedForQueryVectorValues, false);
      CodecUtil.writeFooter(tempScoreQuantizedVectorData);
      IOUtils.close(tempScoreQuantizedVectorData);
      binarizedScoreDataInput =
          segmentWriteState.directory.openInput(
              tempScoreQuantizedVectorData.getName(), segmentWriteState.context);
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          centroids,
          docsWithField);
      success = true;
      final IndexInput finalBinarizedDataInput = binarizedDataInput;
      final IndexInput finalBinarizedScoreDataInput = binarizedScoreDataInput;
      RandomVectorScorerSupplier scorerSupplier =
          vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapBinarizedQueryVectorValues(
                  finalBinarizedScoreDataInput,
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality()),
              new OffHeapBinarizedVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality(),
                  false,
                  fieldInfo.getVectorSimilarityFunction(),
                  vectorsScorer,
                  finalBinarizedDataInput));
      return new BinarizedCloseableRandomVectorScorerSupplier(
          scorerSupplier,
          () -> {
            IOUtils.close(finalBinarizedDataInput, finalBinarizedScoreDataInput);
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory,
                tempQuantizedVectorData.getName(),
                tempScoreQuantizedVectorData.getName());
          },
          docsWithField.cardinality());
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(
            tempQuantizedVectorData,
            tempScoreQuantizedVectorData,
            binarizedDataInput,
            binarizedScoreDataInput);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory,
            tempQuantizedVectorData.getName(),
            tempScoreQuantizedVectorData.getName());
      }
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, binarizedVectorData, rawVectorDelegate);
  }

  static float[][] getCentroids(KnnVectorsReader vectorsReader, String fieldName) {
    if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
      vectorsReader = candidateReader.getFieldReader(fieldName);
    }
    if (vectorsReader instanceof Lucene912BinaryQuantizedVectorsReader reader) {
      return reader.getCentroids(fieldName);
    }
    return null;
  }

  static float[][] mergeAndRecalculateCentroids(MergeState mergeState, FieldInfo fieldInfo)
      throws IOException {
    boolean recalculate = false;
    float[] weightedCentroidAvg = new float[fieldInfo.getVectorDimension()];
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      float[][] centroids = getCentroids(mergeState.knnVectorsReaders[i], fieldInfo.name);
      if (centroids == null) {
        recalculate = true;
        break;
      }
      if (centroids.length > 1) {
        recalculate = true;
        break;
      }
      for (int j = 0; j < centroids[0].length; j++) {
        weightedCentroidAvg[j] +=
            (centroids[0][j]
                / mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name).size());
      }
    }
    if (recalculate) {
      return calculateCentroids(mergeState, fieldInfo);
    } else {
      return new float[][] {weightedCentroidAvg};
    }
  }

  static float[][] calculateCentroids(MergeState mergeState, FieldInfo fieldInfo)
      throws IOException {
    assert fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32);
    // TODO get KMeans to work over FloatVectorValues & support more than one cluster
    float[] sum = new float[fieldInfo.getVectorDimension()];
    int count = 0;
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      FloatVectorValues vectorValues =
          mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
      if (vectorValues == null) {
        continue;
      }
      for (int doc = vectorValues.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = vectorValues.nextDoc()) {
        float[] vector = vectorValues.vectorValue();
        for (int j = 0; j < vector.length; j++) {
          sum[j] += vector[j];
        }
        count++;
      }
    }
    if (count == 0) {
      return null;
    }
    for (int i = 0; i < sum.length; i++) {
      sum[i] /= count;
    }
    return new float[][] {sum};
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
    private final InfoStream infoStream;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;

    FieldWriter(
        FieldInfo fieldInfo,
        InfoStream infoStream,
        FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.infoStream = infoStream;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dimensionSums = new float[fieldInfo.getVectorDimension()];
    }

    @Override
    public List<float[]> getVectors() {
      return flatFieldVectorsWriter.getVectors();
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
      if (flatFieldVectorsWriter.getVectors().size() > 0) {
        for (int i = 0; i < dimensionSums.length; i++) {
          dimensionSums[i] /= flatFieldVectorsWriter.getVectors().size();
        }
      }
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      for (int i = 0; i < vectorValue.length; i++) {
        dimensionSums[i] += vectorValue[i];
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
      size += RamUsageEstimator.sizeOf(dimensionSums);
      return size;
    }

    BinaryQuantizer createQuantizer() throws IOException {
      assert isFinished();
      if (flatFieldVectorsWriter.getVectors().size() == 0) {
        return new BinaryQuantizer(new float[0]);
      }
      return new BinaryQuantizer(dimensionSums);
    }
  }

  // TODO this assumes a single centroid, we will need to adjust so that we can get the appropriate
  // query vector for each centroid, this means there will be num_centroid * num_docs quantized
  // values
  // but we will only need to access the quantized value for the centroid where the other vector
  // belongs
  static class OffHeapBinarizedQueryVectorValues
      implements RandomAccessBinarizedQueryByteVectorValues {
    private final IndexInput slice;
    private final int dimension;
    private final int size;
    protected final byte[] binaryValue;
    protected final ByteBuffer byteBuffer;
    private final int byteSize;
    // 0 centroid distance
    // 1 quantized value lower bound
    // 2 quantized value widths
    protected final float[] correctiveValues = new float[3];
    private int sumQuantizationValues;
    private int lastOrd = -1;

    OffHeapBinarizedQueryVectorValues(IndexInput data, int dimension, int size) {
      this.slice = data;
      this.dimension = dimension;
      this.size = size;
      // 4x the quantized binary dimensions
      int binaryDimensions = (dimension + 1) / 2;
      this.byteBuffer = ByteBuffer.allocate(binaryDimensions);
      this.binaryValue = byteBuffer.array();
      this.byteSize = binaryDimensions + Float.BYTES * 4;
    }

    @Override
    public float getCentroidDistance(int targetOrd, int centroidOrd) throws IOException {
      assert centroidOrd == 0;
      if (lastOrd == targetOrd) {
        return correctiveValues[0];
      }
      readCorrectiveValues(targetOrd, centroidOrd);
      return correctiveValues[0];
    }

    @Override
    public float getVl(int targetOrd, int centroidOrd) throws IOException {
      assert centroidOrd == 0;
      if (lastOrd == targetOrd) {
        return correctiveValues[1];
      }
      readCorrectiveValues(targetOrd, centroidOrd);
      return correctiveValues[1];
    }

    @Override
    public float getWidth(int targetOrd, int centroidOrd) throws IOException {
      assert centroidOrd == 0;
      if (lastOrd == targetOrd) {
        return correctiveValues[2];
      }
      readCorrectiveValues(targetOrd, centroidOrd);
      return correctiveValues[2];
    }

    private void readCorrectiveValues(int targetOrd, int centroidOrd) throws IOException {
      lastOrd = -1;
      slice.seek(((long) targetOrd * byteSize) + binaryValue.length);
      slice.readFloats(correctiveValues, 0, 3);
    }

    @Override
    public int sumQuantizedValues(int targetOrd, int centroidOrd) throws IOException {
      assert centroidOrd == 0;
      if (lastOrd == targetOrd) {
        return sumQuantizationValues;
      }
      lastOrd = -1;
      slice.seek(((long) targetOrd * byteSize) + binaryValue.length + Float.BYTES * 3);
      sumQuantizationValues = slice.readInt();
      return sumQuantizationValues;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public OffHeapBinarizedQueryVectorValues copy() throws IOException {
      return new OffHeapBinarizedQueryVectorValues(slice.clone(), dimension, size);
    }

    @Override
    public IndexInput getSlice() {
      return slice;
    }

    @Override
    public byte[] vectorValue(int targetOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return binaryValue;
      }
      slice.seek((long) targetOrd * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), binaryValue.length);
      slice.readFloats(correctiveValues, 0, 3);
      sumQuantizationValues = slice.readInt();
      lastOrd = targetOrd;
      return binaryValue;
    }

    @Override
    public int getVectorByteLength() {
      return binaryValue.length;
    }
  }

  static class BinarizedForQueryFloatVectorValues extends BinarizedByteVectorValues {
    private float[] corrections = new float[2];
    private final byte[] binarized;
    private final FloatVectorValues values;
    private final BinaryQuantizer quantizer;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    BinarizedForQueryFloatVectorValues(
        FloatVectorValues delegate,
        BinaryQuantizer quantizer,
        VectorSimilarityFunction similarityFunction) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.binarized = new byte[(delegate.dimension() + 1) / 2];
      this.vectorSimilarityFunction = similarityFunction;
    }

    @Override
    public float getDistanceToCentroid() throws IOException {
      return corrections[0];
    }

    @Override
    public byte clusterId() throws IOException {
      // TODO implement more than one cluster
      return 0;
    }

    @Override
    public float getMagnitude() throws IOException {
      return corrections[1];
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return binarized;
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
    public int docID() {
      return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = values.nextDoc();
      if (doc != NO_MORE_DOCS) {
        binarize();
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        binarize();
      }
      return doc;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void binarize() throws IOException {
      corrections =
          quantizer.quantizeForQuery(values.vectorValue(), binarized, vectorSimilarityFunction);
    }
  }

  static class BinarizedFloatVectorValues extends BinarizedByteVectorValues {
    private float[] corrections = new float[2];
    private final byte[] binarized;
    private final FloatVectorValues values;
    private final BinaryQuantizer quantizer;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    BinarizedFloatVectorValues(
        FloatVectorValues delegate,
        BinaryQuantizer quantizer,
        VectorSimilarityFunction similarityFunction) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.binarized = new byte[(delegate.dimension() + 7) / 8];
      this.vectorSimilarityFunction = similarityFunction;
    }

    @Override
    public float getDistanceToCentroid() throws IOException {
      return corrections[0];
    }

    @Override
    public byte clusterId() throws IOException {
      // TODO implement more than one cluster
      return 0;
    }

    @Override
    public float getMagnitude() throws IOException {
      return corrections[1];
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return binarized;
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
    public int docID() {
      return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = values.nextDoc();
      if (doc != NO_MORE_DOCS) {
        binarize();
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        binarize();
      }
      return doc;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void binarize() throws IOException {
      corrections =
          quantizer.quantizeForIndex(values.vectorValue(), binarized, vectorSimilarityFunction);
    }
  }

  static class BinarizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {
    private final RandomVectorScorerSupplier supplier;
    private final Closeable onClose;
    private final int numVectors;

    BinarizedCloseableRandomVectorScorerSupplier(
        RandomVectorScorerSupplier supplier, Closeable onClose, int numVectors) {
      this.supplier = supplier;
      this.onClose = onClose;
      this.numVectors = numVectors;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      return supplier.scorer(ord);
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
      return numVectors;
    }
  }
}
