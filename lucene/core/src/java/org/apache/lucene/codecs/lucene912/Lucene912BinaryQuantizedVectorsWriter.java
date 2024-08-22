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
import static org.apache.lucene.util.quantization.KMeans.DEFAULT_ITRS;
import static org.apache.lucene.util.quantization.KMeans.DEFAULT_RESTARTS;
import static org.apache.lucene.util.quantization.KMeans.DEFAULT_SAMPLE_SIZE;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.BinaryQuantizer;
import org.apache.lucene.util.quantization.KMeans;

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
      final float[][] clusters;
      if (field.flatFieldVectorsWriter.getVectors().size() > numberOfVectorsPerCluster) {
        // Use kmeans
        assert false;
        clusters = new float[0][0];
      } else {
        clusters = new float[1][];
        clusters[0] = field.dimensionSums;
      }
      BinaryQuantizer quantizer =
          new BinaryQuantizer(
              field.fieldInfo.getVectorDimension(), field.fieldInfo.getVectorSimilarityFunction());
      if (sortMap == null) {
        writeField(field, clusters, maxDoc, quantizer);
      } else {
        writeSortingField(field, clusters, maxDoc, sortMap, quantizer);
      }
      field.finish();
    }
  }

  private void writeField(
      FieldWriter fieldData, float[][] clusters, int maxDoc, BinaryQuantizer quantizer)
      throws IOException {
    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeBinarizedVectors(fieldData, clusters, quantizer);
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
      FieldWriter fieldData, float[][] clusters, BinaryQuantizer scalarQuantizer)
      throws IOException {
    assert clusters.length == 1;
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 7) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?
    for (float[] v : fieldData.getVectors()) {
      float[] corrections = scalarQuantizer.quantizeForIndex(v, vector, clusters[0]);
      binarizedVectorData.writeBytes(vector, vector.length);
      if (clusters.length > 1) {
        // binarizedVectorData.writeByte(clusterId);
      }
      correctionsBuffer.putFloat(corrections[0]);
      correctionsBuffer.putFloat(corrections[1]);
      binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
      correctionsBuffer.rewind();
    }
  }

  private void writeSortingField(
      FieldWriter fieldData,
      float[][] clusters,
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
    writeSortedBinarizedVectors(fieldData, clusters, ordMap, scalarQuantizer);
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
      FieldWriter fieldData, float[][] clusters, int[] ordMap, BinaryQuantizer scalarQuantizer)
      throws IOException {
    assert clusters.length == 1;
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 7) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?
    for (int ordinal : ordMap) {
      float[] v = fieldData.getVectors().get(ordinal);
      float[] corrections = scalarQuantizer.quantizeForIndex(v, vector, clusters[0]);
      binarizedVectorData.writeBytes(vector, vector.length);
      if (clusters.length > 1) {
        // binarizedVectorData.writeByte(clusterId);
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
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      final float[][] centroids;
      final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
      double vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
      // If we have more vectors than allowed for a single cluster, we will use KMeans to cluster
      if (vectorCount > numberOfVectorsPerCluster) {
        try (CloseableRandomVectorScorerSupplier vectorScorerSupplier =
            rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState)) {
          assert vectorScorerSupplier.vectors() instanceof RandomAccessVectorValues.Floats;
          // we assume floats here as that is what KMeans currently requires
          RandomAccessVectorValues.Floats vectorValues =
              (RandomAccessVectorValues.Floats) vectorScorerSupplier.vectors();
          KMeans.Results kmeansResult =
              KMeans.cluster(
                  vectorValues,
                  (int) (vectorCount / numberOfVectorsPerCluster + 1),
                  false,
                  42,
                  KMeans.KmeansInitializationMethod.PLUS_PLUS,
                  false,
                  DEFAULT_RESTARTS,
                  DEFAULT_ITRS,
                  (int) Math.min(DEFAULT_SAMPLE_SIZE, vectorCount * 0.01));
          assert kmeansResult.centroids() != null && kmeansResult.centroids().length > 1;
          centroids = kmeansResult.centroids();
        }
      } else {
        // Don't need access to the random vectors, we can just use the merged
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        centroids = new float[][] {mergedCentroid};
      }
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              new BinaryQuantizer(
                  fieldInfo.getVectorDimension(), fieldInfo.getVectorSimilarityFunction()),
              centroids);
      long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(
              binarizedVectorData, binarizedVectorValues, centroids.length > 1);
      long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          centroids,
          docsWithField);
    } else {
      rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    }
  }

  static void writeQueryBinarizedVectorData(
      IndexOutput output,
      BinaryQuantizer quantizer,
      FloatVectorValues floatVectorValues,
      float[][] centroids)
      throws IOException {
    byte[] vector = new byte[(floatVectorValues.dimension() + 1) / 2];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 2).order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      float[] floatVector = floatVectorValues.vectorValue();
      for (int i = 0; i < centroids.length; i++) {
        float[] corrections = quantizer.quantizeForQuery(floatVector, vector, centroids[i]);
        output.writeBytes(vector, vector.length);
        correctionsBuffer.putFloat(corrections[0]);
        correctionsBuffer.putFloat(corrections[1]);
        output.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
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
      // TODO handle quantization output correctly
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
      final float[][] centroids;
      final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
      double vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
      // If we have more vectors than allowed for a single cluster, we will use KMeans to cluster
      if (vectorCount > numberOfVectorsPerCluster) {
        try (CloseableRandomVectorScorerSupplier vectorScorerSupplier =
            rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState)) {
          assert vectorScorerSupplier.vectors() instanceof RandomAccessVectorValues.Floats;
          // we assume floats here as that is what KMeans currently requires
          RandomAccessVectorValues.Floats vectorValues =
              (RandomAccessVectorValues.Floats) vectorScorerSupplier.vectors();
          KMeans.Results kmeansResult =
              KMeans.cluster(
                  vectorValues,
                  (int) (vectorCount / numberOfVectorsPerCluster + 1),
                  false,
                  42,
                  KMeans.KmeansInitializationMethod.PLUS_PLUS,
                  false,
                  DEFAULT_RESTARTS,
                  DEFAULT_ITRS,
                  (int) Math.min(DEFAULT_SAMPLE_SIZE, vectorCount * 0.01));
          assert kmeansResult.centroids() != null && kmeansResult.centroids().length > 1;
          centroids = kmeansResult.centroids();
        }
      } else {
        // Don't need access to the random vectors, we can just use the merged
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        centroids = new float[][] {mergedCentroid};
      }
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
    BinaryQuantizer quantizer =
        new BinaryQuantizer(
            fieldInfo.getVectorDimension(), fieldInfo.getVectorSimilarityFunction());
    try {
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              quantizer,
              centroids);
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(
              tempQuantizedVectorData, binarizedVectorValues, centroids.length > 1);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      binarizedDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      binarizedVectorData.copyBytes(
          binarizedDataInput, binarizedDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = binarizedDataInput.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(binarizedDataInput);
      writeQueryBinarizedVectorData(
          tempScoreQuantizedVectorData,
          quantizer,
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
          centroids);
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
      OffHeapBinarizedVectorValues vectorValues =
          new OffHeapBinarizedVectorValues.DenseOffHeapVectorValues(
              fieldInfo.getVectorDimension(),
              docsWithField.cardinality(),
              centroids,
              quantizer,
              fieldInfo.getVectorSimilarityFunction(),
              vectorsScorer,
              finalBinarizedDataInput);
      RandomVectorScorerSupplier scorerSupplier =
          vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapBinarizedQueryVectorValues(
                  finalBinarizedScoreDataInput,
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality()),
              vectorValues);
      return new BinarizedCloseableRandomVectorScorerSupplier(
          scorerSupplier,
          vectorValues,
          () -> {
            IOUtils.close(finalBinarizedDataInput, finalBinarizedScoreDataInput);
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory,
                tempQuantizedVectorData.getName(),
                tempScoreQuantizedVectorData.getName());
          });
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
      float[][] centroids = getCentroids(knnVectorsReader, fieldInfo.name);
      int vectorCount = knnVectorsReader.getFloatVectorValues(fieldInfo.name).size();
      totalVectorCount += vectorCount;
      // If there aren't centroids, or previously clustered with more than one cluster
      // or if there are deleted docs, we must recalculate the centroid
      if (centroids == null || centroids.length > 1 || mergeState.liveDocs[i] != null) {
        recalculate = true;
        break;
      }
      for (int j = 0; j < centroids[0].length; j++) {
        mergedCentroid[j] += centroids[0][j] * vectorCount;
      }
    }
    if (recalculate) {
      return calculateCentroid(mergeState, fieldInfo, mergedCentroid);
    } else {
      for (int j = 0; j < mergedCentroid.length; j++) {
        mergedCentroid[j] += mergedCentroid[j] / totalVectorCount;
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
      FloatVectorValues vectorValues =
          mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
      if (vectorValues == null) {
        continue;
      }
      for (int doc = vectorValues.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = vectorValues.nextDoc()) {
        float[] vector = vectorValues.vectorValue();
        // TODO Panama sum
        for (int j = 0; j < vector.length; j++) {
          centroid[j] += vector[j];
        }
      }
      count += vectorValues.size();
    }
    if (count == 0) {
      return count;
    }
    // TODO Panama div
    for (int i = 0; i < centroid.length; i++) {
      centroid[i] /= count;
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
    // private final InfoStream infoStream;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;

    FieldWriter(
        FieldInfo fieldInfo,
        InfoStream infoStream,
        FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      // this.infoStream = infoStream;
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

  static class BinarizedFloatVectorValues extends BinarizedByteVectorValues {
    private float[] corrections = new float[2];
    private final byte[] binarized;
    private final float[][] centroids;
    private final FloatVectorValues values;
    private final BinaryQuantizer quantizer;
    private int centroidId;

    BinarizedFloatVectorValues(
        FloatVectorValues delegate, BinaryQuantizer quantizer, float[][] centroids) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.binarized = new byte[(delegate.dimension() + 7) / 8];
      this.centroids = centroids;
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
      if (centroids.length > 1) {
        float[] values = this.values.vectorValue();
        int nearestCentroid = 0;
        float nearestScore = Float.NEGATIVE_INFINITY;
        for (int i = 1; i < centroids.length; i++) {
          float score = VectorSimilarityFunction.EUCLIDEAN.compare(values, centroids[i]);
          if (score > nearestScore) {
            nearestScore = score;
            nearestCentroid = i;
          }
        }
        assert nearestCentroid >= 0 && nearestCentroid < centroids.length;
        centroidId = nearestCentroid;
      } else {
        centroidId = 0;
      }
      corrections =
          quantizer.quantizeForIndex(values.vectorValue(), binarized, centroids[centroidId]);
    }
  }

  static class BinarizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {
    private final RandomVectorScorerSupplier supplier;
    private final RandomAccessVectorValues vectorValues;
    private final Closeable onClose;

    BinarizedCloseableRandomVectorScorerSupplier(
        RandomVectorScorerSupplier supplier,
        RandomAccessVectorValues vectorValues,
        Closeable onClose) {
      this.supplier = supplier;
      this.onClose = onClose;
      this.vectorValues = vectorValues;
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
      return vectorValues.size();
    }

    @Override
    public RandomAccessVectorValues vectors() {
      return vectorValues;
    }
  }
}
