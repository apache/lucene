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

import static org.apache.lucene.codecs.lucene912.Lucene912BinaryQuantizedVectorsFormat.BINARIZED_VECTOR_COMPONENT;
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
import org.apache.lucene.util.quantization.BQSpaceUtils;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;
import org.apache.lucene.util.quantization.KMeans;

/**
 * Writes raw and binarized vector values to index segments for KNN search.
 *
 * @lucene.experimental
 */
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
      final float[][] clusterCenters;
      short[] vectorClusters = null;
      int vectorCount = field.flatFieldVectorsWriter.getVectors().size();
      if (vectorCount > numberOfVectorsPerCluster) {
        RandomAccessVectorValues.Floats vectorValues =
            RandomAccessVectorValues.fromFloats(
                field.flatFieldVectorsWriter.getVectors(), field.fieldInfo.getVectorDimension());
        KMeans.Results kmeansResult = cluster(vectorValues, true);
        clusterCenters = kmeansResult.centroids();
        vectorClusters = kmeansResult.vectorCentroids();
      } else {
        clusterCenters = new float[1][field.dimensionSums.length];
        for (int i = 0; i < field.dimensionSums.length; i++) {
          clusterCenters[0][i] = field.dimensionSums[i] / vectorCount;
        }
      }
      if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
        segmentWriteState.infoStream.message(
            BINARIZED_VECTOR_COMPONENT,
            "Vectors' count:" + vectorCount + "; clusters' count:" + clusterCenters.length);
      }
      int descritizedDimension = BQVectorUtils.discretize(field.fieldInfo.getVectorDimension(), 64);
      BinaryQuantizer quantizer =
          new BinaryQuantizer(descritizedDimension, field.fieldInfo.getVectorSimilarityFunction());
      if (sortMap == null) {
        writeField(field, clusterCenters, vectorClusters, maxDoc, quantizer);
      } else {
        writeSortingField(field, clusterCenters, vectorClusters, maxDoc, sortMap, quantizer);
      }
      field.finish();
    }
  }

  private void writeField(
      FieldWriter fieldData,
      float[][] clusterCenters,
      short[] vectorClusters,
      int maxDoc,
      BinaryQuantizer quantizer)
      throws IOException {
    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeBinarizedVectors(fieldData, clusterCenters, vectorClusters, quantizer);
    long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        clusterCenters,
        fieldData.getDocsWithFieldSet());
  }

  private void writeBinarizedVectors(
      FieldWriter fieldData,
      float[][] clusterCenters,
      short[] vectorClusters,
      BinaryQuantizer scalarQuantizer)
      throws IOException {
    byte[] vector =
        new byte[BQVectorUtils.discretize(fieldData.fieldInfo.getVectorDimension(), 64) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 3).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?
    if (clusterCenters.length > 1) {
      int cnt = 0;
      short clusterId;
      byte bClusterId;
      for (float[] v : fieldData.getVectors()) {
        clusterId = vectorClusters[cnt++];
        float[] corrections =
            scalarQuantizer.quantizeForIndex(v, vector, clusterCenters[clusterId]);
        binarizedVectorData.writeBytes(vector, vector.length);
        bClusterId = BinarizedByteVectorValues.encodeClusterIdToByte(clusterId);
        binarizedVectorData.writeByte(bClusterId);
        // FIXME: handle of sim types like MIP such as COSINE?
        if (scalarQuantizer.getSimilarity() == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(corrections[2]);
        } else {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(0f);
        }
        binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
    } else {
      float[] clusterCenter = clusterCenters[0];
      for (float[] v : fieldData.getVectors()) {
        float[] corrections = scalarQuantizer.quantizeForIndex(v, vector, clusterCenter);
        binarizedVectorData.writeBytes(vector, vector.length);
        // FIXME: handle of sim types like MIP such as COSINE?
        if (scalarQuantizer.getSimilarity() == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(corrections[2]);
        } else {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(0f);
        }
        binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
    }
  }

  private void writeSortingField(
      FieldWriter fieldData,
      float[][] clusterCenters,
      short[] vectorClusters,
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
    writeSortedBinarizedVectors(fieldData, clusterCenters, vectorClusters, ordMap, scalarQuantizer);
    long quantizedVectorLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        quantizedVectorLength,
        clusterCenters,
        newDocsWithField);
  }

  private void writeSortedBinarizedVectors(
      FieldWriter fieldData,
      float[][] clusterCenters,
      short[] vectorClusters,
      int[] ordMap,
      BinaryQuantizer scalarQuantizer)
      throws IOException {
    byte[] vector =
        new byte[BQVectorUtils.discretize(fieldData.fieldInfo.getVectorDimension(), 64) / 8];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 3).order(ByteOrder.LITTLE_ENDIAN);
    // TODO do we need to normalize for cosine?

    if (clusterCenters.length > 1) {
      short clusterId;
      byte bClusterId;
      for (int ordinal : ordMap) {
        float[] v = fieldData.getVectors().get(ordinal);
        clusterId = vectorClusters[ordinal];
        float[] corrections =
            scalarQuantizer.quantizeForIndex(v, vector, clusterCenters[clusterId]);
        binarizedVectorData.writeBytes(vector, vector.length);
        bClusterId = BinarizedByteVectorValues.encodeClusterIdToByte(clusterId);
        binarizedVectorData.writeByte(bClusterId);
        // FIXME: handle of sim types like MIP such as COSINE?
        if (scalarQuantizer.getSimilarity() == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(corrections[2]);
        } else {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(0f);
        }
        binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
    } else {
      float[] clusterCenter = clusterCenters[0];
      for (int ordinal : ordMap) {
        float[] v = fieldData.getVectors().get(ordinal);
        float[] corrections = scalarQuantizer.quantizeForIndex(v, vector, clusterCenter);
        binarizedVectorData.writeBytes(vector, vector.length);
        // FIXME: handle of sim types like MIP such as COSINE?
        if (scalarQuantizer.getSimilarity() == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(corrections[2]);
        } else {
          correctionsBuffer.putFloat(corrections[0]);
          correctionsBuffer.putFloat(corrections[1]);
          correctionsBuffer.putFloat(0f);
        }
        binarizedVectorData.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
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
      int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
      // If we have more vectors than allowed for a single cluster, we will use KMeans to cluster
      if (vectorCount > numberOfVectorsPerCluster) {
        try (CloseableRandomVectorScorerSupplier vectorScorerSupplier =
            rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState)) {
          assert vectorScorerSupplier.vectors() instanceof RandomAccessVectorValues.Floats;
          // we assume floats here as that is what KMeans currently requires
          RandomAccessVectorValues.Floats vectorValues =
              (RandomAccessVectorValues.Floats) vectorScorerSupplier.vectors();
          KMeans.Results kmeansResult = cluster(vectorValues, false);
          assert kmeansResult.centroids() != null && kmeansResult.centroids().length > 1;
          centroids = kmeansResult.centroids();
        }
      } else {
        // Don't need access to the random vectors, we can just use the merged
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        centroids = new float[][] {mergedCentroid};
      }
      if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
        segmentWriteState.infoStream.message(
            BINARIZED_VECTOR_COMPONENT,
            "Vectors' count:" + vectorCount + "; clusters' count:" + centroids.length);
      }
      int descritizedDimension = BQVectorUtils.discretize(fieldInfo.getVectorDimension(), 64);
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              new BinaryQuantizer(descritizedDimension, fieldInfo.getVectorSimilarityFunction()),
              centroids);
      long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(
              binarizedVectorData,
              binarizedVectorValues,
              centroids.length > 1,
              fieldInfo.getVectorSimilarityFunction());
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
    byte[] vector =
        new byte
            [(BQVectorUtils.discretize(floatVectorValues.dimension(), 64) / 8)
                * BQSpaceUtils.B_QUERY];
    final ByteBuffer correctionsBuffer =
        ByteBuffer.allocate(Float.BYTES * 6 + Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      float[] floatVector = floatVectorValues.vectorValue();
      for (int i = 0; i < centroids.length; i++) {
        BinaryQuantizer.QueryFactors factors =
            quantizer.quantizeForQuery(floatVector, vector, centroids[i]);
        output.writeBytes(vector, vector.length);

        correctionsBuffer.putFloat(factors.distToC());
        correctionsBuffer.putFloat(factors.lower());
        correctionsBuffer.putFloat(factors.width());

        // FIXME: handle other similarity types here like COSINE
        if (quantizer.getSimilarity() == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
          correctionsBuffer.putFloat(factors.normVmC());
          correctionsBuffer.putFloat(factors.vDotC());
          correctionsBuffer.putFloat(factors.cDotC());
        } else {
          correctionsBuffer.putFloat(0f);
          correctionsBuffer.putFloat(0f);
          correctionsBuffer.putFloat(0f);
        }
        // ensure we are positive and fit within an unsigned short value.
        assert factors.quantizedSum() >= 0 && factors.quantizedSum() <= 0xffff;
        correctionsBuffer.putShort((short) factors.quantizedSum());

        output.writeBytes(correctionsBuffer.array(), correctionsBuffer.array().length);
        correctionsBuffer.rewind();
      }
    }
  }

  static DocsWithFieldSet writeBinarizedVectorData(
      IndexOutput output,
      BinarizedByteVectorValues binarizedByteVectorValues,
      boolean moreThanOneCluster,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = binarizedByteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = binarizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = binarizedByteVectorValues.vectorValue();
      output.writeBytes(binaryValue, binaryValue.length);
      if (moreThanOneCluster) {
        output.writeByte(
            BinarizedByteVectorValues.encodeClusterIdToByte(binarizedByteVectorValues.clusterId()));
      }
      // FIXME: handle other similarity functions the same as MIP such as COSINE
      // TODO handle quantization output correctly
      if (similarityFunction == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
        output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getOOQ()));
        output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getNormOC()));
        output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getODotC()));
      } else {
        output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getDistanceToCentroid()));
        output.writeInt(Float.floatToIntBits(binarizedByteVectorValues.getMagnitude()));
        output.writeInt(0);
      }
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
      int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
      // If we have more vectors than allowed for a single cluster, we will use KMeans to cluster
      if (vectorCount > numberOfVectorsPerCluster) {
        try (CloseableRandomVectorScorerSupplier vectorScorerSupplier =
            rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState)) {
          assert vectorScorerSupplier.vectors() instanceof RandomAccessVectorValues.Floats;
          // we assume floats here as that is what KMeans currently requires
          RandomAccessVectorValues.Floats vectorValues =
              (RandomAccessVectorValues.Floats) vectorScorerSupplier.vectors();
          KMeans.Results kmeansResult = cluster(vectorValues, false);
          assert kmeansResult.centroids() != null && kmeansResult.centroids().length > 1;
          centroids = kmeansResult.centroids();
        }
      } else {
        // Don't need access to the random vectors, we can just use the merged
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
        centroids = new float[][] {mergedCentroid};
      }
      if (segmentWriteState.infoStream.isEnabled(BINARIZED_VECTOR_COMPONENT)) {
        segmentWriteState.infoStream.message(
            BINARIZED_VECTOR_COMPONENT,
            "Vectors' count:" + vectorCount + "; clusters' count:" + centroids.length);
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
    int descritizedDimension = BQVectorUtils.discretize(fieldInfo.getVectorDimension(), 64);
    BinaryQuantizer quantizer =
        new BinaryQuantizer(descritizedDimension, fieldInfo.getVectorSimilarityFunction());
    try {
      BinarizedFloatVectorValues binarizedVectorValues =
          new BinarizedFloatVectorValues(
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
              quantizer,
              centroids);
      DocsWithFieldSet docsWithField =
          writeBinarizedVectorData(
              tempQuantizedVectorData,
              binarizedVectorValues,
              centroids.length > 1,
              fieldInfo.getVectorSimilarityFunction());
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      binarizedDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      binarizedVectorData.copyBytes(
          binarizedDataInput, binarizedDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
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
                  docsWithField.cardinality(),
                  centroids.length),
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
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null) continue;
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

  private KMeans.Results cluster(
      RandomAccessVectorValues.Floats vectorValues, boolean assignCentroidsToVectors)
      throws IOException {
    return KMeans.cluster(
        vectorValues,
        Math.max(1, vectorValues.size() / numberOfVectorsPerCluster),
        assignCentroidsToVectors,
        42,
        KMeans.KmeansInitializationMethod.FORGY,
        false,
        DEFAULT_RESTARTS,
        DEFAULT_ITRS,
        DEFAULT_SAMPLE_SIZE);
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
    private final int numCentroids;
    protected final byte[][] binaryValue;
    protected final ByteBuffer byteBuffer;
    private final int byteSize;
    // 0 centroid distance
    // 1 quantized value lower bound
    // 2 quantized value widths
    // 3 normVmc
    // 4 vDotC
    // 5 cDotC
    protected final float[][] correctiveValues;
    private int[] sumQuantizationValues;
    private int lastOrd = -1;

    OffHeapBinarizedQueryVectorValues(IndexInput data, int dimension, int size, int numCentroids) {
      this.slice = data;
      this.dimension = dimension;
      this.size = size;
      this.numCentroids = numCentroids;
      // 4x the quantized binary dimensions
      int binaryDimensions = (BQVectorUtils.discretize(dimension, 64) / 8) * BQSpaceUtils.B_QUERY;
      this.byteBuffer = ByteBuffer.allocate(binaryDimensions);
      if (numCentroids == 1) {
        this.binaryValue = new byte[][] {byteBuffer.array()};
      } else {
        this.binaryValue = new byte[numCentroids][binaryDimensions];
      }
      this.sumQuantizationValues = new int[numCentroids];
      this.correctiveValues = new float[numCentroids][6];
      this.byteSize = (binaryDimensions + Float.BYTES * 6 + Short.BYTES) * numCentroids;
    }

    @Override
    public float getCentroidDistance(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][0];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][0];
    }

    @Override
    public float getLower(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][1];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][1];
    }

    @Override
    public float getWidth(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][2];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][2];
    }

    @Override
    public float getNormVmC(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][3];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][3];
    }

    @Override
    public float getVDotC(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][4];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][4];
    }

    @Override
    public float getCDotC(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return correctiveValues[centroidOrd][5];
      }
      readCorrectiveValues(targetOrd);
      return correctiveValues[centroidOrd][5];
    }

    private void readCorrectiveValues(int targetOrd) throws IOException {
      // load values
      vectorValue(targetOrd, 0);
    }

    @Override
    public int sumQuantizedValues(int targetOrd, int centroidOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return sumQuantizationValues[centroidOrd];
      }
      // load values
      // todo improve
      vectorValue(targetOrd, centroidOrd);
      return sumQuantizationValues[centroidOrd];
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int getNumCentroids() {
      return numCentroids;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public OffHeapBinarizedQueryVectorValues copy() throws IOException {
      return new OffHeapBinarizedQueryVectorValues(slice.clone(), dimension, size, numCentroids);
    }

    public IndexInput getSlice() {
      return slice;
    }

    @Override
    public byte[] vectorValue(int targetOrd, int centroid) throws IOException {
      if (lastOrd == targetOrd) {
        return binaryValue[centroid];
      }
      slice.seek((long) targetOrd * byteSize);
      for (int i = 0; i < numCentroids; i++) {
        slice.readBytes(binaryValue[i], 0, binaryValue[i].length);
        slice.readFloats(correctiveValues[i], 0, 6);
        sumQuantizationValues[i] = Short.toUnsignedInt(slice.readShort());
      }
      lastOrd = targetOrd;
      return binaryValue[centroid];
    }
  }

  static class BinarizedFloatVectorValues extends BinarizedByteVectorValues {
    private float[] corrections = new float[3];
    private final byte[] binarized;
    private final float[][] centroids;
    private final FloatVectorValues values;
    private final BinaryQuantizer quantizer;
    private int lastDoc;
    private short clusterId = 0;

    BinarizedFloatVectorValues(
        FloatVectorValues delegate, BinaryQuantizer quantizer, float[][] centroids) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.binarized = new byte[BQVectorUtils.discretize(delegate.dimension(), 64) / 8];
      this.centroids = centroids;
      lastDoc = -1;
    }

    @Override
    public short clusterId() {
      return clusterId;
    }

    @Override
    public float getDistanceToCentroid() {
      return corrections[0];
    }

    @Override
    public float getMagnitude() {
      return corrections[1];
    }

    @Override
    public float getOOQ() {
      return corrections[0];
    }

    @Override
    public float getNormOC() {
      return corrections[1];
    }

    @Override
    public float getODotC() {
      return corrections[2];
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
      lastDoc = doc;
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        binarize();
      }
      lastDoc = doc;
      return doc;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void binarize() throws IOException {
      if (lastDoc == docID()) return;
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
        clusterId = (short) nearestCentroid;
      }
      corrections =
          quantizer.quantizeForIndex(values.vectorValue(), binarized, centroids[clusterId]);
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
