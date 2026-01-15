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
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.packAsBinary;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;

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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

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
  private final Lucene104ScalarQuantizedVectorScorer vectorsScorer;
  private boolean finished;

  /** Sole constructor */
  public Lucene104ScalarQuantizedVectorsWriter(
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
    VectorEncoding vectorEncoding = fieldInfo.getVectorEncoding();
    if (vectorEncoding == VectorEncoding.FLOAT32 || vectorEncoding == VectorEncoding.FLOAT16) {
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
      // after raw vectors are written, normalize vectors for clustering and quantization
      if (COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
        field.normalizeVectors();
      }
      final float[] clusterCenter;
      int vectorCount = field.flatFieldVectorsWriter.getVectors().size();
      clusterCenter = new float[field.dimensionSums.length];
      if (vectorCount > 0) {
        for (int i = 0; i < field.dimensionSums.length; i++) {
          clusterCenter[i] = field.dimensionSums[i] / vectorCount;
        }
        if (COSINE == field.fieldInfo.getVectorSimilarityFunction()) {
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
      FieldWriter<?> fieldData, float[] clusterCenter, int maxDoc, OptimizedScalarQuantizer quantizer)
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
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };
    for (int i = 0; i < fieldData.getVectors().size(); i++) {
      OptimizedScalarQuantizer.QuantizationResult corrections = null;
      if (fieldData.fieldInfo.getVectorEncoding()  == VectorEncoding.FLOAT32) {
        corrections = scalarQuantizer.scalarQuantize((float[]) fieldData.getVectors().get(i), scratch, encoding.getBits(), clusterCenter);
      } else if (fieldData.fieldInfo.getVectorEncoding()  == VectorEncoding.FLOAT16) {
        corrections = scalarQuantizer.scalarQuantize((short[]) fieldData.getVectors().get(i), scratch, encoding.getBits(), clusterCenter);
      }
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, vector);
        case SINGLE_BIT_QUERY_NIBBLE -> packAsBinary(scratch, vector);
        case UNSIGNED_BYTE, SEVEN_BIT -> {}
      }
      vectorData.writeBytes(vector, vector.length);
      vectorData.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.upperInterval()));
      vectorData.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
      vectorData.writeInt(corrections.quantizedComponentSum());
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
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };
    for (int ordinal : ordMap) {
      OptimizedScalarQuantizer.QuantizationResult corrections = null;
      if (fieldData.fieldInfo.getVectorEncoding()  == VectorEncoding.FLOAT32) {
            corrections = scalarQuantizer.scalarQuantize((float[]) fieldData.getVectors().get(ordinal), scratch, encoding.getBits(), clusterCenter);
      } else if (fieldData.fieldInfo.getVectorEncoding()  == VectorEncoding.FLOAT16) {
            corrections = scalarQuantizer.scalarQuantize((short[]) fieldData.getVectors().get(ordinal), scratch, encoding.getBits(), clusterCenter);
      }
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, vector);
        case SINGLE_BIT_QUERY_NIBBLE -> packAsBinary(scratch, vector);
        case UNSIGNED_BYTE, SEVEN_BIT -> {}
      }
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
    VectorEncoding vectorEncoding = fieldInfo.getVectorEncoding();
    if (vectorEncoding != VectorEncoding.FLOAT32 && vectorEncoding != VectorEncoding.FLOAT16) {
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
    QuantizedByteVectorValues quantizedVectorValues = null;
    if (vectorEncoding == VectorEncoding.FLOAT32) {
      FloatVectorValues vectorValues =
          MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        vectorValues = new NormalizedFloatVectorValues(vectorValues);
      }
      quantizedVectorValues =
          new QuantizedFloatVectorValues(
              vectorValues,
              new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction()),
              encoding,
              centroid);
    } else if(vectorEncoding == VectorEncoding.FLOAT16) {
      Float16VectorValues vectorValues =
          MergedVectorValues.mergeFloat16VectorValues(fieldInfo, mergeState);
      if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
        vectorValues = new NormalizedFloat16VectorValues(vectorValues);
      }
      quantizedVectorValues =
          new QuantizedFloat16VectorValues(
              vectorValues,
              new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction()),
              encoding,
              centroid);
    }

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

  static DocsWithFieldSet writeBinarizedVectorAndQueryData(
      IndexOutput binarizedVectorData,
      ScalarEncoding encoding,
      IndexOutput binarizedQueryData,
      KnnVectorValues knnVectorValues,
      float[] centroid,
      OptimizedScalarQuantizer binaryQuantizer,
      VectorEncoding vectorEncoding)
      throws IOException {
    if (encoding.isAsymmetric() == false) {
      throw new IllegalArgumentException("encoding and queryEncoding must be different");
    }
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    int discretizedDims = encoding.getDiscreteDimensions(knnVectorValues.dimension());
    byte[][] quantizationScratch = new byte[2][];
    quantizationScratch[0] = new byte[discretizedDims];
    quantizationScratch[1] = new byte[discretizedDims];
    byte[] toIndex = new byte[encoding.getDocPackedLength(discretizedDims)];
    byte[] toQuery = new byte[encoding.getQueryPackedLength(discretizedDims)];
    KnnVectorValues.DocIndexIterator iterator = knnVectorValues.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      // write index vecto
      OptimizedScalarQuantizer.QuantizationResult[] r = null;
      if (vectorEncoding == VectorEncoding.FLOAT32) {
        r = binaryQuantizer.multiScalarQuantize(
              ((FloatVectorValues) knnVectorValues).vectorValue(iterator.index()),
              quantizationScratch,
              new byte[] {encoding.getBits(), encoding.getQueryBits()},
              centroid);
      } else if (vectorEncoding == VectorEncoding.FLOAT16) {
         r = binaryQuantizer.multiScalarQuantize(
                ((Float16VectorValues) knnVectorValues).vectorValue(iterator.index()),
                quantizationScratch,
                new byte[] {encoding.getBits(), encoding.getQueryBits()},
                centroid);
      }
      // pack and store document bit vector
      packAsBinary(quantizationScratch[0], toIndex);
      binarizedVectorData.writeBytes(toIndex, toIndex.length);
      binarizedVectorData.writeInt(Float.floatToIntBits(r[0].lowerInterval()));
      binarizedVectorData.writeInt(Float.floatToIntBits(r[0].upperInterval()));
      binarizedVectorData.writeInt(Float.floatToIntBits(r[0].additionalCorrection()));
      binarizedVectorData.writeInt(r[0].quantizedComponentSum());
      docsWithField.add(docV);

      // pack and store the 4bit query vector
      transposeHalfByte(quantizationScratch[1], toQuery);
      binarizedQueryData.writeBytes(toQuery, toQuery.length);
      binarizedQueryData.writeInt(Float.floatToIntBits(r[1].lowerInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r[1].upperInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r[1].additionalCorrection()));
      binarizedQueryData.writeInt(r[1].quantizedComponentSum());
    }
    return docsWithField;
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    VectorEncoding vectorEncoding = fieldInfo.getVectorEncoding();
    if (vectorEncoding != VectorEncoding.FLOAT32 && vectorEncoding != VectorEncoding.FLOAT16) {
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
    IndexOutput tempScoreQuantizedVectorData = null;
    IndexInput quantizedDataInput = null;
    IndexInput quantizedScoreDataInput = null;
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    try {
      tempQuantizedVectorData =
          segmentWriteState.directory.createTempOutput(
              vectorData.getName(), "temp", segmentWriteState.context);
      final String tempQuantizedVectorName = tempQuantizedVectorData.getName();
      final String tempScoreQuantizedVectorName;
      if (encoding.isAsymmetric()) {
        tempScoreQuantizedVectorData =
            segmentWriteState.directory.createTempOutput(
                vectorData.getName() + "_score", "temp", segmentWriteState.context);
        tempScoreQuantizedVectorName = tempScoreQuantizedVectorData.getName();
      } else {
        tempScoreQuantizedVectorName = null;
      }
      DocsWithFieldSet docsWithField = null;
      if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
        FloatVectorValues floatVectorValues =
            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
          floatVectorValues = new NormalizedFloatVectorValues(floatVectorValues);
        }
          docsWithField =
            encoding.isAsymmetric()
                ? writeBinarizedVectorAndQueryData(
                    tempQuantizedVectorData,
                    encoding,
                    tempScoreQuantizedVectorData,
                    floatVectorValues,
                    centroid,
                    quantizer, fieldInfo.getVectorEncoding())
                : writeVectorData(
                    tempQuantizedVectorData,
                    new QuantizedFloatVectorValues(floatVectorValues, quantizer, encoding, centroid));
      } else if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT16) {
        Float16VectorValues float16VectorValues =
            MergedVectorValues.mergeFloat16VectorValues(fieldInfo, mergeState);
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
          float16VectorValues = new NormalizedFloat16VectorValues(float16VectorValues);
        }
        docsWithField =
            encoding.isAsymmetric()
                ? writeBinarizedVectorAndQueryData(
                tempQuantizedVectorData,
                encoding,
                tempScoreQuantizedVectorData,
                float16VectorValues,
                centroid,
                quantizer, fieldInfo.getVectorEncoding())
                : writeVectorData(
                    tempQuantizedVectorData,
                    new QuantizedFloat16VectorValues(float16VectorValues, quantizer, encoding, centroid));
      }
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      quantizedDataInput =
          segmentWriteState.directory.openInput(tempQuantizedVectorName, segmentWriteState.context);
      vectorData.copyBytes(
          quantizedDataInput, quantizedDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(quantizedDataInput);
      if (tempScoreQuantizedVectorData != null) {
        CodecUtil.writeFooter(tempScoreQuantizedVectorData);
        IOUtils.close(tempScoreQuantizedVectorData);
        quantizedScoreDataInput =
            segmentWriteState.directory.openInput(
                tempScoreQuantizedVectorName, segmentWriteState.context);
      }
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          centroid,
          cDotC,
          docsWithField);

      final IndexInput finalQuantizedDataInput = quantizedDataInput;
      final IndexInput finalQuantizedScoreDataInput = quantizedScoreDataInput;
      tempQuantizedVectorData = null;
      tempScoreQuantizedVectorData = null;
      quantizedDataInput = null;
      quantizedScoreDataInput = null;

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
      OffHeapScalarQuantizedVectorValues scoreVectorValues = null;
      if (finalQuantizedScoreDataInput != null) {
        scoreVectorValues =
            new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
                true,
                fieldInfo.getVectorDimension(),
                docsWithField.cardinality(),
                centroid,
                cDotC,
                quantizer,
                encoding,
                fieldInfo.getVectorSimilarityFunction(),
                vectorsScorer,
                finalQuantizedScoreDataInput);
      }
      RandomVectorScorerSupplier scorerSupplier =
          scoreVectorValues == null
              ? vectorsScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(), vectorValues)
              : vectorsScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(), scoreVectorValues, vectorValues);
      return new QuantizedCloseableRandomVectorScorerSupplier(
          scorerSupplier,
          vectorValues,
          () -> {
            IOUtils.close(finalQuantizedDataInput, finalQuantizedScoreDataInput);
            if (tempScoreQuantizedVectorName != null) {
              IOUtils.deleteFilesIgnoringExceptions(
                  segmentWriteState.directory, tempScoreQuantizedVectorName);
            }
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory, tempQuantizedVectorName);
          });
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(
          t,
          tempQuantizedVectorData,
          tempScoreQuantizedVectorData,
          quantizedDataInput,
          quantizedScoreDataInput);
      if (tempQuantizedVectorData != null) {
        IOUtils.deleteFilesSuppressingExceptions(
            t, segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
      if (tempScoreQuantizedVectorData != null) {
        IOUtils.deleteFilesSuppressingExceptions(
            t, segmentWriteState.directory, tempScoreQuantizedVectorData.getName());
      }
      throw t;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, rawVectorDelegate);
  }

  static float[] getCentroid(KnnVectorsReader vectorsReader, String fieldName) {
    vectorsReader = vectorsReader.unwrapReaderForField(fieldName);
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
      if (knnVectorsReader == null) {
        continue;
      }

      int vectorCount;
      switch (fieldInfo.getVectorEncoding()) {
        case FLOAT32 -> {
          var values = knnVectorsReader.getFloatVectorValues(fieldInfo.name);
          if (values == null) {
            continue;
          }
          vectorCount = values.size();
        }
        case FLOAT16 -> {
          var values = knnVectorsReader.getFloat16VectorValues(fieldInfo.name);
          if (values == null) {
            continue;
          }
          vectorCount = values.size();
        }
        default -> {
          continue;
        }
      }

      float[] centroid = getCentroid(knnVectorsReader, fieldInfo.name);
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

    VectorEncoding vectorEncoding = fieldInfo.getVectorEncoding();
    assert vectorEncoding == VectorEncoding.FLOAT32 || vectorEncoding == VectorEncoding.FLOAT16;
    // clear out the centroid
    Arrays.fill(centroid, 0);
    int count = 0;
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
      KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
      if (knnVectorsReader == null) continue;

      if (vectorEncoding == VectorEncoding.FLOAT32) {
        FloatVectorValues vectorValues =
            mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
        if (vectorValues == null) {
          continue;
        }
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        for (int doc = iterator.nextDoc();
            doc != NO_MORE_DOCS;
            doc = iterator.nextDoc()) {
          ++count;
          float[] vector = vectorValues.vectorValue(iterator.index());
          for (int j = 0; j < vector.length; j++) {
            centroid[j] += vector[j];
          }
        }
      } else if (vectorEncoding == VectorEncoding.FLOAT16) {
        Float16VectorValues vectorValues =
            mergeState.knnVectorsReaders[i].getFloat16VectorValues(fieldInfo.name);
        if (vectorValues == null) {
          continue;
        }
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        for (int doc = iterator.nextDoc();
            doc != NO_MORE_DOCS;
            doc = iterator.nextDoc()) {
          ++count;
          short[] vector = vectorValues.vectorValue(iterator.index());
          for (int j = 0; j < vector.length; j++) {
            centroid[j] += Float.float16ToFloat(vector[j]);
          }
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
    for (FieldWriter<?> field : fields) {
      // the field tracks the delegate field usage
      total += field.ramBytesUsed();
    }
    return total;
  }

  private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    protected final FieldInfo fieldInfo;
    private boolean finished;
    protected final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;
    protected final float[] dimensionSums;
    protected final FloatArrayList magnitudes = new FloatArrayList();
    protected final int dim;

    FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<T> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dim = fieldInfo.getVectorDimension();
      this.dimensionSums = new float[dim];
    }

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(FieldInfo fieldInfo, FlatFieldVectorsWriter<?> flatFieldVectorsWriter) {
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> throw new UnsupportedOperationException("Byte Vectors aren't supported");
        case FLOAT32 -> new Float32FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) flatFieldVectorsWriter);
        case FLOAT16 -> new Float16FieldWriter(fieldInfo, (FlatFieldVectorsWriter<short[]>) flatFieldVectorsWriter);
      };
    }

    @Override
    public List<T> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() {
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
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      size += flatFieldVectorsWriter.ramBytesUsed();
      size += magnitudes.ramBytesUsed();
      return size;
    }

    public abstract void normalizeVectors();

    private static class Float32FieldWriter extends FieldWriter<float[]> {

      Float32FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
        super(fieldInfo, flatFieldVectorsWriter);
      }

      @Override
      public float[] copyValue(float[] value) {
        return ArrayUtil.copyOfSubArray(value, 0, dim);
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
      public void normalizeVectors() {
        for (int i = 0; i < flatFieldVectorsWriter.getVectors().size(); i++) {
          float[] vector = flatFieldVectorsWriter.getVectors().get(i);
          float magnitude = magnitudes.get(i);
          for (int j = 0; j < vector.length; j++) {
            vector[j] /= magnitude;
          }
        }
      }
    }

    private static class Float16FieldWriter extends FieldWriter<short[]> {

      Float16FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<short[]> flatFieldVectorsWriter) {
        super(fieldInfo, flatFieldVectorsWriter);
      }

      @Override
      public short[] copyValue(short[] value) {
        return ArrayUtil.copyOfSubArray(value, 0, dim);
      }

      @Override
      public void addValue(int docID, short[] vectorValue) throws IOException {
        flatFieldVectorsWriter.addValue(docID, vectorValue);
        if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
          float dp = VectorUtil.dotProduct(vectorValue, vectorValue);
          float divisor = (float) Math.sqrt(dp);
          magnitudes.add(divisor);
          for (int i = 0; i < vectorValue.length; i++) {
            dimensionSums[i] += (Float.float16ToFloat(vectorValue[i]) / divisor);
          }
        } else {
          for (int i = 0; i < vectorValue.length; i++) {
            dimensionSums[i] += Float.float16ToFloat(vectorValue[i]);
          }
        }
      }

      @Override
      public void normalizeVectors() {
        for (int i = 0; i < flatFieldVectorsWriter.getVectors().size(); i++) {
          short[] vector = flatFieldVectorsWriter.getVectors().get(i);
          float magnitude = magnitudes.get(i);
          for (int j = 0; j < vector.length; j++) {
            float normalized = Float.float16ToFloat(vector[j]) / magnitude;
            vector[j] = Float.floatToFloat16(normalized);
          }
        }
      }
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
            case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE ->
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
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(quantized, packed);
        case SINGLE_BIT_QUERY_NIBBLE -> packAsBinary(quantized, packed);
        case UNSIGNED_BYTE, SEVEN_BIT -> {}
      }
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

  static class QuantizedFloat16VectorValues extends QuantizedByteVectorValues {
    private OptimizedScalarQuantizer.QuantizationResult corrections;
    private final byte[] quantized;
    private final byte[] packed;
    private final float[] centroid;
    private final float centroidDP;
    private final Float16VectorValues values;
    private final OptimizedScalarQuantizer quantizer;
    private final ScalarEncoding encoding;

    private int lastOrd = -1;

    QuantizedFloat16VectorValues(
        Float16VectorValues delegate,
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
            case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE ->
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
      return new QuantizedFloat16VectorValues(values.copy(), quantizer, encoding, centroid);
    }

    private void quantize(int ord) throws IOException {
      corrections =
          quantizer.scalarQuantize(
              values.vectorValue(ord), quantized, encoding.getBits(), centroid);
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(quantized, packed);
        case SINGLE_BIT_QUERY_NIBBLE -> packAsBinary(quantized, packed);
        case UNSIGNED_BYTE, SEVEN_BIT -> {}
      }
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

  static final class NormalizedFloat16VectorValues extends Float16VectorValues {
    private final Float16VectorValues values;
    private final short[] normalizedVector;

    NormalizedFloat16VectorValues(Float16VectorValues values) {
      this.values = values;
      this.normalizedVector = new short[values.dimension()];
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
    public short[] vectorValue(int ord) throws IOException {
      System.arraycopy(values.vectorValue(ord), 0, normalizedVector, 0, normalizedVector.length);
      VectorUtil.l2normalize(normalizedVector);
      return normalizedVector;
    }

    @Override
    public DocIndexIterator iterator() {
      return values.iterator();
    }

    @Override
    public NormalizedFloat16VectorValues copy() throws IOException {
      return new NormalizedFloat16VectorValues(values.copy());
    }
  }
}
