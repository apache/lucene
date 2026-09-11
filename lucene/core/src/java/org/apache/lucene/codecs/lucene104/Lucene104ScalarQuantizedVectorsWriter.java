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
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;

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
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
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
  private final boolean enableCentering;
  private final int version;
  private final FlatVectorsWriter rawVectorDelegate;
  private boolean finished;

  /** Sole constructor */
  public Lucene104ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      ScalarEncoding encoding,
      boolean enableCentering,
      FlatVectorsWriter rawVectorDelegate,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
    this.encoding = encoding;
    this.enableCentering = enableCentering;
    this.version =
        enableCentering
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
    if (fieldInfo.getVectorEncoding().isFloatingPoint() && enableCentering == false) {
      // Data-blind mode: keep vectors in memory only and never write full-precision float vectors.
      FlatFieldVectorsWriter<?> storage =
          switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> new InMemoryFloatFieldWriter(fieldInfo);
            case FLOAT16 -> new InMemoryFloat16FieldWriter(fieldInfo);
            case BYTE -> throw new UnsupportedOperationException("Byte Vectors aren't supported");
          };
      FieldWriter<?> fieldWriter = FieldWriter.create(fieldInfo, storage, false);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    FlatFieldVectorsWriter<?> storage = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().isFloatingPoint()) {
      FieldWriter<?> fieldWriter = FieldWriter.create(fieldInfo, storage, enableCentering);
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
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, vector);
        case SINGLE_BIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.packAsBinary(scratch, vector);
        case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(scratch, vector);
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
          case PACKED_NIBBLE, SINGLE_BIT_QUERY_NIBBLE, DIBIT_QUERY_NIBBLE ->
              new byte[encoding.getDocPackedLength(scratch.length)];
        };
    for (int ordinal : ordMap) {
      OptimizedScalarQuantizer.QuantizationResult corrections =
          scalarQuantizer.scalarQuantize(
              fieldData.floatVectorValue(ordinal), scratch, encoding.getBits(), clusterCenter);
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(scratch, vector);
        case SINGLE_BIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.packAsBinary(scratch, vector);
        case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(scratch, vector);
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
      if (version == Lucene104ScalarQuantizedVectorsFormat.VERSION_START) {
        final ByteBuffer buffer =
            ByteBuffer.allocate(field.getVectorDimension() * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(clusterCenter);
        meta.writeBytes(buffer.array(), buffer.array().length);
        meta.writeInt(Float.floatToIntBits(centroidDp));
      }
      // Data-blind (version 1): the centroid and centroidDP are omitted; a zero centroid is
      // substituted at read time.
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

  private QuantizedByteVectorValues mergedQuantizedVectorValues(
      FieldInfo fieldInfo, MergeState mergeState, float[] centroid) throws IOException {
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    FloatVectorValues vectorValues =
        fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT16
            ? new Float16AsFloatVectorValues(
                MergedVectorValues.mergeFloat16VectorValues(fieldInfo, mergeState))
            : MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
    if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
      vectorValues = new NormalizedFloatVectorValues(vectorValues);
    }
    return new QuantizedFloatVectorValues(vectorValues, quantizer, encoding, centroid);
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
    if (fieldInfo.getVectorEncoding().isFloatingPoint() == false) {
      rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
      return;
    }
    if (enableCentering) {
      mergeOneFlatVectorFieldCentered(fieldInfo, mergeState);
    } else {
      mergeOneFlatVectorFieldDataBlind(fieldInfo, mergeState);
    }
  }

  private void mergeOneFlatVectorFieldCentered(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    // Don't need access to the random vectors, we can just use the merged
    rawVectorDelegate.mergeOneFlatVectorField(fieldInfo, mergeState);
    final float[] mergedCentroid = new float[fieldInfo.getVectorDimension()];
    int vectorCount = mergeAndRecalculateCentroids(mergeState, fieldInfo, mergedCentroid);
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT, "Vectors' count:" + vectorCount);
    }
    QuantizedByteVectorValues quantizedVectorValues =
        mergedQuantizedVectorValues(fieldInfo, mergeState, mergedCentroid);
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    DocsWithFieldSet docsWithField = writeVectorData(vectorData, quantizedVectorValues);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    float centroidDp =
        docsWithField.cardinality() > 0 ? VectorUtil.dotProduct(mergedCentroid, mergedCentroid) : 0;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        mergedCentroid,
        centroidDp,
        docsWithField);
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
        float[] centroid = getCentroid(reader, fieldInfo.name);
        if (centroid != null && isAllZero(centroid)) {
          // Quantized-only segment whose bytes already match the output format (encoding and zero
          // centroid): copy them directly.
          values = qvv;
        } else {
          // Bytes were produced against a non-zero (or unknown) centroid, so they cannot be passed
          // through into the zero-centroid output; re-quantize from floats.
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
        zeroCentroid,
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

  static DocsWithFieldSet writeBinarizedQueryData(
      QuantizedByteVectorValues quantizedByteVectorValues,
      ScalarEncoding encoding,
      IndexOutput binarizedQueryData,
      FloatVectorValues floatVectorValues,
      OptimizedScalarQuantizer binaryQuantizer)
      throws IOException {
    if (encoding.isAsymmetric() == false) {
      throw new IllegalArgumentException("encoding and queryEncoding must be different");
    }
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    int discretizedDims = encoding.getDiscreteDimensions(floatVectorValues.dimension());
    byte[] quantizationScratch = new byte[discretizedDims];
    byte[] toQuery = new byte[encoding.getQueryPackedLength(discretizedDims)];
    KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      // write index vector
      OptimizedScalarQuantizer.QuantizationResult r =
          binaryQuantizer.scalarQuantize(
              floatVectorValues.vectorValue(iterator.index()),
              quantizationScratch,
              encoding.getQueryBits(),
              quantizedByteVectorValues.getCentroid());
      docsWithField.add(docV);
      // pack and store the 4bit query vector
      transposeHalfByte(quantizationScratch, toQuery);
      binarizedQueryData.writeBytes(toQuery, toQuery.length);
      binarizedQueryData.writeInt(Float.floatToIntBits(r.lowerInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r.upperInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r.additionalCorrection()));
      binarizedQueryData.writeInt(r.quantizedComponentSum());
    }
    return docsWithField;
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

  static QuantizedByteVectorValues getQuantizedVectorValues(
      KnnVectorsReader vectorsReader, String fieldName) throws IOException {
    vectorsReader = vectorsReader.unwrapReaderForField(fieldName);
    if (vectorsReader instanceof Lucene104ScalarQuantizedVectorsReader reader) {
      return reader.getQuantizedVectorValues(fieldName);
    }
    return null;
  }

  /**
   * Returns whether the segment stores full-precision vectors for this field, or null when the
   * field is absent or byte-encoded. Data-blind segments report {@code false} since only quantized
   * bytes were written.
   */
  private static boolean hasRawVectorValues(KnnVectorsReader vectorsReader, FieldInfo fieldInfo)
      throws IOException {
    vectorsReader = vectorsReader.unwrapReaderForField(fieldInfo.name);
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
      // or if there are deleted docs, we must recalculate the centroid. An all-zero centroid
      // indicates a data-blind segment (no centering was done); it can't be combined with the
      // others, so recompute from the (possibly dequantized) vectors.
      if (centroid == null || isAllZero(centroid) || mergeState.liveDocs[i] != null) {
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

  private static boolean isAllZero(float[] values) {
    for (float value : values) {
      if (value != 0f) {
        return false;
      }
    }
    return true;
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
      if (field.enableCentering) {
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

  /** In-memory storage for fp32 vectors used in data-blind mode; nothing is written to disk. */
  private static class InMemoryFloatFieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(InMemoryFloatFieldWriter.class);
    private final FieldInfo fieldInfo;
    private final List<float[]> vectors = new ArrayList<>();
    private final DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    private boolean finished;
    private int lastDocID = -1;

    InMemoryFloatFieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
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

    @Override
    public float[] copyValue(float[] vectorValue) {
      return ArrayUtil.copyOfSubArray(vectorValue, 0, fieldInfo.getVectorDimension());
    }

    @Override
    public List<float[]> getVectors() {
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
          + (long) vectors.size() * fieldInfo.getVectorDimension() * Float.BYTES;
    }
  }

  /**
   * In-memory storage for fp16 vectors used in data-blind mode; nothing is written to disk. Vectors
   * are kept as fp16 so HNSW wrappers can read them back as {@link Float16VectorValues}.
   */
  private static class InMemoryFloat16FieldWriter extends FlatFieldVectorsWriter<short[]> {
    private static final long SHALLOW_SIZE =
        shallowSizeOfInstance(InMemoryFloat16FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final List<short[]> vectors = new ArrayList<>();
    private final DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    private boolean finished;
    private int lastDocID = -1;

    InMemoryFloat16FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public void addValue(int docID, short[] vectorValue) throws IOException {
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

    @Override
    public short[] copyValue(short[] vectorValue) {
      return ArrayUtil.copyOfSubArray(vectorValue, 0, fieldInfo.getVectorDimension());
    }

    @Override
    public List<short[]> getVectors() {
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
          + (long) vectors.size() * fieldInfo.getVectorDimension() * Short.BYTES;
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
      switch (encoding) {
        case PACKED_NIBBLE -> OffHeapScalarQuantizedVectorValues.packNibbles(quantized, packed);
        case SINGLE_BIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.packAsBinary(quantized, packed);
        case DIBIT_QUERY_NIBBLE -> OptimizedScalarQuantizer.transposeDibit(quantized, packed);
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
