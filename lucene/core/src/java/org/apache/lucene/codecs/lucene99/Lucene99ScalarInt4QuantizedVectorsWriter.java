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

import static org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarInt4QuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.FlatVectorsWriter;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Writes quantized vector values and metadata to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarInt4QuantizedVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene99ScalarInt4QuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;

  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, quantizedVectorData;
  private final FlatVectorsWriter rawVectorDelegate;
  private boolean finished;

  Lucene99ScalarInt4QuantizedVectorsWriter(
      SegmentWriteState state, FlatVectorsWriter rawVectorDelegate) throws IOException {
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99ScalarInt4QuantizedVectorsFormat.META_EXTENSION);

    String quantizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99ScalarInt4QuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      quantizedVectorData =
          state.directory.createOutput(quantizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99ScalarInt4QuantizedVectorsFormat.META_CODEC_NAME,
          Lucene99ScalarInt4QuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          quantizedVectorData,
          Lucene99ScalarInt4QuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene99ScalarInt4QuantizedVectorsFormat.VERSION_CURRENT,
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
  public FlatFieldVectorsWriter<?> addField(
      FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) throws IOException {
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      if (fieldInfo.getVectorDimension() % 2 != 0) {
        throw new IllegalArgumentException(
            "dimension must be a multiple of 2 for "
                + Lucene99ScalarInt4QuantizedVectorsFormat.NAME
                + "; got "
                + fieldInfo.getVectorDimension());
      }
      FieldWriter quantizedWriter =
          new FieldWriter(fieldInfo, segmentWriteState.infoStream, indexWriter);
      fields.add(quantizedWriter);
      indexWriter = quantizedWriter;
    }
    return rawVectorDelegate.addField(fieldInfo, indexWriter);
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    // Since we know we will not be searching for additional indexing, we can just write the
    // the vectors directly to the new segment.
    // No need to use temporary file as we don't have to re-open for reading
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      ScalarQuantizer mergedQuantizationState = mergeQuantiles(fieldInfo, mergeState);
      Lucene99ScalarQuantizedVectorsWriter.MergedQuantizedVectorValues byteVectorValues =
          Lucene99ScalarQuantizedVectorsWriter.MergedQuantizedVectorValues
              .mergeQuantizedByteVectorValues(fieldInfo, mergeState, mergedQuantizationState);
      long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(quantizedVectorData, byteVectorValues);
      long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          mergedQuantizationState.getLowerQuantile(),
          mergedQuantizationState.getUpperQuantile(),
          docsWithField);
    }
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      // Simply merge the underlying delegate, which just copies the raw vector data to a new
      // segment file
      rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
      ScalarQuantizer mergedQuantizationState = mergeQuantiles(fieldInfo, mergeState);
      return mergeOneFieldToIndex(
          segmentWriteState, fieldInfo, mergeState, mergedQuantizationState);
    }
    // We only merge the delegate, since the field type isn't float32, quantization wasn't
    // supported, so bypass it.
    return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : fields) {
      field.finish();
      if (sortMap == null) {
        writeField(field, maxDoc);
      } else {
        writeSortingField(field, maxDoc, sortMap);
      }
    }
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
    if (quantizedVectorData != null) {
      CodecUtil.writeFooter(quantizedVectorData);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter fieldData, int maxDoc) throws IOException {
    // write vector values
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        fieldData.docsWithField);
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      Float lowerQuantile,
      Float upperQuantile,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVInt(field.getVectorDimension());
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    if (count > 0) {
      assert Float.isFinite(lowerQuantile) && Float.isFinite(upperQuantile);
      meta.writeInt(Float.floatToIntBits(lowerQuantile));
      meta.writeInt(Float.floatToIntBits(upperQuantile));
    }
    // write docIDs
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, quantizedVectorData, count, maxDoc, docsWithField);
  }

  private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 1) >> 1];
    byte[] quantized = new byte[fieldData.fieldInfo.getVectorDimension()];
    final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    float[] copy = fieldData.normalize ? new float[fieldData.fieldInfo.getVectorDimension()] : null;
    for (float[] v : fieldData.floatVectors) {
      if (fieldData.normalize) {
        System.arraycopy(v, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        v = copy;
      }

      float offsetCorrection =
          scalarQuantizer.quantize(v, quantized, fieldData.fieldInfo.getVectorSimilarityFunction());
      for (int i = 0; i < vector.length; i++) {
        byte q1 = quantized[i];
        byte q2 = quantized[i + vector.length];
        vector[i] = (byte) ((q1 << 4) | q2);
      }
      quantizedVectorData.writeBytes(vector, vector.length);
      offsetBuffer.putFloat(offsetCorrection);
      quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  private void writeSortingField(FieldWriter fieldData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    final int[] docIdOffsets = new int[sortMap.size()];
    int offset = 1; // 0 means no vector for this (field, document)
    DocIdSetIterator iterator = fieldData.docsWithField.iterator();
    for (int docID = iterator.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = iterator.nextDoc()) {
      int newDocID = sortMap.oldToNew(docID);
      docIdOffsets[newDocID] = offset++;
    }
    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    final int[] ordMap = new int[offset - 1]; // new ord to old ord
    int ord = 0;
    int doc = 0;
    for (int docIdOffset : docIdOffsets) {
      if (docIdOffset != 0) {
        ordMap[ord] = docIdOffset - 1;
        newDocsWithField.add(doc);
        ord++;
      }
      doc++;
    }

    // write vector values
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        quantizedVectorLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        newDocsWithField);
  }

  private void writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[(fieldData.fieldInfo.getVectorDimension() + 1) >> 1];
    byte[] quantized = new byte[fieldData.fieldInfo.getVectorDimension()];
    final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    float[] copy = fieldData.normalize ? new float[fieldData.fieldInfo.getVectorDimension()] : null;
    for (int ordinal : ordMap) {
      float[] v = fieldData.floatVectors.get(ordinal);
      if (fieldData.normalize) {
        System.arraycopy(v, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        v = copy;
      }
      float offsetCorrection =
          scalarQuantizer.quantize(v, quantized, fieldData.fieldInfo.getVectorSimilarityFunction());
      for (int i = 0; i < vector.length; i++) {
        byte q1 = quantized[i];
        byte q2 = quantized[i + vector.length];
        vector[i] = (byte) ((q1 << 4) | q2);
      }
      quantizedVectorData.writeBytes(vector, vector.length);
      offsetBuffer.putFloat(offsetCorrection);
      quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  private ScalarQuantizer mergeQuantiles(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    assert fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32;
    return mergeAndRecalculateQuantiles(mergeState, fieldInfo);
  }

  private Lucene99ScalarQuantizedVectorsWriter.ScalarQuantizedCloseableRandomVectorScorerSupplier
      mergeOneFieldToIndex(
          SegmentWriteState segmentWriteState,
          FieldInfo fieldInfo,
          MergeState mergeState,
          ScalarQuantizer mergedQuantizationState)
          throws IOException {
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT,
          "quantized field="
              + " minQuantile="
              + mergedQuantizationState.getLowerQuantile()
              + " maxQuantile="
              + mergedQuantizationState.getUpperQuantile());
    }
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempQuantizedVectorData =
        segmentWriteState.directory.createTempOutput(
            quantizedVectorData.getName(), "temp", segmentWriteState.context);
    IndexInput quantizationDataInput = null;
    boolean success = false;
    try {
      Lucene99ScalarQuantizedVectorsWriter.MergedQuantizedVectorValues byteVectorValues =
          Lucene99ScalarQuantizedVectorsWriter.MergedQuantizedVectorValues
              .mergeQuantizedByteVectorValues(fieldInfo, mergeState, mergedQuantizationState);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(tempQuantizedVectorData, byteVectorValues);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      quantizationDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      quantizedVectorData.copyBytes(
          quantizationDataInput, quantizationDataInput.length() - CodecUtil.footerLength());
      long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
      CodecUtil.retrieveChecksum(quantizationDataInput);
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          mergedQuantizationState.getLowerQuantile(),
          mergedQuantizationState.getUpperQuantile(),
          docsWithField);
      success = true;
      final IndexInput finalQuantizationDataInput = quantizationDataInput;
      return new Lucene99ScalarQuantizedVectorsWriter
          .ScalarQuantizedCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalQuantizationDataInput);
            segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
          },
          docsWithField.cardinality(),
          new ScalarQuantizedRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              mergedQuantizationState,
              new OffHeapQuantizedHalfByteVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality(),
                  quantizationDataInput)));
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(tempQuantizedVectorData, quantizationDataInput);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
    }
  }

  static ScalarQuantizer mergeAndRecalculateQuantiles(MergeState mergeState, FieldInfo fieldInfo)
      throws IOException {
    // TODO we need to optimize re-calculating quantiles
    FloatVectorValues vectorValues =
        KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
    int numVectors = 0;
    for (int doc = vectorValues.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = vectorValues.nextDoc()) {
      numVectors++;
    }
    return ScalarQuantizer.fromVectorsAutoInterval(
        KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
        fieldInfo.getVectorSimilarityFunction(),
        numVectors,
        4);
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  private static DocsWithFieldSet writeQuantizedVectorData(
      IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    byte[] vector = new byte[(quantizedByteVectorValues.dimension() + 1) >> 1];
    for (int docV = quantizedByteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = quantizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue();
      assert binaryValue.length == quantizedByteVectorValues.dimension()
          : "dim=" + quantizedByteVectorValues.dimension() + " len=" + binaryValue.length;
      OffHeapQuantizedHalfByteVectorValues.compressBytes(binaryValue, vector);
      output.writeBytes(vector, vector.length);
      output.writeInt(Float.floatToIntBits(quantizedByteVectorValues.getScoreCorrectionConstant()));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, quantizedVectorData, rawVectorDelegate);
  }

  static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    private final List<float[]> floatVectors;
    private final FieldInfo fieldInfo;
    private final InfoStream infoStream;
    private final boolean normalize;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;
    private boolean finished;
    private final DocsWithFieldSet docsWithField;

    @SuppressWarnings("unchecked")
    FieldWriter(FieldInfo fieldInfo, InfoStream infoStream, KnnFieldVectorsWriter<?> indexWriter) {
      super((KnnFieldVectorsWriter<float[]>) indexWriter);
      this.fieldInfo = fieldInfo;
      this.normalize = fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
      this.floatVectors = new ArrayList<>();
      this.infoStream = infoStream;
      this.docsWithField = new DocsWithFieldSet();
    }

    void finish() throws IOException {
      if (finished) {
        return;
      }
      if (floatVectors.size() == 0) {
        finished = true;
        return;
      }
      ScalarQuantizer quantizer =
          ScalarQuantizer.fromVectorsAutoInterval(
              new Lucene99ScalarQuantizedVectorsWriter.FloatVectorWrapper(
                  floatVectors,
                  fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE),
              fieldInfo.getVectorSimilarityFunction(),
              floatVectors.size(),
              4);
      minQuantile = quantizer.getLowerQuantile();
      maxQuantile = quantizer.getUpperQuantile();
      if (infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
        infoStream.message(
            QUANTIZED_VECTOR_COMPONENT,
            "quantized field=" + " minQuantile=" + minQuantile + " maxQuantile=" + maxQuantile);
      }
      finished = true;
    }

    ScalarQuantizer createQuantizer() {
      assert finished;
      return new ScalarQuantizer(minQuantile, maxQuantile, 4);
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      if (indexingDelegate != null) {
        size += indexingDelegate.ramBytesUsed();
      }
      if (floatVectors.size() == 0) return size;
      return size + (long) floatVectors.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      docsWithField.add(docID);
      floatVectors.add(vectorValue);
      if (indexingDelegate != null) {
        indexingDelegate.addValue(docID, vectorValue);
      }
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      throw new UnsupportedOperationException();
    }
  }
}
