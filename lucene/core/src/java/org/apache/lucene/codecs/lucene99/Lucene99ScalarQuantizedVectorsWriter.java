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
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.calculateDefaultConfidenceInterval;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Writes quantized vector values and metadata to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarQuantizedVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene99ScalarQuantizedVectorsWriter.class);

  // Used for determining when merged quantiles shifted too far from individual segment quantiles.
  // When merging quantiles from various segments, we need to ensure that the new quantiles
  // are not exceptionally different from an individual segments quantiles.
  // This would imply that the quantization buckets would shift too much
  // for floating point values and justify recalculating the quantiles. This helps preserve
  // accuracy of the calculated quantiles, even in adversarial cases such as vector clustering.
  // This number was determined via empirical testing
  private static final float QUANTILE_RECOMPUTE_LIMIT = 32;
  // Used for determining if a new quantization state requires a re-quantization
  // for a given segment.
  // This ensures that in expectation 4/5 of the vector would be unchanged by requantization.
  // Furthermore, only those values where the value is within 1/5 of the centre of a quantization
  // bin will be changed. In these cases the error introduced by snapping one way or another
  // is small compared to the error introduced by quantization in the first place. Furthermore,
  // empirical testing showed that the relative error by not requantizing is small (compared to
  // the quantization error) and the condition is sensitive enough to detect all adversarial cases,
  // such as merging clustered data.
  private static final float REQUANTIZATION_LIMIT = 0.2f;
  private final SegmentWriteState segmentWriteState;

  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, quantizedVectorData;
  private final Float confidenceInterval;
  private final FlatVectorsWriter rawVectorDelegate;
  private final byte bits;
  private final boolean compress;
  private final int version;
  private boolean finished;

  public Lucene99ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      Float confidenceInterval,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    this(
        state,
        Lucene99ScalarQuantizedVectorsFormat.VERSION_START,
        confidenceInterval,
        (byte) 7,
        false,
        rawVectorDelegate,
        scorer);
    if (confidenceInterval != null && confidenceInterval == 0) {
      throw new IllegalArgumentException("confidenceInterval cannot be set to zero");
    }
  }

  public Lucene99ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      Float confidenceInterval,
      byte bits,
      boolean compress,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    this(
        state,
        Lucene99ScalarQuantizedVectorsFormat.VERSION_ADD_BITS,
        confidenceInterval,
        bits,
        compress,
        rawVectorDelegate,
        scorer);
  }

  private Lucene99ScalarQuantizedVectorsWriter(
      SegmentWriteState state,
      int version,
      Float confidenceInterval,
      byte bits,
      boolean compress,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.confidenceInterval = confidenceInterval;
    this.bits = bits;
    this.compress = compress;
    this.version = version;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99ScalarQuantizedVectorsFormat.META_EXTENSION);

    String quantizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      quantizedVectorData =
          state.directory.createOutput(quantizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99ScalarQuantizedVectorsFormat.META_CODEC_NAME,
          version,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          quantizedVectorData,
          Lucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          version,
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
      if (bits <= 4 && fieldInfo.getVectorDimension() % 2 != 0) {
        throw new IllegalArgumentException(
            "bits="
                + bits
                + " is not supported for odd vector dimensions; vector dimension="
                + fieldInfo.getVectorDimension());
      }
      FieldWriter quantizedWriter =
          new FieldWriter(
              confidenceInterval,
              bits,
              compress,
              fieldInfo,
              segmentWriteState.infoStream,
              indexWriter);
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
      ScalarQuantizer mergedQuantizationState =
          mergeAndRecalculateQuantiles(mergeState, fieldInfo, confidenceInterval, bits);
      MergedQuantizedVectorValues byteVectorValues =
          MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
              fieldInfo, mergeState, mergedQuantizationState);
      long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(quantizedVectorData, byteVectorValues, bits, compress);
      long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          confidenceInterval,
          bits,
          compress,
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
      ScalarQuantizer mergedQuantizationState =
          mergeAndRecalculateQuantiles(mergeState, fieldInfo, confidenceInterval, bits);
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
        confidenceInterval,
        bits,
        compress,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        fieldData.docsWithField);
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      Float confidenceInterval,
      byte bits,
      boolean compress,
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
      if (version >= Lucene99ScalarQuantizedVectorsFormat.VERSION_ADD_BITS) {
        meta.writeInt(confidenceInterval == null ? -1 : Float.floatToIntBits(confidenceInterval));
        meta.writeByte(bits);
        meta.writeByte(compress ? (byte) 1 : (byte) 0);
      } else {
        assert confidenceInterval == null || confidenceInterval != DYNAMIC_CONFIDENCE_INTERVAL;
        meta.writeInt(
            Float.floatToIntBits(
                confidenceInterval == null
                    ? calculateDefaultConfidenceInterval(field.getVectorDimension())
                    : confidenceInterval));
      }
      meta.writeInt(Float.floatToIntBits(lowerQuantile));
      meta.writeInt(Float.floatToIntBits(upperQuantile));
    }
    // write docIDs
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, quantizedVectorData, count, maxDoc, docsWithField);
  }

  private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    byte[] compressedVector =
        fieldData.compress
            ? OffHeapQuantizedByteVectorValues.compressedArray(
                fieldData.fieldInfo.getVectorDimension(), bits)
            : null;
    final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    float[] copy = fieldData.normalize ? new float[fieldData.fieldInfo.getVectorDimension()] : null;
    for (float[] v : fieldData.floatVectors) {
      if (fieldData.normalize) {
        System.arraycopy(v, 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        v = copy;
      }

      float offsetCorrection =
          scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      if (compressedVector != null) {
        OffHeapQuantizedByteVectorValues.compressBytes(vector, compressedVector);
        quantizedVectorData.writeBytes(compressedVector, compressedVector.length);
      } else {
        quantizedVectorData.writeBytes(vector, vector.length);
      }
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
        confidenceInterval,
        bits,
        compress,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        newDocsWithField);
  }

  private void writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[fieldData.fieldInfo.getVectorDimension()];
    byte[] compressedVector =
        fieldData.compress
            ? OffHeapQuantizedByteVectorValues.compressedArray(
                fieldData.fieldInfo.getVectorDimension(), bits)
            : null;
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
          scalarQuantizer.quantize(v, vector, fieldData.fieldInfo.getVectorSimilarityFunction());
      if (compressedVector != null) {
        OffHeapQuantizedByteVectorValues.compressBytes(vector, compressedVector);
        quantizedVectorData.writeBytes(compressedVector, compressedVector.length);
      } else {
        quantizedVectorData.writeBytes(vector, vector.length);
      }
      offsetBuffer.putFloat(offsetCorrection);
      quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  private ScalarQuantizedCloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      SegmentWriteState segmentWriteState,
      FieldInfo fieldInfo,
      MergeState mergeState,
      ScalarQuantizer mergedQuantizationState)
      throws IOException {
    if (segmentWriteState.infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      segmentWriteState.infoStream.message(
          QUANTIZED_VECTOR_COMPONENT,
          "quantized field="
              + " confidenceInterval="
              + confidenceInterval
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
      MergedQuantizedVectorValues byteVectorValues =
          MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(
              fieldInfo, mergeState, mergedQuantizationState);
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(tempQuantizedVectorData, byteVectorValues, bits, compress);
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
          confidenceInterval,
          bits,
          compress,
          mergedQuantizationState.getLowerQuantile(),
          mergedQuantizationState.getUpperQuantile(),
          docsWithField);
      success = true;
      final IndexInput finalQuantizationDataInput = quantizationDataInput;
      return new ScalarQuantizedCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalQuantizationDataInput);
            segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
          },
          docsWithField.cardinality(),
          vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsWithField.cardinality(),
                  mergedQuantizationState,
                  compress,
                  fieldInfo.getVectorSimilarityFunction(),
                  vectorsScorer,
                  quantizationDataInput)));
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(tempQuantizedVectorData, quantizationDataInput);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
    }
  }

  static ScalarQuantizer mergeQuantiles(
      List<ScalarQuantizer> quantizationStates, IntArrayList segmentSizes, byte bits) {
    assert quantizationStates.size() == segmentSizes.size();
    if (quantizationStates.isEmpty()) {
      return null;
    }
    float lowerQuantile = 0f;
    float upperQuantile = 0f;
    int totalCount = 0;
    for (int i = 0; i < quantizationStates.size(); i++) {
      if (quantizationStates.get(i) == null) {
        return null;
      }
      lowerQuantile += quantizationStates.get(i).getLowerQuantile() * segmentSizes.get(i);
      upperQuantile += quantizationStates.get(i).getUpperQuantile() * segmentSizes.get(i);
      totalCount += segmentSizes.get(i);
      if (quantizationStates.get(i).getBits() != bits) {
        return null;
      }
    }
    lowerQuantile /= totalCount;
    upperQuantile /= totalCount;
    return new ScalarQuantizer(lowerQuantile, upperQuantile, bits);
  }

  /**
   * Returns true if the quantiles of the merged state are too far from the quantiles of the
   * individual states.
   *
   * @param mergedQuantizationState The merged quantization state
   * @param quantizationStates The quantization states of the individual segments
   * @return true if the quantiles should be recomputed
   */
  static boolean shouldRecomputeQuantiles(
      ScalarQuantizer mergedQuantizationState, List<ScalarQuantizer> quantizationStates) {
    // calculate the limit for the quantiles to be considered too far apart
    // We utilize upper & lower here to determine if the new upper and merged upper would
    // drastically
    // change the quantization buckets for floats
    // This is a fairly conservative check.
    float limit =
        (mergedQuantizationState.getUpperQuantile() - mergedQuantizationState.getLowerQuantile())
            / QUANTILE_RECOMPUTE_LIMIT;
    for (ScalarQuantizer quantizationState : quantizationStates) {
      if (Math.abs(
              quantizationState.getUpperQuantile() - mergedQuantizationState.getUpperQuantile())
          > limit) {
        return true;
      }
      if (Math.abs(
              quantizationState.getLowerQuantile() - mergedQuantizationState.getLowerQuantile())
          > limit) {
        return true;
      }
    }
    return false;
  }

  private static QuantizedVectorsReader getQuantizedKnnVectorsReader(
      KnnVectorsReader vectorsReader, String fieldName) {
    if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
      vectorsReader =
          ((PerFieldKnnVectorsFormat.FieldsReader) vectorsReader).getFieldReader(fieldName);
    }
    if (vectorsReader instanceof QuantizedVectorsReader) {
      return (QuantizedVectorsReader) vectorsReader;
    }
    return null;
  }

  private static ScalarQuantizer getQuantizedState(
      KnnVectorsReader vectorsReader, String fieldName) {
    QuantizedVectorsReader reader = getQuantizedKnnVectorsReader(vectorsReader, fieldName);
    if (reader != null) {
      return reader.getQuantizationState(fieldName);
    }
    return null;
  }

  /**
   * Merges the quantiles of the segments and recalculates the quantiles if necessary.
   *
   * @param mergeState The merge state
   * @param fieldInfo The field info
   * @param confidenceInterval The confidence interval
   * @param bits The number of bits
   * @return The merged quantiles
   * @throws IOException If there is a low-level I/O error
   */
  public static ScalarQuantizer mergeAndRecalculateQuantiles(
      MergeState mergeState, FieldInfo fieldInfo, Float confidenceInterval, byte bits)
      throws IOException {
    assert fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32);
    List<ScalarQuantizer> quantizationStates = new ArrayList<>(mergeState.liveDocs.length);
    IntArrayList segmentSizes = new IntArrayList(mergeState.liveDocs.length);
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      FloatVectorValues fvv;
      if (mergeState.knnVectorsReaders[i] != null
          && (fvv = mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name)) != null
          && fvv.size() > 0) {
        ScalarQuantizer quantizationState =
            getQuantizedState(mergeState.knnVectorsReaders[i], fieldInfo.name);
        // If we have quantization state, we can utilize that to make merging cheaper
        quantizationStates.add(quantizationState);
        segmentSizes.add(fvv.size());
      }
    }
    ScalarQuantizer mergedQuantiles = mergeQuantiles(quantizationStates, segmentSizes, bits);
    // Segments no providing quantization state indicates that their quantiles were never
    // calculated.
    // To be safe, we should always recalculate given a sample set over all the float vectors in the
    // merged
    // segment view
    if (mergedQuantiles == null
        // For smaller `bits` values, we should always recalculate the quantiles
        // TODO: this is very conservative, could we reuse information for even int4 quantization?
        || bits <= 4
        || shouldRecomputeQuantiles(mergedQuantiles, quantizationStates)) {
      int numVectors = 0;
      FloatVectorValues vectorValues =
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      // iterate vectorValues and increment numVectors
      for (int doc = vectorValues.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = vectorValues.nextDoc()) {
        numVectors++;
      }
      return buildScalarQuantizer(
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState),
          numVectors,
          fieldInfo.getVectorSimilarityFunction(),
          confidenceInterval,
          bits);
    }
    return mergedQuantiles;
  }

  static ScalarQuantizer buildScalarQuantizer(
      FloatVectorValues floatVectorValues,
      int numVectors,
      VectorSimilarityFunction vectorSimilarityFunction,
      Float confidenceInterval,
      byte bits)
      throws IOException {
    if (confidenceInterval != null && confidenceInterval == DYNAMIC_CONFIDENCE_INTERVAL) {
      return ScalarQuantizer.fromVectorsAutoInterval(
          floatVectorValues, vectorSimilarityFunction, numVectors, bits);
    }
    return ScalarQuantizer.fromVectors(
        floatVectorValues,
        confidenceInterval == null
            ? calculateDefaultConfidenceInterval(floatVectorValues.dimension())
            : confidenceInterval,
        numVectors,
        bits);
  }

  /**
   * Returns true if the quantiles of the new quantization state are too far from the quantiles of
   * the existing quantization state. This would imply that floating point values would slightly
   * shift quantization buckets.
   *
   * @param existingQuantiles The existing quantiles for a segment
   * @param newQuantiles The new quantiles for a segment, could be merged, or fully re-calculated
   * @return true if the floating point values should be requantized
   */
  static boolean shouldRequantize(ScalarQuantizer existingQuantiles, ScalarQuantizer newQuantiles) {
    float tol =
        REQUANTIZATION_LIMIT
            * (newQuantiles.getUpperQuantile() - newQuantiles.getLowerQuantile())
            / 128f;
    if (Math.abs(existingQuantiles.getUpperQuantile() - newQuantiles.getUpperQuantile()) > tol) {
      return true;
    }
    return Math.abs(existingQuantiles.getLowerQuantile() - newQuantiles.getLowerQuantile()) > tol;
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  public static DocsWithFieldSet writeQuantizedVectorData(
      IndexOutput output,
      QuantizedByteVectorValues quantizedByteVectorValues,
      byte bits,
      boolean compress)
      throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    final byte[] compressedVector =
        compress
            ? OffHeapQuantizedByteVectorValues.compressedArray(
                quantizedByteVectorValues.dimension(), bits)
            : null;
    for (int docV = quantizedByteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = quantizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue();
      assert binaryValue.length == quantizedByteVectorValues.dimension()
          : "dim=" + quantizedByteVectorValues.dimension() + " len=" + binaryValue.length;
      if (compressedVector != null) {
        OffHeapQuantizedByteVectorValues.compressBytes(binaryValue, compressedVector);
        output.writeBytes(compressedVector, compressedVector.length);
      } else {
        output.writeBytes(binaryValue, binaryValue.length);
      }
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
    private final Float confidenceInterval;
    private final byte bits;
    private final boolean compress;
    private final InfoStream infoStream;
    private final boolean normalize;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;
    private boolean finished;
    private final DocsWithFieldSet docsWithField;

    @SuppressWarnings("unchecked")
    FieldWriter(
        Float confidenceInterval,
        byte bits,
        boolean compress,
        FieldInfo fieldInfo,
        InfoStream infoStream,
        KnnFieldVectorsWriter<?> indexWriter) {
      super((KnnFieldVectorsWriter<float[]>) indexWriter);
      this.confidenceInterval = confidenceInterval;
      this.bits = bits;
      this.fieldInfo = fieldInfo;
      this.normalize = fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE;
      this.floatVectors = new ArrayList<>();
      this.infoStream = infoStream;
      this.docsWithField = new DocsWithFieldSet();
      this.compress = compress;
    }

    void finish() throws IOException {
      if (finished) {
        return;
      }
      if (floatVectors.size() == 0) {
        finished = true;
        return;
      }
      FloatVectorValues floatVectorValues = new FloatVectorWrapper(floatVectors, normalize);
      ScalarQuantizer quantizer =
          buildScalarQuantizer(
              floatVectorValues,
              floatVectors.size(),
              fieldInfo.getVectorSimilarityFunction(),
              confidenceInterval,
              bits);
      minQuantile = quantizer.getLowerQuantile();
      maxQuantile = quantizer.getUpperQuantile();
      if (infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
        infoStream.message(
            QUANTIZED_VECTOR_COMPONENT,
            "quantized field="
                + " confidenceInterval="
                + confidenceInterval
                + " bits="
                + bits
                + " minQuantile="
                + minQuantile
                + " maxQuantile="
                + maxQuantile);
      }
      finished = true;
    }

    ScalarQuantizer createQuantizer() {
      assert finished;
      return new ScalarQuantizer(minQuantile, maxQuantile, bits);
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

  static class FloatVectorWrapper extends FloatVectorValues {
    private final List<float[]> vectorList;
    private final float[] copy;
    private final boolean normalize;
    protected int curDoc = -1;

    FloatVectorWrapper(List<float[]> vectorList, boolean normalize) {
      this.vectorList = vectorList;
      this.copy = new float[vectorList.get(0).length];
      this.normalize = normalize;
    }

    @Override
    public int dimension() {
      return vectorList.get(0).length;
    }

    @Override
    public int size() {
      return vectorList.size();
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (curDoc == -1 || curDoc >= vectorList.size()) {
        throw new IOException("Current doc not set or too many iterations");
      }
      if (normalize) {
        System.arraycopy(vectorList.get(curDoc), 0, copy, 0, copy.length);
        VectorUtil.l2normalize(copy);
        return copy;
      }
      return vectorList.get(curDoc);
    }

    @Override
    public int docID() {
      if (curDoc >= vectorList.size()) {
        return NO_MORE_DOCS;
      }
      return curDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      curDoc++;
      return docID();
    }

    @Override
    public int advance(int target) throws IOException {
      curDoc = target;
      return docID();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }
  }

  static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {
    private final QuantizedByteVectorValues values;

    QuantizedByteVectorValueSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /** Returns a merged view over all the segment's {@link QuantizedByteVectorValues}. */
  static class MergedQuantizedVectorValues extends QuantizedByteVectorValues {
    public static MergedQuantizedVectorValues mergeQuantizedByteVectorValues(
        FieldInfo fieldInfo, MergeState mergeState, ScalarQuantizer scalarQuantizer)
        throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();

      List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        if (mergeState.knnVectorsReaders[i] != null
            && mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name) != null) {
          QuantizedVectorsReader reader =
              getQuantizedKnnVectorsReader(mergeState.knnVectorsReaders[i], fieldInfo.name);
          assert scalarQuantizer != null;
          final QuantizedByteVectorValueSub sub;
          // Either our quantization parameters are way different than the merged ones
          // Or we have never been quantized.
          if (reader == null
              || reader.getQuantizationState(fieldInfo.name) == null
              // For smaller `bits` values, we should always recalculate the quantiles
              // TODO: this is very conservative, could we reuse information for even int4
              // quantization?
              || scalarQuantizer.getBits() <= 4
              || shouldRequantize(reader.getQuantizationState(fieldInfo.name), scalarQuantizer)) {
            sub =
                new QuantizedByteVectorValueSub(
                    mergeState.docMaps[i],
                    new QuantizedFloatVectorValues(
                        mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name),
                        fieldInfo.getVectorSimilarityFunction(),
                        scalarQuantizer));
          } else {
            sub =
                new QuantizedByteVectorValueSub(
                    mergeState.docMaps[i],
                    new OffsetCorrectedQuantizedByteVectorValues(
                        reader.getQuantizedVectorValues(fieldInfo.name),
                        fieldInfo.getVectorSimilarityFunction(),
                        scalarQuantizer,
                        reader.getQuantizationState(fieldInfo.name)));
          }
          subs.add(sub);
        }
      }
      return new MergedQuantizedVectorValues(subs, mergeState);
    }

    private final List<QuantizedByteVectorValueSub> subs;
    private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
    private final int size;

    private int docId;
    private QuantizedByteVectorValueSub current;

    private MergedQuantizedVectorValues(
        List<QuantizedByteVectorValueSub> subs, MergeState mergeState) throws IOException {
      this.subs = subs;
      docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
      int totalSize = 0;
      for (QuantizedByteVectorValueSub sub : subs) {
        totalSize += sub.values.size();
      }
      size = totalSize;
      docId = -1;
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return current.values.vectorValue();
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      current = docIdMerger.next();
      if (current == null) {
        docId = NO_MORE_DOCS;
      } else {
        docId = current.mappedDocID;
      }
      return docId;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int dimension() {
      return subs.get(0).values.dimension();
    }

    @Override
    public float getScoreCorrectionConstant() throws IOException {
      return current.values.getScoreCorrectionConstant();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
    private final FloatVectorValues values;
    private final ScalarQuantizer quantizer;
    private final byte[] quantizedVector;
    private final float[] normalizedVector;
    private float offsetValue = 0f;

    private final VectorSimilarityFunction vectorSimilarityFunction;

    public QuantizedFloatVectorValues(
        FloatVectorValues values,
        VectorSimilarityFunction vectorSimilarityFunction,
        ScalarQuantizer quantizer) {
      this.values = values;
      this.quantizer = quantizer;
      this.quantizedVector = new byte[values.dimension()];
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
        this.normalizedVector = new float[values.dimension()];
      } else {
        this.normalizedVector = null;
      }
    }

    @Override
    public float getScoreCorrectionConstant() {
      return offsetValue;
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
    public byte[] vectorValue() throws IOException {
      return quantizedVector;
    }

    @Override
    public int docID() {
      return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = values.nextDoc();
      if (doc != NO_MORE_DOCS) {
        quantize();
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        quantize();
      }
      return doc;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void quantize() throws IOException {
      if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
        System.arraycopy(values.vectorValue(), 0, normalizedVector, 0, normalizedVector.length);
        VectorUtil.l2normalize(normalizedVector);
        offsetValue =
            quantizer.quantize(normalizedVector, quantizedVector, vectorSimilarityFunction);
      } else {
        offsetValue =
            quantizer.quantize(values.vectorValue(), quantizedVector, vectorSimilarityFunction);
      }
    }
  }

  static final class ScalarQuantizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {

    private final RandomVectorScorerSupplier supplier;
    private final Closeable onClose;
    private final int numVectors;

    ScalarQuantizedCloseableRandomVectorScorerSupplier(
        Closeable onClose, int numVectors, RandomVectorScorerSupplier supplier) {
      this.onClose = onClose;
      this.supplier = supplier;
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

  static final class OffsetCorrectedQuantizedByteVectorValues extends QuantizedByteVectorValues {

    private final QuantizedByteVectorValues in;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final ScalarQuantizer scalarQuantizer, oldScalarQuantizer;

    OffsetCorrectedQuantizedByteVectorValues(
        QuantizedByteVectorValues in,
        VectorSimilarityFunction vectorSimilarityFunction,
        ScalarQuantizer scalarQuantizer,
        ScalarQuantizer oldScalarQuantizer) {
      this.in = in;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      this.scalarQuantizer = scalarQuantizer;
      this.oldScalarQuantizer = oldScalarQuantizer;
    }

    @Override
    public float getScoreCorrectionConstant() throws IOException {
      return scalarQuantizer.recalculateCorrectiveOffset(
          in.vectorValue(), oldScalarQuantizer, vectorSimilarityFunction);
    }

    @Override
    public int dimension() {
      return in.dimension();
    }

    @Override
    public int size() {
      return in.size();
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return in.vectorValue();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
