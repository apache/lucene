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

import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile;
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
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Writes quantized vector values and metadata to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarQuantizedVectorsWriter implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
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
  private final IndexOutput quantizedVectorData;
  private final Float quantile;
  private boolean finished;

  Lucene99ScalarQuantizedVectorsWriter(IndexOutput quantizedVectorData, Float quantile) {
    this.quantile = quantile;
    this.quantizedVectorData = quantizedVectorData;
  }

  QuantizationFieldVectorWriter addField(FieldInfo fieldInfo, InfoStream infoStream) {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "Only float32 vector fields are supported for quantization");
    }
    float quantile =
        this.quantile == null
            ? calculateDefaultQuantile(fieldInfo.getVectorDimension())
            : this.quantile;
    if (infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
      infoStream.message(
          QUANTIZED_VECTOR_COMPONENT,
          "quantizing field="
              + fieldInfo.name
              + " dimension="
              + fieldInfo.getVectorDimension()
              + " quantile="
              + quantile);
    }
    return QuantizationFieldVectorWriter.create(fieldInfo, quantile, infoStream);
  }

  long[] flush(
      Sorter.DocMap sortMap, QuantizationFieldVectorWriter field, DocsWithFieldSet docsWithField)
      throws IOException {
    field.finish();
    return sortMap == null ? writeField(field) : writeSortingField(field, sortMap, docsWithField);
  }

  void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (quantizedVectorData != null) {
      CodecUtil.writeFooter(quantizedVectorData);
    }
  }

  private long[] writeField(QuantizationFieldVectorWriter fieldData) throws IOException {
    long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long quantizedVectorDataLength =
        quantizedVectorData.getFilePointer() - quantizedVectorDataOffset;
    return new long[] {quantizedVectorDataOffset, quantizedVectorDataLength};
  }

  private void writeQuantizedVectors(QuantizationFieldVectorWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[fieldData.dim];
    final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float[] v : fieldData.floatVectors) {
      float offsetCorrection =
          scalarQuantizer.quantize(v, vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeBytes(vector, vector.length);
      offsetBuffer.putFloat(offsetCorrection);
      quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  private long[] writeSortingField(
      QuantizationFieldVectorWriter fieldData,
      Sorter.DocMap sortMap,
      DocsWithFieldSet docsWithField)
      throws IOException {
    final int[] docIdOffsets = new int[sortMap.size()];
    int offset = 1; // 0 means no vector for this (field, document)
    DocIdSetIterator iterator = docsWithField.iterator();
    for (int docID = iterator.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = iterator.nextDoc()) {
      int newDocID = sortMap.oldToNew(docID);
      docIdOffsets[newDocID] = offset++;
    }
    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    final int[] ordMap = new int[offset - 1]; // new ord to old ord
    final int[] oldOrdMap = new int[offset - 1]; // old ord to new ord
    int ord = 0;
    int doc = 0;
    for (int docIdOffset : docIdOffsets) {
      if (docIdOffset != 0) {
        ordMap[ord] = docIdOffset - 1;
        oldOrdMap[docIdOffset - 1] = ord;
        newDocsWithField.add(doc);
        ord++;
      }
      doc++;
    }

    // write vector values
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

    return new long[] {vectorDataOffset, quantizedVectorLength};
  }

  void writeSortedQuantizedVectors(QuantizationFieldVectorWriter fieldData, int[] ordMap)
      throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    byte[] vector = new byte[fieldData.dim];
    final ByteBuffer offsetBuffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] v = fieldData.floatVectors.get(ordinal);
      float offsetCorrection =
          scalarQuantizer.quantize(v, vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeBytes(vector, vector.length);
      offsetBuffer.putFloat(offsetCorrection);
      quantizedVectorData.writeBytes(offsetBuffer.array(), offsetBuffer.array().length);
      offsetBuffer.rewind();
    }
  }

  ScalarQuantizer mergeQuantiles(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
    float quantile =
        this.quantile == null
            ? calculateDefaultQuantile(fieldInfo.getVectorDimension())
            : this.quantile;
    return mergeAndRecalculateQuantiles(mergeState, fieldInfo, quantile);
  }

  ScalarQuantizedCloseableRandomVectorScorerSupplier mergeOneField(
      SegmentWriteState segmentWriteState,
      FieldInfo fieldInfo,
      MergeState mergeState,
      ScalarQuantizer mergedQuantizationState)
      throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
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
          writeQuantizedVectorData(tempQuantizedVectorData, byteVectorValues);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      quantizationDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      quantizedVectorData.copyBytes(
          quantizationDataInput, quantizationDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(quantizationDataInput);
      success = true;
      final IndexInput finalQuantizationDataInput = quantizationDataInput;
      return new ScalarQuantizedCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalQuantizationDataInput);
            segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
          },
          new ScalarQuantizedRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              mergedQuantizationState,
              new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
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

  static ScalarQuantizer mergeQuantiles(
      List<ScalarQuantizer> quantizationStates, List<Integer> segmentSizes, float quantile) {
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
    }
    lowerQuantile /= totalCount;
    upperQuantile /= totalCount;
    return new ScalarQuantizer(lowerQuantile, upperQuantile, quantile);
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
    if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
      vectorsReader = candidateReader.getFieldReader(fieldName);
    }
    if (vectorsReader instanceof QuantizedVectorsReader reader) {
      return reader;
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

  static ScalarQuantizer mergeAndRecalculateQuantiles(
      MergeState mergeState, FieldInfo fieldInfo, float quantile) throws IOException {
    List<ScalarQuantizer> quantizationStates = new ArrayList<>(mergeState.liveDocs.length);
    List<Integer> segmentSizes = new ArrayList<>(mergeState.liveDocs.length);
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
    ScalarQuantizer mergedQuantiles = mergeQuantiles(quantizationStates, segmentSizes, quantile);
    // Segments no providing quantization state indicates that their quantiles were never
    // calculated.
    // To be safe, we should always recalculate given a sample set over all the float vectors in the
    // merged
    // segment view
    if (mergedQuantiles == null || shouldRecomputeQuantiles(mergedQuantiles, quantizationStates)) {
      FloatVectorValues vectorValues =
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      mergedQuantiles = ScalarQuantizer.fromVectors(vectorValues, quantile);
    }
    return mergedQuantiles;
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
  private static DocsWithFieldSet writeQuantizedVectorData(
      IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = quantizedByteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = quantizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue();
      assert binaryValue.length == quantizedByteVectorValues.dimension()
          : "dim=" + quantizedByteVectorValues.dimension() + " len=" + binaryValue.length;
      output.writeBytes(binaryValue, binaryValue.length);
      output.writeInt(Float.floatToIntBits(quantizedByteVectorValues.getScoreCorrectionConstant()));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED;
  }

  static class QuantizationFieldVectorWriter implements Accountable {
    private static final long SHALLOW_SIZE =
        shallowSizeOfInstance(QuantizationFieldVectorWriter.class);
    private final int dim;
    private final List<float[]> floatVectors;
    private final boolean normalize;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final float quantile;
    private final InfoStream infoStream;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;
    private boolean finished;

    static QuantizationFieldVectorWriter create(
        FieldInfo fieldInfo, float quantile, InfoStream infoStream) {
      return new QuantizationFieldVectorWriter(
          fieldInfo.getVectorDimension(),
          quantile,
          fieldInfo.getVectorSimilarityFunction(),
          infoStream);
    }

    QuantizationFieldVectorWriter(
        int dim,
        float quantile,
        VectorSimilarityFunction vectorSimilarityFunction,
        InfoStream infoStream) {
      this.dim = dim;
      this.quantile = quantile;
      this.normalize = vectorSimilarityFunction == VectorSimilarityFunction.COSINE;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      this.floatVectors = new ArrayList<>();
      this.infoStream = infoStream;
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
          ScalarQuantizer.fromVectors(new FloatVectorWrapper(floatVectors, normalize), quantile);
      minQuantile = quantizer.getLowerQuantile();
      maxQuantile = quantizer.getUpperQuantile();
      if (infoStream.isEnabled(QUANTIZED_VECTOR_COMPONENT)) {
        infoStream.message(
            QUANTIZED_VECTOR_COMPONENT,
            "quantized field="
                + " dimension="
                + dim
                + " quantile="
                + quantile
                + " minQuantile="
                + minQuantile
                + " maxQuantile="
                + maxQuantile);
      }
      finished = true;
    }

    public void addValue(float[] vectorValue) throws IOException {
      floatVectors.add(vectorValue);
    }

    float getMinQuantile() {
      assert finished;
      return minQuantile;
    }

    float getMaxQuantile() {
      assert finished;
      return maxQuantile;
    }

    float getQuantile() {
      return quantile;
    }

    ScalarQuantizer createQuantizer() {
      assert finished;
      return new ScalarQuantizer(minQuantile, maxQuantile, quantile);
    }

    @Override
    public long ramBytesUsed() {
      if (floatVectors.size() == 0) return SHALLOW_SIZE;
      return SHALLOW_SIZE + (long) floatVectors.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
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
  }

  private static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {
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
    float getScoreCorrectionConstant() throws IOException {
      return current.values.getScoreCorrectionConstant();
    }
  }

  private static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
    private final FloatVectorValues values;
    private final ScalarQuantizer quantizer;
    private final byte[] quantizedVector;
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
    }

    @Override
    float getScoreCorrectionConstant() {
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
        offsetValue =
            quantizer.quantize(values.vectorValue(), quantizedVector, vectorSimilarityFunction);
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        offsetValue =
            quantizer.quantize(values.vectorValue(), quantizedVector, vectorSimilarityFunction);
      }
      return doc;
    }
  }

  static final class ScalarQuantizedCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {

    private final ScalarQuantizedRandomVectorScorerSupplier supplier;
    private final Closeable onClose;

    ScalarQuantizedCloseableRandomVectorScorerSupplier(
        Closeable onClose, ScalarQuantizedRandomVectorScorerSupplier supplier) {
      this.onClose = onClose;
      this.supplier = supplier;
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
  }

  private static final class OffsetCorrectedQuantizedByteVectorValues
      extends QuantizedByteVectorValues {

    private final QuantizedByteVectorValues in;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final ScalarQuantizer scalarQuantizer, oldScalarQuantizer;

    private OffsetCorrectedQuantizedByteVectorValues(
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
    float getScoreCorrectionConstant() throws IOException {
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
  }
}
