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

package org.apache.lucene.codecs.lucene98;

import static org.apache.lucene.codecs.lucene98.Lucene98ScalarQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes quantized vector values and metadata to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene98ScalarQuantizedVectorsWriter implements Accountable, Closeable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene98ScalarQuantizedVectorsWriter.class);

  private static final float REQUANTIZATION_ERROR_RATE = 1e-5f;
  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, quantizedVectorData;
  private final int quantile;
  private final List<QuantizationVectorWriter> fields = new ArrayList<>();

  private boolean finished;

  Lucene98ScalarQuantizedVectorsWriter(SegmentWriteState state, int quantile) throws IOException {
    this.quantile = quantile;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_META_EXTENSION);

    String quantizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      quantizedVectorData =
          state.directory.createOutput(quantizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene98ScalarQuantizedVectorsFormat.META_CODEC_NAME,
          Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          quantizedVectorData,
          Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_DATA_CODEC_NAME,
          Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  public QuantizationVectorWriter addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("Only float32 vector fields are supported");
    }
    QuantizationVectorWriter newField = QuantizationVectorWriter.create(fieldInfo, quantile);
    fields.add(newField);
    return QuantizationVectorWriter.create(fieldInfo, quantile);
  }

  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (QuantizationVectorWriter field : fields) {
      field.finish();
      if (sortMap == null) {
        writeField(field, maxDoc);
      } else {
        writeSortingField(field, maxDoc, sortMap);
      }
    }
  }

  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (quantizedVectorData != null) {
      CodecUtil.writeFooter(quantizedVectorData);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = BASE_RAM_BYTES_USED;
    for (QuantizationVectorWriter field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(QuantizationVectorWriter fieldData, int maxDoc) throws IOException {
    long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long quantizedVectorDataLength =
        quantizedVectorData.getFilePointer() - quantizedVectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        quantizedVectorDataOffset,
        quantizedVectorDataLength,
        fieldData.getMinQuantile(),
        fieldData.getMaxQuantile(),
        fieldData.docsWithField);
  }

  private void writeQuantizedVectors(QuantizationVectorWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (float[] v : fieldData.floatVectors) {
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection =
          scalarQuantizer.calculateVectorOffset(vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
    }
  }

  private QuantizationState writeSortingField(
      QuantizationVectorWriter fieldData, int maxDoc, Sorter.DocMap sortMap) throws IOException {
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
    long quantizedVectorDataOffset = writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        quantizedVectorDataOffset,
        quantizedVectorLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        newDocsWithField);
    return new QuantizationState(fieldData.minQuantile, fieldData.maxQuantile);
  }

  private long writeSortedQuantizedVectors(QuantizationVectorWriter fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (int ordinal : ordMap) {
      float[] v = fieldData.floatVectors.get(ordinal);
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection =
          scalarQuantizer.calculateVectorOffset(vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
    }
    return vectorDataOffset;
  }

  public void mergeOneField(
      FieldInfo fieldInfo, MergeState mergeState, FloatVectorValues floatVectorValues)
      throws IOException {
    final long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempQuantizedVectorData =
        segmentWriteState.directory.createTempOutput(
            quantizedVectorData.getName(), "temp", segmentWriteState.context);
    IndexInput quantizedVectorDataInput = null;
    boolean success = false;
    try {
      RequantizationState requantizationState = requiresReQuantization(mergeState, fieldInfo);
      final float[] minMaxQuantiles;
      final QuantizedByteVectorValues quantizedByteVectorValues;
      if (requantizationState.requiresRequantization) {
        ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
        quantizedByteVectorValues =
            new QuantizedFloatVectorValues(
                floatVectorValues, fieldInfo.getVectorSimilarityFunction(), quantizer);
        minMaxQuantiles = new float[] {quantizer.getLowerQuantile(), quantizer.getUpperQuantile()};
      } else {
        minMaxQuantiles =
            new float[] {requantizationState.lowerQuantile, requantizationState.upperQuantile};
        quantizedByteVectorValues =
            MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(fieldInfo, mergeState);
      }
      DocsWithFieldSet docsWithField =
          writeQuantizedVectorData(tempQuantizedVectorData, quantizedByteVectorValues);
      CodecUtil.writeFooter(tempQuantizedVectorData);
      IOUtils.close(tempQuantizedVectorData);
      // copy the temporary file vectors to the actual data file
      quantizedVectorDataInput =
          segmentWriteState.directory.openInput(
              tempQuantizedVectorData.getName(), segmentWriteState.context);
      quantizedVectorData.copyBytes(
          quantizedVectorDataInput, quantizedVectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(quantizedVectorDataInput);
      final long quantizedVectorDataLength =
          quantizedVectorDataInput.getFilePointer() - quantizedVectorDataOffset;

      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          quantizedVectorDataOffset,
          quantizedVectorDataLength,
          minMaxQuantiles[0],
          minMaxQuantiles[1],
          docsWithField);
      success = true;
    } finally {
      IOUtils.close(quantizedVectorDataInput);
      if (success) {
        segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
      } else {
        IOUtils.closeWhileHandlingException(tempQuantizedVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempQuantizedVectorData.getName());
      }
    }
  }

  private RequantizationState requiresReQuantization(MergeState mergeState, FieldInfo fieldInfo) {
    float[] minQuantiles = new float[mergeState.liveDocs.length];
    float[] maxQuantiles = new float[mergeState.liveDocs.length];
    float minQuantile = Float.POSITIVE_INFINITY;
    float maxQuantile = Float.NEGATIVE_INFINITY;
    boolean mustRequantize = false;
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      KnnVectorsReader currKnnVectorsReader = mergeState.knnVectorsReaders[i];
      if (mergeState.knnVectorsReaders[i]
          instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
        currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
      }
      if (currKnnVectorsReader instanceof QuantizedKnnVectorsReader reader) {
        QuantizationState quantizationState = reader.getQuantizationState(fieldInfo.name);
        minQuantiles[i] = quantizationState.getLowerQuantile();
        maxQuantiles[i] = quantizationState.getUpperQuantile();
        minQuantile = Math.min(minQuantile, quantizationState.getLowerQuantile());
        maxQuantile = Math.min(maxQuantile, quantizationState.getUpperQuantile());
        for (int j = 0; j < i && mustRequantize == false; j++) {
          mustRequantize =
              Math.abs(quantizationState.getLowerQuantile() - minQuantiles[j])
                      <= REQUANTIZATION_ERROR_RATE
                  || Math.abs(quantizationState.getUpperQuantile() - maxQuantiles[j])
                      <= REQUANTIZATION_ERROR_RATE;
        }
        if (mustRequantize) {
          return new RequantizationState(Float.NaN, Float.NaN, true);
        }
      } else {
        throw new IllegalArgumentException(
            "attempting to merge in unknown codec ["
                + currKnnVectorsReader.toString()
                + "] for field ["
                + fieldInfo.name
                + "]");
      }
    }
    return new RequantizationState(minQuantile, maxQuantile, false);
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      float lowerQuantile,
      float upperQuantile,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(quantizedVectorDataOffset);
    meta.writeVLong(quantizedVectorDataLength);
    meta.writeVInt(field.getVectorDimension());
    meta.writeInt(Float.floatToIntBits(lowerQuantile));
    meta.writeInt(Float.floatToIntBits(upperQuantile));

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    if (count == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (count == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = quantizedVectorData.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(
              docsWithField.iterator(), quantizedVectorData, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(quantizedVectorData.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);

      // write ordToDoc mapping
      long start = quantizedVectorData.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      // dense case and empty case do not need to store ordToMap mapping
      final DirectMonotonicWriter ordToDocWriter =
          DirectMonotonicWriter.getInstance(
              meta, quantizedVectorData, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
      DocIdSetIterator iterator = docsWithField.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ordToDocWriter.add(doc);
      }
      ordToDocWriter.finish();
      meta.writeLong(quantizedVectorData.getFilePointer() - start);
    }
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
      assert binaryValue.length
          == quantizedByteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      output.writeInt(Float.floatToIntBits(quantizedByteVectorValues.getScoreCorrectionConstant()));
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, quantizedVectorData);
  }

  static class QuantizationVectorWriter implements Accountable {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<float[]> floatVectors;
    private final boolean normalize;
    private final int quantile;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private boolean finished;

    private int lastDocID = -1;

    static QuantizationVectorWriter create(FieldInfo fieldInfo, int quantile) {
      return new QuantizationVectorWriter(
          fieldInfo, quantile, fieldInfo.getVectorSimilarityFunction());
    }

    QuantizationVectorWriter(
        FieldInfo fieldInfo, int quantile, VectorSimilarityFunction vectorSimilarityFunction) {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.quantile = quantile;
      this.normalize = vectorSimilarityFunction == VectorSimilarityFunction.COSINE;
      this.docsWithField = new DocsWithFieldSet();
      this.floatVectors = new ArrayList<>();
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    void finish() throws IOException {
      if (finished) {
        return;
      }
      if (quantile == 100 || floatVectors.size() == 0) {
        finished = true;
        return;
      }
      ScalarQuantizer quantizer =
          ScalarQuantizer.fromVectors(new FloatVectorWrapper(floatVectors), quantile);
      minQuantile = quantizer.getLowerQuantile();
      maxQuantile = quantizer.getUpperQuantile();
      finished = true;
    }

    public void addValue(int docID, float[] vectorValue) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      float[] copy = ArrayUtil.copyOfSubArray(vectorValue, 0, dim);
      if (normalize) {
        // vectorize?
        VectorUtil.l2normalize(copy);
      }
      if (quantile == 100) {
        for (float v : copy) {
          minQuantile = Math.min(v, minQuantile);
          maxQuantile = Math.max(v, maxQuantile);
        }
      }
      floatVectors.add(copy);
      lastDocID = docID;
    }

    float getMinQuantile() {
      return minQuantile;
    }

    float getMaxQuantile() {
      return maxQuantile;
    }

    private ScalarQuantizer createQuantizer() throws IOException {
      assert finished;
      return new ScalarQuantizer(new float[] {minQuantile, maxQuantile});
    }

    @Override
    public long ramBytesUsed() {
      if (floatVectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
          + Integer.BYTES
          + (long) floatVectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
    }

    public float[] copyValue(float[] value) {
      return ArrayUtil.copyOfSubArray(value, 0, dim);
    }
  }

  private static class FloatVectorWrapper extends FloatVectorValues {
    private final List<float[]> vectorList;
    protected int curDoc = -1;

    private FloatVectorWrapper(List<float[]> vectorList) {
      this.vectorList = vectorList;
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

  private static class RequantizationState {
    private final float lowerQuantile;
    private final float upperQuantile;
    private final boolean requiresRequantization;

    private RequantizationState(
        float lowerQuantile, float upperQuantile, boolean requiresRequantization) {
      this.lowerQuantile = lowerQuantile;
      this.upperQuantile = upperQuantile;
      this.requiresRequantization = requiresRequantization;
    }
  }

  private static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {

    final QuantizedByteVectorValues values;

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
    public static QuantizedByteVectorValues mergeQuantizedByteVectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as ["
                + fieldInfo.getVectorEncoding()
                + "] as quantized BYTE");
      }
      List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader
            instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldKnnVectorsFormat) {
          knnVectorsReader = perFieldKnnVectorsFormat.getFieldReader(fieldInfo.name);
        }
        if (knnVectorsReader instanceof QuantizedKnnVectorsReader reader) {
          subs.add(
              new QuantizedByteVectorValueSub(
                  mergeState.docMaps[i], reader.getQuantizedVectorValues(fieldInfo.name)));
        } else if (knnVectorsReader != null) {
          throw new UnsupportedOperationException(
              "Cannot merge vectors from codec other than Lucene98QuantizedHnswVectorsFormat");
        }
      }
      return new MergedQuantizedVectorValues(subs, mergeState);
    }

    private final List<QuantizedByteVectorValueSub> subs;
    private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
    private final int size;

    private int docId;
    QuantizedByteVectorValueSub current;

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
    float getScoreCorrectionConstant() {
      return current.values.getScoreCorrectionConstant();
    }
  }

  private class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
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
        quantizer.quantizeTo(values.vectorValue(), quantizedVector);
        offsetValue = quantizer.calculateVectorOffset(quantizedVector, vectorSimilarityFunction);
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = values.advance(target);
      if (doc != NO_MORE_DOCS) {
        quantizer.quantizeTo(values.vectorValue(), quantizedVector);
        offsetValue = quantizer.calculateVectorOffset(quantizedVector, vectorSimilarityFunction);
      }
      return doc;
    }
  }
}
