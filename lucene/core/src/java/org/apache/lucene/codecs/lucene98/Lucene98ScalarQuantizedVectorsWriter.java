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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.apache.lucene.codecs.lucene98.Lucene98ScalarQuantizedVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene98ScalarQuantizedVectorsWriter {

  private final static float REQUANTIZATION_ERROR_RATE = 1e-5f;
  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, quantizedVectorData;
  private final int quantile;

  private final List<FieldWriter> fields = new ArrayList<>();
  private boolean finished;

  Lucene98ScalarQuantizedVectorsWriter(SegmentWriteState state, int quantile) throws IOException {
    this.quantile = quantile;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene98ScalarQuantizedVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);

    String quantizedVectorDataFileName =
            IndexFileNames.segmentFileName(
                    state.segmentInfo.name,
                    state.segmentSuffix,
                    Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      quantizedVectorData = state.directory.createOutput(quantizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene98ScalarQuantizedVectorsFormat.META_CODEC_NAME,
          Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene98ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
              quantizedVectorData,
              Lucene98ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
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

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("Only float32 vector fields are supported");
    }
    FieldWriter newField = FieldWriter.create(fieldInfo, quantile);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter field : fields) {
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
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter fieldData, int maxDoc) throws IOException {
    // write raw vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    writeFloat32Vectors(fieldData);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;


    long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long quantizedVectorDataLength = quantizedVectorData.getFilePointer() - quantizedVectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        quantizedVectorDataOffset,
        quantizedVectorDataLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        fieldData.docsWithField);
  }

  private void writeFloat32Vectors(FieldWriter fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float[] v : fieldData.floatVectors) {
      buffer.asFloatBuffer().put(v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (float[] v : fieldData.floatVectors) {
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection = scalarQuantizer.calculateVectorOffset(vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
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
    long vectorDataOffset = writeSortedFloat32Vectors(fieldData, ordMap);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    long quantizedVectorDataOffset = writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        quantizedVectorDataOffset,
        quantizedVectorLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        newDocsWithField);
  }

  private long writeSortedFloat32Vectors(FieldWriter fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = fieldData.floatVectors.get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
    return vectorDataOffset;
  }

  private long writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (int ordinal : ordMap) {
      float[] v = fieldData.floatVectors.get(ordinal);
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection = scalarQuantizer.calculateVectorOffset(vector, fieldData.vectorSimilarityFunction);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
    }
    return vectorDataOffset;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    final long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexOutput tempQuantizedVectorData =
            segmentWriteState.directory.createTempOutput(
                    quantizedVectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    IndexInput quantizedVectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      DocsWithFieldSet docsWithField = writeVectorData(
              tempVectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);
      // copy the temporary file vectors to the actual data file
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), segmentWriteState.context);
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);
      final long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      int byteSize = fieldInfo.getVectorDimension() * VectorEncoding.FLOAT32.byteSize;
      OffHeapFloatVectorValues.DenseOffHeapVectorValues floatVectorValues =
              new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                      fieldInfo.getVectorDimension(),
                      docsWithField.cardinality(),
                      vectorDataInput,
                      byteSize);
      RequantizationState requantizationState = requiresReQuantization(mergeState, fieldInfo);
      final float[] minMaxQuantiles;
      final QuantizedByteVectorValues quantizedByteVectorValues;
      if (requantizationState.requiresRequantization) {
        FloatVectorValues values = floatVectorValues.copy();
        ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(values, quantile);
        quantizedByteVectorValues = new QuantizedFloatVectorValues(
                floatVectorValues.copy(),
                fieldInfo.getVectorSimilarityFunction(),
                quantizer
        );
        writeQuantizedVectorData(
                tempQuantizedVectorData,
                quantizedByteVectorValues
        );
        CodecUtil.writeFooter(tempVectorData);
        IOUtils.close(tempVectorData);
        minMaxQuantiles = new float[] {quantizer.getLowerQuantile(), quantizer.getUpperQuantile()};
      } else {
        minMaxQuantiles = new float[] {requantizationState.lowerQuantile, requantizationState.upperQuantile};
        quantizedByteVectorValues = MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(fieldInfo, mergeState);
        CodecUtil.writeFooter(tempVectorData);
        IOUtils.close(tempVectorData);
      }
      writeQuantizedVectorData(
              tempQuantizedVectorData,
              quantizedByteVectorValues
      );
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);
      // copy the temporary file vectors to the actual data file
      quantizedVectorDataInput =
              segmentWriteState.directory.openInput(
                      tempQuantizedVectorData.getName(), segmentWriteState.context);
      quantizedVectorData.copyBytes(quantizedVectorDataInput, quantizedVectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(quantizedVectorDataInput);
      final long quantizedVectorDataLength = quantizedVectorDataInput.getFilePointer() - quantizedVectorDataOffset;

      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          quantizedVectorDataOffset,
          quantizedVectorDataLength,
          minMaxQuantiles[0],
          minMaxQuantiles[1],
          docsWithField);
      success = true;
    } finally {
      IOUtils.close(vectorDataInput, quantizedVectorDataInput);
      if (success) {
        segmentWriteState.directory.deleteFile(tempVectorData.getName());
        segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
      } else {
        IOUtils.closeWhileHandlingException(tempVectorData, tempQuantizedVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName(), tempQuantizedVectorData.getName());
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
      if (currKnnVectorsReader instanceof Lucene98ScalarQuantizedVectorsReader reader) {
        float[] minAndMaxQuantiles = reader.getQuantiles(fieldInfo.name);
        minQuantiles[i] = minAndMaxQuantiles[0];
        maxQuantiles[i] = minAndMaxQuantiles[1];
        minQuantile = Math.min(minQuantile, minAndMaxQuantiles[0]);
        maxQuantile = Math.min(maxQuantile, minAndMaxQuantiles[1]);
        for (int j = 0; j < i && mustRequantize == false; j++) {
          mustRequantize =
                  Math.abs(minAndMaxQuantiles[0] - minQuantiles[j]) <= REQUANTIZATION_ERROR_RATE
                  || Math.abs(minAndMaxQuantiles[1] - maxQuantiles[j]) <= REQUANTIZATION_ERROR_RATE;
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

  private Map<Integer, Integer> getOldToNewOrdinalMap(
      MergeState mergeState, FieldInfo fieldInfo, int initializerIndex) throws IOException {

    DocIdSetIterator initializerIterator = mergeState.knnVectorsReaders[initializerIndex].getFloatVectorValues(fieldInfo.name);

    MergeState.DocMap initializerDocMap = mergeState.docMaps[initializerIndex];

    Map<Integer, Integer> newIdToOldOrdinal = new HashMap<>();
    int oldOrd = 0;
    int maxNewDocID = -1;
    for (int oldId = initializerIterator.nextDoc();
        oldId != NO_MORE_DOCS;
        oldId = initializerIterator.nextDoc()) {
      if (isCurrentVectorNull(initializerIterator)) {
        continue;
      }
      int newId = initializerDocMap.get(oldId);
      maxNewDocID = Math.max(newId, maxNewDocID);
      newIdToOldOrdinal.put(newId, oldOrd);
      oldOrd++;
    }

    if (maxNewDocID == -1) {
      return Collections.emptyMap();
    }

    Map<Integer, Integer> oldToNewOrdinalMap = new HashMap<>();

    DocIdSetIterator vectorIterator = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);

    int newOrd = 0;
    for (int newDocId = vectorIterator.nextDoc();
        newDocId <= maxNewDocID;
        newDocId = vectorIterator.nextDoc()) {
      if (isCurrentVectorNull(vectorIterator)) {
        continue;
      }

      if (newIdToOldOrdinal.containsKey(newDocId)) {
        oldToNewOrdinalMap.put(newIdToOldOrdinal.get(newDocId), newOrd);
      }
      newOrd++;
    }

    return oldToNewOrdinalMap;
  }

  private boolean isCurrentVectorNull(DocIdSetIterator docIdSetIterator) throws IOException {
    if (docIdSetIterator instanceof FloatVectorValues) {
      return ((FloatVectorValues) docIdSetIterator).vectorValue() == null;
    }

    if (docIdSetIterator instanceof ByteVectorValues) {
      return ((ByteVectorValues) docIdSetIterator).vectorValue() == null;
    }

    return true;
  }

  private boolean allMatch(Bits bits) {
    if (bits == null) {
      return true;
    }

    for (int i = 0; i < bits.length(); i++) {
      if (!bits.get(i)) {
        return false;
      }
    }
    return true;
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      float lowerQuantile,
      float upperQuantile,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
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
      long offset = vectorData.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(
              docsWithField.iterator(), vectorData, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(vectorData.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);

      // write ordToDoc mapping
      long start = vectorData.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      // dense case and empty case do not need to store ordToMap mapping
      final DirectMonotonicWriter ordToDocWriter =
          DirectMonotonicWriter.getInstance(meta, vectorData, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
      DocIdSetIterator iterator = docsWithField.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ordToDocWriter.add(doc);
      }
      ordToDocWriter.finish();
      meta.writeLong(vectorData.getFilePointer() - start);
    }
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  private static DocsWithFieldSet writeVectorData(
      IndexOutput output, FloatVectorValues floatVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    ByteBuffer buffer =
        ByteBuffer.allocate(floatVectorValues.dimension() * VectorEncoding.FLOAT32.byteSize)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      // write vector
      float[] value = floatVectorValues.vectorValue();
      buffer.asFloatBuffer().put(value);
      output.writeBytes(buffer.array(), buffer.limit());
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  private static void writeQuantizedVectorData(
          IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    for (int docV = quantizedByteVectorValues.nextDoc();
         docV != NO_MORE_DOCS;
         docV = quantizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue();
      assert binaryValue.length == quantizedByteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      output.writeInt(Float.floatToIntBits(quantizedByteVectorValues.getScoreCorrectionConstant()));
    }
  }


  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, quantizedVectorData);
  }

  private static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<float[]> floatVectors;
    private final boolean normalize;
    private final int quantile;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    private int lastDocID = -1;
    private int node = 0;

    static FieldWriter create(FieldInfo fieldInfo, int quantile)
        throws IOException {
      return new FieldWriter(
              fieldInfo,
              quantile,
              fieldInfo.getVectorSimilarityFunction()
      );
    }

    FieldWriter(FieldInfo fieldInfo, int quantile, VectorSimilarityFunction vectorSimilarityFunction) {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.quantile = quantile;
      this.normalize = vectorSimilarityFunction == VectorSimilarityFunction.COSINE;
      this.docsWithField = new DocsWithFieldSet();
      this.floatVectors = new ArrayList<>();
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      float[] copy = copyValue(vectorValue);
      if (normalize) {
        // vectorize?
        VectorUtil.l2normalize(copy);
      }
      if (quantile == 100) {
        // vectorize?
        for (float v : copy) {
          minQuantile = Math.min(v, minQuantile);
          maxQuantile = Math.max(v, maxQuantile);
        }
      }
      floatVectors.add(copy);
      node++;
      lastDocID = docID;
    }

    private ScalarQuantizer createQuantizer() throws IOException {
      if (quantile == 100) {
        return new ScalarQuantizer(new float[]{minQuantile, maxQuantile});
      } else {
        return ScalarQuantizer.fromVectors(new FloatVectorWrapper(floatVectors), quantile);
      }
    }

    @Override
    public long ramBytesUsed() {
      if (floatVectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
              + Integer.BYTES
          + (long) floatVectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
    }

    @Override
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

    private RequantizationState(float lowerQuantile, float upperQuantile, boolean requiresRequantization) {
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
                  "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as quantized BYTE");
        }
        List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
          KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldKnnVectorsFormat) {
            knnVectorsReader = perFieldKnnVectorsFormat.getFieldReader(fieldInfo.name);
          }
          if (knnVectorsReader instanceof Lucene98ScalarQuantizedVectorsReader reader) {
            subs.add(new QuantizedByteVectorValueSub(mergeState.docMaps[i], reader.getQuantizedVectorValues(fieldInfo.name)));
          } else if (knnVectorsReader != null){
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

      private MergedQuantizedVectorValues(List<QuantizedByteVectorValueSub> subs, MergeState mergeState)
              throws IOException {
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

      public QuantizedFloatVectorValues(FloatVectorValues values, VectorSimilarityFunction vectorSimilarityFunction, ScalarQuantizer quantizer) {
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
