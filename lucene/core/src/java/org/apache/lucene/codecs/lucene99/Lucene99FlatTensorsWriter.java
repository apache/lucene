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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatTensorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FloatTensorValue;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene99.Lucene99FlatTensorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes tensor values to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatTensorsWriter extends FlatVectorsWriter {

  private static final long SHALLLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatTensorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, tensorData;
  private final FlatTensorsScorer tensorsScorer;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  // TODO: modify the FlatVectorsScorer to include tensors
  public Lucene99FlatTensorsWriter(SegmentWriteState state, FlatTensorsScorer scorer)
      throws IOException {
    super(null);
    tensorsScorer = scorer;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatTensorsFormat.META_EXTENSION);

    String tensorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatTensorsFormat.VECTOR_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      tensorData = state.directory.createOutput(tensorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99FlatTensorsFormat.META_CODEC_NAME,
          Lucene99FlatTensorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          tensorData,
          Lucene99FlatTensorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene99FlatTensorsFormat.VERSION_CURRENT,
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
  public FlatVectorsScorer getFlatVectorScorer() {
    throw new UnsupportedOperationException("Tensor writer does not support a vector scorer");
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(
      FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) throws IOException {
    FieldWriter<?> newField = FieldWriter.create(fieldInfo, indexWriter);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter<?> field : fields) {
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
    if (tensorData != null) {
      CodecUtil.writeFooter(tensorData);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLLOW_RAM_BYTES_USED;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter<?> fieldData, int maxDoc) throws IOException {
    long tensorDataOffset = tensorData.alignFilePointer(Float.BYTES);
    switch (fieldData.fieldInfo.getTensorEncoding()) {
      case BYTE -> writeByteTensors(fieldData);
      case FLOAT32 -> writeFloat32Tensors(fieldData);
    }
    long tensorDataLength = tensorData.getFilePointer() - tensorDataOffset;

    writeMeta(
        fieldData.fieldInfo, maxDoc, tensorDataOffset, tensorDataLength, fieldData.docsWithField, fieldData.dataOffsets);
  }

//  private void writeFloat32Vectors(FieldWriter<?> fieldData) throws IOException {
//    final ByteBuffer buffer =
//        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
//    for (Object v : fieldData.values) {
//      buffer.asFloatBuffer().put((float[]) v);
//      vectorData.writeBytes(buffer.array(), buffer.array().length);
//    }
//  }

  private void writeFloat32Tensors(FieldWriter<?> fieldData) throws IOException {
    int maxPackedTensorSize = 0;
    for (int i = 0; i < fieldData.values.size(); i++) {
      FloatTensorValue v = (FloatTensorValue) fieldData.values.get(i);
      maxPackedTensorSize = Integer.max(maxPackedTensorSize, v.vectorCount() * v.dimension());
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(maxPackedTensorSize * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);

    fieldData.dataOffsets = new long[fieldData.values.size() + 1];
    int ordinal = 0;
    for (int i = 0; i < fieldData.values.size(); i++) {
      float[] packedTensor = ((FloatTensorValue) fieldData.values.get(i)).packedValue();
      buffer.asFloatBuffer().put(packedTensor);
      fieldData.dataOffsets[ordinal++] = tensorData.getFilePointer();
      tensorData.writeBytes(buffer.array(), packedTensor.length * Float.BYTES);
      buffer.clear();
    }
    assert ordinal == fieldData.values.size();
    fieldData.dataOffsets[ordinal] = tensorData.getFilePointer();
  }

//  private void writeByteVectors(FieldWriter<?> fieldData) throws IOException {
//    for (Object v : fieldData.values) {
//      byte[] vector = (byte[]) v;
//      vectorData.writeBytes(vector, vector.length);
//    }
//  }

  private void writeByteTensors(FieldWriter<?> fieldData) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
//    for (Object v : fieldData.values) {
//      byte[] vector = (byte[]) v;
//      vectorData.writeBytes(vector, vector.length);
//    }
  }

  private void writeSortingField(FieldWriter<?> fieldData, int maxDoc, Sorter.DocMap sortMap)
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

    // write tensor values
    long tensorDataOffset;
    tensorDataOffset = switch (fieldData.fieldInfo.getTensorEncoding()) {
      case BYTE -> writeSortedByteTensors(fieldData, ordMap);
      case FLOAT32 -> writeSortedFloat32Tensors(fieldData, ordMap);
    };
    long tensorDataLength = tensorData.getFilePointer() - tensorDataOffset;

    writeMeta(fieldData.fieldInfo, maxDoc, tensorDataOffset, tensorDataLength, newDocsWithField, fieldData.dataOffsets);
  }

//  private long writeSortedFloat32Vectors(FieldWriter<?> fieldData, int[] ordMap)
//      throws IOException {
//    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
//    final ByteBuffer buffer =
//        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
//    for (int ordinal : ordMap) {
//      float[] vector = (float[]) fieldData.values.get(ordinal);
//      buffer.asFloatBuffer().put(vector);
//      vectorData.writeBytes(buffer.array(), buffer.array().length);
//    }
//    return vectorDataOffset;
//  }

//  private long writeSortedByteVectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
//    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
//    for (int ordinal : ordMap) {
//      byte[] vector = (byte[]) fieldData.values.get(ordinal);
//      vectorData.writeBytes(vector, vector.length);
//    }
//    return vectorDataOffset;
//  }

  private long writeSortedFloat32Tensors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    long tensorDataOffset = tensorData.alignFilePointer(Float.BYTES);
    int maxPackedTensorSize = 0;
    for (int i = 0; i < fieldData.values.size(); i++) {
      FloatTensorValue v = (FloatTensorValue) fieldData.values.get(i);
      maxPackedTensorSize = Integer.max(maxPackedTensorSize, v.vectorCount() * v.dimension());
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(maxPackedTensorSize * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);

    fieldData.dataOffsets = new long[fieldData.values.size() + 1];
    int newOrd = 0;
    for (int ordinal : ordMap) {
      float[] tensor = ((FloatTensorValue) fieldData.values.get(ordinal)).packedValue();
      buffer.asFloatBuffer().put(tensor);
      fieldData.dataOffsets[newOrd++] = tensorData.getFilePointer();
      tensorData.writeBytes(buffer.array(), buffer.array().length * Float.BYTES);
      buffer.clear();
    }
    assert newOrd == fieldData.values.size();
    fieldData.dataOffsets[newOrd] = tensorData.getFilePointer();
    return tensorDataOffset;
  }

  private long writeSortedByteTensors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
//    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
//    for (int ordinal : ordMap) {
//      byte[] vector = (byte[]) fieldData.vectors.get(ordinal);
//      vectorData.writeBytes(vector, vector.length);
//    }
//    return vectorDataOffset;
  }

  // TODO: [Tensors] revisit after defining FlatTensorReaders
  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    // Since we know we will not be searching for additional indexing, we can just write the
    // the vectors directly to the new segment.
    long tensorDataOffset = tensorData.alignFilePointer(Float.BYTES);
    // No need to use temporary file as we don't have to re-open for reading
    DocsWithFieldSet docsWithField =
        switch (fieldInfo.getTensorEncoding()) {
          case BYTE -> writeByteVectorData(
              tensorData,
              KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          case FLOAT32 -> writeVectorData(
              tensorData,
              KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
        };
    long tensorDataLength = tensorData.getFilePointer() - tensorDataOffset;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        tensorDataOffset,
        tensorDataLength,
        docsWithField);
  }

  // TODO: [Tensors] revisit after defining FlatTensorReaders
  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      DocsWithFieldSet docsWithField =
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> writeByteVectorData(
                tempVectorData,
                KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
            case FLOAT32 -> writeVectorData(
                tempVectorData,
                KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
          };
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      // This temp file will be accessed in a random-access fashion to construct the HNSW graph.
      // Note: don't use the context from the state, which is a flush/merge context, not expecting
      // to perform random reads.
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), IOContext.DEFAULT.withReadAdvice(ReadAdvice.RANDOM));
      // copy the temporary file vectors to the actual data file
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          docsWithField);
      success = true;
      final IndexInput finalVectorDataInput = vectorDataInput;
      final RandomVectorScorerSupplier randomVectorScorerSupplier =
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> vectorsScorer.getRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                new OffHeapByteVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    finalVectorDataInput,
                    fieldInfo.getVectorDimension() * Byte.BYTES,
                    vectorsScorer,
                    fieldInfo.getVectorSimilarityFunction()));
            case FLOAT32 -> vectorsScorer.getRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    finalVectorDataInput,
                    fieldInfo.getVectorDimension() * Float.BYTES,
                    vectorsScorer,
                    fieldInfo.getVectorSimilarityFunction()));
          };
      return new FlatCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalVectorDataInput);
            segmentWriteState.directory.deleteFile(tempVectorData.getName());
          },
          docsWithField.cardinality(),
          randomVectorScorerSupplier);
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(vectorDataInput, tempVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName());
      }
    }
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long tensorDataOffset,
      long tensorDataLength,
      DocsWithFieldSet docsWithField,
      long[] tensorDataOffsets)
      throws IOException {
    if (field.hasTensorValues() == false) {
      throw new IllegalStateException("Tensor writer is expected to work on Tensor fields");
    }
    meta.writeInt(field.number);
    meta.writeInt(field.getTensorEncoding().ordinal());
    meta.writeInt(field.getTensorSimilarityFunction().ordinal());
    meta.writeVLong(tensorDataOffset);
    meta.writeVLong(tensorDataLength);
    meta.writeVInt(field.getTensorDimension());

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, tensorData, count, maxDoc, docsWithField);

    // write tensor offsets
    TensorDataOffsetsReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, tensorData, tensorDataOffsets);
  }

  /**
   * Writes the byte vector values to the output and returns a set of documents that contains
   * vectors.
   */
  private static DocsWithFieldSet writeByteVectorData(
      IndexOutput output, ByteVectorValues byteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = byteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = byteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = byteVectorValues.vectorValue();
      assert binaryValue.length == byteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      docsWithField.add(docV);
    }
    return docsWithField;
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

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, tensorData);
  }

  private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> values;

    /* Stores start and end offsets for the packed vector data written for each tensor value.
     * dataOffsets[ordinal] holds the start offset. dataOffsets[ordinal+1] holds the end offset.
     */
    private long[] dataOffsets;

    private int lastDocID = -1;

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) {
      if (fieldInfo.hasTensorValues()) {
        return switch (fieldInfo.getTensorEncoding()) {
          case BYTE -> throw new UnsupportedOperationException("not added support for byte yet");
//              new Lucene99FlatVectorsWriter.FieldWriter<>(
//              fieldInfo, (KnnFieldVectorsWriter<byte[]>) indexWriter) {
//            @Override
//            public byte[] copyValue(byte[] value) {
//              return ArrayUtil.copyOfSubArray(value, 0, dim);
//            }
//          };
          case FLOAT32 -> new Lucene99FlatTensorsWriter.FieldWriter<>(
              fieldInfo, (KnnFieldVectorsWriter<FloatTensorValue>) indexWriter) {
            @Override
            public FloatTensorValue copyValue(FloatTensorValue value) {
              return new FloatTensorValue(value.packedValue(), value.dimension());
            }
          };
        };
      } else {
        int dim = fieldInfo.getVectorDimension();
        return switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> new Lucene99FlatTensorsWriter.FieldWriter<>(
              fieldInfo, (KnnFieldVectorsWriter<byte[]>) indexWriter) {
            @Override
            public byte[] copyValue(byte[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
          case FLOAT32 -> new Lucene99FlatTensorsWriter.FieldWriter<>(
              fieldInfo, (KnnFieldVectorsWriter<float[]>) indexWriter) {
            @Override
            public float[] copyValue(float[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        };
      }
    }

    FieldWriter(FieldInfo fieldInfo, KnnFieldVectorsWriter<T> indexWriter) {
      super(indexWriter);
      this.fieldInfo = fieldInfo;
      this.docsWithField = new DocsWithFieldSet();
      values = new ArrayList<>();
      if (fieldInfo.hasTensorValues()) {
        this.dim = fieldInfo.getTensorDimension();
      } else {
        this.dim = fieldInfo.getVectorDimension();
      }
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      T copy = copyValue(vectorValue);
      docsWithField.add(docID);
      values.add(copy);
      lastDocID = docID;
      if (indexingDelegate != null) {
        indexingDelegate.addValue(docID, copy);
      }
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_RAM_BYTES_USED;
      if (indexingDelegate != null) {
        size += indexingDelegate.ramBytesUsed();
      }
      if (values.size() == 0) return size;
      long valueSize = 0;
      if (fieldInfo.hasTensorValues()) {
        for (T v: values) {
          valueSize += ((FloatTensorValue) v).ramBytesUsed();
        }
      } else {
        valueSize = (long) values.size()
                        * fieldInfo.getVectorDimension()
                        * fieldInfo.getVectorEncoding().byteSize;
      }
      return size
          + docsWithField.ramBytesUsed()
          + (long) values.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + valueSize;
    }
  }

  static final class FlatCloseableRandomVectorScorerSupplier
      implements CloseableRandomVectorScorerSupplier {

    private final RandomVectorScorerSupplier supplier;
    private final Closeable onClose;
    private final int numVectors;

    FlatCloseableRandomVectorScorerSupplier(
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
}
