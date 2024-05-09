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
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Writes vector values to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  public Lucene99FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99FlatVectorsFormat.META_CODEC_NAME,
          Lucene99FlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene99FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene99FlatVectorsFormat.VERSION_CURRENT,
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
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
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
    // write vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        writeByteVectors(fieldData);
        break;
      case FLOAT32:
        writeFloat32Vectors(fieldData);
        break;
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, fieldData.docsWithField);
  }

  private void writeFloat32Vectors(FieldWriter<?> fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (Object v : fieldData.vectors) {
      buffer.asFloatBuffer().put((float[]) v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeByteVectors(FieldWriter<?> fieldData) throws IOException {
    for (Object v : fieldData.vectors) {
      byte[] vector = (byte[]) v;
      vectorData.writeBytes(vector, vector.length);
    }
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

    // write vector values
    final long vectorDataOffset;
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        vectorDataOffset = writeSortedByteVectors(fieldData, ordMap);
        break;
      case FLOAT32:
        vectorDataOffset = writeSortedFloat32Vectors(fieldData, ordMap);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported vector encoding: " + fieldData.fieldInfo.getVectorEncoding());
    }
    ;
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, newDocsWithField);
  }

  private long writeSortedFloat32Vectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = (float[]) fieldData.vectors.get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
    return vectorDataOffset;
  }

  private long writeSortedByteVectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    for (int ordinal : ordMap) {
      byte[] vector = (byte[]) fieldData.vectors.get(ordinal);
      vectorData.writeBytes(vector, vector.length);
    }
    return vectorDataOffset;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    // Since we know we will not be searching for additional indexing, we can just write the
    // the vectors directly to the new segment.
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    // No need to use temporary file as we don't have to re-open for reading
    final DocsWithFieldSet docsWithField;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE:
        docsWithField =
            writeByteVectorData(
                vectorData,
                KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
        break;
      case FLOAT32:
        docsWithField =
            writeVectorData(
                vectorData,
                KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
    }
    ;
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        docsWithField);
  }

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
      final DocsWithFieldSet docsWithField;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          docsWithField =
              writeByteVectorData(
                  tempVectorData,
                  KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          break;
        case FLOAT32:
          docsWithField =
              writeVectorData(
                  tempVectorData,
                  KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(
                      fieldInfo, mergeState));
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
      }
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      // This temp file will be accessed in a random-access fashion to construct the HNSW graph.
      // Note: don't use the context from the state, which is a flush/merge context, not expecting
      // to perform random reads.
      vectorDataInput =
          segmentWriteState.directory.openInput(tempVectorData.getName(), IOContext.RANDOM);
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
      final RandomVectorScorerSupplier randomVectorScorerSupplier;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          randomVectorScorerSupplier =
              vectorsScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(),
                  new OffHeapByteVectorValues.DenseOffHeapVectorValues(
                      fieldInfo.getVectorDimension(),
                      docsWithField.cardinality(),
                      finalVectorDataInput,
                      fieldInfo.getVectorDimension() * Byte.BYTES,
                      vectorsScorer,
                      fieldInfo.getVectorSimilarityFunction()));
          break;
        case FLOAT32:
          randomVectorScorerSupplier =
              vectorsScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(),
                  new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                      fieldInfo.getVectorDimension(),
                      docsWithField.cardinality(),
                      finalVectorDataInput,
                      fieldInfo.getVectorDimension() * Float.BYTES,
                      vectorsScorer,
                      fieldInfo.getVectorSimilarityFunction()));
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
      }
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
      long vectorDataOffset,
      long vectorDataLength,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVInt(field.getVectorDimension());

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, count, maxDoc, docsWithField);
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
    IOUtils.close(meta, vectorData);
  }

  private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;

    private int lastDocID = -1;

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) {
      int dim = fieldInfo.getVectorDimension();
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          return new Lucene99FlatVectorsWriter.FieldWriter<>(
              fieldInfo, (KnnFieldVectorsWriter<byte[]>) indexWriter) {
            @Override
            public byte[] copyValue(byte[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        case FLOAT32:
          return new Lucene99FlatVectorsWriter.FieldWriter<>(
              fieldInfo, (KnnFieldVectorsWriter<float[]>) indexWriter) {
            @Override
            public float[] copyValue(float[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        default:
          throw new IllegalArgumentException(
              "Unsupported vector encoding: " + fieldInfo.getVectorEncoding());
      }
    }

    FieldWriter(FieldInfo fieldInfo, KnnFieldVectorsWriter<T> indexWriter) {
      super(indexWriter);
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
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
      vectors.add(copy);
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
      if (vectors.size() == 0) return size;
      return size
          + docsWithField.ramBytesUsed()
          + (long) vectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) vectors.size()
              * fieldInfo.getVectorDimension()
              * fieldInfo.getVectorEncoding().byteSize;
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
