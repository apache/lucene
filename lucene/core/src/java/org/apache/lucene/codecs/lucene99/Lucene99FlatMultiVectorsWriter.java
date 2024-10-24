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

import static org.apache.lucene.codecs.lucene99.Lucene99FlatMultiVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
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
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteMultiVectorValue;
import org.apache.lucene.util.FloatMultiVectorValue;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Writes vector values to index segments.
 *
 * @lucene.experimental
 */
// noCommit - pending tests
public final class Lucene99FlatMultiVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatMultiVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  public Lucene99FlatMultiVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatMultiVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatMultiVectorsFormat.VECTOR_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99FlatMultiVectorsFormat.META_CODEC_NAME,
          Lucene99FlatMultiVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene99FlatMultiVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene99FlatMultiVectorsFormat.VERSION_CURRENT,
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
    FieldWriter<?> newField;
    if (fieldInfo.hasMultiVectorValues()) {
      newField = FieldWriter.createMultiVectorWriter(fieldInfo, indexWriter);
    } else {
      newField = FieldWriter.create(fieldInfo, indexWriter);
    }
    fields.add(newField);
    return newField;
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    return addField(fieldInfo, null);
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
      case BYTE -> {
        if (fieldData.fieldInfo.hasMultiVectorValues()) {
          writeByteMultiVectors(fieldData);
        } else {
          writeByteVectors(fieldData);
        }
      }
      case FLOAT32 -> {
        if (fieldData.fieldInfo.hasMultiVectorValues()) {
          writeFloat32MultiVectors(fieldData);
        } else {
          writeFloat32Vectors(fieldData);
        }
      }
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    assert vectorDataLength == fieldData.vectorDataLength() :
      "Mismatch in vector data size written to storage. " +
        "Computed = " + fieldData.vectorDataLength() + ", written = " + vectorDataLength;
    if (fieldData.fieldInfo.hasMultiVectorValues()) {
      System.out.println("VIGYA - multi-vector data offsets:");
      for (int i = 0; i < fieldData.vectors.size(); i++) {
        switch (fieldData.fieldInfo.getVectorEncoding()) {
          case BYTE -> {
            assert fieldData.dataOffsets[i+1] - fieldData.dataOffsets[i] ==
              (long) ((ByteMultiVectorValue) fieldData.vectors.get(i)).vectorCount() * fieldData.fieldInfo.getVectorDimension() * fieldData.fieldInfo.getVectorEncoding().byteSize
              : "fielddata multi-vector write mismatch for ordinal = " + i;
          }
          case FLOAT32 -> {
            assert fieldData.dataOffsets[i+1] - fieldData.dataOffsets[i] ==
              (long) ((FloatMultiVectorValue) fieldData.vectors.get(i)).vectorCount() * fieldData.fieldInfo.getVectorDimension() * fieldData.fieldInfo.getVectorEncoding().byteSize
              : "fielddata multi-vector write mismatch for ordinal = " + i;
          }
        }
        System.out.println("\t ordinal = " + i + ", offset = " + fieldData.dataOffsets[i]);
      }
      System.out.println("\t length or end offset = " + fieldData.dataOffsets[fieldData.vectors.size()]);
    }

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        fieldData.docsWithField,
        fieldData.dataOffsets);
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

  /* Write MultiVector Fields */
  private void writeFloat32MultiVectors(FieldWriter<?> fieldData) throws IOException {
    int ordinal = 0;
    ByteBuffer buffer = null;
    fieldData.dataOffsets = new long[fieldData.vectors.size() + 1];
    for (int i = 0; i < fieldData.vectors.size(); i++) {
      float[] packedValue = ((FloatMultiVectorValue) fieldData.vectors.get(i)).packedValue();
      final int valueByteLength = packedValue.length * VectorEncoding.FLOAT32.byteSize;
      if (buffer == null || buffer.capacity() < valueByteLength) {
        buffer = ByteBuffer.allocate(valueByteLength).order(ByteOrder.LITTLE_ENDIAN);
      }
      buffer.asFloatBuffer().put(packedValue);
      fieldData.dataOffsets[ordinal++] = vectorData.getFilePointer();
      vectorData.writeBytes(buffer.array(), packedValue.length * Float.BYTES);
      buffer.clear();
    }
    assert ordinal == fieldData.vectors.size()
        : "ordinal=" + ordinal + "!=" + "fieldData.vectors.size()=" + fieldData.vectors.size();
    fieldData.dataOffsets[ordinal] = vectorData.getFilePointer();
  }

  private void writeByteMultiVectors(FieldWriter<?> fieldData) throws IOException {
    fieldData.dataOffsets = new long[fieldData.vectors.size() + 1];
    int ordinal = 0;
    for (int i = 0; i < fieldData.vectors.size(); i++) {
      byte[] packedValue = ((ByteMultiVectorValue) fieldData.vectors.get(i)).packedValue();
      fieldData.dataOffsets[ordinal++] = vectorData.getFilePointer();
      vectorData.writeBytes(packedValue, packedValue.length);
    }
    assert ordinal == fieldData.vectors.size()
        : "ordinal=" + ordinal + "!=" + "fieldData.vectors.size()=" + fieldData.vectors.size();
    fieldData.dataOffsets[ordinal] = vectorData.getFilePointer();
  }

  /* Write Sorting Fields */
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
    long vectorDataOffset =
        switch (fieldData.fieldInfo.getVectorEncoding()) {
          case BYTE -> (fieldData.fieldInfo.hasMultiVectorValues())
              ? writeSortedByteMultiVectors(fieldData, ordMap)
              : writeSortedByteVectors(fieldData, ordMap);
          case FLOAT32 -> (fieldData.fieldInfo.hasMultiVectorValues())
              ? writeSortedFloat32MultiVectors(fieldData, ordMap)
              : writeSortedFloat32Vectors(fieldData, ordMap);
        };
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        newDocsWithField,
        fieldData.dataOffsets);
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

  /* Write Multi-Vector Sorting Fields */
  private long writeSortedFloat32MultiVectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    System.out.println("VIGYAVIGYA - writing sorted float32 multivectors");
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    int newOrd = 0;
    ByteBuffer buffer = null;
    fieldData.dataOffsets = new long[fieldData.vectors.size() + 1];
    for (int ordinal : ordMap) {
      float[] packedValue = ((FloatMultiVectorValue) fieldData.vectors.get(ordinal)).packedValue();
      final int valueByteLength = packedValue.length * VectorEncoding.FLOAT32.byteSize;
      if (buffer == null || buffer.capacity() < valueByteLength) {
        buffer = ByteBuffer.allocate(valueByteLength).order(ByteOrder.LITTLE_ENDIAN);
      }
      buffer.asFloatBuffer().put(packedValue);
      fieldData.dataOffsets[newOrd++] = vectorData.getFilePointer();
      vectorData.writeBytes(buffer.array(), buffer.array().length * Float.BYTES);
      buffer.clear();
    }
    assert newOrd == fieldData.vectors.size()
        : "ordinal =" + newOrd + ", expected =" + fieldData.vectors.size();
    fieldData.dataOffsets[newOrd] = vectorData.getFilePointer();
    return vectorDataOffset;
  }

  private long writeSortedByteMultiVectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    fieldData.dataOffsets = new long[fieldData.vectors.size() + 1];
    int newOrd = 0;
    for (int ordinal : ordMap) {
      byte[] packedValue = ((ByteMultiVectorValue) fieldData.vectors.get(ordinal)).packedValue();
      fieldData.dataOffsets[newOrd++] = vectorData.getFilePointer();
      vectorData.writeBytes(packedValue, packedValue.length);
    }
    assert newOrd == fieldData.vectors.size()
        : "ordinal =" + newOrd + ", expected =" + fieldData.vectors.size();
    fieldData.dataOffsets[newOrd] = vectorData.getFilePointer();
    return vectorDataOffset;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    // Since we know we will not be searching for additional indexing, we can just write
    // the vectors directly to the new segment.
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    // No need to use temporary file as we don't have to re-open for reading
    DocsAndOffsets writeState =
        (fieldInfo.hasMultiVectorValues())
            ? writeMultiVectorData(fieldInfo, mergeState, vectorData)
            : writeVectorData(fieldInfo, mergeState, vectorData);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        writeState.docsWithField,
        writeState.dataOffsets);
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    System.out.println("VIGYAVIGYA - merging oneField !!");
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      DocsAndOffsets writeState =
          (fieldInfo.hasMultiVectorValues())
              ? writeMultiVectorData(fieldInfo, mergeState, tempVectorData)
              : writeVectorData(fieldInfo, mergeState, tempVectorData);
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      long[] vectorDataOffsets = null;
      if (writeState.dataOffsets != null) {
        // for multi-vectors, dataOffsets need to align with vectorData file pointer
        vectorDataOffsets = new long[writeState.dataOffsets.length];
        long currOffset = vectorData.getFilePointer();
        long shift = currOffset - writeState.dataOffsets[0];
        for (int i = 0; i < writeState.dataOffsets.length; i++) {
          vectorDataOffsets[i] = writeState.dataOffsets[i] + shift;
        }
      }

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
          writeState.docsWithField,
          vectorDataOffsets);
      success = true;

      final IndexInput finalVectorDataInput = vectorDataInput;
      final RandomVectorScorerSupplier randomVectorScorerSupplier =
          (fieldInfo.hasMultiVectorValues())
              ? getRandomMultiVectorScorerSupplier(fieldInfo, writeState, finalVectorDataInput)
              : getRandomVectorScorerSupplier(fieldInfo, writeState, finalVectorDataInput);
      return new FlatCloseableRandomVectorScorerSupplier(
          () -> {
            IOUtils.close(finalVectorDataInput);
            segmentWriteState.directory.deleteFile(tempVectorData.getName());
          },
          writeState.docsWithField.cardinality(),
          randomVectorScorerSupplier);
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(vectorDataInput, tempVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName());
      }
    }
  }

  private RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      FieldInfo fieldInfo, DocsAndOffsets docsAndOffsets, IndexInput dataInput) throws IOException {
    RandomVectorScorerSupplier randomVectorScorerSupplier =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapByteVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsAndOffsets.docsWithField.cardinality(),
                  dataInput,
                  fieldInfo.getVectorDimension() * Byte.BYTES,
                  vectorsScorer,
                  fieldInfo.getVectorSimilarityFunction()));
          case FLOAT32 -> vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(),
              new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                  fieldInfo.getVectorDimension(),
                  docsAndOffsets.docsWithField.cardinality(),
                  dataInput,
                  fieldInfo.getVectorDimension() * Float.BYTES,
                  vectorsScorer,
                  fieldInfo.getVectorSimilarityFunction()));
        };
    return randomVectorScorerSupplier;
  }

  private RandomVectorScorerSupplier getRandomMultiVectorScorerSupplier(
      FieldInfo fieldInfo, DocsAndOffsets docsAndOffsets, IndexInput dataInput) throws IOException {
    assert docsAndOffsets.dataOffsets != null : "multi-vector writes require data offsets";
    LongValues dataOffsets =
        new LongValues() {
          @Override
          public long get(long index) {
            return docsAndOffsets.dataOffsets[(int) index];
          }
        };
    final RandomVectorScorerSupplier randomMultiVectorScorerSupplier =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> vectorsScorer.getRandomMultiVectorScorerSupplier(
              fieldInfo.getMultiVectorSimilarityFunction(),
              new OffHeapByteMultiVectorValues.DenseOffHeapMultiVectorValuesWithOffsets(
                  fieldInfo.getVectorDimension(),
                  docsAndOffsets.docsWithField.cardinality(),
                  dataInput,
                  vectorsScorer,
                  fieldInfo.getMultiVectorSimilarityFunction(),
                  dataOffsets));
          case FLOAT32 -> vectorsScorer.getRandomMultiVectorScorerSupplier(
              fieldInfo.getMultiVectorSimilarityFunction(),
              new OffHeapFloatMultiVectorValues.DenseOffHeapMultiVectorValuesWithOffsets(
                  fieldInfo.getVectorDimension(),
                  docsAndOffsets.docsWithField.cardinality(),
                  dataInput,
                  vectorsScorer,
                  fieldInfo.getMultiVectorSimilarityFunction(),
                  dataOffsets));
        };
    return randomMultiVectorScorerSupplier;
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      DocsWithFieldSet docsWithField,
      long[] multiVectorDataOffsets)
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

    // write multi-vector metadata
    meta.writeByte(field.hasMultiVectorValues() ? (byte) 1 : (byte) 0); // is multiVector?
    if (field.hasMultiVectorValues()) {
      meta.writeInt(field.getMultiVectorSimilarityFunction().aggregation.ordinal());
      MultiVectorDataOffsetsReaderConfiguration.writeStoredMeta(
          DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, multiVectorDataOffsets);
    }
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
      assert binaryValue.length == byteVectorValues.dimension() * VectorEncoding.BYTE.byteSize
          : "vectorValue returned by ByteVectorValues is inconsistent with vector dimension";
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

  private DocsAndOffsets writeVectorData(
      FieldInfo fieldInfo, MergeState mergeState, IndexOutput dataOut) throws IOException {
    DocsWithFieldSet docsWithField =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> writeByteVectorData(
              dataOut, MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          case FLOAT32 -> writeVectorData(
              dataOut, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
        };
    return new DocsAndOffsets(docsWithField, null);
  }

  private DocsAndOffsets writeMultiVectorData(
      FieldInfo fieldInfo, MergeState mergeState, IndexOutput dataOut) throws IOException {
    DocsAndOffsets docsAndOffsets =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> writeByteMultiVectorData(
              dataOut, MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          case FLOAT32 -> writeFloatMultiVectorData(
              dataOut, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
        };
    return docsAndOffsets;
  }

  /* Write Multi-Vector fields for merging */
  private record DocsAndOffsets(DocsWithFieldSet docsWithField, long[] dataOffsets) {
    DocsAndOffsets {
      assert dataOffsets == null || dataOffsets.length == docsWithField.cardinality() + 1;
    }
  }

  /** Writes byte multi-vector values to the output. */
  private static DocsAndOffsets writeByteMultiVectorData(
      IndexOutput output, ByteVectorValues byteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    long[] dataOffsets = new long[byteVectorValues.size() + 1];
    int ordinal = 0;
    for (int docV = byteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = byteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = byteVectorValues.vectorValue();
      assert binaryValue.length == byteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      dataOffsets[ordinal++] = output.getFilePointer();
      output.writeBytes(binaryValue, binaryValue.length);
      docsWithField.add(docV);
    }
    assert ordinal == byteVectorValues.size()
        : "ordinal=" + ordinal + "!=" + "byteVectorValues.size()=" + byteVectorValues.size();
    dataOffsets[ordinal] = output.getFilePointer();
    return new DocsAndOffsets(docsWithField, dataOffsets);
  }

  /** Writes float multi-vector values to the output */
  private static DocsAndOffsets writeFloatMultiVectorData(
      IndexOutput output, FloatVectorValues floatVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    ByteBuffer buffer = null;
    long[] dataOffsets = new long[floatVectorValues.size() + 1];
    int ordinal = 0;
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      float[] value = floatVectorValues.vectorValue();
      final int valueByteLength = value.length * VectorEncoding.FLOAT32.byteSize;
      if (buffer == null || buffer.capacity() < valueByteLength) {
        buffer = ByteBuffer.allocate(valueByteLength).order(ByteOrder.LITTLE_ENDIAN);
      }
      buffer.clear();
      buffer.asFloatBuffer().put(value);
      dataOffsets[ordinal++] = output.getFilePointer();
      output.writeBytes(buffer.array(), valueByteLength);
      docsWithField.add(docV);
    }
    assert ordinal == floatVectorValues.size()
        : "ordinal=" + ordinal + "!=" + "floatVectorValues.size()=" + floatVectorValues.size();
    dataOffsets[ordinal] = output.getFilePointer();
    return new DocsAndOffsets(docsWithField, dataOffsets);
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

    /* Stores start and end offsets for the packed vector data written for each multi-vector value.
     * dataOffsets[ordinal] holds the start offset. dataOffsets[ordinal+1] holds the end offset.
     * Initialized before writing multi-vector fields.
     */
    private long[] dataOffsets = null;

    private int lastDocID = -1;

    @SuppressWarnings("unchecked")
    static FieldWriter<?> create(FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) {
      int dim = fieldInfo.getVectorDimension();
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> new Lucene99FlatMultiVectorsWriter.FieldWriter<>(
            fieldInfo, (KnnFieldVectorsWriter<byte[]>) indexWriter) {
          @Override
          public byte[] copyValue(byte[] value) {
            return ArrayUtil.copyOfSubArray(value, 0, dim);
          }
        };
        case FLOAT32 -> new Lucene99FlatMultiVectorsWriter.FieldWriter<>(
            fieldInfo, (KnnFieldVectorsWriter<float[]>) indexWriter) {
          @Override
          public float[] copyValue(float[] value) {
            return ArrayUtil.copyOfSubArray(value, 0, dim);
          }
        };
      };
    }

    @SuppressWarnings("unchecked")
    static FieldWriter<?> createMultiVectorWriter(
        FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) {
      if (fieldInfo.hasMultiVectorValues() == false) {
        throw new IllegalArgumentException(
            "Cannot create MultiVector writer for a field without multi-vector values");
      }
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> new Lucene99FlatMultiVectorsWriter.FieldWriter<>(
            fieldInfo, (KnnFieldVectorsWriter<ByteMultiVectorValue>) indexWriter) {
          @Override
          public ByteMultiVectorValue copyValue(ByteMultiVectorValue value) {
            return new ByteMultiVectorValue(value.packedValue(), value.dimension());
          }
        };
        case FLOAT32 -> new Lucene99FlatMultiVectorsWriter.FieldWriter<>(
            fieldInfo, (KnnFieldVectorsWriter<FloatMultiVectorValue>) indexWriter) {
          @Override
          public FloatMultiVectorValue copyValue(FloatMultiVectorValue value) {
            return new FloatMultiVectorValue(value.packedValue(), value.dimension());
          }
        };
      };
    }

    FieldWriter(FieldInfo fieldInfo, KnnFieldVectorsWriter<T> indexWriter) {
      super(indexWriter);
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      this.vectors = new ArrayList<>();
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
      if (docID == lastDocID) {
        // TODO: can be implemented for multi-vector fields, by merging all vectors into a single
        // multi-vector
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

    private long vectorDataLength() {
      long totalDataLength = 0;
      if (fieldInfo.hasMultiVectorValues()) {
        for (T v : vectors) {
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> totalDataLength += (long) ((ByteMultiVectorValue) v).vectorCount() *
                    fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize;
            case FLOAT32 -> totalDataLength += (long) ((FloatMultiVectorValue) v).vectorCount() *
                    fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize;
          }
        }
      } else {
        totalDataLength = (long) vectors.size() * fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize;
      }
      return totalDataLength;
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_RAM_BYTES_USED;
      if (indexingDelegate != null) {
        size += indexingDelegate.ramBytesUsed();
      }
      if (vectors.size() == 0) return size;
      long valueSize = 0;
      if (fieldInfo.hasMultiVectorValues()) {
        for (T v : vectors) {
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> valueSize += ((ByteMultiVectorValue) v).ramBytesUsed();
            case FLOAT32 -> valueSize += ((FloatMultiVectorValue) v).ramBytesUsed();
          }
        }
      } else {
        valueSize =
            (long) vectors.size()
                * fieldInfo.getVectorDimension()
                * fieldInfo.getVectorEncoding().byteSize;
      }
      if (dataOffsets != null) {
        size +=
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                + (long) RamUsageEstimator.primitiveSizes.get(long.class) * dataOffsets.length;
      }
      return size
          + docsWithField.ramBytesUsed()
          + (long) vectors.size()
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
