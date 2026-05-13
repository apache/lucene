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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Writes vector values to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData;
  private final IOFunction<FieldInfo, FlatFieldVectorsWriter<?>> fieldWriterFactory;

  private record FieldData(FlatFieldVectorsWriter<?> fieldWriter, FieldInfo fieldInfo) {}

  private final List<FieldData> fields = new ArrayList<>();
  private boolean finished;

  /**
   * Constructs a writer that uses the default factory to build per-field vector storage. This
   * default factory creates instances of {@link FlatFieldVectorsWriter} that store vector data as a
   * List of on-heap arrays, one per-vector (see {@code DefaultFieldWriter}).
   *
   * @param state the segment write state
   * @param scorer the flat vectors scorer used to score vectors at index-build time
   */
  public Lucene99FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer)
      throws IOException {
    this(state, scorer, DefaultFieldWriter::create);
  }

  /**
   * Constructs a writer that uses the supplied {@code strategyFactory} to build per-field vector
   * storage. The factory is consulted on every {@link #addField(FieldInfo)} call and returns a
   * user-defined {@link FlatFieldVectorsWriter}.
   *
   * <p>Note: the strategy is only consulted during indexing (i.e. via {@link #addField(FieldInfo)}
   * and the subsequent {@link #flush}). Merges write directly to the new segment via {@link
   * #mergeOneFlatVectorField} and do not go through the strategy.
   *
   * @param state the segment write state
   * @param scorer the flat vectors scorer used to score vectors at index-build time
   * @param strategyFactory the per-field storage factory; receives the {@link FieldInfo} for the
   *     field being added and returns the {@link FlatFieldVectorsWriter} that will back it
   */
  public Lucene99FlatVectorsWriter(
      SegmentWriteState state,
      FlatVectorsScorer scorer,
      IOFunction<FieldInfo, FlatFieldVectorsWriter<?>> strategyFactory)
      throws IOException {
    super(scorer);
    segmentWriteState = state;
    fieldWriterFactory = strategyFactory;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION);

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
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    var newFieldWriter = fieldWriterFactory.apply(fieldInfo);
    fields.add(new FieldData(newFieldWriter, fieldInfo));
    return newFieldWriter;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (var field : fields) {
      if (sortMap == null) {
        writeField(field.fieldWriter(), field.fieldInfo(), maxDoc);
      } else {
        writeSortingField(field.fieldWriter(), field.fieldInfo(), maxDoc, sortMap);
      }
      field.fieldWriter().finish();
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
    long total = SHALLOW_RAM_BYTES_USED;
    for (var field : fields) {
      total += field.fieldWriter().ramBytesUsed();
    }
    return total;
  }

  private static long alignOutput(IndexOutput output, VectorEncoding encoding) throws IOException {
    return output.alignFilePointer(
        switch (encoding) {
          case BYTE -> Float.BYTES;
          case FLOAT32 -> 64; // optimal alignment for Arm Neoverse machines.
        });
  }

  private void writeField(FlatFieldVectorsWriter<?> fieldWriter, FieldInfo fieldInfo, int maxDoc)
      throws IOException {
    // write vector values
    VectorEncoding encoding = fieldInfo.getVectorEncoding();
    long vectorDataOffset = alignOutput(vectorData, encoding);
    switch (encoding) {
      case BYTE -> writeByteVectors(fieldWriter);
      case FLOAT32 -> writeFloat32Vectors(fieldWriter, fieldInfo);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, fieldWriter.getDocsWithFieldSet());
  }

  private void writeFloat32Vectors(FlatFieldVectorsWriter<?> fieldWriter, FieldInfo fieldInfo)
      throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (Object v : fieldWriter.getVectors()) {
      buffer.asFloatBuffer().put((float[]) v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeByteVectors(FlatFieldVectorsWriter<?> fieldWriter) throws IOException {
    for (Object v : fieldWriter.getVectors()) {
      byte[] vector = (byte[]) v;
      vectorData.writeBytes(vector, vector.length);
    }
  }

  private void writeSortingField(
      FlatFieldVectorsWriter<?> fieldWriter, FieldInfo fieldInfo, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    var docsWithFieldSet = fieldWriter.getDocsWithFieldSet();
    final int[] ordMap = new int[docsWithFieldSet.cardinality()]; // new ord to old ord

    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    mapOldOrdToNewOrd(docsWithFieldSet, sortMap, null, ordMap, newDocsWithField);

    // write vector values
    VectorEncoding encoding = fieldInfo.getVectorEncoding();
    long vectorDataOffset = alignOutput(vectorData, encoding);
    switch (encoding) {
      case BYTE -> writeSortedByteVectors(fieldWriter, ordMap);
      case FLOAT32 -> writeSortedFloat32Vectors(fieldWriter, fieldInfo, ordMap);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, newDocsWithField);
  }

  private void writeSortedFloat32Vectors(
      FlatFieldVectorsWriter<?> fieldWriter, FieldInfo fieldInfo, int[] ordMap) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = (float[]) fieldWriter.getVectors().get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeSortedByteVectors(FlatFieldVectorsWriter<?> fieldWriter, int[] ordMap)
      throws IOException {
    for (int ordinal : ordMap) {
      byte[] vector = (byte[]) fieldWriter.getVectors().get(ordinal);
      vectorData.writeBytes(vector, vector.length);
    }
  }

  @Override
  public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    // Since we know we will not be searching for additional indexing, we can just write the
    // vectors directly to the new segment.
    VectorEncoding encoding = fieldInfo.getVectorEncoding();
    long vectorDataOffset = alignOutput(vectorData, encoding);
    // No need to use temporary file as we don't have to re-open for reading
    DocsWithFieldSet docsWithField =
        switch (encoding) {
          case BYTE ->
              writeByteVectorData(
                  vectorData,
                  KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          case FLOAT32 ->
              writeVectorData(
                  vectorData,
                  KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(
                      fieldInfo, mergeState));
        };
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldInfo,
        segmentWriteState.segmentInfo.maxDoc(),
        vectorDataOffset,
        vectorDataLength,
        docsWithField);
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
    KnnVectorValues.DocIndexIterator iter = byteVectorValues.iterator();
    for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
      // write vector
      byte[] binaryValue = byteVectorValues.vectorValue(iter.index());
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
    KnnVectorValues.DocIndexIterator iter = floatVectorValues.iterator();
    for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
      // write vector
      float[] value = floatVectorValues.vectorValue(iter.index());
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

  /**
   * Default {@link FlatFieldVectorsWriter} implementation: stores vectors on-heap in an {@link
   * ArrayList}, copying each value via {@link #copyValue} on {@link #addValue}. This is the
   * implementation used when {@link Lucene99FlatVectorsWriter} is constructed without a strategy
   * factory.
   */
  private abstract static class DefaultFieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(DefaultFieldWriter.class);
    private final FieldInfo fieldInfo;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;
    private boolean finished;

    private int lastDocID = -1;

    private static FlatFieldVectorsWriter<?> create(FieldInfo fieldInfo) {
      int dim = fieldInfo.getVectorDimension();
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE ->
            new DefaultFieldWriter<byte[]>(fieldInfo) {
              @Override
              public byte[] copyValue(byte[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
        case FLOAT32 ->
            new DefaultFieldWriter<float[]>(fieldInfo) {
              @Override
              public float[] copyValue(float[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
      };
    }

    DefaultFieldWriter(FieldInfo fieldInfo) {
      super();
      this.fieldInfo = fieldInfo;
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
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
      T copy = copyValue(vectorValue);
      docsWithField.add(docID);
      vectors.add(copy);
      lastDocID = docID;
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_RAM_BYTES_USED;
      if (vectors.size() == 0) return size;
      return size
          + docsWithField.ramBytesUsed()
          + (long) vectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) vectors.size()
              * fieldInfo.getVectorDimension()
              * fieldInfo.getVectorEncoding().byteSize;
    }

    @Override
    public List<T> getVectors() {
      return vectors;
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return docsWithField;
    }

    @Override
    public void finish() throws IOException {
      if (finished) {
        return;
      }
      this.finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }
  }
}
