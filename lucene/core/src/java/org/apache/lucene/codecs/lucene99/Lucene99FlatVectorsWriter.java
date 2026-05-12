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
  private final IOFunction<FieldInfo, FieldWriter<?>> fieldWriterFactory;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  public Lucene99FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer)
      throws IOException {
    this(Default::create, state, scorer);
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
    this((fi -> newDelegating(fi, strategyFactory.apply(fi))), state, scorer);
  }

  private Lucene99FlatVectorsWriter(
      IOFunction<FieldInfo, FieldWriter<?>> fieldWriterFactory,
      SegmentWriteState state,
      FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    segmentWriteState = state;
    this.fieldWriterFactory = fieldWriterFactory;
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

  @SuppressWarnings("unchecked")
  private static <T> Delegating<T> newDelegating(
      FieldInfo fi, FlatFieldVectorsWriter<?> flatFieldVectorsWriter) {
    return new Delegating<>(fi, (FlatFieldVectorsWriter<T>) flatFieldVectorsWriter);
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter<?> newField = fieldWriterFactory.apply(fieldInfo);
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
      field.finish();
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
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
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

  private void writeField(FieldWriter<?> fieldData, int maxDoc) throws IOException {
    // write vector values
    VectorEncoding encoding = fieldData.fieldInfo().getVectorEncoding();
    long vectorDataOffset = alignOutput(vectorData, encoding);
    switch (encoding) {
      case BYTE -> writeByteVectors(fieldData);
      case FLOAT32 -> writeFloat32Vectors(fieldData);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo(),
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        fieldData.docsWithField());
  }

  private void writeFloat32Vectors(FieldWriter<?> fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (Object v : fieldData.vectors()) {
      buffer.asFloatBuffer().put((float[]) v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeByteVectors(FieldWriter<?> fieldData) throws IOException {
    for (Object v : fieldData.vectors()) {
      byte[] vector = (byte[]) v;
      vectorData.writeBytes(vector, vector.length);
    }
  }

  private void writeSortingField(FieldWriter<?> fieldData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    final int[] ordMap = new int[fieldData.docsWithField().cardinality()]; // new ord to old ord

    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    mapOldOrdToNewOrd(fieldData.docsWithField(), sortMap, null, ordMap, newDocsWithField);

    // write vector values
    VectorEncoding encoding = fieldData.fieldInfo().getVectorEncoding();
    long vectorDataOffset = alignOutput(vectorData, encoding);
    switch (encoding) {
      case BYTE -> writeSortedByteVectors(fieldData, ordMap);
      case FLOAT32 -> writeSortedFloat32Vectors(fieldData, ordMap);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    writeMeta(fieldData.fieldInfo(), maxDoc, vectorDataOffset, vectorDataLength, newDocsWithField);
  }

  private void writeSortedFloat32Vectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = (float[]) fieldData.vectors().get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeSortedByteVectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
    for (int ordinal : ordMap) {
      byte[] vector = (byte[]) fieldData.vectors().get(ordinal);
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
   * Per-field writer used by {@link Lucene99FlatVectorsWriter}. The outer writer consumes a {@code
   * FieldWriter}; its two concrete implementations share the same write pipeline:
   *
   * <ul>
   *   <li>{@link Default} — used when no strategy factory is supplied (the historic behavior).
   *   <li>{@link Delegating} — forwards to a {@link FlatFieldVectorsWriter} supplied by {@code
   *       fieldWriterFactory}, allowing for customization (e.g. different memory storage)
   * </ul>
   */
  private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {

    abstract FieldInfo fieldInfo();

    abstract int dim();

    abstract DocsWithFieldSet docsWithField();

    abstract List<T> vectors();
  }

  /**
   * Default {@link FieldWriter} implementation: stores vectors on-heap in an {@link ArrayList},
   * copying each value via {@link #copyValue} on {@link #addValue}. This is the implementation used
   * when {@link Lucene99FlatVectorsWriter} is constructed without a strategy factory.
   */
  private abstract static class Default<T> extends FieldWriter<T> {
    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(Default.class);
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;
    private boolean finished;

    private int lastDocID = -1;

    static FieldWriter<?> create(FieldInfo fieldInfo) {
      int dim = fieldInfo.getVectorDimension();
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE ->
            new Lucene99FlatVectorsWriter.Default<byte[]>(fieldInfo) {
              @Override
              public byte[] copyValue(byte[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
        case FLOAT32 ->
            new Lucene99FlatVectorsWriter.Default<float[]>(fieldInfo) {
              @Override
              public float[] copyValue(float[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
      };
    }

    Default(FieldInfo fieldInfo) {
      super();
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    FieldInfo fieldInfo() {
      return fieldInfo;
    }

    @Override
    int dim() {
      return dim;
    }

    @Override
    DocsWithFieldSet docsWithField() {
      return docsWithField;
    }

    @Override
    List<T> vectors() {
      return vectors;
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

  /**
   * {@link FieldWriter} that forwards all state queries and lifecycle operations to a {@link
   * FlatFieldVectorsWriter} supplied by the strategy factory passed to the three-arg constructor.
   * The strategy owns the actual vector storage; this class only adapts it to the {@link
   * FieldWriter} accessor contract that {@link Lucene99FlatVectorsWriter}'s write pipeline expects.
   *
   * <p>{@link #copyValue} intentionally throws: the strategy's {@link
   * FlatFieldVectorsWriter#addValue} is responsible for whatever copy semantics apply to its
   * storage (e.g. an off-heap memcpy).
   */
  private static final class Delegating<T> extends FieldWriter<T> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;

    Delegating(FieldInfo fi, FlatFieldVectorsWriter<T> flatFieldVectorsWriter) {
      this.fieldInfo = fi;
      this.dim = fi.getVectorDimension();
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
    }

    @Override
    FieldInfo fieldInfo() {
      return fieldInfo;
    }

    @Override
    int dim() {
      return dim;
    }

    @Override
    DocsWithFieldSet docsWithField() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    List<T> vectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public void addValue(int docID, T v) throws IOException {
      flatFieldVectorsWriter.addValue(docID, v);
    }

    @Override
    public T copyValue(T v) {
      throw new UnsupportedOperationException(
          "Delegating FieldWriter: copy is the strategy's responsibility inside addValue");
    }

    @Override
    public List<T> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      flatFieldVectorsWriter.finish();
    }

    @Override
    public boolean isFinished() {
      return flatFieldVectorsWriter.isFinished();
    }

    @Override
    public long ramBytesUsed() {
      return flatFieldVectorsWriter.ramBytesUsed();
    }
  }
}
