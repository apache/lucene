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

package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.codecs.CodecUtil;
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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Writes vector values to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene104FlatVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene104FlatVectorsWriter.class);

  private final IndexOutput meta, vectorData;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  public Lucene104FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene104FlatVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene104FlatVectorsFormat.VECTOR_DATA_EXTENSION);

    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene104FlatVectorsFormat.META_CODEC_NAME,
          Lucene104FlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene104FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene104FlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter<?> newField = FieldWriter.create(fieldInfo);
    fields.add(newField);
    return newField;
  }

  record PerFieldIteration(KnnVectorValues.DocIndexIterator iterator, int[] newToOldOrd) {}

  /**
   * This function somewhat transposes the .vec and .vem files before flushing to disk.
   *
   * <p>Today, the .vec file is partitioned per-field, and looks like:
   *
   * <pre>
   *   # field 1 begin:
   *   (vector for field 1, document d1) # position x0
   *   (vector for field 1, document d2)
   *   (vector for field 1, document d3)
   *   # field 1 end, field 2 begin:
   *   (vector for field 2, document d1) # position x1
   *   (vector for field 2, document d3)
   *   # field 2 end, field 3 begin:
   *   (vector for field 3, document d1) # position x2
   *   (vector for field 3, document d2)
   *   # field 3 end, and so on...
   * </pre>
   *
   * <p>The .vem file contains per-field tuples to denote (position, length) of the corresponding
   * vector "block":
   *
   * <pre>
   *   # (field number, offset of vector "block", length of vector "block")
   *   # "..." represents other metadata, including dimension, ord -> doc mapping, etc.
   *   (1, x0, x1 - x0, ...)
   *   (2, x1, x2 - x1, ...)
   *   # and so on...
   * </pre>
   *
   * <p>This function changes the .vec to be partitioned per-document instead, something like:
   *
   * <pre>
   *   # document d1 begin:
   *   (vector for field 1, document d1) # position x0
   *   (vector for field 2, document d1) # position x1
   *   (vector for field 3, document d1) # position x2
   *   # document d1 end, document 2 begin:
   *   (vector for field 1, document d2) # position x3
   *   (vector for field 3, document d2) # position x4
   *   # document d2 end, document 3 begin:
   *   (vector for field 1, document d3) # position x5
   *   (vector for field 2, document d3) # position x6
   *   # document d3 end, and so on...
   * </pre>
   *
   * <p>Correspondingly, the .vem file will contain per-field mappings from ord -> position in the
   * raw file:
   *
   * <pre>
   *   # (field number, ord -> position mapping as array, ...)
   *   # "..." represents other metadata, including dimension, ord -> doc mapping, etc. which is unchanged
   *   (1, [x0, x3, x5], ...) # {ord 0 -> position x0, ord 1 -> position x3, ord 2 -> position x5}
   *   (2, [x1, x4], ...) # {ord 0 -> position x1, ord 1 -> position x4}
   *   (3, [x2, x6], ...) # {ord 0 -> position x2, ord 1 -> position x6}
   *   # and so on...
   * </pre>
   *
   * <p>This is done so that in case of duplicate vectors <i>within</i> a document, we can simply
   * "point" to the pre-existing vector!
   */
  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    // TODO *very* crude!

    int numFields = fields.size();

    // offsets[i][j] denotes offset of vector with ord j of field i
    long[][] offsets = new long[numFields][];

    // iterator over ord, docid, vector
    PerFieldIteration[] iterators = new PerFieldIteration[numFields];
    for (int i = 0; i < numFields; i++) {
      FieldWriter<?> writer = fields.get(i);
      int cardinality = writer.docsWithField.cardinality();
      int[] newToOldOrd;

      DocsWithFieldSet docsWithFieldSet;
      if (sortMap == null) {
        docsWithFieldSet = writer.docsWithField;
        newToOldOrd = null;
      } else {
        docsWithFieldSet = new DocsWithFieldSet();
        newToOldOrd = new int[cardinality];
        mapOldOrdToNewOrd(writer.docsWithField, sortMap, null, newToOldOrd, docsWithFieldSet);
      }

      offsets[i] = new long[cardinality];

      DocIdSetIterator iterator =
          Objects.requireNonNullElse(docsWithFieldSet.iterator(), DocIdSetIterator.empty());
      KnnVectorValues.DocIndexIterator indexIterator =
          KnnVectorValuesPublicAccess.fromDISILocal(iterator);

      // initialize iteration
      indexIterator.nextDoc();

      iterators[i] = new PerFieldIteration(indexIterator, newToOldOrd);
    }

    // get first offset
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);

    long bytesWritten = 0;
    ByteBuffer buffer = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

    // Go over all documents one by one
    // TODO refactor into batched write for merge?
    for (int i = 0; i < maxDoc; i++) {
      // Record positions of ALL vectors in document i
      Map<ByteVector, Long> byteOffsets = new HashMap<>();
      Map<FloatVector, Long> floatOffsets = new HashMap<>();

      for (int j = 0; j < numFields; j++) {
        KnnVectorValues.DocIndexIterator indexIterator = iterators[j].iterator;
        int[] newToOldOrd = iterators[j].newToOldOrd;

        // If field j contains a vector for document i
        if (indexIterator.docID() == i) {
          FieldWriter<?> fieldWriter = fields.get(j);
          int ord = indexIterator.index();

          int oldOrd = ord;
          if (newToOldOrd != null) {
            oldOrd = newToOldOrd[ord];
          }

          switch (fieldWriter.fieldInfo.getVectorEncoding()) {
            case BYTE -> {
              byte[] bytes = (byte[]) fieldWriter.vectors.get(oldOrd);
              ByteVector vector = new ByteVector(bytes);

              // Check if we saw the vector earlier
              Long lookup = byteOffsets.get(vector);
              if (lookup == null) { // If the vector is new
                // Record offset
                offsets[j][ord] = bytesWritten;
                byteOffsets.put(vector, bytesWritten);

                // Write the vector
                int vectorByteLength = bytes.length;
                vectorData.writeBytes(bytes, vectorByteLength);
                bytesWritten += vectorByteLength;
              } else { // If the vector has been encountered before
                // Simply "point" to the older offset
                offsets[j][ord] = lookup;
              }
            }
            case FLOAT32 -> {
              float[] floats = (float[]) fieldWriter.vectors.get(oldOrd);
              FloatVector vector = new FloatVector(floats);

              // Check if we saw the vector earlier
              Long lookup = floatOffsets.get(vector);
              if (lookup == null) { // If the vector is new
                // Record offset
                offsets[j][ord] = bytesWritten;
                floatOffsets.put(vector, bytesWritten);

                // Write the vector
                int vectorByteLength = floats.length * Float.BYTES;
                buffer.asFloatBuffer().put(floats);
                vectorData.writeBytes(buffer.array(), vectorByteLength);
                bytesWritten += vectorByteLength;
              } else { // If the vector has been encountered before
                // Simply "point" to the older offset
                offsets[j][ord] = lookup;
              }
            }
            default -> throw new IllegalArgumentException();
          }

          // Increment per-field iterator
          indexIterator.nextDoc();
        }
      }
    }

    // Finally write per-field metadata
    for (int i = 0; i < numFields; i++) {
      FieldWriter<?> writer = fields.get(i);
      writeMeta(
          writer.fieldInfo,
          maxDoc,
          vectorDataOffset,
          bytesWritten,
          offsets[i],
          writer.docsWithField);
      writer.finish();
    }
  }

  record ByteVector(byte[] vector) {
    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ByteVector(byte[] bytes)) {
        return Arrays.equals(vector, bytes);
      }
      return false;
    }
  }

  record FloatVector(float[] vector) {
    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FloatVector(float[] floats)) {
        return Arrays.equals(vector, floats);
      }
      return false;
    }
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      long[] offsets,
      DocsWithFieldSet docsWithField)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVInt(field.getVectorDimension());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);

    // write offsets (ord -> position mapping)
    meta.writeInt(offsets.length);
    byte[] buffer = new byte[offsets.length * Long.BYTES];
    ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(offsets);
    meta.writeBytes(buffer, buffer.length);

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        Lucene104FlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT,
        meta,
        vectorData,
        count,
        maxDoc,
        docsWithField);
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
    // TODO better estimation
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    mergeOneField(fieldInfo, mergeState);
    VectorSimilarityFunction function = fieldInfo.getVectorSimilarityFunction();
    return switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> {
        ByteVectorValues mergedBytes =
            MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        yield new ByteVectorScorerSupplier(mergedBytes, function);
      }
      case FLOAT32 -> {
        FloatVectorValues mergedFloats =
            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        yield new FloatVectorScorerSupplier(mergedFloats, function);
      }
    };
  }

  @Override
  public void finishMerge(int maxDoc) throws IOException {
    flush(maxDoc, null);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData);
  }

  private abstract static class FieldWriter<T> extends FlatFieldVectorsWriter<T> {
    private static final long SHALLOW_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;
    private boolean finished;

    private int lastDocID = -1;

    static FieldWriter<?> create(FieldInfo fieldInfo) {
      int dim = fieldInfo.getVectorDimension();
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE ->
            new FieldWriter<byte[]>(fieldInfo) {
              @Override
              public byte[] copyValue(byte[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
        case FLOAT32 ->
            new FieldWriter<float[]>(fieldInfo) {
              @Override
              public float[] copyValue(float[] value) {
                return ArrayUtil.copyOfSubArray(value, 0, dim);
              }
            };
      };
    }

    FieldWriter(FieldInfo fieldInfo) {
      super();
      this.fieldInfo = fieldInfo;
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    public void addValue(int docID, T vectorValue) {
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
    public void finish() {
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

  /** Utility class to expose {@link KnnVectorValues#fromDISI(DocIdSetIterator)} */
  private abstract static class KnnVectorValuesPublicAccess extends KnnVectorValues {
    private static DocIndexIterator fromDISILocal(DocIdSetIterator iterator) {
      return KnnVectorValues.fromDISI(iterator);
    }
  }

  private record ByteVectorScorerSupplier(
      ByteVectorValues values, VectorSimilarityFunction function)
      implements CloseableRandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorer() {
        byte[] vector = null;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          vector = values.vectorValue(node);
        }

        @Override
        public float score(int node) throws IOException {
          return function.compare(vector, values.vectorValue(node));
        }

        @Override
        public int maxOrd() {
          return values.size();
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() {
      return new ByteVectorScorerSupplier(values, function);
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public int totalVectorCount() {
      return values.size();
    }
  }

  private record FloatVectorScorerSupplier(
      FloatVectorValues values, VectorSimilarityFunction function)
      implements CloseableRandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorer() {
        float[] vector = null;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          vector = values.vectorValue(node);
        }

        @Override
        public float score(int node) throws IOException {
          return function.compare(vector, values.vectorValue(node));
        }

        @Override
        public int maxOrd() {
          return values.size();
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() {
      return new FloatVectorScorerSupplier(values, function);
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public int totalVectorCount() {
      return values.size();
    }
  }
}
