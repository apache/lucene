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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.SortingCodecReader.SortingValuesIterator;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers up pending vector value(s) per doc, then flushes when segment flushes. Used for {@code
 * SimpleTextKnnVectorsWriter} and for vectors writers before v 9.3 .
 *
 * @lucene.experimental
 */
public abstract class BufferingKnnVectorsWriter extends KnnVectorsWriter {
  private final List<FieldWriter<?>> fields = new ArrayList<>();

  /** Sole constructor */
  protected BufferingKnnVectorsWriter() {}

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter<?> newField;
    switch (fieldInfo.getVectorEncoding()) {
      case FLOAT32:
        newField =
            new FieldWriter<float[]>(fieldInfo) {
              @Override
              public float[] copyValue(float[] vectorValue) {
                return ArrayUtil.copyOfSubArray(vectorValue, 0, fieldInfo.getVectorDimension());
              }
            };
        break;
      case BYTE:
        newField =
            new FieldWriter<byte[]>(fieldInfo) {
              @Override
              public byte[] copyValue(byte[] vectorValue) {
                return ArrayUtil.copyOfSubArray(vectorValue, 0, fieldInfo.getVectorDimension());
              }
            };
        break;
      default:
        throw new UnsupportedOperationException();
    }
    fields.add(newField);
    return newField;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter<?> fieldData : fields) {
      switch (fieldData.fieldInfo.getVectorEncoding()) {
        case FLOAT32:
          BufferedFloatVectorValues bufferedFloatVectorValues =
              new BufferedFloatVectorValues(
                  (List<float[]>) fieldData.vectors,
                  fieldData.fieldInfo.getVectorDimension(),
                  fieldData.docsWithField);
          FloatVectorValues floatVectorValues =
              sortMap != null
                  ? new SortingFloatVectorValues(
                      bufferedFloatVectorValues, fieldData.docsWithField, sortMap)
                  : bufferedFloatVectorValues;
          writeField(fieldData.fieldInfo, floatVectorValues, maxDoc);
          break;
        case BYTE:
          BufferedByteVectorValues bufferedByteVectorValues =
              new BufferedByteVectorValues(
                  (List<byte[]>) fieldData.vectors,
                  fieldData.fieldInfo.getVectorDimension(),
                  fieldData.docsWithField);
          ByteVectorValues byteVectorValues =
              sortMap != null
                  ? new SortingByteVectorValues(
                      bufferedByteVectorValues, fieldData.docsWithField, sortMap)
                  : bufferedByteVectorValues;
          writeField(fieldData.fieldInfo, byteVectorValues, maxDoc);
          break;
      }
    }
  }

  /** Sorting FloatVectorValues that iterate over documents in the order of the provided sortMap */
  private static class SortingFloatVectorValues extends FloatVectorValues {
    private final BufferedFloatVectorValues delegate;
    private final Supplier<SortingValuesIterator> iteratorSupplier;

    SortingFloatVectorValues(
        BufferedFloatVectorValues delegate, DocsWithFieldSet docsWithField, Sorter.DocMap sortMap)
        throws IOException {
      this.delegate = delegate.copy();
      iteratorSupplier = SortingCodecReader.iteratorSupplier(delegate, sortMap);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return delegate.vectorValue(ord);
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public SortingFloatVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return iteratorSupplier.get();
    }
  }

  /** Sorting ByteVectorValues that iterate over documents in the order of the provided sortMap */
  private static class SortingByteVectorValues extends ByteVectorValues {
    private final BufferedByteVectorValues delegate;
    private final Supplier<SortingValuesIterator> iteratorSupplier;

    SortingByteVectorValues(
        BufferedByteVectorValues delegate, DocsWithFieldSet docsWithField, Sorter.DocMap sortMap)
        throws IOException {
      this.delegate = delegate;
      iteratorSupplier = SortingCodecReader.iteratorSupplier(delegate, sortMap);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return delegate.vectorValue(ord);
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public SortingByteVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return iteratorSupplier.get();
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    switch (fieldInfo.getVectorEncoding()) {
      case FLOAT32:
        FloatVectorValues floatVectorValues =
            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        writeField(fieldInfo, floatVectorValues, mergeState.segmentInfo.maxDoc());
        break;
      case BYTE:
        ByteVectorValues byteVectorValues =
            MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        writeField(fieldInfo, byteVectorValues, mergeState.segmentInfo.maxDoc());
        break;
    }
  }

  /** Write the provided float vector field */
  protected abstract void writeField(
      FieldInfo fieldInfo, FloatVectorValues floatVectorValues, int maxDoc) throws IOException;

  /** Write the provided byte vector field */
  protected abstract void writeField(
      FieldInfo fieldInfo, ByteVectorValues byteVectorValues, int maxDoc) throws IOException;

  private abstract static class FieldWriter<T> extends KnnFieldVectorsWriter<T> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;

    private int lastDocID = -1;

    FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    public final void addValue(int docID, T value) {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      vectors.add(copyValue(value));
      lastDocID = docID;
    }

    @Override
    public abstract T copyValue(T vectorValue);

    @Override
    public final long ramBytesUsed() {
      if (vectors.isEmpty()) {
        return 0;
      }
      return docsWithField.ramBytesUsed()
          + vectors.size()
              * (long)
                  (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                      + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + vectors.size() * (long) dim * Float.BYTES;
    }
  }

  private static class BufferedFloatVectorValues extends FloatVectorValues {
    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final int dimension;
    private final DocIdSet docsWithField;
    private final DocIndexIterator iterator;

    BufferedFloatVectorValues(List<float[]> vectors, int dimension, DocIdSet docsWithField)
        throws IOException {
      this.vectors = vectors;
      this.dimension = dimension;
      this.docsWithField = docsWithField;
      this.iterator = fromDISI(docsWithField.iterator());
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    public float[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public DocIndexIterator iterator() {
      return iterator;
    }

    @Override
    public BufferedFloatVectorValues copy() throws IOException {
      return new BufferedFloatVectorValues(vectors, dimension, docsWithField);
    }
  }

  private static class BufferedByteVectorValues extends ByteVectorValues {
    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<byte[]> vectors;
    final int dimension;
    private final DocIdSet docsWithField;
    private final DocIndexIterator iterator;

    BufferedByteVectorValues(List<byte[]> vectors, int dimension, DocIdSet docsWithField)
        throws IOException {
      this.vectors = vectors;
      this.dimension = dimension;
      this.docsWithField = docsWithField;
      iterator = fromDISI(docsWithField.iterator());
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public byte[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public DocIndexIterator iterator() {
      return iterator;
    }

    @Override
    public BufferedByteVectorValues copy() throws IOException {
      return new BufferedByteVectorValues(vectors, dimension, docsWithField);
    }
  }
}
