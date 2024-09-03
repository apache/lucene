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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
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
                  (List<float[]>) fieldData.vectors, fieldData.fieldInfo.getVectorDimension());
          FloatVectorValues floatVectorValues =
              sortMap != null
                  ? new SortingFloatVectorValues(bufferedFloatVectorValues, sortMap)
                  : bufferedFloatVectorValues;
          writeField(fieldData.fieldInfo, floatVectorValues, maxDoc);
          break;
        case BYTE:
          BufferedByteVectorValues bufferedByteVectorValues =
              new BufferedByteVectorValues(
                  (List<byte[]>) fieldData.vectors, fieldData.fieldInfo.getVectorDimension());
          ByteVectorValues byteVectorValues =
              sortMap != null
                  ? new SortingByteVectorValues(bufferedByteVectorValues, sortMap)
                  : bufferedByteVectorValues;
          writeField(fieldData.fieldInfo, byteVectorValues, maxDoc);
          break;
      }
    }
  }

  /**
   * Sorting FloatVectorValues that maps ordinals using the provided sortMap expressed in terms of
   * docids
   */
  private static class SortingFloatVectorValues extends FloatVectorValues {
    private final BufferedFloatVectorValues delegate;
    private final int[] newToOld;

    SortingFloatVectorValues(BufferedFloatVectorValues delegate, Sorter.DocMap sortMap)
        throws IOException {
      this.delegate = delegate.copy();
      newToOld = docMapToOrdMap(delegate, sortMap);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return delegate.vectorValue(newToOld[ord]);
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
  }

  // create a map from new ord to old ord assuming ords are sorted by doc but
  // may be sparse
  private static int[] docMapToOrdMap(KnnVectorValues values, Sorter.DocMap docMap) {
    // fill with -1?
    int[] newDocToOldOrd = new int[docMap.size()];
    int count = 0;
    for (int ord = 0; ord < values.size(); ord++) {
      int oldDoc = values.ordToDoc(ord);
      int newDoc = docMap.oldToNew(oldDoc);
      // no value will be represented by 0
      if (newDoc >= 0) {
        newDocToOldOrd[newDoc] = ord + 1;
        ++count;
      }
    }
    int [] newToOld = new int[count];
    count = 0;
    for (int ord = 0; ord < newDocToOldOrd.length; ord++) {
      if (newDocToOldOrd[ord] > 0) {
        newToOld[count++] = newDocToOldOrd[ord] - 1;
      }
    }
    return newToOld;
  }

  /**
   * Sorting ByteVectorValues that maps ordinals using the provided sortMap expressed in terms of
   * docids
   */
  private static class SortingByteVectorValues extends ByteVectorValues {
    private final BufferedByteVectorValues delegate;
    private final int[] newToOld;

    SortingByteVectorValues(BufferedByteVectorValues delegate, Sorter.DocMap sortMap)
        throws IOException {
      this.delegate = delegate;
      newToOld = docMapToOrdMap(delegate, sortMap);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return delegate.vectorValue(newToOld[ord]);
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

    BufferedFloatVectorValues(List<float[]> vectors, int dimension) {
      this.vectors = vectors;
      this.dimension = dimension;
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
    public BufferedFloatVectorValues copy() {
      return this;
    }
  }

  private static class BufferedByteVectorValues extends ByteVectorValues {
    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<byte[]> vectors;
    final int dimension;

    BufferedByteVectorValues(List<byte[]> vectors, int dimension) {
      this.vectors = vectors;
      this.dimension = dimension;
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
    public BufferedByteVectorValues copy() {
      return this;
    }
  }
}
