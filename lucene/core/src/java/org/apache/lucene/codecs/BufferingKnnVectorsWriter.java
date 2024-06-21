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
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
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
                  fieldData.docsWithField,
                  (List<float[]>) fieldData.vectors,
                  fieldData.fieldInfo.getVectorDimension());
          FloatVectorValues floatVectorValues =
              sortMap != null
                  ? new SortingFloatVectorValues(bufferedFloatVectorValues, sortMap)
                  : bufferedFloatVectorValues;
          writeField(fieldData.fieldInfo, floatVectorValues, maxDoc);
          break;
        case BYTE:
          BufferedByteVectorValues bufferedByteVectorValues =
              new BufferedByteVectorValues(
                  fieldData.docsWithField,
                  (List<byte[]>) fieldData.vectors,
                  fieldData.fieldInfo.getVectorDimension());
          ByteVectorValues byteVectorValues =
              sortMap != null
                  ? new SortingByteVectorValues(bufferedByteVectorValues, sortMap)
                  : bufferedByteVectorValues;
          writeField(fieldData.fieldInfo, byteVectorValues, maxDoc);
          break;
      }
    }
  }

  /** Sorting FloatVectorValues that iterate over documents in the order of the provided sortMap */
  private static class SortingFloatVectorValues extends FloatVectorValues {
    private final BufferedFloatVectorValues randomAccess;
    private final int[] docIdOffsets;
    private int docId = -1;

    SortingFloatVectorValues(BufferedFloatVectorValues delegate, Sorter.DocMap sortMap)
        throws IOException {
      this.randomAccess = delegate.copy();
      this.docIdOffsets = new int[sortMap.size()];

      int offset = 1; // 0 means no vector for this (field, document)
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        docIdOffsets[newDocID] = offset++;
      }
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      while (docId < docIdOffsets.length - 1) {
        ++docId;
        if (docIdOffsets[docId] != 0) {
          return docId;
        }
      }
      docId = NO_MORE_DOCS;
      return docId;
    }

    @Override
    public float[] vectorValue() throws IOException {
      return randomAccess.vectorValue(docIdOffsets[docId] - 1);
    }

    @Override
    public int dimension() {
      return randomAccess.dimension();
    }

    @Override
    public int size() {
      return randomAccess.size();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }
  }

  /** Sorting FloatVectorValues that iterate over documents in the order of the provided sortMap */
  private static class SortingByteVectorValues extends ByteVectorValues {
    private final BufferedByteVectorValues randomAccess;
    private final int[] docIdOffsets;
    private int docId = -1;

    SortingByteVectorValues(BufferedByteVectorValues delegate, Sorter.DocMap sortMap)
        throws IOException {
      this.randomAccess = delegate.copy();
      this.docIdOffsets = new int[sortMap.size()];

      int offset = 1; // 0 means no vector for this (field, document)
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        docIdOffsets[newDocID] = offset++;
      }
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      while (docId < docIdOffsets.length - 1) {
        ++docId;
        if (docIdOffsets[docId] != 0) {
          return docId;
        }
      }
      docId = NO_MORE_DOCS;
      return docId;
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return randomAccess.vectorValue(docIdOffsets[docId] - 1);
    }

    @Override
    public int dimension() {
      return randomAccess.dimension();
    }

    @Override
    public int size() {
      return randomAccess.size();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(byte[] target) {
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
      if (vectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
          + vectors.size()
              * (long)
                  (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                      + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + vectors.size() * (long) dim * Float.BYTES;
    }
  }

  private static class BufferedFloatVectorValues extends FloatVectorValues {
    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final int dimension;

    DocIdSetIterator docsWithFieldIter;
    int ord = -1;

    BufferedFloatVectorValues(
        DocsWithFieldSet docsWithField, List<float[]> vectors, int dimension) {
      this.docsWithField = docsWithField;
      this.vectors = vectors;
      this.dimension = dimension;
      docsWithFieldIter = docsWithField.iterator();
    }

    public BufferedFloatVectorValues copy() {
      return new BufferedFloatVectorValues(docsWithField, vectors, dimension);
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
    public float[] vectorValue() {
      return vectors.get(ord);
    }

    float[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public int docID() {
      return docsWithFieldIter.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithFieldIter.nextDoc();
      if (docID != NO_MORE_DOCS) {
        ++ord;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BufferedByteVectorValues extends ByteVectorValues {
    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<byte[]> vectors;
    final int dimension;

    DocIdSetIterator docsWithFieldIter;
    int ord = -1;

    BufferedByteVectorValues(DocsWithFieldSet docsWithField, List<byte[]> vectors, int dimension) {
      this.docsWithField = docsWithField;
      this.vectors = vectors;
      this.dimension = dimension;
      docsWithFieldIter = docsWithField.iterator();
    }

    public BufferedByteVectorValues copy() {
      return new BufferedByteVectorValues(docsWithField, vectors, dimension);
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
    public byte[] vectorValue() {
      return vectors.get(ord);
    }

    byte[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public int docID() {
      return docsWithFieldIter.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithFieldIter.nextDoc();
      if (docID != NO_MORE_DOCS) {
        ++ord;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(byte[] target) {
      throw new UnsupportedOperationException();
    }
  }
}
