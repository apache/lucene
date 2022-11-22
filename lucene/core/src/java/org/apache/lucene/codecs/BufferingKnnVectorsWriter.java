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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers up pending vector value(s) per doc, then flushes when segment flushes. Used for {@code
 * SimpleTextKnnVectorsWriter} and for vectors writers before v 9.3 .
 *
 * @lucene.experimental
 */
public abstract class BufferingKnnVectorsWriter extends KnnVectorsWriter {
  private final List<FieldWriter> fields = new ArrayList<>();

  /** Sole constructor */
  protected BufferingKnnVectorsWriter() {}

  @Override
  public KnnFieldVectorsWriter<float[]> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter newField = new FieldWriter(fieldInfo);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter fieldData : fields) {
      KnnVectorsReader knnVectorsReader =
          new KnnVectorsReader() {
            @Override
            public long ramBytesUsed() {
              return 0;
            }

            @Override
            public void close() {
              throw new UnsupportedOperationException();
            }

            @Override
            public void checkIntegrity() {
              throw new UnsupportedOperationException();
            }

            @Override
            public VectorValues getVectorValues(String field) throws IOException {
              BufferedVectorValues vectorValues =
                  new BufferedVectorValues(
                      fieldData.docsWithField,
                      fieldData.vectors,
                      fieldData.fieldInfo.getVectorDimension());
              return sortMap != null
                  ? new SortingVectorValues(vectorValues, sortMap)
                  : vectorValues;
            }

            @Override
            public TopDocs search(
                String field, float[] target, int k, Bits acceptDocs, int visitedLimit) {
              throw new UnsupportedOperationException();
            }
          };

      writeField(fieldData.fieldInfo, knnVectorsReader, maxDoc);
    }
  }

  /** Sorting VectorValues that iterate over documents in the order of the provided sortMap */
  private static class SortingVectorValues extends VectorValues {
    private final BufferedVectorValues randomAccess;
    private final int[] docIdOffsets;
    private int docId = -1;

    SortingVectorValues(BufferedVectorValues delegate, Sorter.DocMap sortMap) throws IOException {
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
    public BytesRef binaryValue() throws IOException {
      return randomAccess.binaryValue(docIdOffsets[docId] - 1);
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
    public long cost() {
      return size();
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

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    KnnVectorsReader knnVectorsReader =
        new KnnVectorsReader() {

          @Override
          public long ramBytesUsed() {
            return 0;
          }

          @Override
          public void close() {}

          @Override
          public TopDocs search(
              String field, float[] target, int k, Bits acceptDocs, int visitedLimit) {
            throw new UnsupportedOperationException();
          }

          @Override
          public VectorValues getVectorValues(String field) throws IOException {
            return MergedVectorValues.mergeVectorValues(fieldInfo, mergeState);
          }

          @Override
          public void checkIntegrity() {}
        };
    writeField(fieldInfo, knnVectorsReader, mergeState.segmentInfo.maxDoc());
  }

  /** Write the provided field */
  protected abstract void writeField(
      FieldInfo fieldInfo, KnnVectorsReader knnVectorsReader, int maxDoc) throws IOException;

  private static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<float[]> vectors;

    private int lastDocID = -1;

    public FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    public void addValue(int docID, Object value) {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      float[] vectorValue =
          switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> (float[]) value;
            case BYTE -> bytesToFloats((BytesRef) value);
          };
      docsWithField.add(docID);
      vectors.add(copyValue(vectorValue));
      lastDocID = docID;
    }

    private float[] bytesToFloats(BytesRef b) {
      // This is used only by SimpleTextKnnVectorsWriter
      float[] floats = new float[dim];
      for (int i = 0; i < dim; i++) {
        floats[i] = b.bytes[i + b.offset];
      }
      return floats;
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      return ArrayUtil.copyOfSubArray(vectorValue, 0, dim);
    }

    @Override
    public long ramBytesUsed() {
      if (vectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
          + vectors.size()
              * (long)
                  (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                      + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + vectors.size() * (long) dim * Float.BYTES;
    }
  }

  private static class BufferedVectorValues extends VectorValues {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final int dimension;

    final ByteBuffer buffer;
    final BytesRef binaryValue;
    final ByteBuffer raBuffer;
    final BytesRef raBinaryValue;

    DocIdSetIterator docsWithFieldIter;
    int ord = -1;

    BufferedVectorValues(DocsWithFieldSet docsWithField, List<float[]> vectors, int dimension) {
      this.docsWithField = docsWithField;
      this.vectors = vectors;
      this.dimension = dimension;
      buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      binaryValue = new BytesRef(buffer.array());
      raBuffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      raBinaryValue = new BytesRef(raBuffer.array());
      docsWithFieldIter = docsWithField.iterator();
    }

    public BufferedVectorValues copy() {
      return new BufferedVectorValues(docsWithField, vectors, dimension);
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
    public BytesRef binaryValue() {
      buffer.asFloatBuffer().put(vectorValue());
      return binaryValue;
    }

    public BytesRef binaryValue(int targetOrd) {
      raBuffer.asFloatBuffer().put(vectors.get(targetOrd));
      return raBinaryValue;
    }

    @Override
    public float[] vectorValue() {
      return vectors.get(ord);
    }

    public float[] vectorValue(int targetOrd) {
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
    public long cost() {
      return docsWithFieldIter.cost();
    }
  }
}
