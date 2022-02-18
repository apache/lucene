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

package org.apache.lucene.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;

/**
 * Buffers up pending vector value per doc on disk until segment flushes.
 *
 * @lucene.experimental
 */
class VectorValuesWriter {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final DocsWithFieldSet docsWithField;
  private final int dim;
  private final int byteSize;
  private final ByteBuffer buffer;
  private final Directory directory;
  private final IndexOutput dataOut;

  private int lastDocID = -1;

  private long bytesUsed;

  VectorValuesWriter(
      FieldInfo fieldInfo, Counter iwBytesUsed, Directory directory, String segmentName)
      throws IOException {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    docsWithField = new DocsWithFieldSet();
    this.directory = directory;
    String fileName = segmentName + "_" + fieldInfo.getName() + "_buffered_vectors";
    dataOut = directory.createTempOutput(fileName, "temp", IOContext.DEFAULT);
    dim = fieldInfo.getVectorDimension();
    byteSize = dim * Float.BYTES;
    buffer = ByteBuffer.allocate(byteSize).order(ByteOrder.LITTLE_ENDIAN);
    bytesUsed = docsWithField.ramBytesUsed() + byteSize;
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(bytesUsed);
    }
  }

  /**
   * Adds a value for the given document. Only a single value may be added.
   *
   * @param docID the value is added to this document
   * @param vectorValue the value to add
   * @throws IllegalArgumentException if a value has already been added to the given document
   */
  public void addValue(int docID, float[] vectorValue) throws IOException {
    if (docID == lastDocID) {
      throw new IllegalArgumentException(
          "VectorValuesField \""
              + fieldInfo.name
              + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (vectorValue.length != dim) {
      throw new IllegalArgumentException(
          "Attempt to index a vector of dimension "
              + vectorValue.length
              + " but \""
              + fieldInfo.name
              + "\" has dimension "
              + fieldInfo.getVectorDimension());
    }
    assert docID > lastDocID;
    docsWithField.add(docID);

    buffer.asFloatBuffer().put(vectorValue);
    dataOut.writeBytes(buffer.array(), byteSize);
    updateBytesUsed();
    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = docsWithField.ramBytesUsed() + byteSize;
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    }
    bytesUsed = newBytesUsed;
  }

  /**
   * Flush this field's values to storage, sorting the values in accordance with sortMap
   *
   * @param sortMap specifies the order of documents being flushed, or null if they are to be
   *     flushed in docid order
   * @param knnVectorsWriter the Codec's vector writer that handles the actual encoding and I/O
   * @throws IOException if there is an error writing the field and its values
   */
  public void flush(Sorter.DocMap sortMap, KnnVectorsWriter knnVectorsWriter) throws IOException {
    IndexInput dataIn = null;
    boolean success = false;
    try {
      CodecUtil.writeFooter(dataOut);
      IOUtils.close(dataOut);
      dataIn = directory.openInput(dataOut.getName(), IOContext.READ);
      CodecUtil.retrieveChecksum(dataIn);

      final IndexInput finalDataIn = dataIn;
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
              VectorValues vectorValues =
                  new OffHeapBufferedVectorValues(
                      dim, docsWithField.cardinality(), docsWithField, finalDataIn);
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
      knnVectorsWriter.writeField(fieldInfo, knnVectorsReader);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dataIn);
        directory.deleteFile(dataOut.getName());
      } else {
        IOUtils.closeWhileHandlingException(dataOut);
        IOUtils.closeWhileHandlingException(dataIn);
        IOUtils.deleteFilesIgnoringExceptions(directory, dataOut.getName());
      }
    }
  }

  void abort() {
    IOUtils.closeWhileHandlingException(dataOut);
    IOUtils.deleteFilesIgnoringExceptions(directory, dataOut.getName());
  }

  static class SortingVectorValues extends VectorValues
      implements RandomAccessVectorValuesProducer {

    private final VectorValues delegate;
    private final RandomAccessVectorValues randomAccess;
    private final int[] docIdOffsets;
    private final int[] ordMap;
    private int docId = -1;

    SortingVectorValues(VectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      randomAccess = ((RandomAccessVectorValuesProducer) delegate).randomAccess();
      docIdOffsets = new int[sortMap.size()];

      int offset = 1; // 0 means no vector for this (field, document)
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        docIdOffsets[newDocID] = offset++;
      }

      // set up ordMap to map from new dense ordinal to old dense ordinal
      ordMap = new int[offset - 1];
      int ord = 0;
      for (int docIdOffset : docIdOffsets) {
        if (docIdOffset != 0) {
          ordMap[ord++] = docIdOffset - 1;
        }
      }
      assert ord == ordMap.length;
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
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return size();
    }

    @Override
    public RandomAccessVectorValues randomAccess() {

      // Must make a new delegate randomAccess so that we have our own distinct float[]
      final RandomAccessVectorValues delegateRA =
          ((RandomAccessVectorValuesProducer) SortingVectorValues.this.delegate).randomAccess();

      return new RandomAccessVectorValues() {

        @Override
        public int size() {
          return delegateRA.size();
        }

        @Override
        public int dimension() {
          return delegateRA.dimension();
        }

        @Override
        public float[] vectorValue(int targetOrd) throws IOException {
          return delegateRA.vectorValue(ordMap[targetOrd]);
        }

        @Override
        public BytesRef binaryValue(int targetOrd) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  static class OffHeapBufferedVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    private final int dimension;
    private final DocsWithFieldSet docsWithField;
    private final int size;
    private final IndexInput dataIn;
    private final DocIdSetIterator docsWithFieldIter;
    private final BytesRef binaryValue;
    private final ByteBuffer byteBuffer;
    private final int byteSize;
    private final float[] value;
    private int ord = -1;

    OffHeapBufferedVectorValues(
        int dimension, int size, DocsWithFieldSet docsWithField, IndexInput dataIn) {
      this.dimension = dimension;
      this.size = size;
      this.docsWithField = docsWithField;
      this.dataIn = dataIn;
      docsWithFieldIter = docsWithField.iterator();
      byteSize = Float.BYTES * dimension;
      byteBuffer = ByteBuffer.allocate(byteSize);
      value = new float[dimension];
      binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public float[] vectorValue() throws IOException {
      dataIn.seek((long) ord * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      dataIn.seek((long) ord * byteSize);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
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

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new OffHeapBufferedVectorValues(dimension, size, docsWithField, dataIn.clone());
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      readValue(targetOrd);
      return binaryValue;
    }

    private void readValue(int targetOrd) throws IOException {
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }
  }
}
