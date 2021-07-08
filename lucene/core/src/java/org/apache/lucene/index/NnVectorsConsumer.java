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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers up pending vector value(s) per doc, then flushes when segment flushes.
 *
 * @lucene.experimental
 */
class NnVectorsConsumer {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final List<float[]> vectors = new ArrayList<>();
  private final DocsWithFieldSet docsWithField;

  private int lastDocID = -1;

  private long bytesUsed;

  NnVectorsConsumer(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithField = new DocsWithFieldSet();
    this.bytesUsed = docsWithField.ramBytesUsed();
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
  public void addValue(int docID, float[] vectorValue) {
    if (docID == lastDocID) {
      throw new IllegalArgumentException(
          "VectorValuesField \""
              + fieldInfo.name
              + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (vectorValue.length != fieldInfo.getNnVectorDimension()) {
      throw new IllegalArgumentException(
          "Attempt to index a vector of dimension "
              + vectorValue.length
              + " but \""
              + fieldInfo.name
              + "\" has dimension "
              + fieldInfo.getNnVectorDimension());
    }
    assert docID > lastDocID;
    docsWithField.add(docID);
    vectors.add(ArrayUtil.copyOfSubArray(vectorValue, 0, vectorValue.length));
    updateBytesUsed();
    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed =
        docsWithField.ramBytesUsed()
            + vectors.size()
                * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                    + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
            + vectors.size() * vectors.get(0).length * Float.BYTES;
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
   * @param nnVectorsWriter the Codec's vector writer that handles the actual encoding and I/O
   * @throws IOException if there is an error writing the field and its values
   */
  public void flush(Sorter.DocMap sortMap, org.apache.lucene.codecs.NnVectorsWriter nnVectorsWriter) throws IOException {
    NnVectors nnVectors =
        new BufferedNnVectors(
            docsWithField,
            vectors,
            fieldInfo.getNnVectorDimension(),
            fieldInfo.getVectorSimilarityFunction());
    if (sortMap != null) {
      nnVectorsWriter.writeField(fieldInfo, new SortingNnVectors(nnVectors, sortMap));
    } else {
      nnVectorsWriter.writeField(fieldInfo, nnVectors);
    }
  }

  static class SortingNnVectors extends NnVectors
      implements RandomAccessNnVectorsProducer {

    private final NnVectors delegate;
    private final RandomAccessNnVectors randomAccess;
    private final int[] docIdOffsets;
    private final int[] ordMap;
    private int docId = -1;

    SortingNnVectors(NnVectors delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      randomAccess = ((RandomAccessNnVectorsProducer) delegate).randomAccess();
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
    public SimilarityFunction similarityFunction() {
      return delegate.similarityFunction();
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
    public RandomAccessNnVectors randomAccess() {

      // Must make a new delegate randomAccess so that we have our own distinct float[]
      final RandomAccessNnVectors delegateRA =
          ((RandomAccessNnVectorsProducer) SortingNnVectors.this.delegate).randomAccess();

      return new RandomAccessNnVectors() {

        @Override
        public int size() {
          return delegateRA.size();
        }

        @Override
        public int dimension() {
          return delegateRA.dimension();
        }

        @Override
        public SimilarityFunction similarityFunction() {
          return delegateRA.similarityFunction();
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

  private static class BufferedNnVectors extends NnVectors
      implements RandomAccessNnVectors, RandomAccessNnVectorsProducer {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a NnVectorsConsumer, which are copied when added to it
    final List<float[]> vectors;
    final SimilarityFunction similarityFunction;
    final int dimension;

    final ByteBuffer buffer;
    final BytesRef binaryValue;
    final ByteBuffer raBuffer;
    final BytesRef raBinaryValue;

    DocIdSetIterator docsWithFieldIter;
    int ord = -1;

    BufferedNnVectors(
        DocsWithFieldSet docsWithField,
        List<float[]> vectors,
        int dimension,
        SimilarityFunction similarityFunction) {
      this.docsWithField = docsWithField;
      this.vectors = vectors;
      this.dimension = dimension;
      this.similarityFunction = similarityFunction;
      buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      binaryValue = new BytesRef(buffer.array());
      raBuffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      raBinaryValue = new BytesRef(raBuffer.array());
      docsWithFieldIter = docsWithField.iterator();
    }

    @Override
    public RandomAccessNnVectors randomAccess() {
      return new BufferedNnVectors(docsWithField, vectors, dimension, similarityFunction);
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
    public SimilarityFunction similarityFunction() {
      return similarityFunction;
    }

    @Override
    public BytesRef binaryValue() {
      buffer.asFloatBuffer().put(vectorValue());
      return binaryValue;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) {
      raBuffer.asFloatBuffer().put(vectors.get(targetOrd));
      return raBinaryValue;
    }

    @Override
    public float[] vectorValue() {
      return vectors.get(ord);
    }

    @Override
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
