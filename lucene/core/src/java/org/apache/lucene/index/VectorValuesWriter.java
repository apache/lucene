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
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Buffers up pending vector value(s) per doc, then flushes when segment flushes.
 *
 * @lucene.experimental
 */
class VectorValuesWriter {
  private final PackedLongValues.Builder allVectorIds; // stream of all vectorIDs
  private PackedLongValues.Builder perDocumentVectors; // vectorIDs per doc
  private int currentDoc = -1;
  private int currentVectorId = 0;
  private int[] currentValues = new int[8];
  private int currentUpto;
  private int maxCount;
  private PackedLongValues finalOrds;
  private PackedLongValues finalOrdCounts;
  private int[] finalSortedValues;
  
  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final List<float[]> vectors = new ArrayList<>();
  private final DocsWithFieldSet docsWithField;
  private long bytesUsed;

  VectorValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed, ByteBlockPool pool) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.allVectorIds = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    this.docsWithField = new DocsWithFieldSet();
    bytesUsed =
            allVectorIds.ramBytesUsed()
                    + docsWithField.ramBytesUsed()
                    + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(bytesUsed);
  }
  
  public void addValue(int docID, float[] vectorValue) {
    assert docID >= currentDoc;
    if (vectorValue.length != fieldInfo.getVectorDimension()) {
      throw new IllegalArgumentException(
          "Attempt to index a vector of dimension "
              + vectorValue.length
              + " but \""
              + fieldInfo.name
              + "\" has dimension "
              + fieldInfo.getVectorDimension());
    }
    if (docID != currentDoc) {
      finishCurrentDoc();
      currentDoc = docID;
    }
    addOneVector(vectorValue);
    updateBytesUsed();
    vectors.add(ArrayUtil.copyOfSubArray(vectorValue, 0, vectorValue.length));
  }

  // finalize currentDoc: this deduplicates the current term ids
  private void finishCurrentDoc() {
    if (currentDoc == -1) {
      return;
    }
    Arrays.sort(currentValues, 0, currentUpto);
    int currentDocumentVectors = 0;
    for (int i = 0; i < currentUpto; i++) {
      int vectorId = currentValues[i];
      allVectorIds.add(vectorId);
      currentDocumentVectors++;
    }
    
    if (perDocumentVectors != null) {
      perDocumentVectors.add(currentDocumentVectors);
    } else if (currentDocumentVectors != 1) {
      perDocumentVectors = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
      for (int i = 0; i < docsWithField.cardinality(); ++i) {
        perDocumentVectors.add(1);
      }
      perDocumentVectors.add(currentDocumentVectors);
    }
    maxCount = Math.max(maxCount, currentDocumentVectors);
    currentUpto = 0;
    docsWithField.add(currentDoc);
  }

  private void addOneVector(float[] vector) {
    int vectorId = currentVectorId ++;

    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length + 1);
      iwBytesUsed.addAndGet((currentValues.length - currentUpto) * Integer.BYTES);
    }

    currentValues[currentUpto] = vectorId;
    currentUpto++;
  }

  private void updateBytesUsed() {
    final long newBytesUsed =
            allVectorIds.ramBytesUsed()
                    + (perDocumentVectors == null ? 0 : perDocumentVectors.ramBytesUsed())
                    + docsWithField.ramBytesUsed()
                    + RamUsageEstimator.sizeOf(currentValues)
            + vectors.size()
                * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                    + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
            //+ vectors.size() * vectors.get(0).length * Float.BYTES;
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
    final PackedLongValues ords;
    final PackedLongValues ordCounts;

    if (finalOrds == null) {
      assert finalOrdCounts == null && finalSortedValues == null;
      finishCurrentDoc();
      ords = allVectorIds.build();
      ordCounts = perDocumentVectors == null ? null : perDocumentVectors.build();
    } else {
      ords = finalOrds;
      ordCounts = finalOrdCounts;
    }

    
    KnnVectorsReader knnVectorsReader =
        new KnnVectorsReader() {
          @Override
          public long ramBytesUsed() {
            return 0;
          }

          @Override
          public void close() throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public void checkIntegrity() throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public VectorValues getVectorValues(String field) throws IOException {
            VectorValues vectorValues =
                new BufferedVectorValues(docsWithField, vectors, fieldInfo.getVectorDimension(),ords,ordCounts);
            return sortMap != null ? new SortingVectorValues(vectorValues, sortMap) : vectorValues;
          }

          @Override
          public TopDocs search(
                  String field, float[] target, int k, Bits acceptDocs, int visitedLimit, HnswGraphSearcher.Multivalued strategy)
              throws IOException {
            throw new UnsupportedOperationException();
          }
        };

    knnVectorsWriter.writeField(fieldInfo, knnVectorsReader);
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
    public long nextOrd() throws IOException {
      return delegate.nextOrd();
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
    public RandomAccessVectorValues randomAccess() throws IOException {

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

        @Override
        public int ordToDoc(int ord) {
          return 0;
        }
      };
    }
  }

  private static class BufferedVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final int dimension;

    final ByteBuffer buffer;
    final BytesRef binaryValue;
    final ByteBuffer raBuffer;
    final BytesRef raBinaryValue;

    final PackedLongValues ords;
    final PackedLongValues ordCounts;
    final PackedLongValues.Iterator vectorIdsIter;
    final PackedLongValues.Iterator vectorIdsPerDocumentCountIterator;
    DocIdSetIterator docsWithFieldIter;
    final int[] currentDocVectorsId;

    private int vectorsPerDocumentCount;
    private int visitedVectorIdPerCurrentDoc;

    BufferedVectorValues(DocsWithFieldSet docsWithField, List<float[]> vectors, int dimension, PackedLongValues ords,
                         PackedLongValues ordCounts) {
      this.currentDocVectorsId = new int[vectors.size()];//should be the max recorded per document
      this.docsWithField = docsWithField;
      this.vectors = vectors;
      this.dimension = dimension;
      buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      binaryValue = new BytesRef(buffer.array());
      raBuffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      raBinaryValue = new BytesRef(raBuffer.array());

      this.vectorIdsIter = ords.iterator();
      this.vectorIdsPerDocumentCountIterator = ordCounts.iterator();
      docsWithFieldIter = docsWithField.iterator();
      this.ords = ords;
      this.ordCounts = ordCounts;
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new BufferedVectorValues(docsWithField, vectors, dimension, ords, ordCounts);
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

    @Override
    public BytesRef binaryValue(int targetOrd) {
      raBuffer.asFloatBuffer().put(vectors.get(targetOrd));
      return raBinaryValue;
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    public float[] vectorValue() {
      return vectors.get(currentDocVectorsId[visitedVectorIdPerCurrentDoc-1]);
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
        vectorsPerDocumentCount = (int) vectorIdsPerDocumentCountIterator.next();
        assert vectorsPerDocumentCount > 0;
        for (int i = 0; i < vectorsPerDocumentCount; i++) {
          currentDocVectorsId[i] = Math.toIntExact(vectorIdsIter.next());
        }
        visitedVectorIdPerCurrentDoc = 0;
      }
      return docID;
    }

    @Override
    public long nextOrd() {
      if (visitedVectorIdPerCurrentDoc == vectorsPerDocumentCount) {
        return -1;
      } else {
        return currentDocVectorsId[visitedVectorIdPerCurrentDoc++];
      }
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
