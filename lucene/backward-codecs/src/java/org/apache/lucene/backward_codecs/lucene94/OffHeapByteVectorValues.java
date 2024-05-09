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

package org.apache.lucene.backward_codecs.lucene94;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
abstract class OffHeapByteVectorValues extends ByteVectorValues
    implements RandomAccessVectorValues.Bytes {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected int lastOrd = -1;
  protected final byte[] binaryValue;
  protected final ByteBuffer byteBuffer;
  protected final int byteSize;
  protected final VectorSimilarityFunction vectorSimilarityFunction;

  OffHeapByteVectorValues(
      int dimension,
      int size,
      IndexInput slice,
      VectorSimilarityFunction vectorSimilarityFunction,
      int byteSize) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.byteSize = byteSize;
    this.vectorSimilarityFunction = vectorSimilarityFunction;
    byteBuffer = ByteBuffer.allocate(byteSize);
    binaryValue = byteBuffer.array();
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
  public byte[] vectorValue(int targetOrd) throws IOException {
    if (lastOrd != targetOrd) {
      readValue(targetOrd);
      lastOrd = targetOrd;
    }
    return binaryValue;
  }

  private void readValue(int targetOrd) throws IOException {
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
  }

  static OffHeapByteVectorValues load(
      Lucene94HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset == -2 || fieldEntry.vectorEncoding != VectorEncoding.BYTE) {
      return new EmptyOffHeapVectorValues(fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    int byteSize = fieldEntry.dimension;
    if (fieldEntry.docsWithFieldOffset == -1) {
      return new DenseOffHeapVectorValues(
          fieldEntry.dimension,
          fieldEntry.size,
          bytesSlice,
          fieldEntry.similarityFunction,
          byteSize);
    } else {
      return new SparseOffHeapVectorValues(
          fieldEntry, vectorData, bytesSlice, fieldEntry.similarityFunction, byteSize);
    }
  }

  static class DenseOffHeapVectorValues extends OffHeapByteVectorValues {

    private int doc = -1;

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        IndexInput slice,
        VectorSimilarityFunction vectorSimilarityFunction,
        int byteSize) {
      super(dimension, size, slice, vectorSimilarityFunction, byteSize);
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return vectorValue(doc);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      assert docID() < target;
      if (target >= size) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public DenseOffHeapVectorValues copy() throws IOException {
      return new DenseOffHeapVectorValues(
          dimension, size, slice.clone(), vectorSimilarityFunction, byteSize);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(byte[] query) throws IOException {
      DenseOffHeapVectorValues copy = this.copy();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(copy.vectorValue(), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapByteVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final Lucene94HnswVectorsReader.FieldEntry fieldEntry;

    public SparseOffHeapVectorValues(
        Lucene94HnswVectorsReader.FieldEntry fieldEntry,
        IndexInput dataIn,
        IndexInput slice,
        VectorSimilarityFunction vectorSimilarityFunction,
        int byteSize)
        throws IOException {

      super(fieldEntry.dimension, fieldEntry.size, slice, vectorSimilarityFunction, byteSize);
      this.fieldEntry = fieldEntry;
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry.addressesOffset, fieldEntry.addressesLength);
      this.dataIn = dataIn;
      this.ordToDoc = DirectMonotonicReader.getInstance(fieldEntry.meta, addressesData);
      this.disi =
          new IndexedDISI(
              dataIn,
              fieldEntry.docsWithFieldOffset,
              fieldEntry.docsWithFieldLength,
              fieldEntry.jumpTableEntryCount,
              fieldEntry.denseRankPower,
              fieldEntry.size);
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return vectorValue(disi.index());
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      assert docID() < target;
      return disi.advance(target);
    }

    @Override
    public SparseOffHeapVectorValues copy() throws IOException {
      return new SparseOffHeapVectorValues(
          fieldEntry, dataIn, slice.clone(), vectorSimilarityFunction, byteSize);
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      if (acceptDocs == null) {
        return null;
      }
      return new Bits() {
        @Override
        public boolean get(int index) {
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return size;
        }
      };
    }

    @Override
    public VectorScorer scorer(byte[] query) throws IOException {
      SparseOffHeapVectorValues copy = this.copy();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(copy.vectorValue(), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapByteVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null, VectorSimilarityFunction.COSINE, 0);
    }

    private int doc = -1;

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public byte[] vectorValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public OffHeapByteVectorValues copy() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] vectorValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Override
    public VectorScorer scorer(byte[] query) {
      return null;
    }
  }
}
