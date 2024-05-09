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
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
abstract class OffHeapFloatVectorValues extends FloatVectorValues
    implements RandomAccessVectorValues.Floats {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final int byteSize;
  protected int lastOrd = -1;
  protected final float[] value;
  protected final VectorSimilarityFunction vectorSimilarityFunction;

  OffHeapFloatVectorValues(
      int dimension,
      int size,
      IndexInput slice,
      VectorSimilarityFunction vectorSimilarityFunction,
      int byteSize) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.byteSize = byteSize;
    value = new float[dimension];
    this.vectorSimilarityFunction = vectorSimilarityFunction;
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
  public float[] vectorValue(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return value;
    }
    slice.seek((long) targetOrd * byteSize);
    slice.readFloats(value, 0, value.length);
    lastOrd = targetOrd;
    return value;
  }

  static OffHeapFloatVectorValues load(
      Lucene94HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset == -2) {
      return new EmptyOffHeapVectorValues(fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    int byteSize = fieldEntry.dimension * fieldEntry.vectorEncoding.byteSize;
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

  static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

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
    public float[] vectorValue() throws IOException {
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
    public VectorScorer scorer(float[] query) throws IOException {
      DenseOffHeapVectorValues values = this.copy();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return values.vectorSimilarityFunction.compare(values.vectorValue(), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return values;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapFloatVectorValues {
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
    public float[] vectorValue() throws IOException {
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
    public VectorScorer scorer(float[] query) throws IOException {
      SparseOffHeapVectorValues values = this.copy();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return values.vectorSimilarityFunction.compare(values.vectorValue(), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return values;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

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
    public float[] vectorValue() throws IOException {
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
    public OffHeapFloatVectorValues copy() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
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
    public VectorScorer scorer(float[] query) {
      return null;
    }
  }
}
