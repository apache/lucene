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
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
abstract class OffHeapFloatVectorValues extends FloatVectorValues {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final int byteSize;
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
  public Floats vectors() {
    return new Floats() {
      final IndexInput vectorSlice = slice.clone();
      int lastOrd = -1;
      float[] value = new float[dimension];

      @Override
      public float[] get(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
          return value;
        }
        vectorSlice.seek((long) targetOrd * byteSize);
        vectorSlice.readFloats(value, 0, value.length);
        lastOrd = targetOrd;
        return value;
      }
    };
  }

  static OffHeapFloatVectorValues load(
      Lucene94HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset() == -2) {
      return new EmptyOffHeapVectorValues(fieldEntry.dimension());
    }
    IndexInput bytesSlice =
        vectorData.slice(
            "vector-data", fieldEntry.vectorDataOffset(), fieldEntry.vectorDataLength());
    int byteSize =
        switch (fieldEntry.vectorEncoding()) {
          case BYTE -> fieldEntry.dimension();
          case FLOAT32 -> fieldEntry.dimension() * Float.BYTES;
        };
    if (fieldEntry.docsWithFieldOffset() == -1) {
      return new DenseOffHeapVectorValues(
          fieldEntry.dimension(),
          fieldEntry.size(),
          bytesSlice,
          fieldEntry.similarityFunction(),
          byteSize);
    } else {
      return new SparseOffHeapVectorValues(
          fieldEntry, vectorData, bytesSlice, fieldEntry.similarityFunction(), byteSize);
    }
  }

  static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        IndexInput slice,
        VectorSimilarityFunction vectorSimilarityFunction,
        int byteSize) {
      super(dimension, size, slice, vectorSimilarityFunction, byteSize);
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      Floats floats = vectors();
      DocIndexIterator iterator = iterator();

      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(floats.get(iterator.index()), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapFloatVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexInput dataIn;
    private final Lucene94HnswVectorsReader.FieldEntry fieldEntry;

    public SparseOffHeapVectorValues(
        Lucene94HnswVectorsReader.FieldEntry fieldEntry,
        IndexInput dataIn,
        IndexInput slice,
        VectorSimilarityFunction vectorSimilarityFunction,
        int byteSize)
        throws IOException {

      super(fieldEntry.dimension(), fieldEntry.size(), slice, vectorSimilarityFunction, byteSize);
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry.addressesOffset(), fieldEntry.addressesLength());
      this.ordToDoc = DirectMonotonicReader.getInstance(fieldEntry.meta(), addressesData);
      this.dataIn = dataIn;
      this.fieldEntry = fieldEntry;
    }

    private IndexedDISI createDISI() throws IOException {
      return new IndexedDISI(
          dataIn.clone(),
          fieldEntry.docsWithFieldOffset(),
          fieldEntry.docsWithFieldLength(),
          fieldEntry.jumpTableEntryCount(),
          fieldEntry.denseRankPower(),
          fieldEntry.size());
    }

    @Override
    public DocIndexIterator iterator() throws IOException {
      return IndexedDISI.asDocIndexIterator(createDISI());
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
      IndexedDISI disi = createDISI();
      Floats vectors = vectors();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(vectors.get(disi.index()), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return disi;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null, VectorSimilarityFunction.COSINE, 0);
    }

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public Floats vectors() {
      return Floats.EMPTY;
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
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
