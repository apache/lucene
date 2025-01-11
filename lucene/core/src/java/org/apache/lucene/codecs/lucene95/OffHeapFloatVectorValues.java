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

package org.apache.lucene.codecs.lucene95;

import java.io.IOException;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene99.MultiVectorOrdConfiguration;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
public abstract class OffHeapFloatVectorValues extends FloatVectorValues implements HasIndexSlice {

  protected final int dimension;
  protected final int ordCount;
  protected final int docCount;
  protected final IndexInput slice;
  protected final int byteSize;
  protected int lastOrd = -1;
  protected final float[] value;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer flatVectorsScorer;

  protected final boolean isMultiValued;
  protected final LongValues docIndexToBaseOrd;
  protected final LongValues ordToDocMap;
  protected final LongValues baseOrdMap;
  protected final LongValues nextBaseOrdMap;

  OffHeapFloatVectorValues(
      int dimension,
      int ordCount,
      int docCount,
      IndexInput slice,
      int byteSize,
      FlatVectorsScorer flatVectorsScorer,
      VectorSimilarityFunction similarityFunction,
      boolean isMultiValued,
      LongValues docIndexToBaseOrd,
      LongValues ordToDocMap,
      LongValues baseOrdMap,
      LongValues nextBaseOrdMap) {
    this.dimension = dimension;
    this.ordCount = ordCount;
    this.docCount = docCount;
    this.slice = slice;
    this.byteSize = byteSize;
    this.similarityFunction = similarityFunction;
    this.flatVectorsScorer = flatVectorsScorer;
    value = new float[dimension];
    this.isMultiValued = isMultiValued;
    this.docIndexToBaseOrd = docIndexToBaseOrd;
    this.ordToDocMap = ordToDocMap;
    this.baseOrdMap = baseOrdMap;
    this.nextBaseOrdMap = nextBaseOrdMap;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public int size() {
    return ordCount;
  }

  @Override
  public int docCount() {
    return docCount;
  }

  @Override
  public IndexInput getSlice() {
    return slice;
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

  @Override
  public int baseOrd(int ord) {
    if (isMultiValued == true) {
      return (int) baseOrdMap.get(ord);
    }
    return super.baseOrd(ord);
  }

  @Override
  public int vectorCount(int ord) {
    if (isMultiValued == true) {
      return (int) nextBaseOrdMap.get(ord) - baseOrd(ord);
    }
    return super.vectorCount(ord);
  }

  @Override
  public int docIndexToBaseOrd(int index) {
    if (isMultiValued == true) {
      return (int) docIndexToBaseOrd.get(index);
    }
    return super.docIndexToBaseOrd(index);
  }

  public static OffHeapFloatVectorValues load(
      VectorSimilarityFunction vectorSimilarityFunction,
      FlatVectorsScorer flatVectorsScorer,
      OrdToDocDISIReaderConfiguration configuration,
      MultiVectorOrdConfiguration mvOrdConfiguration,
      VectorEncoding vectorEncoding,
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.docsWithFieldOffset == -2 || vectorEncoding != VectorEncoding.FLOAT32) {
      return new EmptyOffHeapVectorValues(dimension, flatVectorsScorer, vectorSimilarityFunction);
    }
    boolean isMultiValued = false;
    LongValues docIndexToBaseOrd = null;
    LongValues ordToDocMap = null;
    LongValues baseOrdMap = null;
    LongValues nextBaseOrdMap = null;
    int docCount = configuration.size;
    int ordCount = docCount;
    IndexInput bytesSlice = vectorData.slice("vector-data", vectorDataOffset, vectorDataLength);
    if (mvOrdConfiguration != null) {
      isMultiValued = mvOrdConfiguration.ordCount() > docCount;
      docIndexToBaseOrd = mvOrdConfiguration.getDocIndexToBaseOrdReader(bytesSlice);
      ordToDocMap = mvOrdConfiguration.getOrdToDocReader(bytesSlice);
      baseOrdMap = mvOrdConfiguration.getBaseOrdReader(bytesSlice);
      nextBaseOrdMap = mvOrdConfiguration.getNextBaseOrdReader(bytesSlice);
      ordCount = mvOrdConfiguration.ordCount();
    }
    int byteSize = dimension * Float.BYTES;
    if (configuration.docsWithFieldOffset == -1) {
      return new DenseOffHeapVectorValues(
          dimension,
          ordCount,
          docCount,
          bytesSlice,
          byteSize,
          flatVectorsScorer,
          vectorSimilarityFunction,
          isMultiValued,
          docIndexToBaseOrd,
          ordToDocMap,
          baseOrdMap,
          nextBaseOrdMap);
    } else {
      return new SparseOffHeapVectorValues(
          ordCount,
          configuration,
          vectorData,
          bytesSlice,
          dimension,
          byteSize,
          flatVectorsScorer,
          vectorSimilarityFunction,
          isMultiValued,
          docIndexToBaseOrd,
          ordToDocMap,
          baseOrdMap,
          nextBaseOrdMap);
    }
  }

  /**
   * Dense vector values that are stored off-heap. This is the most common case when every doc has a
   * vector.
   */
  public static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

    public DenseOffHeapVectorValues(
        int dimension,
        int ordCount,
        int docCount,
        IndexInput slice,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction,
        boolean isMultiValued,
        LongValues docOrdCount,
        LongValues ordToDocMap,
        LongValues baseOrdMap,
        LongValues nextBaseOrdMap
        ) {
      super(dimension, ordCount, docCount, slice, byteSize, flatVectorsScorer, similarityFunction, isMultiValued, docOrdCount, ordToDocMap, baseOrdMap, nextBaseOrdMap);
    }

    @Override
    public DenseOffHeapVectorValues copy() throws IOException {
      return new DenseOffHeapVectorValues(
          dimension, ordCount, docCount, slice.clone(), byteSize, flatVectorsScorer, similarityFunction, isMultiValued, docIndexToBaseOrd, ordToDocMap, baseOrdMap, nextBaseOrdMap);
    }

    @Override
    public int ordToDoc(int ord) {
      if (isMultiValued == true) {
        return (int) ordToDocMap.get(ord);
      }
      return ord;
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          if (acceptDocs == null) {
            return index >= 0 && index < size();
          }
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return size();
        }
      };
    }

    @Override
    public DocIndexIterator iterator() {
      return multiValueWrappedIterator(createDenseIterator());
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      DenseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer randomVectorScorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return randomVectorScorer.score(iterator.index());
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
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapVectorValues(
        int ordCount,
        OrdToDocDISIReaderConfiguration configuration,
        IndexInput dataIn,
        IndexInput slice,
        int dimension,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction,
        boolean isMultiValued,
        LongValues docIndexToBaseOrd,
        LongValues ordToDocMap,
        LongValues baseOrdMap,
        LongValues nextBaseOrdMap)
        throws IOException {

      super(dimension, ordCount, configuration.size, slice, byteSize, flatVectorsScorer, similarityFunction,
          isMultiValued, docIndexToBaseOrd, ordToDocMap, baseOrdMap, nextBaseOrdMap);
      this.configuration = configuration;
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(configuration.addressesOffset, configuration.addressesLength);
      this.dataIn = dataIn;
      this.ordToDoc = DirectMonotonicReader.getInstance(configuration.meta, addressesData);
      this.disi =
          new IndexedDISI(
              dataIn,
              configuration.docsWithFieldOffset,
              configuration.docsWithFieldLength,
              configuration.jumpTableEntryCount,
              configuration.denseRankPower,
              configuration.size);
    }

    @Override
    public SparseOffHeapVectorValues copy() throws IOException {
      return new SparseOffHeapVectorValues(
          size(),
          configuration,
          dataIn,
          slice.clone(),
          dimension,
          byteSize,
          flatVectorsScorer,
          similarityFunction,
          isMultiValued,
          docIndexToBaseOrd,
          ordToDocMap,
          baseOrdMap,
          nextBaseOrdMap);
    }

    @Override
    public int ordToDoc(int ord) {
      if (isMultiValued == true) {
        return (int) ordToDocMap.get(ord);
      }
      return (int) ordToDoc.get(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          if (acceptDocs == null) {
            return index >= 0 && index < size();
          }
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return size();
        }
      };
    }

    @Override
    public DocIndexIterator iterator() {
      return multiValueWrappedIterator(IndexedDISI.asDocIndexIterator(disi));
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      SparseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer randomVectorScorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return randomVectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

    public EmptyOffHeapVectorValues(
        int dimension,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction) {
      super(dimension, 0, 0, null, 0, flatVectorsScorer, similarityFunction,
          false, null, null, null, null);
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
    public EmptyOffHeapVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return new Bits.MatchNoBits(0);
    }

    @Override
    public VectorScorer scorer(float[] query) {
      return null;
    }
  }
}
