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
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
public abstract class OffHeapByteVectorValues extends ByteVectorValues {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final int byteSize;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer flatVectorsScorer;

  OffHeapByteVectorValues(
      int dimension,
      int size,
      IndexInput slice,
      int byteSize,
      FlatVectorsScorer flatVectorsScorer,
      VectorSimilarityFunction similarityFunction) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.byteSize = byteSize;
    this.similarityFunction = similarityFunction;
    this.flatVectorsScorer = flatVectorsScorer;
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
  public Bytes values() throws IOException {
    return new OffHeapBytes();
  }

  // can't be anonymous because it extends + implements
  class OffHeapBytes extends Bytes implements HasIndexSlice {

    private final ByteBuffer byteBuffer;
    private final byte[] binaryValue;
    private final IndexInput input;
    private int lastOrd = -1;

    OffHeapBytes() throws IOException {
      byteBuffer = ByteBuffer.allocate(byteSize);
      binaryValue = byteBuffer.array();
      input = slice.clone();
    }

    @Override
    public byte[] get(int targetOrd) throws IOException {
      if (lastOrd != targetOrd) {
        readValue(targetOrd);
        lastOrd = targetOrd;
      }
      return binaryValue;
    }

    @Override
    public IndexInput getSlice() {
      return input;
    }

    private void readValue(int targetOrd) throws IOException {
      input.seek((long) targetOrd * byteSize);
      input.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }
  }

  public static OffHeapByteVectorValues load(
      VectorSimilarityFunction vectorSimilarityFunction,
      FlatVectorsScorer flatVectorsScorer,
      OrdToDocDISIReaderConfiguration configuration,
      VectorEncoding vectorEncoding,
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.isEmpty() || vectorEncoding != VectorEncoding.BYTE) {
      return new EmptyOffHeapVectorValues(dimension, flatVectorsScorer, vectorSimilarityFunction);
    }
    IndexInput bytesSlice = vectorData.slice("vector-data", vectorDataOffset, vectorDataLength);
    if (configuration.isDense()) {
      return new DenseOffHeapVectorValues(
          dimension,
          configuration.size,
          bytesSlice,
          dimension,
          flatVectorsScorer,
          vectorSimilarityFunction);
    } else {
      return new SparseOffHeapVectorValues(
          configuration,
          vectorData,
          bytesSlice,
          dimension,
          dimension,
          flatVectorsScorer,
          vectorSimilarityFunction);
    }
  }

  /**
   * Dense vector values that are stored off-heap. This is the most common case when every doc has a
   * vector.
   */
  public static class DenseOffHeapVectorValues extends OffHeapByteVectorValues {
    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        IndexInput slice,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction vectorSimilarityFunction) {
      super(dimension, size, slice, byteSize, flatVectorsScorer, vectorSimilarityFunction);
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
    public VectorScorer scorer(byte[] query) throws IOException {
      DocIndexIterator iterator = iterator();
      RandomVectorScorer scorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, this, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(iterator.docID());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapByteVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;
    private final IndexedDISI disi;
    private DocIndexIterator iterator;

    public SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        IndexInput dataIn,
        IndexInput slice,
        int dimension,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {

      super(
          dimension,
          configuration.size,
          slice,
          byteSize,
          flatVectorsScorer,
          vectorSimilarityFunction);
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(configuration.addressesOffset, configuration.addressesLength);
      this.ordToDoc = DirectMonotonicReader.getInstance(configuration.meta, addressesData);
      this.dataIn = dataIn;
      this.configuration = configuration;
      this.disi = createDISI();
    }

    IndexedDISI createDISI() throws IOException {
      return new IndexedDISI(
              dataIn.clone(),
              configuration.docsWithFieldOffset,
              configuration.docsWithFieldLength,
              configuration.jumpTableEntryCount,
              configuration.denseRankPower,
              configuration.size);
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    @Override
    public DocIndexIterator iterator() {
      // we can only create a single iterator since creating a new IndexedDISI
      // could throw an IOException
      assert iterator == null;
      iterator = IndexedDISI.asDocIndexIterator(disi);
      return iterator;
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
      RandomVectorScorer scorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, this, query);
      return new VectorScorer() {
        IndexedDISI disi = createDISI();

        @Override
        public float score() throws IOException {
          return scorer.score(disi.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return disi;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapByteVectorValues {

    public EmptyOffHeapVectorValues(
        int dimension,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction vectorSimilarityFunction) {
      super(dimension, 0, null, 0, flatVectorsScorer, vectorSimilarityFunction);
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
    public Bytes values() {
      return Bytes.EMPTY;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
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
