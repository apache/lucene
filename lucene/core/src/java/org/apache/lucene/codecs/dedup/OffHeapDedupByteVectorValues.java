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

package org.apache.lucene.codecs.dedup;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Off-heap {@link ByteVectorValues} for {@link DedupFlatVectorsFormat}. */
abstract class OffHeapDedupByteVectorValues extends ByteVectorValues {

  protected final int dimension;
  protected final int size;
  protected final int uniqueCount;
  protected final IndexInput poolSlice;
  protected final int byteSize;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer flatVectorsScorer;
  protected final int[] docOrdToVecOrd;
  protected final byte[] binaryValue;
  protected final ByteBuffer byteBuffer;
  protected int lastVecOrd = -1;

  OffHeapDedupByteVectorValues(
      int dimension,
      int size,
      int uniqueCount,
      IndexInput poolSlice,
      int byteSize,
      FlatVectorsScorer flatVectorsScorer,
      VectorSimilarityFunction similarityFunction,
      int[] docOrdToVecOrd) {
    this.dimension = dimension;
    this.size = size;
    this.uniqueCount = uniqueCount;
    this.poolSlice = poolSlice;
    this.byteSize = byteSize;
    this.similarityFunction = similarityFunction;
    this.flatVectorsScorer = flatVectorsScorer;
    this.docOrdToVecOrd = docOrdToVecOrd;
    this.byteBuffer = ByteBuffer.allocate(byteSize);
    this.binaryValue = byteBuffer.array();
  }

  static OffHeapDedupByteVectorValues create(
      DedupFlatVectorsReader.FieldEntry entry, IndexInput vectorData, FlatVectorsScorer scorer)
      throws IOException {
    if (entry.cardinality == 0) {
      return new Empty(entry.pool.dim, scorer, entry.similarity);
    }
    IndexInput poolSlice = vectorData.slice("dedup-pool", entry.pool.offset, entry.pool.length);
    int byteSize = entry.pool.byteSize;
    int[] docOrdToVecOrd = entry.getOrLoadDocOrdToVecOrd(vectorData);
    if (entry.ordToDoc.isDense()) {
      return new Dense(
          entry.pool.dim,
          entry.cardinality,
          entry.pool.uniqueCount,
          poolSlice,
          byteSize,
          scorer,
          entry.similarity,
          docOrdToVecOrd);
    }
    return new Sparse(
        entry.ordToDoc,
        vectorData,
        poolSlice,
        entry.pool.dim,
        entry.cardinality,
        entry.pool.uniqueCount,
        byteSize,
        scorer,
        entry.similarity,
        docOrdToVecOrd);
  }

  /** See {@link OffHeapDedupFloatVectorValues#poolView}. */
  KnnVectorValues poolView() throws IOException {
    if (uniqueCount == 0 || poolSlice == null) {
      return new PoolView(dimension, 0, null, byteSize);
    }
    return new PoolView(dimension, uniqueCount, poolSlice.clone(), byteSize);
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
  public int getVectorByteLength() {
    return byteSize;
  }

  @Override
  public byte[] vectorValue(int targetOrd) throws IOException {
    int vecOrd = docOrdToVecOrd[targetOrd];
    if (vecOrd != lastVecOrd) {
      poolSlice.seek((long) vecOrd * byteSize);
      poolSlice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
      lastVecOrd = vecOrd;
    }
    return binaryValue;
  }

  @Override
  public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
    if (ordsToPrefetch == null) {
      return;
    }
    int finalNumOrds = Math.min(numOrds, ordsToPrefetch.length);
    if (finalNumOrds <= 1) {
      return;
    }
    for (int i = 0; i < finalNumOrds; i++) {
      int vecOrd = docOrdToVecOrd[ordsToPrefetch[i]];
      poolSlice.prefetch((long) vecOrd * byteSize, byteSize);
    }
  }

  static final class Dense extends OffHeapDedupByteVectorValues {
    Dense(
        int dimension,
        int size,
        int uniqueCount,
        IndexInput poolSlice,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction,
        int[] docOrdToVecOrd) {
      super(
          dimension,
          size,
          uniqueCount,
          poolSlice,
          byteSize,
          flatVectorsScorer,
          similarityFunction,
          docOrdToVecOrd);
    }

    @Override
    public Dense copy() throws IOException {
      return new Dense(
          dimension,
          size,
          uniqueCount,
          poolSlice.clone(),
          byteSize,
          flatVectorsScorer,
          similarityFunction,
          docOrdToVecOrd);
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public VectorScorer scorer(byte[] query) throws IOException {
      Dense copy = copy();
      DocIndexIterator iter = copy.iterator();
      RandomVectorScorer scorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(iter.docID());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iter;
        }

        @Override
        public VectorScorer.Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerDense(scorer, iter, matchingDocs);
        }
      };
    }
  }

  static final class Sparse extends OffHeapDedupByteVectorValues {
    private final OrdToDocDISIReaderConfiguration configuration;
    private final int fieldDocCount;
    private final IndexInput dataIn;
    private final IndexedDISI disi;
    private final DirectMonotonicReader ordToDoc;

    Sparse(
        OrdToDocDISIReaderConfiguration configuration,
        IndexInput dataIn,
        IndexInput poolSlice,
        int dimension,
        int fieldDocCount,
        int uniqueCount,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction,
        int[] docOrdToVecOrd)
        throws IOException {
      super(
          dimension,
          fieldDocCount,
          uniqueCount,
          poolSlice,
          byteSize,
          flatVectorsScorer,
          similarityFunction,
          docOrdToVecOrd);
      this.configuration = configuration;
      this.fieldDocCount = fieldDocCount;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
      this.disi = configuration.getIndexedDISI(dataIn);
    }

    @Override
    public Sparse copy() throws IOException {
      return new Sparse(
          configuration,
          dataIn,
          poolSlice.clone(),
          dimension,
          fieldDocCount,
          uniqueCount,
          byteSize,
          flatVectorsScorer,
          similarityFunction,
          docOrdToVecOrd);
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
    public DocIndexIterator iterator() {
      return IndexedDISI.asDocIndexIterator(disi);
    }

    @Override
    public VectorScorer scorer(byte[] query) throws IOException {
      Sparse copy = copy();
      DocIndexIterator iter = copy.iterator();
      RandomVectorScorer scorer =
          flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(iter.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iter;
        }
      };
    }
  }

  static final class Empty extends OffHeapDedupByteVectorValues {
    Empty(int dimension, FlatVectorsScorer flatVectorsScorer, VectorSimilarityFunction sim) {
      super(dimension, 0, 0, null, 0, flatVectorsScorer, sim, new int[0]);
    }

    @Override
    public Empty copy() {
      return this;
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
    public byte[] vectorValue(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer scorer(byte[] query) {
      return null;
    }
  }

  // -----------------------------------------------------------------------
  // PoolView — vec-ord-keyed view of the field's pool, with HasIndexSlice for SIMD
  // -----------------------------------------------------------------------

  static final class PoolView extends ByteVectorValues implements HasIndexSlice {
    private final int dimension;
    private final int uniqueCount;
    private final IndexInput poolSlice;
    private final int byteSize;
    private final byte[] reuse;

    PoolView(int dimension, int uniqueCount, IndexInput poolSlice, int byteSize) {
      this.dimension = dimension;
      this.uniqueCount = uniqueCount;
      this.poolSlice = poolSlice;
      this.byteSize = byteSize;
      this.reuse = new byte[byteSize];
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return uniqueCount;
    }

    @Override
    public int getVectorByteLength() {
      return byteSize;
    }

    @Override
    public IndexInput getSlice() {
      return poolSlice;
    }

    @Override
    public byte[] vectorValue(int vecOrd) throws IOException {
      poolSlice.seek((long) vecOrd * byteSize);
      poolSlice.readBytes(reuse, 0, byteSize);
      return reuse;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public PoolView copy() throws IOException {
      return new PoolView(
          dimension, uniqueCount, poolSlice == null ? null : poolSlice.clone(), byteSize);
    }
  }
}
