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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MultiVectorSimilarityFunction;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read multi-vector values from the index input. This supports both iterated and random access. */
public abstract class OffHeapFloatMultiVectorValues extends FloatVectorValues
    implements RandomAccessVectorValues.Floats {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final IndexInput vectorData;
  protected int lastOrd = -1;
  protected float[] value;
  protected ByteBuffer valueBuffer;
  protected final MultiVectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer flatMultiVectorScorer;
  protected final DirectMonotonicReader dataOffsetsReader;
  protected final MultiVectorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration;

  public OffHeapFloatMultiVectorValues(
      int dimension,
      int size,
      IndexInput slice,
      IndexInput dataIn,
      FlatVectorsScorer flatMultiVectorScorer,
      MultiVectorSimilarityFunction similarityFunction,
      MultiVectorDataOffsetsReaderConfiguration dataOffsetsConfiguration)
      throws IOException {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.vectorData = dataIn;
    this.flatMultiVectorScorer = flatMultiVectorScorer;
    this.similarityFunction = similarityFunction;
    this.dataOffsetsReaderConfiguration = dataOffsetsConfiguration;
    if (vectorData != null && dataOffsetsReaderConfiguration != null) {
      this.dataOffsetsReader = dataOffsetsReaderConfiguration.getDirectMonotonicReader(vectorData);
    } else {
      this.dataOffsetsReader = null;
    }
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
  public IndexInput getSlice() {
    return slice;
  }

  @Override
  public int getVectorByteLength() {
    return value.length * VectorEncoding.FLOAT32.byteSize;
  }

  protected float[] vectorValue(int targetOrd, LongValues dataOffsets) throws IOException {
    if (lastOrd == targetOrd) {
      return value;
    }
    long offset = dataOffsets.get(targetOrd);

    // Todo: switch to using ByteBuffer
    int length = (int) (dataOffsets.get(targetOrd + 1) - offset) / Float.BYTES;
    value = new float[length];
    slice.seek(offset);
    slice.readFloats(value, 0, length);

    //    // Use a ByteBuffer to avoid reallocation
    //    int length = (int)(dataOffsets.get(targetOrd + 1) - offset);
    //    if (valueBuffer == null || valueBuffer.capacity() < length) {
    //      valueBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
    //    }
    //    valueBuffer.reset();
    //    slice.seek(offset);
    //    slice.readBytes(valueBuffer.array(), valueBuffer.arrayOffset(), length);
    //    value = valueBuffer.slice(0,
    // length).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().array();

    lastOrd = targetOrd;
    return value;
  }

  @Override
  public float[] vectorValue(int targetOrd) throws IOException {
    return vectorValue(targetOrd, dataOffsetsReader);
  }

  public static OffHeapFloatMultiVectorValues load(
      MultiVectorSimilarityFunction multiVectorSimilarityFunction,
      FlatVectorsScorer flatMultiVectorScorer,
      OrdToDocDISIReaderConfiguration configuration,
      MultiVectorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration,
      VectorEncoding encoding,
      int dimension,
      long multiVectorDataOffset,
      long multiVectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.docsWithFieldOffset == -2 || encoding != VectorEncoding.FLOAT32) {
      return new EmptyOffHeapMultiVectorValues(
          dimension, flatMultiVectorScorer, multiVectorSimilarityFunction);
    }
    IndexInput vectorDataSlice =
        vectorData.slice("multi-vector-data", multiVectorDataOffset, multiVectorDataLength);
    if (configuration.docsWithFieldOffset == -1) {
      return new DenseOffHeapMultiVectorValues(
          dimension,
          configuration.size,
          vectorDataSlice,
          vectorData,
          flatMultiVectorScorer,
          multiVectorSimilarityFunction,
          dataOffsetsReaderConfiguration);
    } else {
      return new SparseOffHeapMultiVectorValues(
          dimension,
          vectorDataSlice,
          vectorData,
          flatMultiVectorScorer,
          multiVectorSimilarityFunction,
          dataOffsetsReaderConfiguration,
          configuration);
    }
  }

  /**
   * Dense multiVector values that are stored off-heap. This is the case when every doc has a
   * multiVector, and docId is the same as multiVector ordinal.
   */
  public static class DenseOffHeapMultiVectorValues extends OffHeapFloatMultiVectorValues {

    private int doc = -1;

    public DenseOffHeapMultiVectorValues(
        int dimension,
        int size,
        IndexInput slice,
        IndexInput vectorData,
        FlatVectorsScorer flatmultiVectorScorer,
        MultiVectorSimilarityFunction similarityFunction,
        MultiVectorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration)
        throws IOException {
      super(
          dimension,
          size,
          slice,
          vectorData,
          flatmultiVectorScorer,
          similarityFunction,
          dataOffsetsReaderConfiguration);
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
    public DenseOffHeapMultiVectorValues copy() throws IOException {
      return new DenseOffHeapMultiVectorValues(
          dimension,
          size,
          slice.clone(),
          vectorData.clone(),
          flatMultiVectorScorer,
          similarityFunction,
          dataOffsetsReaderConfiguration);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      DenseOffHeapMultiVectorValues copy = copy();
      RandomVectorScorer randomVectorScorer =
          flatMultiVectorScorer.getRandomMultiVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return randomVectorScorer.score(copy.doc);
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  /** Dense off-heap multiVector values created from in-memory data offsets */
  public static class DenseOffHeapMultiVectorValuesWithOffsets
      extends DenseOffHeapMultiVectorValues {
    final LongValues dataOffsets;

    public DenseOffHeapMultiVectorValuesWithOffsets(
        int dimension,
        int size,
        IndexInput slice,
        FlatVectorsScorer flatmultiVectorsScorer,
        MultiVectorSimilarityFunction similarityFunction,
        LongValues multiVectorDataOffsets)
        throws IOException {
      super(dimension, size, slice, null, flatmultiVectorsScorer, similarityFunction, null);
      dataOffsets = multiVectorDataOffsets;
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      return vectorValue(targetOrd, dataOffsets);
    }

    @Override
    public DenseOffHeapMultiVectorValues copy() throws IOException {
      return new DenseOffHeapMultiVectorValuesWithOffsets(
          dimension, size, slice.clone(), flatMultiVectorScorer, similarityFunction, dataOffsets);
    }
  }

  private static class SparseOffHeapMultiVectorValues extends OffHeapFloatMultiVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapMultiVectorValues(
        int dimension,
        IndexInput slice,
        IndexInput vectorData,
        FlatVectorsScorer flatMultiVectorScorer,
        MultiVectorSimilarityFunction similarityFunction,
        MultiVectorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration,
        OrdToDocDISIReaderConfiguration configuration)
        throws IOException {
      super(
          dimension,
          configuration.size,
          slice,
          vectorData,
          flatMultiVectorScorer,
          similarityFunction,
          dataOffsetsReaderConfiguration);
      this.configuration = configuration;
      this.ordToDoc = configuration.getDirectMonotonicReader(vectorData);
      this.disi =
          new IndexedDISI(
              vectorData,
              configuration.docsWithFieldOffset,
              configuration.docsWithFieldLength,
              configuration.jumpTableEntryCount,
              configuration.denseRankPower,
              configuration.size);
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
    public SparseOffHeapMultiVectorValues copy() throws IOException {
      return new SparseOffHeapMultiVectorValues(
          dimension,
          slice,
          vectorData,
          flatMultiVectorScorer,
          similarityFunction,
          dataOffsetsReaderConfiguration,
          configuration);
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
      SparseOffHeapMultiVectorValues copy = copy();
      RandomVectorScorer randomVectorScorer =
          flatMultiVectorScorer.getRandomMultiVectorScorer(similarityFunction, copy, query);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return randomVectorScorer.score(copy.disi.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  private static class EmptyOffHeapMultiVectorValues extends OffHeapFloatMultiVectorValues {

    public EmptyOffHeapMultiVectorValues(
        int dimension,
        FlatVectorsScorer flatMultiVectorScorer,
        MultiVectorSimilarityFunction similarityFunction)
        throws IOException {
      super(dimension, 0, null, null, flatMultiVectorScorer, similarityFunction, null);
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
    public int advance(int target) {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public EmptyOffHeapMultiVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) {
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
