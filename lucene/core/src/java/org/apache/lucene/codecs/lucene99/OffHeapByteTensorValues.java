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

import org.apache.lucene.codecs.hnsw.FlatTensorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Read the tensor values from the index input. This supports both iterated and random access. */
public abstract class OffHeapByteTensorValues extends ByteVectorValues
    implements RandomAccessVectorValues.Bytes {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final IndexInput tensorData;
  protected int lastOrd = -1;
  protected byte[] value;
  protected ByteBuffer valueBuffer;
  protected final TensorSimilarityFunction similarityFunction;
  protected final FlatTensorsScorer flatTensorsScorer;
  protected final DirectMonotonicReader dataOffsetsReader;
  protected final TensorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration;

  public OffHeapByteTensorValues(
      int dimension,
      int size,
      IndexInput slice,
      IndexInput dataIn,
      FlatTensorsScorer flatTensorsScorer,
      TensorSimilarityFunction similarityFunction,
      TensorDataOffsetsReaderConfiguration dataOffsetsConfiguration) throws IOException {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.tensorData = dataIn;
    this.flatTensorsScorer = flatTensorsScorer;
    this.similarityFunction = similarityFunction;
    this.dataOffsetsReaderConfiguration = dataOffsetsConfiguration;
    if (tensorData != null && dataOffsetsReaderConfiguration != null) {
      final RandomAccessInput dataOffsetsSlice =
          tensorData.randomAccessSlice(
              dataOffsetsReaderConfiguration.addressesOffset, dataOffsetsReaderConfiguration.addressesLength);
      this.dataOffsetsReader = DirectMonotonicReader.getInstance(dataOffsetsReaderConfiguration.meta, dataOffsetsSlice);
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
    return value.length * VectorEncoding.BYTE.byteSize;
  }

  protected byte[] vectorValue(int targetOrd, LongValues dataOffsets) throws IOException {
    if (lastOrd == targetOrd) {
      return value;
    }
    long offset = dataOffsets.get(targetOrd);
    int length = (int)(dataOffsets.get(targetOrd + 1) - offset);
    if (value == null || valueBuffer.capacity() < length) {
      valueBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
    }

    slice.seek(offset);
    valueBuffer.reset();
    slice.readBytes(valueBuffer.array(), valueBuffer.arrayOffset(), length);
    value = valueBuffer.slice(0, length).array();
    assert value.length == length;
    lastOrd = targetOrd;
    return value;
  }

  @Override
  public byte[] vectorValue(int targetOrd) throws IOException {
    return vectorValue(targetOrd, dataOffsetsReader);
  }

  public static OffHeapByteTensorValues load(
      TensorSimilarityFunction tensorSimilarityFunction,
      FlatTensorsScorer flatTensorsScorer,
      OrdToDocDISIReaderConfiguration configuration,
      TensorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration,
      VectorEncoding tensorEncoding,
      int dimension,
      long tensorDataOffset,
      long tensorDataLength,
      IndexInput tensorData)
      throws IOException {
    if (configuration.docsWithFieldOffset == -2 || tensorEncoding != VectorEncoding.BYTE) {
      return new EmptyOffHeapTensorValues(dimension, flatTensorsScorer, tensorSimilarityFunction);
    }
    IndexInput tensorDataSlice = tensorData.slice("tensor-data", tensorDataOffset, tensorDataLength);
    if (configuration.docsWithFieldOffset == -1) {
      return new DenseOffHeapTensorValues(
          dimension,
          configuration.size,
          tensorDataSlice,
          tensorData,
          flatTensorsScorer,
          tensorSimilarityFunction,
          dataOffsetsReaderConfiguration);
    } else {
      return new SparseOffHeapTensorValues(
          dimension,
          tensorDataSlice,
          tensorData,
          flatTensorsScorer,
          tensorSimilarityFunction,
          dataOffsetsReaderConfiguration,
          configuration);
    }
  }

  /**
   * Dense tensor values that are stored off-heap.
   * This is the case when every doc has a tensor, and docId is the same as tensor ordinal.
   */
  public static class DenseOffHeapTensorValues extends OffHeapByteTensorValues {

    private int doc = -1;

    public DenseOffHeapTensorValues(
        int dimension,
        int size,
        IndexInput slice,
        IndexInput tensorData,
        FlatTensorsScorer flatTensorsScorer,
        TensorSimilarityFunction similarityFunction,
        TensorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration) throws IOException {
      super(dimension, size, slice, tensorData, flatTensorsScorer, similarityFunction, dataOffsetsReaderConfiguration);
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
    public DenseOffHeapTensorValues copy() throws IOException {
      return new DenseOffHeapTensorValues(
          dimension, size, slice.clone(), tensorData.clone(), flatTensorsScorer, similarityFunction, dataOffsetsReaderConfiguration);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(byte[] query) throws IOException {
      DenseOffHeapTensorValues copy = copy();
      RandomVectorScorer randomVectorScorer =
          flatTensorsScorer.getRandomTensorScorer(similarityFunction, copy, query);
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

  public static class DenseOffHeapTensorValuesWithOffsets extends OffHeapByteTensorValues.DenseOffHeapTensorValues {
    final LongValues dataOffsets;

    public DenseOffHeapTensorValuesWithOffsets(
        int dimension,
        int size,
        IndexInput slice,
        FlatTensorsScorer flatTensorsScorer,
        TensorSimilarityFunction similarityFunction,
        LongValues tensorDataOffsets) throws IOException {
      super(dimension, size, slice, null, flatTensorsScorer, similarityFunction, null);
      dataOffsets = tensorDataOffsets;
    }

    @Override
    public byte[] vectorValue(int targetOrd) throws IOException {
      return vectorValue(targetOrd, dataOffsets);
    }

    @Override
    public DenseOffHeapTensorValues copy() throws IOException {
      return new DenseOffHeapTensorValuesWithOffsets(
          dimension, size, slice.clone(), flatTensorsScorer, similarityFunction, dataOffsets);
    }
  }

  private static class SparseOffHeapTensorValues extends OffHeapByteTensorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapTensorValues(
        int dimension,
        IndexInput slice,
        IndexInput tensorData,
        FlatTensorsScorer flatTensorsScorer,
        TensorSimilarityFunction similarityFunction,
        TensorDataOffsetsReaderConfiguration dataOffsetsReaderConfiguration,
        OrdToDocDISIReaderConfiguration configuration) throws IOException {
      super(dimension, configuration.size, slice, tensorData, flatTensorsScorer, similarityFunction, dataOffsetsReaderConfiguration);
      this.configuration = configuration;
      final RandomAccessInput disiAddressesData =
          tensorData.randomAccessSlice(configuration.addressesOffset, configuration.addressesLength);
      this.ordToDoc = DirectMonotonicReader.getInstance(configuration.meta, disiAddressesData);
      this.disi =
          new IndexedDISI(
              tensorData,
              configuration.docsWithFieldOffset,
              configuration.docsWithFieldLength,
              configuration.jumpTableEntryCount,
              configuration.denseRankPower,
              configuration.size);
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
    public SparseOffHeapTensorValues copy() throws IOException {
      return new SparseOffHeapTensorValues(
          dimension,
          slice,
          tensorData,
          flatTensorsScorer,
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
    public VectorScorer scorer(byte[] query) throws IOException {
      SparseOffHeapTensorValues copy = copy();
      RandomVectorScorer randomVectorScorer =
          flatTensorsScorer.getRandomTensorScorer(similarityFunction, copy, query);
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

  private static class EmptyOffHeapTensorValues extends OffHeapByteTensorValues {

    public EmptyOffHeapTensorValues(
        int dimension,
        FlatTensorsScorer flatTensorsScorer,
        TensorSimilarityFunction similarityFunction) throws IOException {
      super(dimension, 0, null, null, flatTensorsScorer, similarityFunction, null);
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
    public int advance(int target) {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public EmptyOffHeapTensorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] vectorValue(int targetOrd) {
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
