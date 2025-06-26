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
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reads quantized vector values from the index input and returns float vector values after
 * dequantizing them.
 *
 * <p>This class provides functionality to read byte vectors that have been quantized and stored in
 * the index, and then dequantize them back to float vectors with some precision loss. The
 * implementation is based on {@code OffHeapQuantizedByteVectorValues} with modifications to the
 * {@code vectorValue()} method to return float vectors after dequantizing the byte vectors.
 *
 * <p>Usage: This class is used for read-only indexes where full-precision float vectors have been
 * dropped from the index to save storage space. Full-precision vectors can be removed from an index
 * using a method as implemnted in {@code
 * TestLucene99ScalarQuantizedVectorsFormat.simulateEmptyRawVectors()}.
 *
 * @lucene.internal
 */
abstract class OffHeapQuantizedFloatVectorValues extends FloatVectorValues
    implements HasIndexSlice {

  protected final int dimension;
  protected final int size;
  protected final int numBytes;
  protected final ScalarQuantizer scalarQuantizer;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer vectorsScorer;
  protected final boolean compress;

  protected final IndexInput slice;
  protected final float[] floatValue;
  protected final byte[] binaryValue;
  protected final ByteBuffer byteBuffer;
  protected final int byteSize;
  protected int curOrd = -1;
  protected final float[] scoreCorrectionConstant = new float[1];

  OffHeapQuantizedFloatVectorValues(
      int dimension,
      int size,
      ScalarQuantizer scalarQuantizer,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      boolean compress,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.scalarQuantizer = scalarQuantizer;
    this.compress = compress;
    if (scalarQuantizer.getBits() <= 4 && compress) {
      this.numBytes = (dimension + 1) >> 1;
    } else {
      this.numBytes = dimension;
    }
    this.byteSize = this.numBytes + Float.BYTES;
    byteBuffer = ByteBuffer.allocate(dimension);
    binaryValue = byteBuffer.array();
    floatValue = new float[dimension];
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
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
    if (curOrd == targetOrd) {
      return floatValue;
    }
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), numBytes);
    slice.readFloats(scoreCorrectionConstant, 0, 1);
    OffHeapQuantizedByteVectorValues.decompressBytes(binaryValue, numBytes);
    scalarQuantizer.deQuantize(binaryValue, floatValue);
    curOrd = targetOrd;
    return floatValue;
  }

  @Override
  public IndexInput getSlice() {
    return slice;
  }

  @Override
  public int getVectorByteLength() {
    return numBytes;
  }

  public static OffHeapQuantizedFloatVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int size,
      ScalarQuantizer scalarQuantizer,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      boolean compress,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      IndexInput vectorData)
      throws IOException {

    if (size == 0) {
      return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
    }
    IndexInput bytesSlice =
        vectorData.slice(
            "quantized-float-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);

    if (configuration.isDense()) {
      return new DenseOffHeapVectorValues(
          dimension,
          size,
          scalarQuantizer,
          compress,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          scalarQuantizer,
          compress,
          vectorData,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    }
  }

  /**
   * Dense vector values that are stored off-heap. This is the most common case when every doc has a
   * vector.
   */
  private static class DenseOffHeapVectorValues extends OffHeapQuantizedFloatVectorValues {

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        ScalarQuantizer scalarQuantizer,
        boolean compress,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, compress, slice);
    }

    @Override
    public DenseOffHeapVectorValues copy() throws IOException {
      return new DenseOffHeapVectorValues(
          dimension,
          size,
          scalarQuantizer,
          compress,
          similarityFunction,
          vectorsScorer,
          slice.clone());
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
    public VectorScorer scorer(float[] target) throws IOException {
      DenseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapQuantizedFloatVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        ScalarQuantizer scalarQuantizer,
        boolean compress,
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, compress, slice);
      this.configuration = configuration;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
      this.disi = configuration.getIndexedDISI(dataIn);
    }

    @Override
    public DocIndexIterator iterator() {
      return IndexedDISI.asDocIndexIterator(disi);
    }

    @Override
    public SparseOffHeapVectorValues copy() throws IOException {
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          scalarQuantizer,
          compress,
          dataIn,
          similarityFunction,
          vectorsScorer,
          slice.clone());
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
    public VectorScorer scorer(float[] target) throws IOException {
      SparseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapQuantizedFloatVectorValues {

    public EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(
          dimension,
          0,
          new ScalarQuantizer(-1, 1, (byte) 7),
          similarityFunction,
          vectorsScorer,
          false,
          null);
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
    public DocIndexIterator iterator() {
      return createDenseIterator();
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
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Override
    public VectorScorer scorer(float[] target) {
      return null;
    }
  }
}
