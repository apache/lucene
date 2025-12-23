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

package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.deQuantize;

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
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/**
 * Reads quantized vector values from the index input and returns float vector values after
 * dequantizing them.
 *
 * <p>This class provides functionality to read quantized vectors which are stored in the index, and
 * then dequantize them back to float vectors with some precision loss. The implementation is based
 * on {@code OffHeapScalarQuantizedVectorValues} with modifications to the {@code vectorValue()}
 * method to return float vectors after dequantizing the vectors.
 *
 * <p>Usage: This class is used for read-only indexes where full-precision float vectors have been
 * dropped from the index to save storage space. Full-precision vectors can be removed from an index
 * using a method as implemented in {@code
 * TestLucene104ScalarQuantizedVectorsFormat.simulateEmptyRawVectors()}.
 *
 * @lucene.internal
 */
abstract class OffHeapScalarQuantizedFloatVectorValues extends FloatVectorValues
    implements HasIndexSlice {

  final int dimension;
  final int size;
  final VectorSimilarityFunction similarityFunction;
  final FlatVectorsScorer vectorsScorer;

  final IndexInput slice;
  final float[] vectorValue;
  final byte[] byteValue;
  final ByteBuffer byteBuffer;
  final byte[] unpackedByteVectorValue;
  final int byteSize;
  private int lastOrd = -1;
  final float[] correctiveValues;
  int quantizedComponentSum;
  final Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding;
  final float[] centroid;

  OffHeapScalarQuantizedFloatVectorValues(
      int dimension,
      int size,
      float[] centroid,
      Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
    this.slice = slice;
    this.centroid = centroid;
    this.correctiveValues = new float[3];
    this.encoding = encoding;
    int docPackedLength = encoding.getDocPackedLength(dimension);
    this.byteSize = docPackedLength + (Float.BYTES * 3) + Integer.BYTES;
    this.byteBuffer = ByteBuffer.allocate(docPackedLength);
    this.vectorValue = new float[dimension];
    this.byteValue = byteBuffer.array();
    this.unpackedByteVectorValue = new byte[dimension];
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
      return vectorValue;
    }

    // read quantized byte vector, correctiveValues and quantizedComponentSum
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteValue.length);
    slice.readFloats(correctiveValues, 0, 3);
    quantizedComponentSum = slice.readInt();

    // unpack bytes
    switch (encoding) {
      case PACKED_NIBBLE ->
          OffHeapScalarQuantizedVectorValues.unpackNibbles(byteValue, unpackedByteVectorValue);
      case SINGLE_BIT_QUERY_NIBBLE ->
          OptimizedScalarQuantizer.unpackBinary(byteValue, unpackedByteVectorValue);
      case UNSIGNED_BYTE, SEVEN_BIT -> {
        deQuantize(
            byteValue,
            vectorValue,
            encoding.getBits(),
            correctiveValues[0],
            correctiveValues[1],
            centroid);
        lastOrd = targetOrd;
        return vectorValue;
      }
    }

    // dequantize
    deQuantize(
        unpackedByteVectorValue,
        vectorValue,
        encoding.getBits(),
        correctiveValues[0],
        correctiveValues[1],
        centroid);

    lastOrd = targetOrd;
    return vectorValue;
  }

  public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int targetOrd)
      throws IOException {
    if (lastOrd == targetOrd) {
      return new OptimizedScalarQuantizer.QuantizationResult(
          correctiveValues[0], correctiveValues[1], correctiveValues[2], quantizedComponentSum);
    }
    slice.seek(((long) targetOrd * byteSize) + byteValue.length);
    slice.readFloats(correctiveValues, 0, 3);
    quantizedComponentSum = slice.readInt();
    return new OptimizedScalarQuantizer.QuantizationResult(
        correctiveValues[0], correctiveValues[1], correctiveValues[2], quantizedComponentSum);
  }

  @Override
  public int getVectorByteLength() {
    return dimension;
  }

  @Override
  public IndexInput getSlice() {
    return slice;
  }

  static OffHeapScalarQuantizedFloatVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int size,
      Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      float[] centroid,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.isEmpty()) {
      return new OffHeapScalarQuantizedFloatVectorValues.EmptyOffHeapVectorValues(
          dimension, similarityFunction, vectorsScorer);
    }
    assert centroid != null;
    IndexInput bytesSlice =
        vectorData.slice(
            "scalar-quantized-float-vector-data",
            quantizedVectorDataOffset,
            quantizedVectorDataLength);
    if (configuration.isDense()) {
      return new OffHeapScalarQuantizedFloatVectorValues.DenseOffHeapVectorValues(
          dimension, size, centroid, encoding, similarityFunction, vectorsScorer, bytesSlice);
    } else {
      return new OffHeapScalarQuantizedFloatVectorValues.SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          centroid,
          encoding,
          vectorData,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    }
  }

  /** Dense off-heap scalar quantized vector values */
  private static class DenseOffHeapVectorValues extends OffHeapScalarQuantizedFloatVectorValues {
    DenseOffHeapVectorValues(
        int dimension,
        int size,
        float[] centroid,
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(dimension, size, centroid, encoding, similarityFunction, vectorsScorer, slice);
    }

    @Override
    public OffHeapScalarQuantizedFloatVectorValues.DenseOffHeapVectorValues copy()
        throws IOException {
      return new OffHeapScalarQuantizedFloatVectorValues.DenseOffHeapVectorValues(
          dimension, size, centroid, encoding, similarityFunction, vectorsScorer, slice.clone());
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      OffHeapScalarQuantizedFloatVectorValues.DenseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer scorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public VectorScorer.Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerDense(scorer, iterator, matchingDocs);
        }
      };
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }
  }

  /** Sparse off-heap scalar quantized vector values */
  private static class SparseOffHeapVectorValues extends OffHeapScalarQuantizedFloatVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        float[] centroid,
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(dimension, size, centroid, encoding, similarityFunction, vectorsScorer, slice);
      this.configuration = configuration;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
      this.disi = configuration.getIndexedDISI(dataIn);
    }

    @Override
    public OffHeapScalarQuantizedFloatVectorValues.SparseOffHeapVectorValues copy()
        throws IOException {
      return new OffHeapScalarQuantizedFloatVectorValues.SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          centroid,
          encoding,
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
    public DocIndexIterator iterator() {
      return IndexedDISI.asDocIndexIterator(disi);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      OffHeapScalarQuantizedFloatVectorValues.SparseOffHeapVectorValues copy = copy();
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer scorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public VectorScorer.Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerSparse(scorer, iterator, matchingDocs);
        }
      };
    }
  }

  /** Empty vector values */
  private static class EmptyOffHeapVectorValues extends OffHeapScalarQuantizedFloatVectorValues {
    EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(
          dimension,
          0,
          null,
          Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.UNSIGNED_BYTE,
          similarityFunction,
          vectorsScorer,
          null);
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public OffHeapScalarQuantizedFloatVectorValues.DenseOffHeapVectorValues copy() {
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
