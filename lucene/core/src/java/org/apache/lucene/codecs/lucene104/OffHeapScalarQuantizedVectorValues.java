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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/**
 * Scalar quantized vector values loaded from off-heap
 *
 * @lucene.internal
 */
public abstract class OffHeapScalarQuantizedVectorValues extends QuantizedByteVectorValues {
  final int dimension;
  final int size;
  final VectorSimilarityFunction similarityFunction;
  final FlatVectorsScorer vectorsScorer;

  final IndexInput slice;
  final byte[] vectorValue;
  final ByteBuffer byteBuffer;
  final int byteSize;
  private int lastOrd = -1;
  final float[] correctiveValues;
  int quantizedComponentSum;
  final OptimizedScalarQuantizer quantizer;
  final ScalarEncoding encoding;
  final float[] centroid;
  final float centroidDp;
  final boolean isQuerySide;

  OffHeapScalarQuantizedVectorValues(
      int dimension,
      int size,
      float[] centroid,
      float centroidDp,
      OptimizedScalarQuantizer quantizer,
      ScalarEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    this(
        false,
        dimension,
        size,
        centroid,
        centroidDp,
        quantizer,
        encoding,
        similarityFunction,
        vectorsScorer,
        slice);
  }

  OffHeapScalarQuantizedVectorValues(
      boolean isQuerySide,
      int dimension,
      int size,
      float[] centroid,
      float centroidDp,
      OptimizedScalarQuantizer quantizer,
      ScalarEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    assert isQuerySide == false || encoding.isAsymmetric();
    this.isQuerySide = isQuerySide;
    this.dimension = dimension;
    this.size = size;
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
    this.slice = slice;
    this.centroid = centroid;
    this.centroidDp = centroidDp;
    this.correctiveValues = new float[3];
    this.encoding = encoding;
    int docPackedLength =
        isQuerySide
            ? encoding.getQueryPackedLength(dimension)
            : encoding.getDocPackedLength(dimension);
    this.byteSize = docPackedLength + (Float.BYTES * 3) + Integer.BYTES;
    this.byteBuffer = ByteBuffer.allocate(docPackedLength);
    this.vectorValue = byteBuffer.array();
    this.quantizer = quantizer;
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
    if (lastOrd == targetOrd) {
      return vectorValue;
    }
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), vectorValue.length);
    slice.readFloats(correctiveValues, 0, 3);
    quantizedComponentSum = slice.readInt();
    lastOrd = targetOrd;
    return vectorValue;
  }

  @Override
  public float getCentroidDP() {
    return centroidDp;
  }

  @Override
  public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int targetOrd)
      throws IOException {
    if (lastOrd == targetOrd) {
      return new OptimizedScalarQuantizer.QuantizationResult(
          correctiveValues[0], correctiveValues[1], correctiveValues[2], quantizedComponentSum);
    }
    slice.seek(((long) targetOrd * byteSize) + vectorValue.length);
    slice.readFloats(correctiveValues, 0, 3);
    quantizedComponentSum = slice.readInt();
    return new OptimizedScalarQuantizer.QuantizationResult(
        correctiveValues[0], correctiveValues[1], correctiveValues[2], quantizedComponentSum);
  }

  @Override
  public OptimizedScalarQuantizer getQuantizer() {
    return quantizer;
  }

  @Override
  public ScalarEncoding getScalarEncoding() {
    return encoding;
  }

  @Override
  public float[] getCentroid() {
    return centroid;
  }

  @Override
  public int getVectorByteLength() {
    return dimension;
  }

  static void packNibbles(byte[] unpacked, byte[] packed) {
    assert unpacked.length == packed.length * 2;
    for (int i = 0; i < packed.length; i++) {
      int x = unpacked[i] << 4 | unpacked[packed.length + i];
      packed[i] = (byte) x;
    }
  }

  static void unpackNibbles(byte[] packed, byte[] unpacked) {
    assert unpacked.length == packed.length * 2;
    for (int i = 0; i < packed.length; i++) {
      unpacked[i] = (byte) ((packed[i] >> 4) & 0x0F);
      unpacked[packed.length + i] = (byte) (packed[i] & 0x0F);
    }
  }

  static OffHeapScalarQuantizedVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int size,
      OptimizedScalarQuantizer quantizer,
      ScalarEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      float[] centroid,
      float centroidDp,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.isEmpty()) {
      return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
    }
    assert centroid != null;
    IndexInput bytesSlice =
        vectorData.slice(
            "quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
    if (configuration.isDense()) {
      return new DenseOffHeapVectorValues(
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          vectorData,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    }
  }

  /** Dense off-heap scalar quantized vector values */
  static class DenseOffHeapVectorValues extends OffHeapScalarQuantizedVectorValues {
    DenseOffHeapVectorValues(
        int dimension,
        int size,
        float[] centroid,
        float centroidDp,
        OptimizedScalarQuantizer quantizer,
        ScalarEncoding encoding,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarityFunction,
          vectorsScorer,
          slice);
    }

    DenseOffHeapVectorValues(
        boolean isQuerySide,
        int dimension,
        int size,
        float[] centroid,
        float centroidDp,
        OptimizedScalarQuantizer quantizer,
        ScalarEncoding encoding,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(
          isQuerySide,
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarityFunction,
          vectorsScorer,
          slice);
    }

    @Override
    public OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues copy() throws IOException {
      return new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
          isQuerySide,
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarityFunction,
          vectorsScorer,
          slice.clone());
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      assert isQuerySide == false;
      OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues copy = copy();
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
  private static class SparseOffHeapVectorValues extends OffHeapScalarQuantizedVectorValues {
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
        float centroidDp,
        OptimizedScalarQuantizer quantizer,
        ScalarEncoding encoding,
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
          encoding,
          similarityFunction,
          vectorsScorer,
          slice);
      assert isQuerySide == false;
      this.configuration = configuration;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
      this.disi = configuration.getIndexedDISI(dataIn);
    }

    @Override
    public SparseOffHeapVectorValues copy() throws IOException {
      assert isQuerySide == false;
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          centroid,
          centroidDp,
          quantizer,
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
      assert isQuerySide == false;
      SparseOffHeapVectorValues copy = copy();
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

  private static class EmptyOffHeapVectorValues extends OffHeapScalarQuantizedVectorValues {
    EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(
          dimension,
          0,
          null,
          Float.NaN,
          null,
          ScalarEncoding.UNSIGNED_BYTE,
          similarityFunction,
          vectorsScorer,
          null);
      assert isQuerySide == false;
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues copy() {
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
