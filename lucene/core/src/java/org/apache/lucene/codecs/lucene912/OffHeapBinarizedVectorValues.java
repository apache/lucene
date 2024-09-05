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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/** Binarized vector values loaded from off-heap */
public abstract class OffHeapBinarizedVectorValues extends BinarizedByteVectorValues
    implements RandomAccessBinarizedByteVectorValues {

  protected final int dimension;
  protected final int size;
  protected final int numBytes;
  protected final VectorSimilarityFunction similarityFunction;
  protected final FlatVectorsScorer vectorsScorer;

  protected final IndexInput slice;
  protected final byte[] binaryValue;
  protected final ByteBuffer byteBuffer;
  protected short clusterId = 0;
  protected final int byteSize;
  private int lastOrd = -1;
  protected final float[] correctiveValues;
  protected final BinaryQuantizer binaryQuantizer;
  protected final float[][] centroids;
  private final int correctionsCount;
  protected final LongValues vectorOrdToCentroidOrd;

  // TODO do we want to use `LongValues` to store vectorOrd -> centroidOrd mapping?
  OffHeapBinarizedVectorValues(
      int dimension,
      int size,
      float[][] centroids,
      BinaryQuantizer quantizer,
      LongValues vectorOrdToCentroidOrd,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
    this.slice = slice;
    this.centroids = centroids;
    this.vectorOrdToCentroidOrd = vectorOrdToCentroidOrd;
    if (centroids != null && centroids.length > 1) {
      assert vectorOrdToCentroidOrd != null;
    }
    this.numBytes = BQVectorUtils.discretize(dimension, 64) / 8;
    this.correctionsCount =
        similarityFunction == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT ? 3 : 2;
    this.correctiveValues = new float[this.correctionsCount];
    this.byteSize = numBytes + (Float.BYTES * correctionsCount);
    this.byteBuffer = ByteBuffer.allocate(numBytes);
    this.binaryValue = byteBuffer.array();
    this.binaryQuantizer = quantizer;
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
      return binaryValue;
    }
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), numBytes);
    if (vectorOrdToCentroidOrd != null) {
      clusterId = (short) vectorOrdToCentroidOrd.get(targetOrd);
    }
    slice.readFloats(correctiveValues, 0, correctionsCount);
    lastOrd = targetOrd;
    return binaryValue;
  }

  @Override
  public float[] getCorrectiveTerms() {
    return correctiveValues;
  }

  @Override
  public float getCentroidDistance(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[0];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, correctionsCount);
    return correctiveValues[0];
  }

  @Override
  public float getVectorMagnitude(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[1];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, correctionsCount);
    return correctiveValues[1];
  }

  @Override
  public float getOOQ(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[0];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, correctionsCount);
    return correctiveValues[0];
  }

  @Override
  public float getNormOC(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[1];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, correctionsCount);
    return correctiveValues[1];
  }

  @Override
  public float getODotC(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[2];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, correctionsCount);
    return correctiveValues[2];
  }

  @Override
  public short getClusterId(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return clusterId;
    }
    if (vectorOrdToCentroidOrd == null) {
      return clusterId;
    }
    clusterId = (short) vectorOrdToCentroidOrd.get(targetOrd);
    return clusterId;
  }

  @Override
  public BinaryQuantizer getQuantizer() {
    return binaryQuantizer;
  }

  @Override
  public float[][] getCentroids() {
    return centroids;
  }

  @Override
  public IndexInput getSlice() {
    return slice;
  }

  @Override
  public int getVectorByteLength() {
    return numBytes;
  }

  public static OffHeapBinarizedVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int size,
      BinaryQuantizer binaryQuantizer,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      float[][] centroids,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      long centroidOffset,
      long centroidLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.isEmpty()) {
      return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
    }
    assert centroids != null;
    LongValues vectorOrdToCentroidOrd = null;
    if (centroids.length > 1) {
      assert centroidOffset >= 0;
      assert centroidLength > 0;
      RandomAccessInput centroidSlice =
          vectorData
              .slice("vector-centroid-data", centroidOffset, centroidLength)
              .randomAccessSlice(0, centroidLength);
      vectorOrdToCentroidOrd =
          DirectReader.getInstance(
              centroidSlice, DirectWriter.unsignedBitsRequired(centroids.length));
    }
    IndexInput bytesSlice =
        vectorData.slice(
            "quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
    if (configuration.isDense()) {
      return new DenseOffHeapVectorValues(
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(
          configuration,
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
          vectorData,
          similarityFunction,
          vectorsScorer,
          bytesSlice);
    }
  }

  /** Dense off-heap binarized vector values */
  public static class DenseOffHeapVectorValues extends OffHeapBinarizedVectorValues {
    private int doc = -1;

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        float[][] centroids,
        BinaryQuantizer binaryQuantizer,
        LongValues vectorOrdToCentroidOrd,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
          similarityFunction,
          vectorsScorer,
          slice);
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return vectorValue(doc);
    }

    @Override
    public short clusterId() throws IOException {
      return super.getClusterId(doc);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      assert docID() < target;
      if (target >= size) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public DenseOffHeapVectorValues copy() throws IOException {
      return new DenseOffHeapVectorValues(
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
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
      DenseOffHeapVectorValues copy = copy();
      RandomVectorScorer scorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(copy.doc);
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  /** Sparse off-heap binarized vector values */
  private static class SparseOffHeapVectorValues extends OffHeapBinarizedVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        float[][] centroids,
        BinaryQuantizer binaryQuantizer,
        LongValues vectorOrdToCentroidOrd,
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
          similarityFunction,
          vectorsScorer,
          slice);
      this.configuration = configuration;
      this.dataIn = dataIn;
      this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
      this.disi = configuration.getIndexedDISI(dataIn);
    }

    @Override
    public byte[] vectorValue() throws IOException {
      return vectorValue(disi.index());
    }

    @Override
    public short clusterId() throws IOException {
      return super.getClusterId(disi.index());
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
          configuration,
          dimension,
          size,
          centroids,
          binaryQuantizer,
          vectorOrdToCentroidOrd,
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
      RandomVectorScorer scorer =
          vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return scorer.score(copy.disi.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return copy;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapBinarizedVectorValues {
    private int doc = -1;

    EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(dimension, 0, null, null, null, similarityFunction, vectorsScorer, null);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public byte[] vectorValue() {
      throw new UnsupportedOperationException();
    }

    @Override
    public short clusterId() {
      return 0;
    }

    @Override
    public DenseOffHeapVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      return null;
    }
  }
}
