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
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.packed.DirectMonotonicReader;

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
  protected final int byteSize;
  protected int lastOrd = -1;
  protected final float[] correctiveValues = new float[2];

  OffHeapBinarizedVectorValues(
      int dimension,
      int size,
      VectorSimilarityFunction similarityFunction,
      FlatVectorsScorer vectorsScorer,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.similarityFunction = similarityFunction;
    this.vectorsScorer = vectorsScorer;
    this.slice = slice;
    int binaryDimensions = (dimension + 63) / 64 * 64;
    this.numBytes = binaryDimensions / 8;
    this.byteSize = numBytes + 8;
    this.byteBuffer = ByteBuffer.allocate(numBytes);
    this.binaryValue = byteBuffer.array();
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
    slice.readFloats(correctiveValues, 0, 2);
    lastOrd = targetOrd;
    return binaryValue;
  }

  @Override
  public float getDistanceToCentroid() throws IOException {
    return correctiveValues[0];
  }

  @Override
  public float getMagnitude() throws IOException {
    return correctiveValues[1];
  }

  @Override
  public float getCentroidDistance(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[0];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, 2);
    return correctiveValues[0];
  }

  @Override
  public float getVectorMagnitude(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return correctiveValues[0];
    }
    slice.seek(((long) targetOrd * byteSize) + numBytes);
    slice.readFloats(correctiveValues, 0, 2);
    return correctiveValues[1];
  }

  @Override
  public IndexInput getSlice() {
    return slice;
  }

  @Override
  public int getVectorByteLength() {
    return numBytes;
  }

  public static class DenseOffHeapVectorValues extends OffHeapBinarizedVectorValues {
    private int doc = -1;

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice) {
      super(dimension, size, similarityFunction, vectorsScorer, slice);
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
          dimension, size, similarityFunction, vectorsScorer, slice.clone());
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      // TODO
      throw new UnsupportedOperationException();
    }
  }

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
        IndexInput dataIn,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice)
        throws IOException {
      super(dimension, size, similarityFunction, vectorsScorer, slice);
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
          configuration, dimension, size, dataIn, similarityFunction, vectorsScorer, slice.clone());
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
      // TODO
      throw new UnsupportedOperationException();
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapBinarizedVectorValues {
    private int doc = -1;

    EmptyOffHeapVectorValues(
        int dimension,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer) {
      super(dimension, 0, similarityFunction, vectorsScorer, null);
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
    public DenseOffHeapVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      throw null;
    }
  }
}
