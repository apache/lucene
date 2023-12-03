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
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
public abstract class OffHeapFloatVectorValues extends FloatVectorValues
    implements RandomAccessVectorValues<float[]> {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final int byteSize;
  protected int lastOrd = -1;
  protected final float[] value;

  OffHeapFloatVectorValues(int dimension, int size, IndexInput slice, int byteSize) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    this.byteSize = byteSize;
    value = new float[dimension];
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
      return value;
    }
    slice.seek((long) targetOrd * byteSize);
    slice.readFloats(value, 0, value.length);
    lastOrd = targetOrd;
    return value;
  }

  public static OffHeapFloatVectorValues load(
      OrdToDocDISIReaderConfiguration configuration,
      VectorEncoding vectorEncoding,
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      IndexInput vectorData)
      throws IOException {
    if (configuration.docsWithFieldOffset == -2 || vectorEncoding != VectorEncoding.FLOAT32) {
      return new EmptyOffHeapVectorValues(dimension);
    }
    IndexInput bytesSlice = vectorData.slice("vector-data", vectorDataOffset, vectorDataLength);
    int byteSize = dimension * Float.BYTES;
    if (configuration.docsWithFieldOffset == -1) {
      return new DenseOffHeapVectorValues(dimension, configuration.size, bytesSlice, byteSize);
    } else {
      return new SparseOffHeapVectorValues(
          configuration, vectorData, bytesSlice, dimension, byteSize);
    }
  }

  /**
   * Dense vector values that are stored off-heap. This is the most common case when every doc has a
   * vector.
   */
  public static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

    private int doc = -1;

    public DenseOffHeapVectorValues(int dimension, int size, IndexInput slice, int byteSize) {
      super(dimension, size, slice, byteSize);
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
    public RandomAccessVectorValues<float[]> copy() throws IOException {
      return new DenseOffHeapVectorValues(dimension, size, slice.clone(), byteSize);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapFloatVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final OrdToDocDISIReaderConfiguration configuration;

    public SparseOffHeapVectorValues(
        OrdToDocDISIReaderConfiguration configuration,
        IndexInput dataIn,
        IndexInput slice,
        int dimension,
        int byteSize)
        throws IOException {

      super(dimension, configuration.size, slice, byteSize);
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
    public RandomAccessVectorValues<float[]> copy() throws IOException {
      return new SparseOffHeapVectorValues(
          configuration, dataIn, slice.clone(), dimension, byteSize);
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
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null, 0);
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
    public int advance(int target) throws IOException {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public RandomAccessVectorValues<float[]> copy() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
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
  }
}
