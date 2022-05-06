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

package org.apache.lucene.codecs.lucene92;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
abstract class OffHeapVectorValues extends VectorValues
    implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final BytesRef binaryValue;
  protected final ByteBuffer byteBuffer;
  protected final int byteSize;
  protected final float[] value;

  OffHeapVectorValues(int dimension, int size, IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    byteSize = Float.BYTES * dimension;
    byteBuffer = ByteBuffer.allocate(byteSize);
    value = new float[dimension];
    binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
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
  public long cost() {
    return size;
  }

  @Override
  public float[] vectorValue(int targetOrd) throws IOException {
    slice.seek((long) targetOrd * byteSize);
    slice.readFloats(value, 0, value.length);
    return value;
  }

  @Override
  public BytesRef binaryValue(int targetOrd) throws IOException {
    readValue(targetOrd);
    return binaryValue;
  }

  private void readValue(int targetOrd) throws IOException {
    slice.seek((long) targetOrd * byteSize);
    slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
  }

  public abstract int ordToDoc(int ord);

  static OffHeapVectorValues load(
      Lucene92HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset == -2) {
      return new EmptyOffHeapVectorValues(fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    if (fieldEntry.docsWithFieldOffset == -1) {
      return new DenseOffHeapVectorValues(fieldEntry.dimension, fieldEntry.size, bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(fieldEntry, vectorData, bytesSlice);
    }
  }

  abstract Bits getAcceptOrds(Bits acceptDocs);

  static class DenseOffHeapVectorValues extends OffHeapVectorValues {

    private int doc = -1;

    public DenseOffHeapVectorValues(int dimension, int size, IndexInput slice) {
      super(dimension, size, slice);
    }

    @Override
    public float[] vectorValue() throws IOException {
      slice.seek((long) doc * byteSize);
      slice.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      slice.seek((long) doc * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
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
    public RandomAccessVectorValues randomAccess() throws IOException {
      return new DenseOffHeapVectorValues(dimension, size, slice.clone());
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final Lucene92HnswVectorsReader.FieldEntry fieldEntry;

    public SparseOffHeapVectorValues(
        Lucene92HnswVectorsReader.FieldEntry fieldEntry, IndexInput dataIn, IndexInput slice)
        throws IOException {

      super(fieldEntry.dimension, fieldEntry.size, slice);
      this.fieldEntry = fieldEntry;
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry.addressesOffset, fieldEntry.addressesLength);
      this.dataIn = dataIn;
      this.ordToDoc = DirectMonotonicReader.getInstance(fieldEntry.meta, addressesData);
      this.disi =
          new IndexedDISI(
              dataIn,
              fieldEntry.docsWithFieldOffset,
              fieldEntry.docsWithFieldLength,
              fieldEntry.jumpTableEntryCount,
              fieldEntry.denseRankPower,
              fieldEntry.size);
    }

    @Override
    public float[] vectorValue() throws IOException {
      slice.seek((long) (disi.index()) * byteSize);
      slice.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      slice.seek((long) (disi.index()) * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
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
    public RandomAccessVectorValues randomAccess() throws IOException {
      return new SparseOffHeapVectorValues(fieldEntry, dataIn, slice.clone());
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
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

  private static class EmptyOffHeapVectorValues extends OffHeapVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null);
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
    public BytesRef binaryValue() throws IOException {
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
    public long cost() {
      return 0;
    }

    @Override
    public RandomAccessVectorValues randomAccess() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }
  }
}
