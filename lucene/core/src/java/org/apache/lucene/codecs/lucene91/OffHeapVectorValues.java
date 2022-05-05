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

package org.apache.lucene.codecs.lucene91;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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
      Lucene91HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset == -2) {
      return new Lucene91HnswVectorsReader.EmptyOffHeapVectorValues(fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    if (fieldEntry.docsWithFieldOffset == -1) {
      return new Lucene91HnswVectorsReader.DenseOffHeapVectorValues(
          fieldEntry.dimension, fieldEntry.size, bytesSlice);
    } else {
      return new Lucene91HnswVectorsReader.SparseOffHeapVectorValues(
          fieldEntry, vectorData, bytesSlice);
    }
  }

  abstract Bits getAcceptOrds(Bits acceptDocs);
}
