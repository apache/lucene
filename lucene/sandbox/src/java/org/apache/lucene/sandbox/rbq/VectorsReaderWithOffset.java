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

package org.apache.lucene.sandbox.rbq;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

// FIXME: Temporary to help with debugging and iteration
public class VectorsReaderWithOffset implements RandomAccessVectorValues.Floats {
  private final IndexInput slice;
  private final int size;
  private final int dim;
  private final int byteSize;
  private int lastOrd = -1;
  private final float[] value;
  private final int offset;

  public VectorsReaderWithOffset(IndexInput slice, int size, int dim, int offset) {
    this.slice = slice;
    this.size = size;
    this.dim = dim;
    // We assume that the start of ever vector entry includes an integer/float that indicates its
    // dimension count
    this.byteSize = Float.BYTES * dim + offset;
    value = new float[dim];
    this.offset = offset;
  }

  @Override
  public int dimension() {
    return dim;
  }

  @Override
  public IndexInput getSlice() {
    return slice;
  }

  @Override
  public int ordToDoc(int ord) {
    throw new IllegalStateException("Not supported");
  }

  @Override
  public Bits getAcceptOrds(Bits acceptDocs) {
    throw new IllegalStateException("Not supported");
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int getVectorByteLength() {
    return byteSize;
  }

  @Override
  public float[] vectorValue(int targetOrd) throws IOException {
    if (lastOrd == targetOrd) {
      return value;
    }
    // Get to the appropriate vector for the ordinal, then skip the first 4 bytes storing its
    // dimension count
    long seekPos = (long) targetOrd * byteSize + offset;
    slice.seek(seekPos);
    slice.readFloats(value, 0, value.length);
    lastOrd = targetOrd;
    return value;
  }

  @Override
  public Floats copy() throws IOException {
    return new VectorsReaderWithOffset(slice, size, dim, offset);
  }
}
