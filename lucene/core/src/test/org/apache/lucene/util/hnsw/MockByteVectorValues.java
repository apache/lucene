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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

class MockByteVectorValues extends ByteVectorValues {
  private final int dimension;
  private final byte[][] denseValues;
  protected final byte[][] values;
  private final int numVectors;
  private final BytesRef binaryValue;
  private final byte[] scratch;

  static MockByteVectorValues fromValues(byte[][] values) {
    byte[] firstNonNull = null;
    int j = 0;
    while (firstNonNull == null && j < values.length) {
      firstNonNull = values[j++];
    }
    int dimension = firstNonNull.length;
    int maxDoc = values.length;
    byte[][] denseValues = new byte[maxDoc][];
    int count = 0;
    for (int i = 0; i < maxDoc; i++) {
      if (values[i] != null) {
        denseValues[count++] = values[i];
      }
    }
    return new MockByteVectorValues(values, dimension, denseValues, count);
  }

  MockByteVectorValues(byte[][] values, int dimension, byte[][] denseValues, int numVectors) {
    this.dimension = dimension;
    this.values = values;
    this.denseValues = denseValues;
    this.numVectors = numVectors;
    // used by tests that build a graph from bytes rather than floats
    binaryValue = new BytesRef(dimension);
    binaryValue.length = dimension;
    scratch = new byte[dimension];
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public MockByteVectorValues copy() {
    return new MockByteVectorValues(
        ArrayUtil.copyArray(values), dimension, ArrayUtil.copyArray(denseValues), numVectors);
  }

  @Override
  public byte[] vectorValue(int ord) {
    if (LuceneTestCase.random().nextBoolean()) {
      return values[ord];
    } else {
      // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
      // This should help us catch cases of aliasing where the same ByteVectorValues source is used
      // twice in a
      // single computation.
      System.arraycopy(values[ord], 0, scratch, 0, dimension);
      return scratch;
    }
  }

  @Override
  public DocIndexIterator iterator() {
    return createDenseIterator();
  }
}
