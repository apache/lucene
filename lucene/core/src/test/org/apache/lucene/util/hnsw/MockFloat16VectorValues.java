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

import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

class MockFloat16VectorValues extends Float16VectorValues {
  private final int dimension;
  private final short[][] denseValues;
  protected final short[][] values;
  private final int numVectors;
  private final short[] scratch;

  static MockFloat16VectorValues fromValues(short[][] values) {
    short[] firstNonNull = null;
    int j = 0;
    while (firstNonNull == null && j < values.length) {
      firstNonNull = values[j++];
    }
    int dimension = firstNonNull.length;
    int maxDoc = values.length;
    short[][] denseValues = new short[maxDoc][];
    int count = 0;
    for (int i = 0; i < maxDoc; i++) {
      if (values[i] != null) {
        denseValues[count++] = values[i];
      }
    }
    return new MockFloat16VectorValues(values, dimension, denseValues, count);
  }

  MockFloat16VectorValues(short[][] values, int dimension, short[][] denseValues, int numVectors) {
    this.dimension = dimension;
    this.values = values;
    this.denseValues = denseValues;
    this.numVectors = numVectors;
    this.scratch = new short[dimension];
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
  public MockFloat16VectorValues copy() {
    return new MockFloat16VectorValues(
        ArrayUtil.copyArray(values), dimension, ArrayUtil.copyArray(denseValues), numVectors);
  }

  @Override
  public short[] vectorValue(int ord) {
    if (LuceneTestCase.random().nextBoolean()) {
      return values[ord];
    } else {
      // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
      // This should help us catch cases of aliasing where the same vector values source is used
      // twice in a single computation.
      System.arraycopy(values[ord], 0, scratch, 0, dimension);
      return scratch;
    }
  }

  @Override
  public DocIndexIterator iterator() {
    return createDenseIterator();
  }
}
