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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import org.apache.lucene.util.BytesRef;

abstract class AbstractMockVectorValues<T> implements RandomAccessVectorValues<T> {

  protected final int dimension;
  protected final T[] denseValues;
  protected final T[] values;
  protected final int numVectors;
  protected final BytesRef binaryValue;

  protected int pos = -1;

  AbstractMockVectorValues(T[] values, int dimension, T[] denseValues, int numVectors) {
    this.dimension = dimension;
    this.values = values;
    this.denseValues = denseValues;
    // used by tests that build a graph from bytes rather than floats
    binaryValue = new BytesRef(dimension);
    binaryValue.length = dimension;
    this.numVectors = numVectors;
  }

  @Override
  public int size() {
    return numVectors;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public T vectorValue(int targetOrd) {
    return denseValues[targetOrd];
  }

  @Override
  public abstract AbstractMockVectorValues<T> copy();

  public abstract T vectorValue() throws IOException;

  private boolean seek(int target) {
    if (target >= 0 && target < values.length && values[target] != null) {
      pos = target;
      return true;
    } else {
      return false;
    }
  }

  public int docID() {
    return pos;
  }

  public int nextDoc() {
    return advance(pos + 1);
  }

  public int advance(int target) {
    while (++pos < values.length) {
      if (seek(pos)) {
        return pos;
      }
    }
    return NO_MORE_DOCS;
  }
}
