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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

class MockByteVectorValues extends AbstractMockVectorValues<BytesRef> {
  private final byte[] scratch;

  static MockByteVectorValues fromValues(byte[][] byteValues) {
    int dimension = byteValues[0].length;
    BytesRef[] values = new BytesRef[byteValues.length];
    for (int i = 0; i < byteValues.length; i++) {
      values[i] = byteValues[i] == null ? null : new BytesRef(byteValues[i]);
    }
    BytesRef[] denseValues = new BytesRef[values.length];
    int count = 0;
    for (int i = 0; i < byteValues.length; i++) {
      if (values[i] != null) {
        denseValues[count++] = values[i];
      }
    }
    return new MockByteVectorValues(values, dimension, denseValues, count);
  }

  MockByteVectorValues(BytesRef[] values, int dimension, BytesRef[] denseValues, int numVectors) {
    super(values, dimension, denseValues, numVectors);
    scratch = new byte[dimension];
  }

  @Override
  public MockByteVectorValues copy() {
    return new MockByteVectorValues(values, dimension, denseValues, numVectors);
  }

  @Override
  public BytesRef vectorValue() {
    if (LuceneTestCase.random().nextBoolean()) {
      return values[pos];
    } else {
      // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
      // This should help us catch cases of aliasing where the same ByteVectorValues source is used
      // twice in a
      // single computation.
      System.arraycopy(values[pos].bytes, values[pos].offset, scratch, 0, dimension);
      return new BytesRef(scratch);
    }
  }
}
