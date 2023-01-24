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
import org.apache.lucene.util.ArrayUtil;

class MockVectorValues extends AbstractMockVectorValues<float[]> {
  private final float[] scratch;

  static MockVectorValues fromValues(float[][] values) {
    int dimension = values[0].length;
    int maxDoc = values.length;
    float[][] denseValues = new float[maxDoc][];
    int count = 0;
    for (int i = 0; i < maxDoc; i++) {
      if (values[i] != null) {
        denseValues[count++] = values[i];
      }
    }
    return new MockVectorValues(values, dimension, denseValues, count);
  }

  MockVectorValues(float[][] values, int dimension, float[][] denseValues, int numVectors) {
    super(values, dimension, denseValues, numVectors);
    this.scratch = new float[dimension];
  }

  @Override
  public MockVectorValues copy() {
    return new MockVectorValues(
        ArrayUtil.copyOfSubArray(values, 0, values.length),
        dimension,
        ArrayUtil.copyOfSubArray(denseValues, 0, denseValues.length),
        numVectors);
  }

  @Override
  public float[] vectorValue() {
    if (LuceneTestCase.random().nextBoolean()) {
      return values[pos];
    } else {
      // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
      // This should help us catch cases of aliasing where the same vector values source is used
      // twice in a single computation.
      System.arraycopy(values[pos], 0, scratch, 0, dimension);
      return scratch;
    }
  }

  @Override
  public float[] vectorValue(int targetOrd) {
    return denseValues[targetOrd];
  }
}
