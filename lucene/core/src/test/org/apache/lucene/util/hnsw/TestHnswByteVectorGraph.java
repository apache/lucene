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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.AbstractVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;

/** Tests HNSW KNN graphs */
public class TestHnswByteVectorGraph extends HnswGraphTestCase<BytesRef> {

  @Before
  public void setup() {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  Query knnQuery(String field, BytesRef vector, int k) {
    return new KnnByteVectorQuery(field, vector, k);
  }

  @Override
  BytesRef randomVector(int dim) {
    return new BytesRef(randomVector8(random(), dim));
  }

  @Override
  AbstractMockVectorValues<BytesRef> vectorValues(int size, int dimension) {
    return MockByteVectorValues.fromValues(createRandomByteVectors(size, dimension, random()));
  }

  static boolean fitsInByte(float v) {
    return v <= 127 && v >= -128 && v % 1 == 0;
  }

  @Override
  AbstractMockVectorValues<BytesRef> vectorValues(float[][] values) {
    byte[][] bValues = new byte[values.length][];
    // The case when all floats fit within a byte already.
    boolean scaleSimple = fitsInByte(values[0][0]);
    for (int i = 0; i < values.length; i++) {
      bValues[i] = new byte[values[i].length];
      for (int j = 0; j < values[i].length; j++) {
        final float v;
        if (scaleSimple) {
          assert fitsInByte(values[i][j]);
          v = values[i][j];
        } else {
          v = values[i][j] * 127;
        }
        bValues[i][j] = (byte) v;
      }
    }
    return MockByteVectorValues.fromValues(bValues);
  }

  @Override
  AbstractVectorValues<BytesRef> vectorValues(LeafReader reader, String fieldName)
      throws IOException {
    return reader.getByteVectorValues(fieldName);
  }

  @Override
  Field knnVectorField(String name, BytesRef vector, VectorSimilarityFunction similarityFunction) {
    return new KnnByteVectorField(name, vector, similarityFunction);
  }

  @Override
  RandomAccessVectorValues<BytesRef> circularVectorValues(int nDoc) {
    return new CircularByteVectorValues(nDoc);
  }

  @Override
  BytesRef getTargetVector() {
    return new BytesRef(new byte[] {1, 0});
  }
}
