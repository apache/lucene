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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.junit.Before;

/** Tests HNSW KNN graphs */
public class TestHnswByteVectorGraph extends HnswGraphTestCase<byte[]> {

  @Before
  public void setup() {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  Query knnQuery(String field, byte[] vector, int k) {
    return new KnnByteVectorQuery(field, vector, k);
  }

  @Override
  byte[] randomVector(int dim) {
    return randomVector8(random(), dim);
  }

  @Override
  AbstractMockVectorValues<byte[]> vectorValues(int size, int dimension) {
    return MockByteVectorValues.fromValues(createRandomByteVectors(size, dimension, random()));
  }

  static boolean fitsInByte(float v) {
    return v <= 127 && v >= -128 && v % 1 == 0;
  }

  @Override
  AbstractMockVectorValues<byte[]> vectorValues(float[][] values) {
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
  AbstractMockVectorValues<byte[]> vectorValues(
      int size,
      int dimension,
      AbstractMockVectorValues<byte[]> pregeneratedVectorValues,
      int pregeneratedOffset) {
    byte[][] vectors = new byte[size][];
    byte[][] randomVectors =
        createRandomByteVectors(size - pregeneratedVectorValues.values.length, dimension, random());

    for (int i = 0; i < pregeneratedOffset; i++) {
      vectors[i] = randomVectors[i];
    }

    int currentDoc;
    while ((currentDoc = pregeneratedVectorValues.nextDoc()) != NO_MORE_DOCS) {
      vectors[pregeneratedOffset + currentDoc] = pregeneratedVectorValues.values[currentDoc];
    }

    for (int i = pregeneratedOffset + pregeneratedVectorValues.values.length;
        i < vectors.length;
        i++) {
      vectors[i] = randomVectors[i - pregeneratedVectorValues.values.length];
    }

    return MockByteVectorValues.fromValues(vectors);
  }

  @Override
  AbstractMockVectorValues<byte[]> vectorValues(LeafReader reader, String fieldName)
      throws IOException {
    ByteVectorValues vectorValues = reader.getByteVectorValues(fieldName);
    byte[][] vectors = new byte[reader.maxDoc()][];
    while (vectorValues.nextDoc() != NO_MORE_DOCS) {
      vectors[vectorValues.docID()] =
          ArrayUtil.copyOfSubArray(
              vectorValues.vectorValue(), 0, vectorValues.vectorValue().length);
    }
    return MockByteVectorValues.fromValues(vectors);
  }

  @Override
  Field knnVectorField(String name, byte[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnByteVectorField(name, vector, similarityFunction);
  }

  @Override
  RandomAccessVectorValues<byte[]> circularVectorValues(int nDoc) {
    return new CircularByteVectorValues(nDoc);
  }

  @Override
  byte[] getTargetVector() {
    return new byte[] {1, 0};
  }
}
