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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

/** Tests indexing of a knn-graph */
public class TestKnnByteGraph extends KnnGraphTestCase<byte[]> {

  @Override
  protected byte[][] initEmptyArray(int size) {
    return new byte[size][];
  }

  @Override
  protected byte[][][] init2dArray(int size) {
    return new byte[size][][];
  }

  @Override
  protected VectorEncoding getVectorEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  protected byte[][] randomVectors(int numDoc, int dimension) {
    byte[][] values = new byte[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      if (random().nextBoolean()) {
        values[i] = randomVector(dimension);
      }
    }
    return values;
  }

  @Override
  protected byte[] randomVector(int dimension) {
    byte[] vector = new byte[dimension];
    random().nextBytes(vector);
    return vector;
  }

  @Override
  protected byte[] vector(float[] values) {
    boolean normalized = false;
    if (similarityFunction == VectorSimilarityFunction.DOT_PRODUCT) {
      normalized = true;
      VectorUtil.l2normalize(values);
    }
    byte[] vector = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      vector[i] = normalized ? (byte) (values[i] * 127) : (byte) (values[i]);
    }
    return vector;
  }

  @Override
  protected FieldType vectorFieldType(int dims, VectorSimilarityFunction similarityFunction) {
    return KnnByteVectorField.createFieldType(dims, similarityFunction);
  }

  @Override
  protected Field vectorField(String name, byte[] vector, FieldType fieldType) {
    return new KnnByteVectorField(name, vector, fieldType);
  }

  @Override
  protected void addKnnFieldToDoc(
      Document doc, byte[] vector, VectorSimilarityFunction similarityFunction) {
    FieldType fieldType = KnnByteVectorField.createFieldType(vector.length, similarityFunction);
    doc.add(new KnnByteVectorField(KNN_GRAPH_FIELD, vector, fieldType));
  }

  @Override
  protected void validateArray(byte[] expected, byte[] actual, int doc, int id) {
    assertArrayEquals(
        "float vector did not match for doc "
            + doc
            + ", id="
            + id
            + ": "
            + Arrays.toString(expected),
        expected,
        actual);
  }

  @Override
  protected byte[] vectorValue(DocIdSetIterator vectorValues) throws IOException {
    return ((ByteVectorValues) vectorValues).vectorValue();
  }

  @Override
  protected DocIdSetIterator vectorValues(LeafReader reader, String vectorField)
      throws IOException {
    return reader.getByteVectorValues(vectorField);
  }

  @Override
  protected TopDocs search(LeafReaderContext ctx, byte[] vector, int k, Bits liveDocs)
      throws IOException {
    return ctx.reader()
        .searchNearestVectors(KNN_GRAPH_FIELD, vector, k, liveDocs, Integer.MAX_VALUE);
  }

  @Override
  protected Query generateQuery(String field, byte[] query, int k) {
    return new KnnByteVectorQuery(field, query, k);
  }

  @Override
  protected Map<byte[], int[]> getGridQueries() {
    return Map.ofEntries(
        Map.entry(new byte[] {0, 0}, new int[] {0, 3, 15, 18, 5}),
        Map.entry(new byte[] {1, 5}, new int[] {13, 10, 16, 23, 1}));
  }
}
