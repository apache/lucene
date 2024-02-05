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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

/** Tests indexing of a knn-graph */
public class TestKnnFloatGraph extends KnnGraphTestCase<float[]> {

  @Override
  protected VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  protected float[][] initEmptyArray(int size) {
    return new float[size][];
  }

  @Override
  protected float[][][] init2dArray(int size) {
    return new float[size][][];
  }

  @Override
  protected float[][] randomVectors(int numDoc, int dimension) {
    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      if (random().nextBoolean()) {
        values[i] = randomVector(dimension);
      }
    }
    return values;
  }

  @Override
  protected float[] randomVector(int dimension) {
    float[] vector = new float[dimension];
    for (int j = 0; j < dimension; j++) {
      vector[j] = random().nextFloat();
    }
    VectorUtil.l2normalize(vector);
    return vector;
  }

  @Override
  protected float[] vector(float[] values) {
    if (similarityFunction == VectorSimilarityFunction.DOT_PRODUCT) {
      VectorUtil.l2normalize(values);
    }
    return values;
  }

  @Override
  protected FieldType vectorFieldType(int dims, VectorSimilarityFunction similarityFunction) {
    return KnnFloatVectorField.createFieldType(dims, similarityFunction);
  }

  @Override
  protected Field vectorField(String name, float[] vector, FieldType fieldType) {
    return new KnnFloatVectorField(name, vector, fieldType);
  }

  @Override
  protected void addKnnFieldToDoc(
      Document doc, float[] vector, VectorSimilarityFunction similarityFunction) {
    FieldType fieldType = KnnFloatVectorField.createFieldType(vector.length, similarityFunction);
    doc.add(new KnnFloatVectorField(KNN_GRAPH_FIELD, vector, fieldType));
  }

  @Override
  protected void validateArray(float[] expected, float[] actual, int doc, int id) {
    assertArrayEquals(
        "float vector did not match for doc "
            + doc
            + ", id="
            + id
            + ": "
            + Arrays.toString(expected),
        expected,
        actual,
        0);
  }

  @Override
  protected DocIdSetIterator vectorValues(LeafReader reader, String vectorField)
      throws IOException {
    return reader.getFloatVectorValues(vectorField);
  }

  @Override
  protected float[] vectorValue(DocIdSetIterator vectorValues) throws IOException {
    return ((FloatVectorValues) vectorValues).vectorValue();
  }

  @Override
  protected TopDocs search(LeafReaderContext ctx, float[] vector, int k, Bits liveDocs)
      throws IOException {
    return ctx.reader()
        .searchNearestVectors(KNN_GRAPH_FIELD, vector, k, liveDocs, Integer.MAX_VALUE);
  }

  @Override
  protected Query generateQuery(String field, float[] floats, int k) {
    return new KnnFloatVectorQuery(field, floats, k);
  }

  @Override
  protected Map<float[], int[]> getGridQueries() {
    return Map.ofEntries(
        Map.entry(new float[] {0.0f, 0.1f}, new int[] {0, 15, 3, 18, 5}),
        Map.entry(new float[] {0.3f, 0.8f}, new int[] {15, 18, 0, 3, 5}));
  }
}
