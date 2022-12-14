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
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.AbstractVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;

/** Tests HNSW KNN graphs */
public class TestHnswFloatVectorGraph extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  Query knnQuery(String field, float[] vector, int k) {
    return new KnnVectorQuery(field, vector, k);
  }

  @Override
  float[] randomVector(int dim) {
    return randomVector(random(), dim);
  }

  @Override
  AbstractMockVectorValues<float[]> vectorValues(int size, int dimension) {
    return MockVectorValues.fromValues(createRandomFloatVectors(size, dimension, random()));
  }

  @Override
  AbstractMockVectorValues<float[]> vectorValues(float[][] values) {
    return MockVectorValues.fromValues(values);
  }

  @Override
  AbstractVectorValues<float[]> vectorValues(LeafReader reader, String fieldName)
      throws IOException {
    return reader.getVectorValues(fieldName);
  }

  @Override
  Field knnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnVectorField(name, vector, similarityFunction);
  }

  @Override
  RandomAccessVectorValues<float[]> circularVectorValues(int nDoc) {
    return new CircularVectorValues(nDoc);
  }

  @Override
  float[] getTargetVector() {
    return new float[] {1f, 0f};
  }

  public void testSearchWithSkewedAcceptOrds() throws IOException {
    int nDoc = 1000;
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    RandomAccessVectorValues<float[]> vectors = circularVectorValues(nDoc);
    HnswGraphBuilder<float[]> builder =
        HnswGraphBuilder.create(
            vectors, getVectorEncoding(), similarityFunction, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.copy());

    // Skip over half of the documents that are closest to the query vector
    FixedBitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 500; i < nDoc; i++) {
      acceptOrds.set(i);
    }
    NeighborQueue nn =
        HnswGraphSearcher.search(
            getTargetVector(),
            10,
            vectors.copy(),
            getVectorEncoding(),
            similarityFunction,
            hnsw,
            acceptOrds,
            Integer.MAX_VALUE);

    int[] nodes = nn.nodes();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.length);
    int sum = 0;
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
      sum += node;
    }
    // We still expect to get reasonable recall. The lowest non-skipped docIds
    // are closest to the query vector: sum(500,509) = 5045
    assertTrue("sum(result docs)=" + sum, sum < 5100);
  }
}
