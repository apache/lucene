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
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloat16VectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;

/** Tests HNSW KNN graphs */
public class TestHnswFloat16VectorGraph extends HnswGraphTestCase<short[]> {

  @Before
  public void setup() {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT16;
  }

  @Override
  Query knnQuery(String field, short[] vector, int k) {
    return new KnnFloat16VectorQuery(field, vector, k);
  }

  @Override
  short[] randomVector(int dim) {
    return randomFloat16Vector(random(), dim);
  }

  @Override
  MockFloat16VectorValues vectorValues(int size, int dimension) {
    return MockFloat16VectorValues.fromValues(createRandomFloat16Vectors(size, dimension, random()));
  }

  @Override
  MockFloat16VectorValues vectorValues(float[][] values) {
    short[][] float16Values = new short[values.length][];
    for (int i = 0; i < values.length; i++) {

      float16Values[i] = new short[values[i].length];
      for (int j = 0; j < values[i].length; j++) {
        float16Values[i][j] = Float.floatToFloat16(values[i][j]);
      }
    }
    return MockFloat16VectorValues.fromValues(float16Values);
  }

  @Override
  MockFloat16VectorValues vectorValues(LeafReader reader, String fieldName) throws IOException {
    Float16VectorValues vectorValues = reader.getFloat16VectorValues(fieldName);
    short[][] vectors = new short[reader.maxDoc()][];
    for (int i = 0; i < vectorValues.size(); i++) {
      vectors[vectorValues.ordToDoc(i)] =
          ArrayUtil.copyOfSubArray(vectorValues.vectorValue(i), 0, vectorValues.dimension());
    }
    return MockFloat16VectorValues.fromValues(vectors);
  }

  @Override
  MockFloat16VectorValues vectorValues(
      int size, int dimension, KnnVectorValues pregeneratedVectorValues, int pregeneratedOffset) {
    MockFloat16VectorValues pvv = (MockFloat16VectorValues) pregeneratedVectorValues;
    short[][] vectors = new short[size][];
    short[][] randomVectors =
        createRandomFloat16Vectors(size - pvv.values.length, dimension, random());

    for (int i = 0; i < pregeneratedOffset; i++) {
      vectors[i] = randomVectors[i];
    }

    for (int currentOrd = 0; currentOrd < pvv.size(); currentOrd++) {
      vectors[pregeneratedOffset + currentOrd] = pvv.values[currentOrd];
    }

    for (int i = pregeneratedOffset + pvv.values.length; i < vectors.length; i++) {
      vectors[i] = randomVectors[i - pvv.values.length];
    }

    return MockFloat16VectorValues.fromValues(vectors);
  }

  @Override
  Field knnVectorField(String name, short[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnFloat16VectorField(name, vector, similarityFunction, VectorEncoding.FLOAT16);
  }

  @Override
  CircularFloat16VectorValues circularVectorValues(int nDoc) {
    return new CircularFloat16VectorValues(nDoc);
  }

  @Override
  short[] getTargetVector() {
    return new short[] {Float.floatToFloat16(1f), Float.floatToFloat16(0f)};
  }

  public void testSearchWithSkewedAcceptOrds() throws IOException {
    int nDoc = 1000;
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    Float16VectorValues vectors = circularVectorValues(nDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    // Skip over half of the documents that are closest to the query vector
    FixedBitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 500; i < nDoc; i++) {
      acceptOrds.set(i);
    }
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()), 10, hnsw, acceptOrds, Integer.MAX_VALUE);

    TopDocs nodes = nn.topDocs();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.scoreDocs.length);
    int sum = 0;
    for (ScoreDoc node : nodes.scoreDocs) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node.doc));
      sum += node.doc;
    }
    // We still expect to get reasonable recall. The lowest non-skipped docIds
    // are closest to the query vector: sum(500,509) = 5045
    assertTrue("sum(result docs)=" + sum, sum < 5100);
  }
}
