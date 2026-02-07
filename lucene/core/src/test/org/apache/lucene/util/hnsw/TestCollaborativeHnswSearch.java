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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.CollaborativeKnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.ArrayUtil;
import org.junit.Before;

/** Tests collaborative HNSW search with dynamic threshold updates */
public class TestCollaborativeHnswSearch extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  Query knnQuery(String field, float[] vector, int k) {
    return new KnnFloatVectorQuery(field, vector, k);
  }

  @Override
  float[] randomVector(int dim) {
    return randomVector(random(), dim);
  }

  @Override
  KnnVectorValues vectorValues(int size, int dimension) {
    return MockVectorValues.fromValues(createRandomFloatVectors(size, dimension, random()));
  }

  @Override
  KnnVectorValues vectorValues(float[][] values) {
    return MockVectorValues.fromValues(values);
  }

  @Override
  KnnVectorValues vectorValues(LeafReader reader, String fieldName) throws IOException {
    FloatVectorValues vectorValues = reader.getFloatVectorValues(fieldName);
    float[][] vectors = new float[reader.maxDoc()][];
    for (int i = 0; i < vectorValues.size(); i++) {
      vectors[vectorValues.ordToDoc(i)] =
          ArrayUtil.copyOfSubArray(vectorValues.vectorValue(i), 0, vectorValues.dimension());
    }
    return MockVectorValues.fromValues(vectors);
  }

  @Override
  KnnVectorValues vectorValues(
      int size, int dimension, KnnVectorValues pregeneratedVectorValues, int pregeneratedOffset) {
    MockVectorValues pvv = (MockVectorValues) pregeneratedVectorValues;
    float[][] vectors = new float[size][];
    float[][] randomVectors =
        createRandomFloatVectors(size - pvv.values.length, dimension, random());

    for (int i = 0; i < pregeneratedOffset; i++) {
      vectors[i] = randomVectors[i];
    }

    for (int currentOrd = 0; currentOrd < pvv.size(); currentOrd++) {
      vectors[pregeneratedOffset + currentOrd] = pvv.values[currentOrd];
    }

    for (int i = pregeneratedOffset + pvv.values.length; i < vectors.length; i++) {
      vectors[i] = randomVectors[i - pvv.values.length];
    }

    return MockVectorValues.fromValues(vectors);
  }

  @Override
  Field knnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnFloatVectorField(name, vector, similarityFunction);
  }

  @Override
  KnnVectorValues circularVectorValues(int nDoc) {
    return new CircularFloatVectorValues(nDoc);
  }

  @Override
  float[] getTargetVector() {
    return new float[] {1f, 0f};
  }

  public void testCollaborativePruning() throws IOException {
    int nDoc = 20000;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, 2);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    float[] target = getTargetVector();
    RandomVectorScorer scorer = buildScorer(vectors, target);

    // 1. Standard search to establish baseline
    TopKnnCollector standardCollector = new TopKnnCollector(10, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // 2. Collaborative search where we raise the bar externally
    TopDocs topDocs = standardCollector.topDocs();
    float highBar = topDocs.scoreDocs[4].score;

    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(-1.0f));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(10, Integer.MAX_VALUE, globalMinSimBits);

    // Set the high bar to simulate another shard having found these matches
    globalMinSimBits.set(Float.floatToRawIntBits(highBar));

    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("Standard visited: " + standardVisited);
      System.out.println("Collaborative visited: " + collaborativeVisited);
      System.out.println("Pruning bar: " + highBar);
    }

    assertTrue(
        "Collaborative search ("
            + collaborativeVisited
            + ") should visit fewer nodes than standard search ("
            + standardVisited
            + ")",
        collaborativeVisited < standardVisited);
  }

  public void testHighKPruning() throws IOException {
    // High K (1000) on a larger dataset
    int nDoc = 30000;
    int k = 1000;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, 16);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    float[] target = randomVector(16);
    RandomVectorScorer scorer = buildScorer(vectors, target);
    TopKnnCollector standardCollector = new TopKnnCollector(k, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // Simulate another shard having found the top 100 results already
    TopDocs topDocs = standardCollector.topDocs();
    float globalBar = topDocs.scoreDocs[99].score;
    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(globalBar));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(k, Integer.MAX_VALUE, globalMinSimBits);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-K Standard visited: " + standardVisited);
      System.out.println("High-K Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-K Collaborative search should visit significantly fewer nodes",
        collaborativeVisited < (standardVisited / 2));
  }

  public void testHighDimensionPruning() throws IOException {
    // Standard 128-dimension embeddings
    int nDoc = 10000;
    int dim = 128;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, dim);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    float[] target = randomVector(dim);
    RandomVectorScorer scorer = buildScorer(vectors, target);
    TopKnnCollector standardCollector = new TopKnnCollector(100, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // High bar from global search
    float highBar = standardCollector.topDocs().scoreDocs[10].score;
    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(highBar));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(100, Integer.MAX_VALUE, globalMinSimBits);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-Dim Standard visited: " + standardVisited);
      System.out.println("High-Dim Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-Dim Collaborative search should prune effectively",
        collaborativeVisited < standardVisited);
  }
}
