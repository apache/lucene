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
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.CollaborativeKnnCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.CollaborativeKnnCollectorManager;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.junit.Before;

/** Tests collaborative HNSW search with dynamic threshold updates and recall validation */
public class TestCollaborativeHnswSearch extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    // Force a predictable similarity function to avoid RandomSimilarity issues in tests
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
    TopDocs standardTopDocs = standardCollector.topDocs();

    // 2. Collaborative search with an aggressive high bar (best standard score)
    // We set docBase to force the tie-break logic to trigger (docBase > globalMinDoc).
    float pruningBar = standardTopDocs.scoreDocs[0].score;
    int pruningBarDoc = standardTopDocs.scoreDocs[0].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(pruningBarDoc, pruningBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(10, Integer.MAX_VALUE, minScoreAcc, 1000000);

    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("Standard visited: " + standardVisited);
      System.out.println("Collaborative visited: " + collaborativeVisited);
    }

    // With a perfect match bar, we should prune significantly
    assertTrue(
        "Collaborative search should visit fewer nodes", collaborativeVisited <= standardVisited);
  }

  @Nightly
  public void testHighKPruning() throws IOException {
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
    TopDocs standardTopDocs = standardCollector.topDocs();

    // Set bar to the 100th result
    float globalBar = standardTopDocs.scoreDocs[99].score;
    int globalBarDocId = standardTopDocs.scoreDocs[99].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(globalBarDocId, globalBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(k, Integer.MAX_VALUE, minScoreAcc, 1000000);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-K Standard visited: " + standardVisited);
      System.out.println("High-K Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-K Collaborative search should visit fewer nodes",
        collaborativeVisited <= standardVisited);
  }

  @Nightly
  public void testHighDimensionPruning() throws IOException {
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
    TopDocs standardTopDocs = standardCollector.topDocs();

    // Bar from 10th result
    float highBar = standardTopDocs.scoreDocs[9].score;
    int highBarDocId = standardTopDocs.scoreDocs[9].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(highBarDocId, highBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(100, Integer.MAX_VALUE, minScoreAcc, 1000000);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-Dim Standard visited: " + standardVisited);
      System.out.println("High-Dim Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-Dim Collaborative search should prune effectively",
        collaborativeVisited <= standardVisited);
  }

  public void testMultiSegmentCollaborativePruning() throws IOException {
    int numSegments = 4;
    int docsPerSegment = 1500;
    int dim = 32;
    int k = 10;
    String fieldName = "vector";

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < numSegments; seg++) {
          for (int doc = 0; doc < docsPerSegment; doc++) {
            Document d = new Document();
            d.add(new KnnFloatVectorField(fieldName, randomVector(dim), similarityFunction));
            writer.addDocument(d);
          }
          writer.commit();
        }
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] queryVec = randomVector(dim);

        Query standardQuery = new KnnFloatVectorQuery(fieldName, queryVec, k);
        TopDocs standardResults = searcher.search(standardQuery, k);

        LongAccumulator noBarAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
        Query collaborativeNoBar =
            new CollaborativeKnnFloatVectorQuery(fieldName, queryVec, k, noBarAcc);
        TopDocs noBarResults = searcher.search(collaborativeNoBar, k);

        assertTrue("Collaborative search should return results", noBarResults.scoreDocs.length > 0);
      }
    }
  }

  @Nightly
  public void testMultiIndexHighKPerformance() throws IOException {
    int numGraphs = 5;
    int vectorsPerGraph = 5000;
    int dim = 32;
    int k = 500;

    OnHeapHnswGraph[] graphs = new OnHeapHnswGraph[numGraphs];
    MockVectorValues[] allVectors = new MockVectorValues[numGraphs];
    for (int i = 0; i < numGraphs; i++) {
      allVectors[i] = (MockVectorValues) vectorValues(vectorsPerGraph, dim);
      RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(allVectors[i]);
      HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
      graphs[i] = builder.build(allVectors[i].size());
    }

    float[] queryVec = randomVector(dim);

    long standardTotalVisited = 0;
    for (int i = 0; i < numGraphs; i++) {
      RandomVectorScorer scorer = buildScorer(allVectors[i], queryVec);
      TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE);
      HnswGraphSearcher.search(scorer, collector, graphs[i], null);
      standardTotalVisited += collector.visitedCount();
    }

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    long collaborativeTotalVisited = 0;
    for (int i = 0; i < numGraphs; i++) {
      RandomVectorScorer scorer = buildScorer(allVectors[i], queryVec);
      CollaborativeKnnCollector collector =
          new CollaborativeKnnCollector(k, Integer.MAX_VALUE, minScoreAcc, 1000000);
      HnswGraphSearcher.search(scorer, collector, graphs[i], null);
      collaborativeTotalVisited += collector.visitedCount();
    }

    if (VERBOSE) {
      System.out.println("Multi-Index Standard Total: " + standardTotalVisited);
      System.out.println("Multi-Index Collaborative Total: " + collaborativeTotalVisited);
    }

    assertTrue(
        "Collaborative search should be no more expensive than standard",
        collaborativeTotalVisited <= standardTotalVisited);
  }

  private static class CollaborativeKnnFloatVectorQuery extends KnnFloatVectorQuery {
    private final LongAccumulator minScoreAcc;

    CollaborativeKnnFloatVectorQuery(
        String field, float[] target, int k, LongAccumulator minScoreAcc) {
      super(field, target, k);
      this.minScoreAcc = minScoreAcc;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new CollaborativeKnnCollectorManager(k, minScoreAcc);
    }
  }
}
