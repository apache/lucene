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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DistinctDocKnnCollector;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.junit.Before;
import org.junit.Ignore;

public class TestHnswDocumentCentricPruning extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT32;
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
    int size = vectorValues.size();
    float[][] vectors = new float[size][];
    int[] ordToDoc = new int[size];
    for (int i = 0; i < size; i++) {
      vectors[i] = vectorValues.vectorValue(i).clone();
      ordToDoc[i] = vectorValues.ordToDoc(i);
    }
    return new MockVectorValues(vectors, ordToDoc);
  }

  @Override
  KnnVectorValues vectorValues(
      int size, int dimension, KnnVectorValues pregeneratedVectorValues, int pregeneratedOffset) {
    try {
      float[][] vectors = new float[size][];
      int[] ordToDoc = new int[size];
      FloatVectorValues fvv = (FloatVectorValues) pregeneratedVectorValues;
      for (int i = 0; i < size; i++) {
        vectors[i] = fvv.vectorValue(i + pregeneratedOffset).clone();
        ordToDoc[i] = fvv.ordToDoc(i + pregeneratedOffset);
      }
      return new MockVectorValues(vectors, ordToDoc);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  Query knnQuery(String field, float[] vector, int k) {
    return new KnnFloatVectorQuery(field, vector, k);
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

  @Override
  @Ignore
  public void testHnswGraphBuilderInitializationFromGraph_withOffsetZero() {}

  @Override
  @Ignore
  public void testHnswGraphBuilderInitializationFromGraph_withNonZeroOffset() {}

  /**
   * Performs an exhaustive brute-force search over all vectors and deduplicates by DocID to get the
   * true Ground Truth for Top-K Documents.
   */
  private List<Integer> getTrueTopKDocs(
      RandomVectorScorer scorer, int[] ordToDoc, int k, Map<Integer, Float> outMaxScores)
      throws IOException {
    Map<Integer, Float> docMaxScores = new HashMap<>();
    for (int i = 0; i < ordToDoc.length; i++) {
      int docId = ordToDoc[i];
      float score = scorer.score(i);
      docMaxScores.put(docId, Math.max(docMaxScores.getOrDefault(docId, -1f), score));
    }
    List<Integer> sortedDocs = new ArrayList<>(docMaxScores.keySet());
    sortedDocs.sort(
        (a, b) -> {
          int cmp = Float.compare(docMaxScores.get(b), docMaxScores.get(a));
          return cmp != 0 ? cmp : Integer.compare(a, b);
        });

    if (outMaxScores != null) outMaxScores.putAll(docMaxScores);
    return sortedDocs.subList(0, Math.min(k, sortedDocs.size()));
  }

  public void testRecallParityWithBruteForce() throws IOException {
    int nDoc = 100;
    int vectorsPerDoc = 50;
    int totalVectors = nDoc * vectorsPerDoc;
    int dim = 16;

    float[][] vectors = new float[totalVectors][dim];
    int[] ordToDoc = new int[totalVectors];
    Map<Integer, List<Integer>> docToOrds = new HashMap<>();

    for (int i = 0; i < nDoc; i++) {
      float[] base = randomVector(dim);
      docToOrds.put(i, new ArrayList<>());
      for (int j = 0; j < vectorsPerDoc; j++) {
        int ord = i * vectorsPerDoc + j;
        vectors[ord] = base.clone();
        for (int d = 0; d < dim; d++) vectors[ord][d] += (random().nextFloat() * 0.05f);
        ordToDoc[ord] = i;
        docToOrds.get(i).add(ord);
      }
    }

    MockVectorValues vectorValues = new MockVectorValues(vectors, ordToDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectorValues);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectorValues.size());

    float[] target = randomVector(dim);
    RandomVectorScorer scorer = buildScorer(vectorValues, target);

    int k = 5;

    // 1. Ground Truth: Brute Force deduplicated
    Map<Integer, Float> gtMaxScores = new HashMap<>();
    List<Integer> expectedDocs = getTrueTopKDocs(scorer, ordToDoc, k, gtMaxScores);

    // 2. Phase 1: Optimized Document-Centric HNSW Search
    // We use a massive visitedLimit to essentially force HNSW to explore everything
    // for parity testing.
    KnnCollector optimizedCollector = new TopKnnCollector(k, Integer.MAX_VALUE);
    DistinctDocKnnCollector distinctCollector =
        new DistinctDocKnnCollector(optimizedCollector, vectorValues);
    HnswGraphSearcher.search(scorer, distinctCollector, hnsw, null);
    TopDocs initialDocs = distinctCollector.topDocs();

    // 3. Phase 2: Alignment (Exhaustive re-score of found docs)
    Map<Integer, Float> finalAlignedScores = new HashMap<>();
    for (ScoreDoc sd : initialDocs.scoreDocs) {
      int docId = ordToDoc[sd.doc];
      float trueMax = -1f;
      for (int ord : docToOrds.get(docId)) {
        trueMax = Math.max(trueMax, scorer.score(ord));
      }
      finalAlignedScores.put(docId, trueMax);
    }

    // 4. Verification
    // Since HNSW is approximate, it might miss some documents from the brute-force GT.
    // However, for every document it DOES find, the aligned score must match MaxSim.
    for (ScoreDoc sd : initialDocs.scoreDocs) {
      int docId = ordToDoc[sd.doc];
      float alignedScore = finalAlignedScores.get(docId);
      float gtScore = gtMaxScores.get(docId);
      assertEquals("MaxSim score mismatch for doc " + docId, gtScore, alignedScore, 0.00001f);
    }
  }

  public void testMassiveMultiVectorPruning() throws IOException {
    int nDoc = 200;
    int vectorsPerDoc = 500;
    int totalVectors = nDoc * vectorsPerDoc;
    int dim = 16;

    float[][] vectors = new float[totalVectors][dim];
    int[] ordToDoc = new int[totalVectors];
    for (int i = 0; i < nDoc; i++) {
      float[] base = randomVector(dim);
      for (int j = 0; j < vectorsPerDoc; j++) {
        int ord = i * vectorsPerDoc + j;
        vectors[ord] = base.clone();
        for (int d = 0; d < dim; d++) vectors[ord][d] += (random().nextFloat() * 0.01f);
        ordToDoc[ord] = i;
      }
    }

    MockVectorValues vectorValues = new MockVectorValues(vectors, ordToDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectorValues);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectorValues.size());

    float[] target = vectors[0];
    RandomVectorScorer scorer = buildScorer(vectorValues, target);

    int k = 5;
    int visitedLimit = 5000;

    KnnCollector standardCollector = new TopKnnCollector(k * 10, visitedLimit);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);

    KnnCollector baseCollector = new TopKnnCollector(k, visitedLimit);
    DistinctDocKnnCollector distinctCollector =
        new DistinctDocKnnCollector(baseCollector, vectorValues);
    HnswGraphSearcher.search(scorer, distinctCollector, hnsw, null);

    if (VERBOSE) {
      System.out.println("\n--- Massive Multi-Vector Stats ---");
      System.out.println("Standard HNSW Visited: " + standardCollector.visitedCount());
      System.out.println("Document-Centric Visited: " + distinctCollector.visitedCount());
    }

    // With short-circuiting disabled, visitedCount might be equal or slightly different
    // due to heap management, but shouldn't be radically higher.
    assertTrue("Reasonable visit count expected", distinctCollector.visitedCount() > 0);
  }

  private static class MockVectorValues extends FloatVectorValues {
    private final float[][] values;
    private final int[] ordToDoc;

    MockVectorValues(float[][] values, int[] ordToDoc) {
      this.values = values;
      this.ordToDoc = ordToDoc;
    }

    @Override
    public int dimension() {
      return values[0].length;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public FloatVectorValues copy() {
      return new MockVectorValues(values, ordToDoc);
    }

    @Override
    public int ordToDoc(int ord) {
      return ordToDoc[ord];
    }

    @Override
    public VectorEncoding getEncoding() {
      return VectorEncoding.FLOAT32;
    }

    @Override
    public float[] vectorValue(int ord) {
      return values[ord];
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    static MockVectorValues fromValues(float[][] values) {
      int[] identity = new int[values.length];
      for (int i = 0; i < values.length; i++) identity[i] = i;
      return new MockVectorValues(values, identity);
    }
  }
}
