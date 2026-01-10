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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

/** Tests for flat mode HNSW graphs */
public class TestHnswGraphFlatMode extends LuceneTestCase {

  private static final int DIM = 128; // High-dimensional vectors where flat mode is beneficial

  /**
   * Test that flat mode creates a graph with only a single level (level 0).
   */
  public void testFlatModeCreatesSingleLevel() throws IOException {
    int numVectors = 100;
    List<float[]> vectors = createRandomVectors(numVectors, DIM);

    HnswGraphBuilder builder = createBuilder(vectors, true); // flatMode = true

    for (int i = 0; i < numVectors; i++) {
      builder.addGraphNode(i);
    }

    OnHeapHnswGraph graph = builder.getGraph();

    // Verify graph has only one level
    assertEquals("Flat mode should produce a graph with exactly 1 level", 1, graph.numLevels());

    // Verify all nodes are on level 0
    HnswGraph.NodesIterator nodesOnLevel0 = graph.getNodesOnLevel(0);
    assertEquals("All nodes should be on level 0", numVectors, nodesOnLevel0.size());
  }

  /**
   * Test that hierarchical mode (flatMode=false) creates multiple levels.
   */
  public void testHierarchicalModeCreatesMultipleLevels() throws IOException {
    int numVectors = 1000; // More vectors to ensure multiple levels
    List<float[]> vectors = createRandomVectors(numVectors, DIM);

    HnswGraphBuilder builder = createBuilder(vectors, false); // flatMode = false

    for (int i = 0; i < numVectors; i++) {
      builder.addGraphNode(i);
    }

    OnHeapHnswGraph graph = builder.getGraph();

    // Hierarchical mode should create more than 1 level
    assertTrue("Hierarchical mode should produce a graph with > 1 level", graph.numLevels() > 1);
  }

  /**
   * Test that flat and hierarchical modes produce similar recall.
   */
  public void testFlatModeRecallEquivalence() throws IOException {
    int numVectors = 500;
    int numQueries = 50;
    int k = 10;

    List<float[]> vectors = createRandomVectors(numVectors, DIM);
    List<float[]> queries = createRandomVectors(numQueries, DIM);

    // Build flat graph
    HnswGraphBuilder flatBuilder = createBuilder(vectors, true);
    for (int i = 0; i < numVectors; i++) {
      flatBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph flatGraph = flatBuilder.getGraph();

    // Build hierarchical graph
    HnswGraphBuilder hierarchicalBuilder = createBuilder(vectors, false);
    for (int i = 0; i < numVectors; i++) {
      hierarchicalBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph hierarchicalGraph = hierarchicalBuilder.getGraph();

    // Compare recall for both graphs
    double flatRecall = calculateAverageRecall(flatGraph, vectors, queries, k);
    double hierarchicalRecall = calculateAverageRecall(hierarchicalGraph, vectors, queries, k);

    // Recall should be within 5% (flat mode may even be slightly better for high-dim vectors)
    double recallDiff = Math.abs(flatRecall - hierarchicalRecall);
    assertTrue(
        "Flat mode recall (" + flatRecall + ") should be within 5% of hierarchical recall ("
            + hierarchicalRecall + "), but difference is " + recallDiff,
        recallDiff < 0.05);
  }

  /**
   * Test that flat mode uses less memory than hierarchical mode.
   */
  public void testFlatModeMemorySavings() throws IOException {
    int numVectors = 1000;
    List<float[]> vectors = createRandomVectors(numVectors, DIM);

    // Build flat graph
    HnswGraphBuilder flatBuilder = createBuilder(vectors, true);
    for (int i = 0; i < numVectors; i++) {
      flatBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph flatGraph = flatBuilder.getGraph();
    long flatMemory = flatGraph.ramBytesUsed();

    // Build hierarchical graph
    HnswGraphBuilder hierarchicalBuilder = createBuilder(vectors, false);
    for (int i = 0; i < numVectors; i++) {
      hierarchicalBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph hierarchicalGraph = hierarchicalBuilder.getGraph();
    long hierarchicalMemory = hierarchicalGraph.ramBytesUsed();

    // Flat mode should use less memory
    assertTrue(
        "Flat mode memory (" + flatMemory + " bytes) should be less than hierarchical mode ("
            + hierarchicalMemory + " bytes)",
        flatMemory < hierarchicalMemory);

    double memorySavings = 1.0 - ((double) flatMemory / hierarchicalMemory);
    System.out.println("Flat mode memory savings: " + String.format("%.1f%%", memorySavings * 100));

    // Should see at least 10% memory savings (paper reports ~38%, but this depends on graph size)
    assertTrue(
        "Expected at least 10% memory savings, but got " + String.format("%.1f%%", memorySavings * 100),
        memorySavings >= 0.10);
  }

  /**
   * Test that flat mode graphs can be searched correctly.
   */
  public void testFlatModeSearch() throws IOException {
    int numVectors = 200;
    int k = 10;
    List<float[]> vectors = createRandomVectors(numVectors, DIM);

    HnswGraphBuilder builder = createBuilder(vectors, true);
    for (int i = 0; i < numVectors; i++) {
      builder.addGraphNode(i);
    }
    OnHeapHnswGraph graph = builder.getGraph();

    // Perform a search
    float[] query = createRandomVector(DIM);
    RandomVectorScorer scorer = createScorer(vectors, query);
    HnswGraphSearcher searcher = new HnswGraphSearcher(new NeighborQueue(k, true), new FixedBitSet(numVectors));

    NeighborQueue results = new NeighborQueue(k, false);
    searcher.search(scorer, results, graph);

    // Should find k neighbors
    assertTrue("Should find at least some neighbors", results.size() > 0);
    assertTrue("Should find at most k neighbors", results.size() <= k);
  }

  // Helper methods

  private List<float[]> createRandomVectors(int count, int dimension) {
    List<float[]> vectors = new ArrayList<>(count);
    Random random = random();
    for (int i = 0; i < count; i++) {
      vectors.add(createRandomVector(dimension));
    }
    return vectors;
  }

  private float[] createRandomVector(int dimension) {
    Random random = random();
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random.nextFloat();
    }
    return vector;
  }

  private HnswGraphBuilder createBuilder(List<float[]> vectors, boolean flatMode) throws IOException {
    RandomVectorScorerSupplier scorerSupplier = createScorerSupplier(vectors);
    return HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextLong(), flatMode);
  }

  private RandomVectorScorerSupplier createScorerSupplier(List<float[]> vectors) {
    FloatVectorValues vectorValues = FloatVectorValues.fromFloats(vectors, DIM);
    return RandomVectorScorerSupplier.createFloats(vectorValues, VectorSimilarityFunction.EUCLIDEAN);
  }

  private RandomVectorScorer createScorer(List<float[]> vectors, float[] query) {
    FloatVectorValues vectorValues = FloatVectorValues.fromFloats(vectors, DIM);
    RandomVectorScorerSupplier scorerSupplier = RandomVectorScorerSupplier.createFloats(vectorValues, VectorSimilarityFunction.EUCLIDEAN);
    try {
      return scorerSupplier.scorer(new float[][] {query}[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private double calculateAverageRecall(
      OnHeapHnswGraph graph,
      List<float[]> vectors,
      List<float[]> queries,
      int k) throws IOException {
    double totalRecall = 0.0;
    HnswGraphSearcher searcher = new HnswGraphSearcher(new NeighborQueue(k, true), new FixedBitSet(vectors.size()));

    for (float[] query : queries) {
      // Get ground truth (exact nearest neighbors)
      int[] groundTruth = findExactNearestNeighbors(vectors, query, k);

      // Get HNSW results
      RandomVectorScorer scorer = createScorer(vectors, query);
      NeighborQueue results = new NeighborQueue(k, false);
      searcher.search(scorer, results, graph);

      // Calculate recall
      int[] hnswResults = new int[results.size()];
      for (int i = 0; i < results.size(); i++) {
        hnswResults[i] = results.pop();
      }

      totalRecall += calculateRecall(groundTruth, hnswResults);
    }

    return totalRecall / queries.size();
  }

  private int[] findExactNearestNeighbors(List<float[]> vectors, float[] query, int k) {
    // Simple brute force nearest neighbor search
    float[] distances = new float[vectors.size()];
    for (int i = 0; i < vectors.size(); i++) {
      distances[i] = euclideanDistance(vectors.get(i), query);
    }

    // Find k smallest distances
    int[] indices = new int[Math.min(k, vectors.size())];
    for (int i = 0; i < indices.length; i++) {
      int minIdx = 0;
      float minDist = Float.MAX_VALUE;
      for (int j = 0; j < distances.length; j++) {
        if (distances[j] < minDist) {
          minDist = distances[j];
          minIdx = j;
        }
      }
      indices[i] = minIdx;
      distances[minIdx] = Float.MAX_VALUE; // Mark as used
    }

    return indices;
  }

  private float euclideanDistance(float[] a, float[] b) {
    float sum = 0;
    for (int i = 0; i < a.length; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  private double calculateRecall(int[] groundTruth, int[] results) {
    int matches = 0;
    for (int result : results) {
      for (int truth : groundTruth) {
        if (result == truth) {
          matches++;
          break;
        }
      }
    }
    return (double) matches / groundTruth.length;
  }
}
