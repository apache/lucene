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
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

/**
 * Benchmark test comparing flat mode vs hierarchical mode HNSW performance.
 *
 * This test is not run as part of the standard test suite. Run it manually to
 * benchmark the performance differences between flat and hierarchical HNSW modes.
 */
@LuceneTestCase.SuppressCodecs("*") // Not a codec test
public class BenchmarkHnswFlatMode extends LuceneTestCase {

  private static final int[] DIMENSIONS = {32, 64, 128, 256, 512, 768, 1536};
  private static final int NUM_VECTORS = 10000;
  private static final int NUM_QUERIES = 100;
  private static final int K = 10;

  /**
   * Comprehensive benchmark comparing flat vs hierarchical mode across different dimensions.
   *
   * According to the paper "Down with the Hierarchy", flat mode should:
   * - Save ~38% memory
   * - Achieve equal or better recall for d >= 32
   * - Have equal or better latency
   */
  public void testBenchmarkFlatVsHierarchical() throws IOException {
    System.out.println("\n=== HNSW Flat Mode vs Hierarchical Mode Benchmark ===\n");
    System.out.println(String.format("%-10s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s",
        "Dimension", "Flat Build (ms)", "Hier Build (ms)", "Flat Memory", "Hier Memory", "Memory Savings", "Recall Diff"));
    System.out.println("-".repeat(120));

    for (int dim : DIMENSIONS) {
      BenchmarkResult result = runBenchmark(dim);

      double buildTimeSavings = (1.0 - (result.flatBuildTime / result.hierBuildTime)) * 100;
      double memorySavings = (1.0 - ((double) result.flatMemory / result.hierMemory)) * 100;
      double recallDiff = (result.flatRecall - result.hierRecall) * 100;

      System.out.println(String.format("%-10d | %15.2f | %15.2f | %12.2f MB | %12.2f MB | %13.1f%% | %14.2f%%",
          dim,
          result.flatBuildTime,
          result.hierBuildTime,
          result.flatMemory / (1024.0 * 1024.0),
          result.hierMemory / (1024.0 * 1024.0),
          memorySavings,
          recallDiff));
    }

    System.out.println("\nLegend:");
    System.out.println("  - Positive Memory Savings % indicates flat mode uses less memory");
    System.out.println("  - Recall Diff % shows (flat recall - hierarchical recall)");
    System.out.println("  - For d >= 32, flat mode should show ~38% memory savings with similar recall\n");
  }

  private static class BenchmarkResult {
    double flatBuildTime;
    double hierBuildTime;
    long flatMemory;
    long hierMemory;
    double flatRecall;
    double hierRecall;
  }

  private BenchmarkResult runBenchmark(int dimension) throws IOException {
    BenchmarkResult result = new BenchmarkResult();

    // Generate test data
    List<float[]> vectors = createRandomVectors(NUM_VECTORS, dimension);
    List<float[]> queries = createRandomVectors(NUM_QUERIES, dimension);

    // Benchmark flat mode
    long flatStart = System.currentTimeMillis();
    HnswGraphBuilder flatBuilder = createBuilder(vectors, dimension, true);
    for (int i = 0; i < NUM_VECTORS; i++) {
      flatBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph flatGraph = flatBuilder.getGraph();
    result.flatBuildTime = System.currentTimeMillis() - flatStart;
    result.flatMemory = flatGraph.ramBytesUsed();
    result.flatRecall = calculateAverageRecall(flatGraph, vectors, queries, dimension, K);

    // Benchmark hierarchical mode
    long hierStart = System.currentTimeMillis();
    HnswGraphBuilder hierBuilder = createBuilder(vectors, dimension, false);
    for (int i = 0; i < NUM_VECTORS; i++) {
      hierBuilder.addGraphNode(i);
    }
    OnHeapHnswGraph hierGraph = hierBuilder.getGraph();
    result.hierBuildTime = System.currentTimeMillis() - hierStart;
    result.hierMemory = hierGraph.ramBytesUsed();
    result.hierRecall = calculateAverageRecall(hierGraph, vectors, queries, dimension, K);

    return result;
  }

  // Helper methods

  private List<float[]> createRandomVectors(int count, int dimension) {
    List<float[]> vectors = new ArrayList<>(count);
    Random random = new Random(42); // Fixed seed for reproducibility
    for (int i = 0; i < count; i++) {
      float[] vector = new float[dimension];
      for (int j = 0; j < dimension; j++) {
        vector[j] = random.nextFloat();
      }
      vectors.add(vector);
    }
    return vectors;
  }

  private HnswGraphBuilder createBuilder(List<float[]> vectors, int dimension, boolean flatMode) throws IOException {
    FloatVectorValues vectorValues = FloatVectorValues.fromFloats(vectors, dimension);
    RandomVectorScorerSupplier scorerSupplier = RandomVectorScorerSupplier.createFloats(vectorValues, VectorSimilarityFunction.EUCLIDEAN);
    return HnswGraphBuilder.create(scorerSupplier, 16, 100, 42, flatMode);
  }

  private double calculateAverageRecall(
      OnHeapHnswGraph graph,
      List<float[]> vectors,
      List<float[]> queries,
      int dimension,
      int k) throws IOException {

    double totalRecall = 0.0;
    HnswGraphSearcher searcher = new HnswGraphSearcher(new NeighborQueue(k, true), new FixedBitSet(vectors.size()));

    for (float[] query : queries) {
      // Get ground truth
      int[] groundTruth = findExactNearestNeighbors(vectors, query, k);

      // Get HNSW results
      FloatVectorValues vectorValues = FloatVectorValues.fromFloats(vectors, dimension);
      RandomVectorScorerSupplier scorerSupplier = RandomVectorScorerSupplier.createFloats(vectorValues, VectorSimilarityFunction.EUCLIDEAN);
      RandomVectorScorer scorer = scorerSupplier.scorer(query);

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
    float[] distances = new float[vectors.size()];
    for (int i = 0; i < vectors.size(); i++) {
      distances[i] = euclideanDistance(vectors.get(i), query);
    }

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
      distances[minIdx] = Float.MAX_VALUE;
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
