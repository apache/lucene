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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestClustering extends LuceneTestCase {
  static Clustering.Result cluster(
      List<float[]> vectors, int count, VectorSimilarityFunction similarity)
      throws java.io.IOException {
    return cluster(vectors, count, similarity, null, null);
  }

  static Clustering.Result cluster(
      List<float[]> vectors,
      int count,
      VectorSimilarityFunction similarity,
      float[][] seeds,
      int[] assignments)
      throws java.io.IOException {
    return Clustering.cluster(
        org.apache.lucene.index.FloatVectorValues.fromFloats(
            vectors, vectors.isEmpty() ? 1 : vectors.get(0).length),
        count,
        similarity,
        seeds,
        assignments,
        0,
        1.4,
        org.apache.lucene.util.InfoStream.NO_OUTPUT);
  }

  public void testReaperMatchesExhaustiveLloyd() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      double reaped = 0, reference = 0;
      for (int run = 0; run < 20; run++) {
        List<float[]> vectors = new ArrayList<>();
        int count = 30 + random().nextInt(70);
        int dim = 2 + random().nextInt(7);
        for (int i = 0; i < count; i++) {
          float[] v = new float[dim];
          for (int d = 0; d < dim; d++) {
            v[d] = random().nextFloat() * 20 - 10;
          }
          vectors.add(v);
        }
        int k = 2 + random().nextInt(8);
        var actual = TestClustering.cluster(vectors, k, similarity);
        var expected = exhaustive(vectors, k, similarity);
        if (k == 2) {
          // With two cells the own-pair movement bound covers every competitor, so skips are exact.
          assertArrayEquals(expected.assignments(), actual.assignments());
          for (int c = 0; c < k; c++) {
            assertArrayEquals(expected.centroids()[c], actual.centroids()[c], 0f);
          }
        }
        // Otherwise a third cell may overtake a skipped document, so the reaper can settle in a
        // different local optimum; it must be an equally good one.
        reaped += objective(vectors, similarity, actual);
        reference += objective(vectors, similarity, expected);
      }
      assertTrue(reaped + " vs " + reference, reaped <= OBJECTIVE_TOLERANCE * reference);
    }
  }

  // Measured within 1% over six seeds; 3% leaves headroom without hiding a broken bound.
  static final double OBJECTIVE_TOLERANCE = 1.03;

  /** The Lloyd objective: summed squared distance from each vector to its assigned centroid. */
  static double objective(
      List<float[]> vectors, VectorSimilarityFunction similarity, Clustering.Result result) {
    double sum = 0;
    for (int i = 0; i < vectors.size(); i++) {
      float[] v = vectors.get(i).clone();
      if (similarity == VectorSimilarityFunction.COSINE) Clustering.normalize(v);
      double distance = Clustering.distance(v, result.centroids()[result.assignments()[i]]);
      sum += distance * distance;
    }
    return sum;
  }

  public void testFinalSpillMatchesExhaustiveSelection() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      for (int run = 0; run < 20; run++) {
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          vectors.add(
              new float[] {random().nextFloat(), random().nextFloat(), random().nextFloat()});
        }
        var plain = cluster(vectors, 8, similarity);
        int limit = 1 + random().nextInt(10);
        double margin = run % 2 == 0 ? 1.05 : 1.4;
        var spilled =
            Clustering.cluster(
                org.apache.lucene.index.FloatVectorValues.fromFloats(vectors, 3),
                8,
                similarity,
                null,
                null,
                limit,
                margin,
                org.apache.lucene.util.InfoStream.NO_OUTPUT);
        assertArrayEquals(plain.assignments(), spilled.assignments());
        assertEquals(plain.iterations(), spilled.iterations());
        for (int c = 0; c < 8; c++)
          assertArrayEquals(plain.centroids()[c], spilled.centroids()[c], 0f);
        for (int i = 0; i < 100; i++) {
          float[] v = vectors.get(i).clone();
          if (similarity == VectorSimilarityFunction.COSINE) Clustering.normalize(v);
          int primary = plain.assignments()[i];
          double threshold = margin * Clustering.distance(v, plain.centroids()[primary]);
          // Boundary documents (any other cell within the margin) spill by SOAR loss:
          // |v - c|^2 + (r . (v - c))^2 / |r|^2, where r is the primary residual.
          float[] center = plain.centroids()[primary];
          boolean boundary = false;
          double[] loss = new double[8];
          for (int c = 0; c < 8; c++) {
            if (c == primary) continue;
            double distance = Clustering.distance(v, plain.centroids()[c]);
            boundary |= distance <= threshold;
            double parallel = 0, norm = 0;
            for (int d = 0; d < v.length; d++) {
              double r = v[d] - center[d];
              parallel += ((double) v[d] - plain.centroids()[c][d]) * r;
              norm += r * r;
            }
            loss[c] = distance * distance + (norm == 0 ? 0 : parallel * parallel / norm);
          }
          List<Integer> expected = new ArrayList<>();
          for (int c = 0; c < 8 && boundary; c++) if (c != primary) expected.add(c);
          expected.sort(java.util.Comparator.comparingDouble(c -> loss[c]));
          int[] want = expected.stream().limit(limit).mapToInt(Integer::intValue).toArray();
          int[] got = spilled.spill()[i];
          assertArrayEquals(want, got == null ? new int[0] : got);
        }
      }
    }
  }

  public void testDegenerateInputs() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      assertEquals(0, TestClustering.cluster(List.of(), 8, similarity).centroids().length);
      for (float[] vector : List.of(new float[] {0, 0}, new float[] {1, 2})) {
        var result = TestClustering.cluster(List.of(vector, vector, vector), 8, similarity);
        assertEquals(3, result.centroids().length);
        assertArrayEquals(new int[] {0, 0, 0}, result.assignments());
      }
      var single =
          TestClustering.cluster(List.of(new float[] {1, 2}, new float[] {3, 4}), 1, similarity);
      assertArrayEquals(new int[] {0, 0}, single.assignments());
    }
  }

  // Independent full assignment scan at every iteration, with the same deterministic seeds.
  private static Clustering.Result exhaustive(
      List<float[]> input, int k, VectorSimilarityFunction similarity) {
    float[][] vectors = input.stream().map(float[]::clone).toArray(float[][]::new);
    if (similarity == VectorSimilarityFunction.COSINE) {
      for (float[] v : vectors) {
        Clustering.normalize(v);
      }
    }
    int[] seeds = new int[vectors.length];
    Arrays.setAll(seeds, i -> i);
    Random random = new Random(42);
    float[][] centers = new float[k][];
    for (int c = 0; c < k; c++) {
      int j = c + random.nextInt(vectors.length - c);
      centers[c] = vectors[seeds[j]].clone();
      seeds[j] = seeds[c];
      if (similarity != VectorSimilarityFunction.EUCLIDEAN) {
        Clustering.normalize(centers[c]);
      }
    }
    int[] assignment = new int[vectors.length];
    Arrays.fill(assignment, -1);
    for (int iteration = 0; iteration < 1000; iteration++) {
      int changed = 0;
      for (int i = 0; i < vectors.length; i++) {
        int best = assignment[i] < 0 ? 0 : assignment[i];
        for (int c = 0; c < k; c++) {
          if (Clustering.distance(vectors[i], centers[c])
              < Clustering.distance(vectors[i], centers[best])) {
            best = c;
          }
        }
        if (best != assignment[i]) {
          assignment[i] = best;
          changed++;
        }
      }
      if (changed == 0) {
        return new Clustering.Result(centers, assignment, iteration, vectors.length, null);
      }
      double[][] sums = new double[k][vectors[0].length];
      int[] sizes = new int[k];
      for (int i = 0; i < vectors.length; i++) {
        sizes[assignment[i]]++;
        for (int d = 0; d < vectors[i].length; d++) {
          sums[assignment[i]][d] += vectors[i][d];
        }
      }
      for (int c = 0; c < k; c++) {
        if (sizes[c] > 0) {
          for (int d = 0; d < centers[c].length; d++) {
            centers[c][d] = (float) (sums[c][d] / sizes[c]);
          }
          if (similarity != VectorSimilarityFunction.EUCLIDEAN) {
            Clustering.normalize(centers[c]);
          }
        }
      }
    }
    throw new AssertionError("reference did not converge");
  }

  public void testCascadeReaperMatchesFullCascadeReroute() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      List<float[]> vectors = new ArrayList<>();
      for (int i = 0; i < 240; i++) {
        float[] v = new float[9];
        for (int d = 0; d < v.length; d++) v[d] = random().nextFloat() * 2 - 1;
        vectors.add(v);
      }
      var values = org.apache.lucene.index.FloatVectorValues.fromFloats(vectors, 9);
      var expected =
          Clustering.cluster(
              values,
              48,
              similarity,
              null,
              null,
              1,
              1.05,
              1,
              new Clustering.Options(32, 0, false, org.apache.lucene.util.InfoStream.NO_OUTPUT));
      var actual =
          Clustering.cluster(
              values,
              48,
              similarity,
              null,
              null,
              1,
              1.05,
              4,
              new Clustering.Options(32, 0, true, org.apache.lucene.util.InfoStream.NO_OUTPUT));
      // Skipping is approximate once there are more than two cells: compare the objective, not
      // the assignments. Parallel reaping must still reproduce serial reaping exactly.
      double reaped = objective(vectors, similarity, actual);
      double reference = objective(vectors, similarity, expected);
      assertTrue(reaped + " vs " + reference, reaped <= OBJECTIVE_TOLERANCE * reference);
      var serial =
          Clustering.cluster(
              values,
              48,
              similarity,
              null,
              null,
              1,
              1.05,
              1,
              new Clustering.Options(32, 0, true, org.apache.lucene.util.InfoStream.NO_OUTPUT));
      assertArrayEquals(serial.assignments(), actual.assignments());
      for (int c = 0; c < 48; c++)
        assertArrayEquals(serial.centroids()[c], actual.centroids()[c], 1e-6f);
      for (int i = 0; i < 240; i++) assertArrayEquals(serial.spill()[i], actual.spill()[i]);
    }
  }

  public void testCascadeDefendsIncumbentOutsideShortlist() throws Exception {
    // Every code is the same direction. Nitrox2's top 32 ties exclude cells 32..63,
    // although each of those incumbents is at exact distance zero from its document.
    List<float[]> vectors = new ArrayList<>();
    float[][] seeds = new float[64][];
    int[] assignments = new int[64];
    for (int i = 0; i < 64; i++) {
      seeds[i] = new float[] {i + 1, 0};
      vectors.add(seeds[i]);
      assignments[i] = i;
    }
    var actual =
        Clustering.cluster(
            org.apache.lucene.index.FloatVectorValues.fromFloats(vectors, 2),
            64,
            VectorSimilarityFunction.EUCLIDEAN,
            seeds,
            assignments,
            1,
            1.05,
            4,
            Clustering.Options.DEFAULT);
    assertArrayEquals(assignments, actual.assignments());
    assertEquals(0, actual.initialRouted());
  }

  public void testCascadeWorkAndConvergenceDiagnostics() throws Exception {
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      float[] v = new float[8];
      for (int d = 0; d < v.length; d++) v[d] = random().nextFloat();
      vectors.add(v);
    }
    List<String> messages = new ArrayList<>();
    var trace =
        new org.apache.lucene.util.InfoStream() {
          @Override
          public boolean isEnabled(String component) {
            return component.equals("IVFE");
          }

          @Override
          public void message(String component, String message) {
            messages.add(message);
          }

          @Override
          public void close() {}
        };
    Clustering.cluster(
        org.apache.lucene.index.FloatVectorValues.fromFloats(vectors, 8),
        64,
        VectorSimilarityFunction.EUCLIDEAN,
        null,
        null,
        1,
        1.05,
        4,
        new Clustering.Options(32, 0.005, true, trace));
    String initial = messages.get(0);
    assertTrue(initial.contains("shortlist=32"));
    assertEquals(500 * 32, metric(initial, "fpComparisons"));
    String last = messages.getLast();
    assertEquals(2, metric(last, "stopAt"));
    assertTrue(metric(last, "moved") <= 2);
    for (String message : messages.subList(1, messages.size())) {
      assertTrue(metric(message, "fpComparisons") <= 33 * metric(message, "routed"));
    }
  }

  public void testStableShortlistsAllowReaperSkips() throws Exception {
    float[][] centers = new float[64][64];
    List<float[]> vectors = new ArrayList<>();
    for (int c = 0; c < 64; c++) {
      centers[c][c] = 1;
      vectors.add(centers[c]);
    }
    List<String> messages = new ArrayList<>();
    var trace =
        new org.apache.lucene.util.InfoStream() {
          @Override
          public boolean isEnabled(String component) {
            return true;
          }

          @Override
          public void message(String component, String message) {
            messages.add(message);
          }

          @Override
          public void close() {}
        };
    var result =
        Clustering.cluster(
            org.apache.lucene.index.FloatVectorValues.fromFloats(vectors, 64),
            64,
            VectorSimilarityFunction.EUCLIDEAN,
            centers,
            null,
            1,
            1.05,
            4,
            new Clustering.Options(32, 0, true, trace));
    assertEquals(1, result.iterations());
    assertEquals(0, metric(messages.getLast(), "routed"));
    assertEquals(0, metric(messages.getLast(), "fpComparisons"));
    for (int c = 0; c < 64; c++) assertEquals(c, result.assignments()[c]);
  }

  private static long metric(String message, String name) {
    for (String part : message.split(" ")) {
      if (part.startsWith(name + "=")) return Long.parseLong(part.substring(name.length() + 1));
    }
    throw new AssertionError(message);
  }

  public void testGraphSearch() {
    // Points on a line with thermometer codes: both the exact and the Hamming distance between
    // nodes i and j grow with |i - j|.
    float[][] centers = new float[100][1];
    byte[][] codes = new byte[100][13];
    for (int i = 0; i < centers.length; i++) {
      centers[i][0] = i;
      for (int bit = 0; bit < i; bit++) codes[i][bit / 8] |= (byte) (1 << (bit % 8));
    }
    int[][] graph = CentroidGraph.build(centers, codes);
    for (int i = 0; i < centers.length; i++) {
      // The search nominates, the exact ranking decides: equidistant neighbours tie to the lower.
      int[] visited = CentroidGraph.search(codes, graph, codes[i], 1, _ -> true);
      long[] ranked = CentroidGraph.rank(centers, centers[i], visited);
      assertEquals(i, (int) ranked[0]);
      assertEquals(i == 0 ? 1 : i - 1, (int) ranked[1]);
      int[] all = CentroidGraph.search(codes, graph, codes[i], centers.length, _ -> true);
      assertEquals(centers.length, all.length);
      assertEquals(centers.length, Arrays.stream(all).distinct().count());
      // Rejected nodes are traversed but never returned.
      final int self = i;
      int[] others = CentroidGraph.search(codes, graph, codes[i], 1, n -> n != self);
      assertTrue(Arrays.stream(others).noneMatch(n -> n == self));
      assertEquals(i == 0 ? 1 : i - 1, (int) CentroidGraph.rank(centers, centers[i], others)[0]);
      assertTrue(graph[i].length <= 18);
    }
    // Diversification: on a line, a node keeps one neighbour per side rather than its 16 nearest,
    // since anything farther on the same side is nearer to the kept neighbour than to the node.
    assertTrue(Arrays.stream(graph[50]).anyMatch(n -> n < 50));
    assertTrue(Arrays.stream(graph[50]).anyMatch(n -> n > 50));
  }
}
