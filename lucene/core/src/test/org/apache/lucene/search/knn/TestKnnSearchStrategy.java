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
package org.apache.lucene.search.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestKnnSearchStrategy {

  private static KnnSearchStrategy.Hnsw unfiltered() {
    return new KnnSearchStrategy.Hnsw(0);
  }

  @ParameterizedTest
  @CsvSource(
      useHeadersInDisplayName = true,
      textBlock =
          """
      k, numVecs, selectivity, avgDegree, expected
      1, 1, 1.0, 16.0, 1
      1, 1, 0.0, 16.0, 0
      10, 2, 1.0, 16.0, 2
      100, 10_000, 0.1, 1.0, 387
      100, 100, 0.1, 16.0, 100
      10_000, 2_147_483_647, .0001, 128, 71_871_564
      """)
  void testExpectedVisitedNodesBoundaryCases(
      int k, int numVecs, float selectivity, float avgDegree, int expected) {
    KnnSearchStrategy strategy = unfiltered();

    int actual = strategy.expectedVisitedNodes(k, numVecs, selectivity, avgDegree);

    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource(
      useHeadersInDisplayName = true,
      textBlock =
          """
      k, numVecs, selectivity, avgDegree, expected
      10, 1_000_000, 1.0, 16.0, 354
      100, 1_000_000, 1.0, 16.0, 1152
      1_000, 1_000_000, 1.0, 16.0, 7254
      10_000, 1_000_000, 1.0, 16.0, 41932
      100_000, 1_000_000, 1.0, 16.0, 217427
      """)
  void testExpectedVisitedNodesSublinearInK(
      int k, int numVecs, float selectivity, float avgDegree, int expected) {
    KnnSearchStrategy strategy = unfiltered();
    int actual = strategy.expectedVisitedNodes(k, numVecs, selectivity, avgDegree);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource(
      useHeadersInDisplayName = true,
      textBlock =
          """
      k, numVecs, selectivity, avgDegree, expected
      100, 1_000, 1.0, 16.0, 609
      100, 100_000, 1.0, 16.0, 1032
      100, 10_000_000, 1.0, 16.0, 1263
      100, 1_000_000_000, 1.0, 16.0, 1474
      """)
  void testExpectedVisitedNodesSublinearInN(
      int k, int numVecs, float selectivity, float avgDegree, int expected) {
    KnnSearchStrategy strategy = unfiltered();
    int actual = strategy.expectedVisitedNodes(k, numVecs, selectivity, avgDegree);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource(
      useHeadersInDisplayName = true,
      textBlock =
          """
      k, numVecs, selectivity, avgDegree, expected
      1000, 100_000, 1.0, 1.0, 1000
      1000, 100_000, 1.0, 16.0, 6352
      1000, 100_000, 1.0, 64.0, 25132
      1000, 100_000, 1.0, 128.0, 46521
      1000, 100_000, 1.0, 256.0, 66077
      """)
  void testExpectedVisitedSaturateWithDegree(
      int k, int numVecs, float selectivity, float avgDegree, int expected) {
    KnnSearchStrategy strategy = unfiltered();
    int actual = strategy.expectedVisitedNodes(k, numVecs, selectivity, avgDegree);
    assertEquals(expected, actual);
  }

  @Test
  void testExpectedVisitedNodesFilteredSearchUsesTraditionalEstimate() {
    KnnSearchStrategy.Hnsw strat = new KnnSearchStrategy.Hnsw(60);
    int actual = strat.expectedVisitedNodes(100, 100000, .5f, 16);
    assertEquals(1151, actual);
  }

  @Test
  void testExpectedVisitedNodesWithUnknownDegreeDefaultsTo16() {
    int knownDegree = unfiltered().expectedVisitedNodes(1, 10_000, 1f, 16f);
    int unknownDegResult = unfiltered().expectedVisitedNodes(1, 10_000, 1f, 0f);

    assertEquals(206, knownDegree);
    assertEquals(206, unknownDegResult);
  }
}
