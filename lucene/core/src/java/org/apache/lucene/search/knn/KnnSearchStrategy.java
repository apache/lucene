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

import java.util.Objects;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HnswQueueSaturationCollector;

/**
 * KnnSearchStrategy is a strategy for kNN search, providing additional search strategy
 * configuration
 *
 * @lucene.experimental
 */
public abstract class KnnSearchStrategy {
  public static final int DEFAULT_FILTERED_SEARCH_THRESHOLD = 60;

  /** Override and implement search strategy instance equivalence properly in a subclass. */
  @Override
  public abstract boolean equals(Object obj);

  /**
   * Override and implement search strategy hash code properly in a subclass.
   *
   * @see #equals(Object)
   */
  @Override
  public abstract int hashCode();

  /** Signal processing of the next block of vectors. */
  public abstract void nextVectorsBlock();

  /**
   * Estimates the number of distinct nodes this search strategy expects to visit, bounded by the
   * graph size. Used to decide between an HNSW search and an exhaustive scan.
   *
   * @param k neighbors to find
   * @param graphSize size of the graph (must be > 0)
   * @param avgDegree average out-degree of nodes in the bottom layer of the graph, or 0 if unknown
   * @return expected number of distinct visited nodes, in [0, graphSize]
   */
  public int expectedVisitedNodes(int k, int graphSize, float selectivity, float avgDegree) {
    return (int) (k * Math.log(graphSize));
  }

  /**
   * A strategy for kNN search that uses HNSW
   *
   * @lucene.experimental
   */
  public static class Hnsw extends KnnSearchStrategy {
    public static final Hnsw DEFAULT = new Hnsw(DEFAULT_FILTERED_SEARCH_THRESHOLD);

    private final int filteredSearchThreshold;

    /**
     * Create a new Hnsw strategy
     *
     * @param filteredSearchThreshold threshold for filtered search, a percentage value from 0 to
     *     100 where 0 means never use filtered search and 100 means always use filtered search.
     */
    public Hnsw(int filteredSearchThreshold) {
      if (filteredSearchThreshold < 0 || filteredSearchThreshold > 100) {
        throw new IllegalArgumentException("filteredSearchThreshold must be >= 0 and <= 100");
      }
      this.filteredSearchThreshold = filteredSearchThreshold;
    }

    public int filteredSearchThreshold() {
      return filteredSearchThreshold;
    }

    /**
     * Squashes x to range [0, n)
     *
     * <p>This function is smooth and monotonic in x.
     */
    private static double squash(double x, double limit) {
      if (limit <= 1) {
        return 0;
      }
      // Expected number of unique items discovered when sampling `x` times from a universe of
      // `limit` items uniformly at random.
      return limit * (1 - Math.exp(x * Math.log1p(-1.0 / limit)));
    }

    @Override
    public int expectedVisitedNodes(
        int k, int graphSize, float selectivity, int m, float avgDegree) {
      if (useFilteredSearch(selectivity)) {
        // TODO: Account for selectivity in ACORN-1
        return super.expectedVisitedNodes(k, graphSize, selectivity, m, avgDegree);
      }

      // Newer segments store their avg node degree, but for older segments choose 16 to preserve
      // legacy behavior as much as possible.
      double expectedDegree = avgDegree > 0 ? avgDegree : 16;

      double requiredScores = Math.min(k, graphSize * selectivity);
      double s = Math.max(.001, selectivity); // highly selectively queries should not blow up cost
      double logN = Math.log(graphSize);
      double descent = .70 * logN * logN; // Cost of finding layer0 entry point.

      double localUniverse = Math.min(graphSize, expectedDegree * k / s);
      double baseCost = expectedDegree * logN; // Unfiltered k=1 cost.
      double filteredCostEstimate = .28 * Math.pow(k / s, .71) * expectedDegree * logN;

      return (int)
          Math.max(
              requiredScores,
              descent + Math.max(baseCost, squash(localUniverse, filteredCostEstimate)));
    }

    /**
     * Whether to use filtered search based on the ratio of vectors that pass the filter
     *
     * @param ratioPassingFilter ratio of vectors that pass the filter
     * @return true if filtered search should be used
     */
    public final boolean useFilteredSearch(float ratioPassingFilter) {
      assert ratioPassingFilter >= 0 && ratioPassingFilter <= 1;
      return ratioPassingFilter * 100 < filteredSearchThreshold;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Hnsw hnsw = (Hnsw) o;
      return filteredSearchThreshold == hnsw.filteredSearchThreshold;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(filteredSearchThreshold);
    }

    @Override
    public void nextVectorsBlock() {}
  }

  /**
   * A strategy for kNN search that uses a set of entry points to start the search
   *
   * @lucene.experimental
   */
  public static class Seeded extends KnnSearchStrategy {
    private final DocIdSetIterator entryPoints;
    private final int numberOfEntryPoints;
    private final KnnSearchStrategy originalStrategy;

    public Seeded(
        DocIdSetIterator entryPoints, int numberOfEntryPoints, KnnSearchStrategy originalStrategy) {
      if (numberOfEntryPoints < 0) {
        throw new IllegalArgumentException("numberOfEntryPoints must be >= 0");
      }
      this.numberOfEntryPoints = numberOfEntryPoints;
      if (numberOfEntryPoints > 0 && entryPoints == null) {
        throw new IllegalArgumentException("entryPoints must not be null");
      }
      this.entryPoints = entryPoints == null ? DocIdSetIterator.empty() : entryPoints;
      this.originalStrategy = originalStrategy;
    }

    /**
     * Iterator of valid entry points for the kNN search
     *
     * @return DocIdSetIterator of entry points
     */
    public DocIdSetIterator entryPoints() {
      return entryPoints;
    }

    /**
     * Number of valid entry points for the kNN search
     *
     * @return number of entry points
     */
    public int numberOfEntryPoints() {
      return numberOfEntryPoints;
    }

    /**
     * Original strategy to use after seeding
     *
     * @return original strategy
     */
    public KnnSearchStrategy originalStrategy() {
      return originalStrategy;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Seeded seeded = (Seeded) o;
      return numberOfEntryPoints == seeded.numberOfEntryPoints
          && Objects.equals(entryPoints, seeded.entryPoints)
          && Objects.equals(originalStrategy, seeded.originalStrategy);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entryPoints, numberOfEntryPoints, originalStrategy);
    }

    @Override
    public void nextVectorsBlock() {
      originalStrategy.nextVectorsBlock();
    }
  }

  /**
   * A strategy for kNN search on HNSW that early exits when nearest neighbor collection rate
   * saturates.
   *
   * @lucene.experimental
   */
  public static class Patience extends Hnsw {
    private final HnswQueueSaturationCollector collector;

    public Patience(HnswQueueSaturationCollector collector, int filteredSearchThreshold) {
      super(filteredSearchThreshold);
      this.collector = collector;
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj) && Objects.equals(collector, ((Patience) obj).collector);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.filteredSearchThreshold, collector);
    }

    @Override
    public void nextVectorsBlock() {
      collector.nextCandidate();
    }
  }
}
