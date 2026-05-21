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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * AbstractHnswGraphSearcher is the base class for HnswGraphSearcher implementations.
 *
 * @lucene.experimental
 */
abstract class AbstractHnswGraphSearcher {

  static final int UNK_EP = -1;

  /**
   * Search a given level of the graph starting at the given entry points.
   *
   * @param results the collector to collect the results
   * @param scorer the scorer to compare the query with the nodes
   * @param level the level of the graph to search
   * @param eps the entry points to start the search from
   * @param graph the HNSWGraph
   * @param acceptOrds the ordinals to accept for the results
   */
  abstract void searchLevel(
      KnnCollector results,
      RandomVectorScorer scorer,
      int level,
      final int[] eps,
      HnswGraph graph,
      Bits acceptOrds)
      throws IOException;

  /**
   * Function to find the best entry point from which to search the zeroth graph layer.
   *
   * @param scorer the scorer to compare the query with the nodes
   * @param graph the HNSWGraph
   * @param collector the knn result collector
   * @return the best entry point, `-1` indicates graph entry node not set, or visitation limit
   *     exceeded
   * @throws IOException When accessing the vectors or graph fails
   */
  abstract int[] findBestEntryPoint(
      RandomVectorScorer scorer, HnswGraph graph, KnnCollector collector) throws IOException;

  /**
   * Search the graph for the given scorer. Gathering results in the provided collector that pass
   * the provided acceptOrds.
   *
   * @param results the collector to collect the results
   * @param scorer the scorer to compare the query with the nodes
   * @param graph the HNSWGraph
   * @param acceptOrds the ordinals to accept for the results
   * @throws IOException When accessing the vectors or graph fails
   */
  public void search(
      KnnCollector results, RandomVectorScorer scorer, HnswGraph graph, Bits acceptOrds)
      throws IOException {
    int[] eps = findBestEntryPoint(scorer, graph, results);
    assert eps != null && eps.length > 0;
    if (eps[0] == UNK_EP) {
      return;
    }
    searchLevel(results, scorer, 0, eps, graph, acceptOrds);
  }

  protected static void scoreEntryPoints(
      KnnCollector results,
      RandomVectorScorer scorer,
      BitSet visited,
      int[] eps,
      Bits acceptOrds,
      NeighborQueue candidates,
      float[] scores)
      throws IOException {
    assert eps != null && eps.length > 0;
    assert scores != null && scores.length >= eps.length;
    scorer.bulkScore(eps, scores, eps.length);
    results.incVisitedCount(eps.length);
    float[] siblingScores = null;
    int[] siblingsOrd = new int[0];
    for (int i = 0; i < eps.length; i++) {
      float score = scores[i];
      int ep = eps[i];
      visited.set(ep);
      candidates.add(ep, score);
      if (acceptOrds == null || acceptOrds.get(ep)) {
        // Fetch siblingsOrd BEFORE collect() so the parent is not yet in the heap
        // The instanceof check is needed: this method is also called with a
        // GraphBuilderKnnCollector
        if (results instanceof OrdinalTranslatedKnnCollector collector) {
          if (collector.isSiblingExpansionCollector()) {
            siblingsOrd = collector.getSiblingOrdinals(ep, visited, siblingsOrd);
            for (int ord : siblingsOrd) visited.set(ord);
          }
        }
        // Collect the ep node here so after we have a correctly updated minCompetitiveSimilarity
        results.collect(ep, score);
        if (siblingsOrd.length > 0) {
          siblingScores =
              scoreHnswNodes(
                  results,
                  scorer,
                  candidates,
                  acceptOrds,
                  siblingsOrd,
                  siblingScores);
        }
      }
    }
  }

  /**
   * Scores and collects siblings, adding competitive ones to the candidate queue. Reuses and
   * returns the siblingScores buffer, reallocating only if too small.
   */
  protected static float[] scoreHnswNodes(
      KnnCollector results,
      RandomVectorScorer scorer,
      NeighborQueue candidates,
      Bits acceptOrds,
      int[] hnswNodesOrd,
      float[] scores)
      throws IOException {
    int numNodes = hnswNodesOrd.length;
    // If scores not defined yet or too small to collect scores a new one is created
    // Otherwise we reuse the old one that will be overridden in bulkScore with new scores
    if (scores == null || scores.length < numNodes) {
      scores = new float[numNodes];
    }
    float maxScore = scorer.bulkScore(hnswNodesOrd, scores, numNodes);
    results.incVisitedCount(numNodes);
    if (maxScore > results.minCompetitiveSimilarity()) {
      float minSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
      for (int j = 0; j < numNodes; j++) {
        float sibScore = scores[j];
        // We avoid adding to candidates a sibling with a bad score
        if (sibScore >= minSimilarity) {
          candidates.add(hnswNodesOrd[j], sibScore);
          if (acceptOrds == null || acceptOrds.get(hnswNodesOrd[j])) {
            results.collect(hnswNodesOrd[j], sibScore);
            minSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
          }
        }
      }
    }
    return scores;
  }
}
