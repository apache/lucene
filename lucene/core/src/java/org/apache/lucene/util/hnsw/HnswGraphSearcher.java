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
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Searches an HNSW graph to find nearest neighbors to a query vector. For more background on the
 * search algorithm, see {@link HnswGraph}.
 */
public final class HnswGraphSearcher {
  private final VectorSimilarityFunction similarityFunction;
  /**
   * Scratch data structures that are used in each {@link #searchLevel} call. These can be expensive
   * to allocate, so they're cleared and reused across calls.
   */
  private final NeighborQueue candidates;

  private final BitSet visited;

  /**
   * Creates a new graph searcher.
   *
   * @param similarityFunction the similarity function to compare vectors
   * @param candidates max heap that will track the candidate nodes to explore
   * @param visited bit set that will track nodes that have already been visited
   */
  public HnswGraphSearcher(
      VectorSimilarityFunction similarityFunction, NeighborQueue candidates, BitSet visited) {
    this.similarityFunction = similarityFunction;
    this.candidates = candidates;
    this.visited = visited;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param vectors the vector values
   * @param similarityFunction the similarity function to compare vectors
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @return a priority queue holding the closest neighbors found
   */
  public static NeighborQueue search(
      float[] query,
      int topK,
      RandomAccessVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    HnswGraphSearcher graphSearcher =
        new HnswGraphSearcher(
            similarityFunction,
            new NeighborQueue(topK, true),
            new SparseFixedBitSet(vectors.size()));
    NeighborQueue results;
    int[] eps = new int[] {graph.entryNode()};
    int numVisited = 0;
    for (int level = graph.numLevels() - 1; level >= 1; level--) {
      results = graphSearcher.searchLevel(query, 1, level, eps, vectors, graph, null, visitedLimit);

      numVisited += results.visitedCount();
      visitedLimit -= results.visitedCount();

      if (results.incomplete()) {
        results.setVisitedCount(numVisited);
        return results;
      }
      eps[0] = results.pop();
    }
    results =
        graphSearcher.searchLevel(query, topK, 0, eps, vectors, graph, acceptOrds, visitedLimit);
    results.setVisitedCount(results.visitedCount() + numVisited);
    return results;
  }

  /**
   * Searches for the nearest neighbors of a query vector in a given level.
   *
   * <p>If the search stops early because it reaches the visited nodes limit, then the results will
   * be marked incomplete through {@link NeighborQueue#incomplete()}.
   *
   * @param query search query vector
   * @param topK the number of nearest to query results to return
   * @param level level to search
   * @param eps the entry points for search at this level expressed as level 0th ordinals
   * @param vectors vector values
   * @param graph the graph values
   * @return a priority queue holding the closest neighbors found
   */
  public NeighborQueue searchLevel(
      float[] query,
      int topK,
      int level,
      final int[] eps,
      RandomAccessVectorValues vectors,
      HnswGraph graph)
      throws IOException {
    return searchLevel(query, topK, level, eps, vectors, graph, null, Integer.MAX_VALUE);
  }

  private NeighborQueue searchLevel(
      float[] query,
      int topK,
      int level,
      final int[] eps,
      RandomAccessVectorValues vectors,
      HnswGraph graph,
      Bits acceptOrds,
      int visitedLimit)
      throws IOException {
    int size = graph.size();
    NeighborQueue results = new NeighborQueue(topK, false);
    clearScratchState();

    int numVisited = 0;
    for (int ep : eps) {
      if (visited.getAndSet(ep) == false) {
        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }
        float score = similarityFunction.compare(query, vectors.vectorValue(ep));
        numVisited++;
        candidates.add(ep, score);
        if (acceptOrds == null || acceptOrds.get(ep)) {
          results.add(ep, score);
        }
      }
    }

    // A bound that holds the minimum similarity to the query vector that a candidate vector must
    // have to be considered.
    float minAcceptedSimilarity = Float.NEGATIVE_INFINITY;
    if (results.size() >= topK) {
      minAcceptedSimilarity = results.topScore();
    }
    while (candidates.size() > 0 && results.incomplete() == false) {
      // get the best candidate (closest or best scoring)
      float topCandidateSimilarity = candidates.topScore();
      if (topCandidateSimilarity < minAcceptedSimilarity) {
        break;
      }

      int topCandidateNode = candidates.pop();
      graph.seek(level, topCandidateNode);
      int friendOrd;
      while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.getAndSet(friendOrd)) {
          continue;
        }

        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }
        float friendSimilarity = similarityFunction.compare(query, vectors.vectorValue(friendOrd));
        numVisited++;
        if (friendSimilarity >= minAcceptedSimilarity) {
          candidates.add(friendOrd, friendSimilarity);
          if (acceptOrds == null || acceptOrds.get(friendOrd)) {
            if (results.insertWithOverflow(friendOrd, friendSimilarity) && results.size() >= topK) {
              minAcceptedSimilarity = results.topScore();
            }
          }
        }
      }
    }
    while (results.size() > topK) {
      results.pop();
    }
    results.setVisitedCount(numVisited);
    return results;
  }

  private void clearScratchState() {
    candidates.clear();
    visited.clear(0, visited.length());
  }
}
