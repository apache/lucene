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
   * multi valued approach
   */
  public enum Multivalued {
    NONE {
      @Override
      float updateScore(float originalScore, float newScore) {
        return 0;
      }
    },
    MAX {
      @Override
      float updateScore(float originalScore, float newScore) {
        return Math.max(originalScore,newScore);
      }
    },
    SUM {
      @Override
      float updateScore(float originalScore, float newScore) {
        return originalScore + newScore;
      }
    };

    abstract float updateScore(float originalScore, float newScore);
  }
  
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
          int visitedLimit,
          Multivalued strategy)
      throws IOException {
    HnswGraphSearcher graphSearcher =
        new HnswGraphSearcher(
            similarityFunction,
            new NeighborQueue(topK, !similarityFunction.reversed),
            new SparseFixedBitSet(vectors.size()));
    NeighborQueue results;
    int[] eps = new int[] {graph.entryNode()};
    int numVisited = 0;
    for (int level = graph.numLevels() - 1; level >= 1; level--) {
      results = graphSearcher.searchLevel(query, 1, level, eps, vectors, graph, null, visitedLimit, strategy);
      eps[0] = results.pop();

      numVisited += results.visitedCount();
      visitedLimit -= results.visitedCount();
    }
    results =
        graphSearcher.searchLevel(query, topK, 0, eps, vectors, graph, acceptOrds, visitedLimit, strategy);
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
    return searchLevel(query, topK, level, eps, vectors, graph, null, Integer.MAX_VALUE, Multivalued.NONE);
  }

  private NeighborQueue searchLevel(
      float[] query,
      int topK,
      int level,
      final int[] entryPoints,
      RandomAccessVectorValues vectors,
      HnswGraph graph,
      Bits acceptOrds,
      int visitedLimit,
      Multivalued strategy)
      throws IOException {
    int size = graph.size();
    NeighborQueue results = new NeighborQueue(topK, similarityFunction.reversed);
    clearScratchState();

    int numVisited = 0;
    for (int vectorId : entryPoints) {
      if (visited.getAndSet(vectorId) == false) {
        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }
        float score = similarityFunction.compare(query, vectors.vectorValue(vectorId));
        numVisited++;
        candidates.add(vectorId, score);
        int docId = vectors.ordToDoc(vectorId);
        if (acceptOrds == null || acceptOrds.get(docId)) {
          results.add(docId, score, strategy);
        }
      }
    }

    // A bound that holds the minimum similarity to the query vector that a candidate vector must
    // have to be considered.
    BoundsChecker maxDistance = BoundsChecker.create(similarityFunction.reversed);
    if (results.size() >= topK) {
      maxDistance.set(results.topScore());
    }
    while (candidates.size() > 0 && results.incomplete() == false) {
      // get the best candidate (closest or best scoring)
      float topCandidateScore = candidates.topScore();
      if (maxDistance.check(topCandidateScore)) {
        break;
      }

      int topCandidateVectorId = candidates.pop();
      graph.seek(level, topCandidateVectorId);
      int friendVectorId;
      while ((friendVectorId = graph.nextNeighbor()) != NO_MORE_DOCS) {
        assert friendVectorId < size : "friendOrd=" + friendVectorId + "; size=" + size;
        if (visited.getAndSet(friendVectorId)) {
          continue;
        }

        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }
        float score = similarityFunction.compare(query, vectors.vectorValue(friendVectorId));
        numVisited++;
        if (maxDistance.check(score) == false) {
          candidates.add(friendVectorId, score);
          if (acceptOrds == null || acceptOrds.get(friendVectorId)) {
            if (results.insertWithOverflow(vectors.ordToDoc(friendVectorId), score, strategy) && results.size() >= topK) {
              maxDistance.set(results.topScore());
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
