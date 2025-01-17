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
import static org.apache.lucene.util.hnsw.HnswGraph.UNKNOWN_MAX_CONN;

import java.io.IOException;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Searches an HNSW graph to find nearest neighbors to a query vector. This particular
 * implementation is optimized for a filtered search, inspired by the ACORN-1 algorithm.
 * https://arxiv.org/abs/2403.04871 However, this implementation is augmented in some ways, mainly:
 *
 * <ul>
 *   <li>It dynamically determines when the optimized filter step should occur based on some
 *       filtered lambda. This is done per small world
 *   <li>The number of candidates to consider for exploration is increased above the original small
 *       world's connection level
 *   <li>The graph searcher keeps exploring until searching is exhausted or the candidate limit is
 *       reached. This may go beyond two hops from the origin small world
 * </ul>
 */
public class FilteredHnswGraphSearcher {
  // How many filtered candidates must be found to consider N-hop neighbors
  private static final float EXPANDED_EXPLORATION_LAMBDA = 0.10f;

  /**
   * Scratch data structures that are used in each {@link #searchBaseLevel} call. These can be
   * expensive to allocate, so they're cleared and reused across calls.
   */
  private final NeighborQueue candidates;

  private final BitSet visited;
  private final BitSet explorationVisited;
  private final HnswGraph graph;

  /**
   * Creates a new graph searcher.
   *
   * @param candidates max heap that will track the candidate nodes to explore
   * @param visited bit set that will track nodes that have already been visited
   */
  private FilteredHnswGraphSearcher(
      NeighborQueue candidates, BitSet explorationVisited, BitSet visited, HnswGraph graph) {
    this.candidates = candidates;
    this.visited = visited;
    this.explorationVisited = explorationVisited;
    this.graph = graph;
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param scorer the scorer to compare the query with the nodes
   * @param knnCollector a collector of top knn results to be returned
   * @param graph the graph values. May represent the entire graph, or a level in a hierarchical
   *     graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   */
  public static void search(
      RandomVectorScorer scorer, KnnCollector knnCollector, HnswGraph graph, Bits acceptOrds)
      throws IOException {
    if (acceptOrds == null) {
      throw new IllegalArgumentException("acceptOrds must not be null to used filtered search");
    }
    int filterSize = 1;
    if (acceptOrds instanceof BitSet bitSet) {
      filterSize = bitSet.cardinality();
    }
    FilteredHnswGraphSearcher graphSearcher =
        new FilteredHnswGraphSearcher(
            new NeighborQueue(knnCollector.k(), true),
            bitSet(filterSize, getGraphSize(graph), graph.maxConns(), knnCollector.k()),
            new SparseFixedBitSet(getGraphSize(graph)),
            graph);
    int ep = graphSearcher.findBestEntryPoint(scorer, knnCollector);
    int maxExplorationMultiplier = Math.min(graph.size() / filterSize, 8);
    if (ep != -1) {
      graphSearcher.searchBaseLevel(knnCollector, scorer, ep, maxExplorationMultiplier, acceptOrds);
    }
  }

  private static BitSet bitSet(int filterSize, int graphSize, int graphMaxConns, int topk) {
    float percentFiltered = (float) filterSize / graphSize;
    assert percentFiltered > 0.0f && percentFiltered < 1.0f;
    int totalOps =
        graphMaxConns != UNKNOWN_MAX_CONN
            ? (int) Math.log(Math.sqrt(graphMaxConns)) * (int) Math.log(graphSize) * topk
            : (int) Math.log(graphSize) * topk;
    int approximateVisitation = (int) (totalOps / percentFiltered);
    return bitSet(approximateVisitation, graphSize);
  }

  private static BitSet bitSet(int expectedBits, int totalBits) {
    if (expectedBits < (totalBits >>> 7)) {
      return new SparseFixedBitSet(totalBits);
    } else {
      return new FixedBitSet(totalBits);
    }
  }

  /**
   * Function to find the best entry point from which to search the zeroth graph layer.
   *
   * @param scorer the scorer to compare the query with the nodes
   * @param collector the knn result collector
   * @return the best entry point, `-1` indicates graph entry node not set, or visitation limit
   *     exceeded
   * @throws IOException When accessing the vector fails
   */
  private int findBestEntryPoint(RandomVectorScorer scorer, KnnCollector collector)
      throws IOException {
    int currentEp = graph.entryNode();
    if (currentEp == -1 || graph.numLevels() == 1) {
      return currentEp;
    }
    int size = getGraphSize(graph);
    prepareScratchState();
    float currentScore = scorer.score(currentEp);
    collector.incVisitedCount(1);
    boolean foundBetter;
    for (int level = graph.numLevels() - 1; level >= 1; level--) {
      foundBetter = true;
      visited.set(currentEp);
      // Keep searching the given level until we stop finding a better candidate entry point
      while (foundBetter) {
        foundBetter = false;
        graphSeek(graph, level, currentEp);
        int friendOrd;
        while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS) {
          assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
          if (visited.getAndSet(friendOrd)) {
            continue;
          }
          if (collector.earlyTerminated()) {
            return -1;
          }
          float friendSimilarity = scorer.score(friendOrd);
          collector.incVisitedCount(1);
          if (friendSimilarity > currentScore) {
            currentScore = friendSimilarity;
            currentEp = friendOrd;
            foundBetter = true;
          }
        }
      }
    }
    return collector.earlyTerminated() ? -1 : currentEp;
  }

  /**
   * Add the closest neighbors found to a priority queue (heap). These are returned in REVERSE
   * proximity order -- the most distant neighbor of the topK found, i.e. the one with the lowest
   * score/comparison value, will be at the top of the heap, while the closest neighbor will be the
   * last to be popped.
   */
  void searchBaseLevel(
      KnnCollector results,
      RandomVectorScorer scorer,
      int ep,
      int maxExplorationMultiplier,
      Bits acceptOrds)
      throws IOException {

    int size = getGraphSize(graph);

    prepareScratchState();

    if (visited.getAndSet(ep) == false) {
      if (results.earlyTerminated()) {
        return;
      }
      float score = scorer.score(ep);
      results.incVisitedCount(1);
      candidates.add(ep, score);
      if (acceptOrds.get(ep)) {
        results.collect(ep, score);
      }
    }
    // Collect the vectors to score and potentially add as candidates
    IntArrayQueue toScore = new IntArrayQueue(graph.maxConns() * maxExplorationMultiplier);
    IntArrayQueue toExplore = new IntArrayQueue(graph.maxConns() * maxExplorationMultiplier);
    // A bound that holds the minimum similarity to the query vector that a candidate vector must
    // have to be considered.
    float minAcceptedSimilarity = results.minCompetitiveSimilarity();
    while (candidates.size() > 0 && results.earlyTerminated() == false) {
      // get the best candidate (closest or best scoring)
      float topCandidateSimilarity = candidates.topScore();
      if (topCandidateSimilarity < minAcceptedSimilarity) {
        break;
      }
      int topCandidateNode = candidates.pop();
      // Pre-fetch neighbors into an array
      // This is necessary because we need to call `seek` on each neighbor to consider 2-hop
      // neighbors
      graph.seek(0, topCandidateNode);
      int neighborCount = graph.neighborCount();
      toScore.clear();
      toExplore.clear();
      int friendOrd;
      while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS && toScore.isFull() == false) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.get(friendOrd) || explorationVisited.getAndSet(friendOrd)) {
          continue;
        }
        if (acceptOrds.get(friendOrd)) {
          toScore.add(friendOrd);
        } else {
          toExplore.add(friendOrd);
        }
      }
      // adjust to locally number of filtered candidates to explore
      float filteredAmount = toExplore.count() / (float) neighborCount;
      int maxToScoreCount =
          (int) (neighborCount * Math.min(maxExplorationMultiplier, 1f / (1f - filteredAmount)));
      // There is enough filtered, or we don't have enough candidates to score and explore
      if (toScore.count() < maxToScoreCount && filteredAmount > EXPANDED_EXPLORATION_LAMBDA) {
        // Now we need to explore the neighbors of the neighbors
        int exploreFriend;
        while ((exploreFriend = toExplore.poll()) != NO_MORE_DOCS
            && toScore.count() < maxToScoreCount) {
          graphSeek(graph, 0, exploreFriend);
          int friendOfAFriendOrd;
          while ((friendOfAFriendOrd = graph.nextNeighbor()) != NO_MORE_DOCS
              && toScore.count() < maxToScoreCount) {
            if (visited.get(friendOfAFriendOrd)
                || explorationVisited.getAndSet(friendOfAFriendOrd)) {
              continue;
            }
            if (acceptOrds.get(friendOfAFriendOrd)) {
              toScore.add(friendOfAFriendOrd);
            }
          }
        }
      }
      // Score the vectors and add them to the candidate list
      int toScoreOrd;
      while ((toScoreOrd = toScore.poll()) != NO_MORE_DOCS) {
        float friendSimilarity = scorer.score(toScoreOrd);
        results.incVisitedCount(1);
        if (friendSimilarity > minAcceptedSimilarity) {
          candidates.add(toScoreOrd, friendSimilarity);
          if (results.collect(toScoreOrd, friendSimilarity)) {
            minAcceptedSimilarity = results.minCompetitiveSimilarity();
          }
        }
      }
    }
  }

  private void prepareScratchState() {
    visited.clear();
    explorationVisited.clear();
  }

  /**
   * Seek a specific node in the given graph. The default implementation will just call {@link
   * HnswGraph#seek(int, int)}
   *
   * @throws IOException when seeking the graph
   */
  void graphSeek(HnswGraph graph, int level, int targetNode) throws IOException {
    graph.seek(level, targetNode);
  }

  private static int getGraphSize(HnswGraph graph) {
    return graph.maxNodeId() + 1;
  }

  private static class IntArrayQueue {
    private int[] nodes;
    private int upto;
    private int size;

    IntArrayQueue(int capacity) {
      nodes = new int[capacity];
    }

    int count() {
      return size - upto;
    }

    void expand(int capacity) {
      if (nodes.length < capacity) {
        int[] newNodes = new int[capacity];
        System.arraycopy(nodes, 0, newNodes, 0, size);
        nodes = newNodes;
      }
    }

    void add(int node) {
      assert isFull() == false;
      if (size == nodes.length) {
        expand(size * 2);
      }
      nodes[size++] = node;
    }

    boolean isFull() {
      return size == nodes.length;
    }

    int poll() {
      if (upto == size) {
        return NO_MORE_DOCS;
      }
      return nodes[upto++];
    }

    void clear() {
      upto = 0;
      size = 0;
    }
  }
}
