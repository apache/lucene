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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Navigable Small-world graph. Provides efficient approximate nearest neighbor search for high
 * dimensional vectors. See <a href="https://doi.org/10.1016/j.is.2013.10.006">Approximate nearest
 * neighbor algorithm based on navigable small world graphs [2014]</a> and <a
 * href="https://arxiv.org/abs/1603.09320">this paper [2018]</a> for details.
 *
 * <p>The nomenclature is a bit different here from what's used in those papers:
 *
 * <h2>Hyperparameters</h2>
 *
 * <ul>
 *   <li><code>numSeed</code> is the equivalent of <code>m</code> in the 2014 paper; it controls the
 *       number of random entry points to sample.
 *   <li><code>beamWidth</code> in {@link HnswGraphBuilder} has the same meaning as <code>efConst
 *       </code> in the 2018 paper. It is the number of nearest neighbor candidates to track while
 *       searching the graph for each newly inserted node.
 *   <li><code>maxConn</code> has the same meaning as <code>M</code> in the later paper; it controls
 *       how many of the <code>efConst</code> neighbors are connected to the new node
 * </ul>
 *
 * <p>Note: The graph may be searched by multiple threads concurrently, but updates are not
 * thread-safe. Also note: there is no notion of deletions. Document searching built on top of this
 * must do its own deletion-filtering.
 */
public final class HnswGraph extends KnnGraphValues {

  private final int maxConn;
  private int numOfLevels; // the current the number of levels in the graph
  private int entryNode; // the current graph entry node on the top level

  // Nodes by level expressed as the level 0's nodes' ordinals.
  // As level 0 contains all nodes, nodesByLevel.get(0) is null.
  private final List<int[]> nodesByLevel;

  // graph is a list of graph levels.
  // Each level is represented as List<NeighborArray> – nodes' connections on this level.
  // Each entry in the list has the top maxConn neighbors of a node. The nodes correspond to vectors
  // added to HnswBuilder, and the node values are the ordinals of those vectors.
  // Thus, on all levels, neighbors expressed as the level 0's nodes' ordinals.
  private final List<List<NeighborArray>> graph;

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  HnswGraph(int maxConn, int levelOfFirstNode) {
    this.maxConn = maxConn;
    this.numOfLevels = levelOfFirstNode + 1;
    this.graph = new ArrayList<>(numOfLevels);
    this.entryNode = 0;
    for (int i = 0; i < numOfLevels; i++) {
      graph.add(new ArrayList<>());
      // Typically with diversity criteria we see nodes not fully occupied;
      // average fanout seems to be about 1/2 maxConn.
      // There is some indexing time penalty for under-allocating, but saves RAM
      graph.get(i).add(new NeighborArray(Math.max(32, maxConn / 4)));
    }

    this.nodesByLevel = new ArrayList<>(numOfLevels);
    nodesByLevel.add(null); // we don't need this for 0th level, as it contians all nodes
    for (int l = 1; l < numOfLevels; l++) {
      nodesByLevel.add(new int[] {0});
    }
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param numSeed the size of the queue maintained while searching
   * @param vectors vector values
   * @param graphValues the graph values. May represent the entire graph, or a level in a
   *     hierarchical graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @param random a source of randomness, used for generating entry points to the graph
   * @return a priority queue holding the closest neighbors found
   */
  public static NeighborQueue search(
      float[] query,
      int topK,
      int numSeed,
      RandomAccessVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      KnnGraphValues graphValues,
      Bits acceptOrds,
      Random random)
      throws IOException {

    int size = graphValues.size();
    int boundedNumSeed = Math.max(topK, Math.min(numSeed, 2 * size));
    NeighborQueue results;

    int[] eps = new int[] {graphValues.entryNode()};
    for (int level = graphValues.numOfLevels() - 1; level >= 1; level--) {
      results =
          HnswGraph.searchLevel(
              query, 1, level, eps, vectors, similarityFunction, graphValues, null);
      eps[0] = results.pop();
    }
    results =
        HnswGraph.searchLevel(
            query, boundedNumSeed, 0, eps, vectors, similarityFunction, graphValues, acceptOrds);
    while (results.size() > topK) {
      results.pop();
    }
    return results;
  }

  /**
   * Searches for the nearest neighbors of a query vector in a given level
   *
   * @param query search query vector
   * @param topK the number of nearest to query results to return
   * @param level level to search
   * @param eps the entry points for search at this level expressed as level 0th ordinals
   * @param vectors vector values
   * @param similarityFunction similarity function
   * @param graphValues the graph values
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @return a priority queue holding the closest neighbors found
   */
  static NeighborQueue searchLevel(
      float[] query,
      int topK,
      int level,
      final int[] eps,
      RandomAccessVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      KnnGraphValues graphValues,
      Bits acceptOrds)
      throws IOException {

    int size = graphValues.size();
    int queueSize = Math.max(eps.length, topK);
    // MIN heap, holding the top results
    NeighborQueue results = new NeighborQueue(queueSize, similarityFunction.reversed);
    // MAX heap, from which to pull the candidate nodes
    NeighborQueue candidates = new NeighborQueue(queueSize, !similarityFunction.reversed);
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    SparseFixedBitSet visited = new SparseFixedBitSet(size);

    for (int ep : eps) {
      if (visited.get(ep) == false) {
        visited.set(ep);
        float score = similarityFunction.compare(query, vectors.vectorValue(ep));
        candidates.add(ep, score);
        if (acceptOrds == null || acceptOrds.get(ep)) {
          results.add(ep, score);
        }
      }
    }

    // Set the bound to the worst current result and below reject any newly-generated candidates
    // failing to exceed this bound
    BoundsChecker bound = BoundsChecker.create(similarityFunction.reversed);
    bound.set(results.topScore());
    while (candidates.size() > 0) {
      // get the best candidate (closest or best scoring)
      float topCandidateScore = candidates.topScore();
      if (results.size() >= topK) {
        if (bound.check(topCandidateScore)) {
          break;
        }
      }
      int topCandidateNode = candidates.pop();
      graphValues.seek(level, topCandidateNode);
      int friendOrd;
      while ((friendOrd = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.get(friendOrd)) {
          continue;
        }
        visited.set(friendOrd);

        float score = similarityFunction.compare(query, vectors.vectorValue(friendOrd));
        if (results.size() < topK || bound.check(score) == false) {
          candidates.add(friendOrd, score);
          if (acceptOrds == null || acceptOrds.get(friendOrd)) {
            results.insertWithOverflow(friendOrd, score);
            bound.set(results.topScore());
          }
        }
      }
    }
    while (results.size() > topK) {
      results.pop();
    }
    results.setVisitedCount(visited.approximateCardinality());
    return results;
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public NeighborArray getNeighbors(int level, int node) {
    if (level == 0) {
      return graph.get(level).get(node);
    }
    int nodeIndex = Arrays.binarySearch(nodesByLevel.get(level), 0, graph.get(level).size(), node);
    assert nodeIndex >= 0;
    return graph.get(level).get(nodeIndex);
  }

  @Override
  public int size() {
    return graph.get(0).size(); // all nodes are located on the 0th level
  }

  /**
   * Add node on the given level
   *
   * @param level level to add a node on
   * @param node the node to add, represented as an ordinal on the level 0.
   */
  public void addNode(int level, int node) {
    if (level > 0) {
      // if the new node introduces a new level, add more levels to the graph,
      // and make this node the graph's new entry point
      if (level >= numOfLevels) {
        for (int i = numOfLevels; i <= level; i++) {
          graph.add(new ArrayList<>());
          nodesByLevel.add(new int[] {node});
        }
        numOfLevels = level + 1;
        entryNode = node;
      } else {
        // Add this node id to this level's nodes
        int[] nodes = nodesByLevel.get(level);
        int idx = graph.get(level).size();
        if (idx < nodes.length) {
          nodes[idx] = node;
        } else {
          nodes = ArrayUtil.grow(nodes);
          nodes[idx] = node;
          nodesByLevel.set(level, nodes);
        }
      }
    }

    graph.get(level).add(new NeighborArray(maxConn + 1));
  }

  @Override
  public void seek(int level, int targetNode) {
    cur = getNeighbors(level, targetNode);
    upto = -1;
  }

  @Override
  public int nextNeighbor() {
    if (++upto < cur.size()) {
      return cur.node[upto];
    }
    return NO_MORE_DOCS;
  }

  /**
   * Returns the current number of levels in the graph
   *
   * @return the current number of levels in the graph
   */
  @Override
  public int numOfLevels() {
    return numOfLevels;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public int entryNode() {
    return entryNode;
  }

  @Override
  public DocIdSetIterator getAllNodesOnLevel(int level) {
    return new DocIdSetIterator() {
      int[] nodes = level == 0 ? null : nodesByLevel.get(level);
      int size = level == 0 ? size() : graph.get(level).size();
      int idx = -1;

      @Override
      public int docID() {
        return level == 0 ? idx : nodes[idx];
      }

      @Override
      public int nextDoc() {
        idx++;
        if (idx >= size) {
          idx = NO_MORE_DOCS;
          return NO_MORE_DOCS;
        }
        return level == 0 ? idx : nodes[idx];
      }

      @Override
      public long cost() {
        return size;
      }

      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException("Not supported");
      }
    };
  }
}
