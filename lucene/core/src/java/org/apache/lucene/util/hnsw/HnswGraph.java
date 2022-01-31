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
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Hierarchical Navigable Small World graph. Provides efficient approximate nearest neighbor search
 * for high dimensional vectors. See <a href="https://arxiv.org/abs/1603.09320">Efficient and robust
 * approximate nearest neighbor search using Hierarchical Navigable Small World graphs [2018]</a>
 * paper for details.
 *
 * <p>The nomenclature is a bit different here from what's used in the paper:
 *
 * <h2>Hyperparameters</h2>
 *
 * <ul>
 *   <li><code>beamWidth</code> in {@link HnswGraphBuilder} has the same meaning as <code>efConst
 *       </code> in the paper. It is the number of nearest neighbor candidates to track while
 *       searching the graph for each newly inserted node.
 *   <li><code>maxConn</code> has the same meaning as <code>M</code> in the paper; it controls how
 *       many of the <code>efConst</code> neighbors are connected to the new node
 * </ul>
 *
 * <p>Note: The graph may be searched by multiple threads concurrently, but updates are not
 * thread-safe. The search method optionally takes a set of "accepted nodes", which can be used to
 * exclude deleted documents.
 */
public final class HnswGraph extends KnnGraphValues {

  private final int maxConn;
  private int numLevels; // the current number of levels in the graph
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
    this.numLevels = levelOfFirstNode + 1;
    this.graph = new ArrayList<>(numLevels);
    this.entryNode = 0;
    for (int i = 0; i < numLevels; i++) {
      graph.add(new ArrayList<>());
      // Typically with diversity criteria we see nodes not fully occupied;
      // average fanout seems to be about 1/2 maxConn.
      // There is some indexing time penalty for under-allocating, but saves RAM
      graph.get(i).add(new NeighborArray(Math.max(32, maxConn / 4)));
    }

    this.nodesByLevel = new ArrayList<>(numLevels);
    nodesByLevel.add(null); // we don't need this for 0th level, as it contains all nodes
    for (int l = 1; l < numLevels; l++) {
      nodesByLevel.add(new int[] {0});
    }
  }

  /**
   * Searches HNSW graph for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param vectors vector values
   * @param graphValues the graph values. May represent the entire graph, or a level in a
   *     hierarchical graph.
   * @param acceptOrds {@link Bits} that represents the allowed document ordinals to match, or
   *     {@code null} if they are all allowed to match.
   * @return a priority queue holding the closest neighbors found
   */
  public static NeighborQueue search(
      float[] query,
      int topK,
      RandomAccessVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      KnnGraphValues graphValues,
      Bits acceptOrds)
      throws IOException {

    NeighborQueue results;
    int[] eps = new int[] {graphValues.entryNode()};
    for (int level = graphValues.numLevels() - 1; level >= 1; level--) {
      results = searchLevel(query, 1, level, eps, vectors, similarityFunction, graphValues, null);
      eps[0] = results.pop();
    }
    results =
        searchLevel(query, topK, 0, eps, vectors, similarityFunction, graphValues, acceptOrds);
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
    // MIN heap, holding the top results
    NeighborQueue results = new NeighborQueue(topK, similarityFunction.reversed);
    // MAX heap, from which to pull the candidate nodes
    NeighborQueue candidates = new NeighborQueue(topK, !similarityFunction.reversed);
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    SparseFixedBitSet visited = new SparseFixedBitSet(size);
    for (int ep : eps) {
      if (visited.getAndSet(ep) == false) {
        float score = similarityFunction.compare(query, vectors.vectorValue(ep));
        candidates.add(ep, score);
        if (acceptOrds == null || acceptOrds.get(ep)) {
          results.add(ep, score);
        }
      }
    }

    // A bound that holds the minimum similarity to the query vector that a candidate vector must
    // have to be considered.
    BoundsChecker bound = BoundsChecker.create(similarityFunction.reversed);
    if (results.size() >= topK) {
      bound.set(results.topScore());
    }
    while (candidates.size() > 0) {
      // get the best candidate (closest or best scoring)
      float topCandidateScore = candidates.topScore();
      if (bound.check(topCandidateScore)) {
        break;
      }
      int topCandidateNode = candidates.pop();
      graphValues.seek(level, topCandidateNode);
      int friendOrd;
      while ((friendOrd = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.getAndSet(friendOrd)) {
          continue;
        }

        float score = similarityFunction.compare(query, vectors.vectorValue(friendOrd));
        if (bound.check(score) == false) {
          candidates.add(friendOrd, score);
          if (acceptOrds == null || acceptOrds.get(friendOrd)) {
            if (results.insertWithOverflow(friendOrd, score) && results.size() >= topK) {
              bound.set(results.topScore());
            }
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
      if (level >= numLevels) {
        for (int i = numLevels; i <= level; i++) {
          graph.add(new ArrayList<>());
          nodesByLevel.add(new int[] {node});
        }
        numLevels = level + 1;
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
  public int numLevels() {
    return numLevels;
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
  public NodesIterator getNodesOnLevel(int level) {
    if (level == 0) {
      return new NodesIterator(size());
    } else {
      return new NodesIterator(nodesByLevel.get(level), graph.get(level).size());
    }
  }
}
