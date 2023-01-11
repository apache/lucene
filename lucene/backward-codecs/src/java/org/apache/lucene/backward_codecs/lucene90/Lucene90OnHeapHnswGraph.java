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

package org.apache.lucene.backward_codecs.lucene90;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * An {@link HnswGraph} where all nodes and connections are held in memory. This class is used to
 * construct the HNSW graph before it's written to the index.
 */
public final class Lucene90OnHeapHnswGraph extends HnswGraph {

  private final int maxConn;

  // Each entry lists the top maxConn neighbors of a node. The nodes correspond to vectors added to
  // HnswBuilder, and the
  // node values are the ordinals of those vectors.
  private final List<Lucene90NeighborArray> graph;

  // KnnGraphValues iterator members
  private int upto;
  private Lucene90NeighborArray cur;

  Lucene90OnHeapHnswGraph(int maxConn) {
    graph = new ArrayList<>();
    // Typically with diversity criteria we see nodes not fully occupied; average fanout seems to be
    // about 1/2 maxConn. There is some indexing time penalty for under-allocating, but saves RAM
    graph.add(new Lucene90NeighborArray(Math.max(32, maxConn / 4)));
    this.maxConn = maxConn;
  }

  /**
   * Searches for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param numSeed the size of the queue maintained while searching, and controls the number of
   *     random entry points to sample
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
      RandomAccessVectorValues<float[]> vectors,
      VectorSimilarityFunction similarityFunction,
      HnswGraph graphValues,
      Bits acceptOrds,
      int visitedLimit,
      SplittableRandom random)
      throws IOException {
    int size = graphValues.size();

    // MIN heap, holding the top results
    NeighborQueue results = new NeighborQueue(numSeed, false);
    // MAX heap, from which to pull the candidate nodes
    NeighborQueue candidates = new NeighborQueue(numSeed, true);

    int numVisited = 0;
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    SparseFixedBitSet visited = new SparseFixedBitSet(size);
    // get initial candidates at random
    int boundedNumSeed = Math.min(numSeed, 2 * size);
    for (int i = 0; i < boundedNumSeed; i++) {
      int entryPoint = random.nextInt(size);
      if (visited.getAndSet(entryPoint) == false) {
        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }
        // explore the topK starting points of some random numSeed probes
        float score = similarityFunction.compare(query, vectors.vectorValue(entryPoint));
        candidates.add(entryPoint, score);
        if (acceptOrds == null || acceptOrds.get(entryPoint)) {
          results.add(entryPoint, score);
        }
        numVisited++;
      }
    }

    // Set the bound to the worst current result and below reject any newly-generated candidates
    // failing
    // to exceed this bound
    Lucene90BoundsChecker bound = Lucene90BoundsChecker.create(false);
    bound.set(results.topScore());
    while (candidates.size() > 0 && results.incomplete() == false) {
      // get the best candidate (closest or best scoring)
      float topCandidateSimilarity = candidates.topScore();
      if (results.size() >= topK) {
        if (bound.check(topCandidateSimilarity)) {
          break;
        }
      }
      int topCandidateNode = candidates.pop();
      graphValues.seek(0, topCandidateNode);
      int friendOrd;
      while ((friendOrd = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
        assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
        if (visited.getAndSet(friendOrd)) {
          continue;
        }

        if (numVisited >= visitedLimit) {
          results.markIncomplete();
          break;
        }

        float friendSimilarity = similarityFunction.compare(query, vectors.vectorValue(friendOrd));
        if (results.size() < numSeed || bound.check(friendSimilarity) == false) {
          candidates.add(friendOrd, friendSimilarity);
          if (acceptOrds == null || acceptOrds.get(friendOrd)) {
            results.insertWithOverflow(friendOrd, friendSimilarity);
            bound.set(results.topScore());
          }
        }
        numVisited++;
      }
    }
    while (results.size() > topK) {
      results.pop();
    }
    results.setVisitedCount(numVisited);
    return results;
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param node the node whose neighbors are returned
   */
  public Lucene90NeighborArray getNeighbors(int node) {
    return graph.get(node);
  }

  @Override
  public int size() {
    return graph.size();
  }

  int addNode() {
    graph.add(new Lucene90NeighborArray(maxConn + 1));
    return graph.size() - 1;
  }

  @Override
  public void seek(int level, int targetNode) {
    cur = getNeighbors(targetNode);
    upto = -1;
  }

  @Override
  public int nextNeighbor() {
    if (++upto < cur.size()) {
      return cur.node()[upto];
    }
    return NO_MORE_DOCS;
  }

  @Override
  public int numLevels() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int entryNode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) {
    throw new UnsupportedOperationException();
  }
}
