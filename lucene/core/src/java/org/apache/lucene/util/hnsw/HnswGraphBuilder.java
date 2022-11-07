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

import static java.lang.Math.log;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyperparameters.
 *
 * @param <T> the type of vector
 */
public final class HnswGraphBuilder<T> {

  /** Default random seed for level generation * */
  private static final long DEFAULT_RAND_SEED = 42;
  /** A name for the HNSW component for the info-stream * */
  public static final String HNSW_COMPONENT = "HNSW";

  /** Random seed for level generation; public to expose for testing * */
  public static long randSeed = DEFAULT_RAND_SEED;

  private final int M; // max number of connections on upper layers
  private final int beamWidth;
  private final double ml;
  private final NeighborArray scratch;

  private final VectorSimilarityFunction similarityFunction;
  private final VectorEncoding vectorEncoding;
  private final RandomAccessVectorValues vectors;
  private final SplittableRandom random;
  private final HnswGraphSearcher<T> graphSearcher;

  final OnHeapHnswGraph hnsw;

  private InfoStream infoStream = InfoStream.getDefault();

  // we need two sources of vectors in order to perform diversity check comparisons without
  // colliding
  private final RandomAccessVectorValues vectorsCopy;

  private final Set<Integer> initializedNodes;

  public static HnswGraphBuilder<?> create(
      RandomAccessVectorValues vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed)
      throws IOException {
    return new HnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth, seed, null);
  }

  public static HnswGraphBuilder<?> create(
      RandomAccessVectorValues vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      GraphInitializerConfig graphInitializerConfig)
      throws IOException {
    return new HnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth, seed, graphInitializerConfig);
  }

  /**
   * Reads all the vectors from a VectorValues, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param vectors the vectors whose relations are represented by the graph - must provide a
   *     different view over those vectors than the one used to add via addGraphNode.
   * @param M – graph fanout parameter used to calculate the maximum number of connections a node
   *     can have – M on upper layers, and M * 2 on the lowest level.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   * @param graphInitializerConfig configuration for initializing this graph from another graph. If
   *     null, the graph will be built from scratch
   */
  private HnswGraphBuilder(
      RandomAccessVectorValues vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      GraphInitializerConfig graphInitializerConfig)
      throws IOException {
    this.vectors = vectors;
    this.vectorsCopy = vectors.copy();
    this.vectorEncoding = Objects.requireNonNull(vectorEncoding);
    this.similarityFunction = Objects.requireNonNull(similarityFunction);
    if (M <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.M = M;
    this.beamWidth = beamWidth;
    // normalization factor for level generation; currently not configurable
    this.ml = 1 / Math.log(1.0 * M);
    this.random = new SplittableRandom(seed);
    this.hnsw = new OnHeapHnswGraph(M, getLevelOfFirstNode(graphInitializerConfig));
    fastForwardRandom(graphInitializerConfig);
    this.graphSearcher =
        new HnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(beamWidth, true),
            new FixedBitSet(this.vectors.size()));
    // in scratch we store candidates in reverse order: worse candidates are first
    scratch = new NeighborArray(Math.max(beamWidth, M + 1), false);
    this.initializedNodes = new HashSet<>();

    if (graphInitializerConfig != null) {
      initializeFromGraph(graphInitializerConfig);
    }
  }

  private void fastForwardRandom(GraphInitializerConfig graphInitializerConfig) {
    // TODO: Remove in the future or refactor. Used for testing purposes to confirm that graphs
    // produced are the same
    if (graphInitializerConfig == null) {
      return;
    }

    for (int i = 0; i < graphInitializerConfig.initializerGraph.size(); i++) {
      getRandomGraphLevel(ml, random);
    }
  }

  private void initializeFromGraph(GraphInitializerConfig graphInitializerConfig)
      throws IOException {
    int levelOfNode0 = hnsw.numLevels() - 1;
    BytesRef binaryValue = null;
    float[] vectorValue = null;
    boolean isNewZeroFromInitializerGraph = false;
    for (int level = 0; level < graphInitializerConfig.initializerGraph.numLevels(); level++) {
      HnswGraph.NodesIterator it = graphInitializerConfig.initializerGraph.getNodesOnLevel(level);

      while (it.hasNext()) {
        int oldOrd = it.nextInt();
        int newOrd = graphInitializerConfig.oldToNewOrdinalMap.get(oldOrd);

        if (newOrd != 0) {
          this.hnsw.addNode(level, newOrd);
        } else {
          isNewZeroFromInitializerGraph = true;
        }

        if (level == 0) {
          this.initializedNodes.add(newOrd);
        }

        switch (this.vectorEncoding) {
          case FLOAT32 -> vectorValue = vectors.vectorValue(newOrd);
          case BYTE -> binaryValue = vectors.binaryValue(newOrd);
        }
        NeighborArray newNeighbors = this.hnsw.getNeighbors(level, newOrd);
        graphInitializerConfig.initializerGraph.seek(level, oldOrd);
        for (int oldNeighbor = graphInitializerConfig.initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = graphInitializerConfig.initializerGraph.nextNeighbor()) {
          int newNeighbor = graphInitializerConfig.oldToNewOrdinalMap.get(oldNeighbor);

          // TODO: Could we avoid computing scores here and lazily compute them when they are
          // needed?
          float score =
              switch (this.vectorEncoding) {
                case FLOAT32 -> this.similarityFunction.compare(
                    vectorValue, vectors.vectorValue(newNeighbor));
                case BYTE -> this.similarityFunction.compare(
                    binaryValue, vectors.binaryValue(newNeighbor));
              };
          newNeighbors.insertSorted(newNeighbor, score);
        }
      }
    }

    // The zero node is added to the graph implicitly in the constructor. This works because it is
    // guaranteed to be the
    // initial entrypoint, so nodes added after will consider it as a neighbor (and vice versa).
    // However, when
    // initializing the graph from another graph, the entrypoint will either be in the other graph
    // and 0 wont ever be
    // visited or, 0 will be the entrypoint and none of the initialized graph will be visited. To
    // prevent this, we
    // backfill 0's neighbors if 0 is not a part of the initializer graph already.
    if (!isNewZeroFromInitializerGraph) {
      int levelOfEpInitializer = graphInitializerConfig.initializerGraph.numLevels() - 1;
      int[] ep =
          new int[] {
            graphInitializerConfig.oldToNewOrdinalMap.get(
                graphInitializerConfig.initializerGraph.entryNode())
          };

      T value = getValue(0, vectors);
      addNeighbors(0, value, levelOfEpInitializer, levelOfNode0, ep);
    }
  }

  /**
   * Reads all the vectors from two copies of a random access VectorValues. Providing two copies
   * enables efficient retrieval without extra data copying, while avoiding collision of the
   * returned values.
   *
   * @param vectorsToAdd the vectors for which to build a nearest neighbors graph. Must be an
   *     independent accessor for the vectors
   */
  public OnHeapHnswGraph build(RandomAccessVectorValues vectorsToAdd) throws IOException {
    if (vectorsToAdd == this.vectors) {
      throw new IllegalArgumentException(
          "Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + vectorsToAdd.size() + " vectors");
    }
    addVectors(vectorsToAdd);
    return hnsw;
  }

  private void addVectors(RandomAccessVectorValues vectorsToAdd) throws IOException {
    long start = System.nanoTime(), t = start;
    // start at node 1! node 0 is added implicitly, in the constructor
    for (int node = 1; node < vectorsToAdd.size(); node++) {
      if (initializedNodes.contains(node)) {
        continue;
      }

      addGraphNode(node, vectorsToAdd);
      if ((node % 10000 == 0) && infoStream.isEnabled(HNSW_COMPONENT)) {
        t = printGraphBuildStatus(node, start, t);
      }
    }
  }

  /** Set info-stream to output debugging information * */
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  public OnHeapHnswGraph getGraph() {
    return hnsw;
  }

  /** Inserts a doc with vector value to the graph */
  public void addGraphNode(int node, T value) throws IOException {
    final int nodeLevel = getRandomGraphLevel(ml, random);
    int curMaxLevel = hnsw.numLevels() - 1;
    int[] eps = new int[] {hnsw.entryNode()};

    // if a node introduces new levels to the graph, add this new node on new levels
    for (int level = nodeLevel; level >= 0; level--) {
      hnsw.addNode(level, node);
    }

    addNeighbors(node, value, curMaxLevel, nodeLevel, eps);
  }

  public void addGraphNode(int node, RandomAccessVectorValues values) throws IOException {
    addGraphNode(node, getValue(node, values));
  }

  private void addNeighbors(int node, T value, int previousMaxLevel, int nodeLevel, int[] eps)
      throws IOException {
    NeighborQueue candidates;

    // for levels > nodeLevel search with topk = 1
    for (int level = previousMaxLevel; level > nodeLevel; level--) {
      candidates = graphSearcher.searchLevel(value, 1, level, eps, vectors, hnsw);
      eps = new int[] {candidates.pop()};
    }
    // for levels <= nodeLevel search with topk = beamWidth, and add connections
    for (int level = Math.min(nodeLevel, previousMaxLevel); level >= 0; level--) {
      candidates = graphSearcher.searchLevel(value, beamWidth, level, eps, vectors, hnsw);
      eps = candidates.nodes();
      addDiverseNeighbors(level, node, candidates);
    }
  }

  @SuppressWarnings("unchecked")
  private T getValue(int node, RandomAccessVectorValues values) throws IOException {
    return switch (vectorEncoding) {
      case BYTE -> (T) values.binaryValue(node);
      case FLOAT32 -> (T) values.vectorValue(node);
    };
  }

  private long printGraphBuildStatus(int node, long start, long t) {
    long now = System.nanoTime();
    infoStream.message(
        HNSW_COMPONENT,
        String.format(
            Locale.ROOT,
            "built %d in %d/%d ms",
            node,
            ((now - t) / 1_000_000),
            ((now - start) / 1_000_000)));
    return now;
  }

  private void addDiverseNeighbors(int level, int node, NeighborQueue candidates)
      throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    assert neighbors.size() == 0; // new node
    popToScratch(candidates);
    int maxConnOnLevel = level == 0 ? M * 2 : M;
    selectAndLinkDiverse(neighbors, scratch, maxConnOnLevel);

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    int size = neighbors.size();
    for (int i = 0; i < size; i++) {
      int nbr = neighbors.node[i];
      NeighborArray nbrNbr = hnsw.getNeighbors(level, nbr);
      nbrNbr.insertSorted(node, neighbors.score[i]);
      if (nbrNbr.size() > maxConnOnLevel) {
        int indexToRemove = findWorstNonDiverse(nbrNbr);
        nbrNbr.removeIndex(indexToRemove);
      }
    }
  }

  private void selectAndLinkDiverse(
      NeighborArray neighbors, NeighborArray candidates, int maxConnOnLevel) throws IOException {
    // Select the best maxConnOnLevel neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConnOnLevel && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      assert cNode < hnsw.size();
      if (diversityCheck(cNode, cScore, neighbors)) {
        neighbors.add(cNode, cScore);
      }
    }
  }

  private void popToScratch(NeighborQueue candidates) {
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float maxSimilarity = candidates.topScore();
      scratch.add(candidates.pop(), maxSimilarity);
    }
  }

  /**
   * @param candidate the vector of a new candidate neighbor of a node n
   * @param score the score of the new candidate and node n, to be compared with scores of the
   *     candidate and n's neighbors
   * @param neighbors the neighbors selected so far
   * @return whether the candidate is diverse given the existing neighbors
   */
  private boolean diversityCheck(int candidate, float score, NeighborArray neighbors)
      throws IOException {
    return isDiverse(candidate, neighbors, score);
  }

  private boolean isDiverse(int candidate, NeighborArray neighbors, float score)
      throws IOException {
    return switch (vectorEncoding) {
      case BYTE -> isDiverse(vectors.binaryValue(candidate), neighbors, score);
      case FLOAT32 -> isDiverse(vectors.vectorValue(candidate), neighbors, score);
    };
  }

  private boolean isDiverse(float[] candidate, NeighborArray neighbors, float score)
      throws IOException {
    for (int i = 0; i < neighbors.size(); i++) {
      float neighborSimilarity =
          similarityFunction.compare(candidate, vectorsCopy.vectorValue(neighbors.node[i]));
      if (neighborSimilarity >= score) {
        return false;
      }
    }
    return true;
  }

  private boolean isDiverse(BytesRef candidate, NeighborArray neighbors, float score)
      throws IOException {
    for (int i = 0; i < neighbors.size(); i++) {
      float neighborSimilarity =
          similarityFunction.compare(candidate, vectorsCopy.binaryValue(neighbors.node[i]));
      if (neighborSimilarity >= score) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(NeighborArray neighbors) throws IOException {
    for (int i = neighbors.size() - 1; i > 0; i--) {
      if (isWorstNonDiverse(i, neighbors, neighbors.score[i])) {
        return i;
      }
    }
    return neighbors.size() - 1;
  }

  private boolean isWorstNonDiverse(
      int candidate, NeighborArray neighbors, float minAcceptedSimilarity) throws IOException {
    return switch (vectorEncoding) {
      case BYTE -> isWorstNonDiverse(
          candidate, vectors.binaryValue(candidate), neighbors, minAcceptedSimilarity);
      case FLOAT32 -> isWorstNonDiverse(
          candidate, vectors.vectorValue(candidate), neighbors, minAcceptedSimilarity);
    };
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, float[] candidate, NeighborArray neighbors, float minAcceptedSimilarity)
      throws IOException {
    for (int i = candidateIndex - 1; i > -0; i--) {
      float neighborSimilarity =
          similarityFunction.compare(candidate, vectorsCopy.vectorValue(neighbors.node[i]));
      // node i is too similar to node j given its score relative to the base node
      if (neighborSimilarity >= minAcceptedSimilarity) {
        return false;
      }
    }
    return true;
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, BytesRef candidate, NeighborArray neighbors, float minAcceptedSimilarity)
      throws IOException {
    for (int i = candidateIndex - 1; i > -0; i--) {
      float neighborSimilarity =
          similarityFunction.compare(candidate, vectorsCopy.binaryValue(neighbors.node[i]));
      // node i is too similar to node j given its score relative to the base node
      if (neighborSimilarity >= minAcceptedSimilarity) {
        return false;
      }
    }
    return true;
  }

  private int getLevelOfFirstNode(GraphInitializerConfig initializerConfig) throws IOException {

    if (initializerConfig == null) {
      return getRandomGraphLevel(ml, random);
    }

    int newFirstNodeToOldOrd = -1;
    for (Map.Entry<Integer, Integer> ordEntry : initializerConfig.oldToNewOrdinalMap.entrySet()) {
      if (ordEntry.getValue() == 0) {
        newFirstNodeToOldOrd = ordEntry.getKey();
        break;
      }
    }

    if (newFirstNodeToOldOrd == -1) {
      return getRandomGraphLevel(ml, random);
    }

    int levelOfFirstNode = 0;
    while (initializerConfig.initializerGraph.numLevels() > levelOfFirstNode + 1) {
      if (!isNodeInLevel(
          initializerConfig.initializerGraph.getNodesOnLevel(levelOfFirstNode + 1),
          newFirstNodeToOldOrd)) {
        break;
      }
      levelOfFirstNode++;
    }

    return levelOfFirstNode;
  }

  private static boolean isNodeInLevel(HnswGraph.NodesIterator nodesIterator, int ordinal) {
    while (nodesIterator.hasNext()) {
      int next = nodesIterator.nextInt();
      if (next == ordinal) {
        return true;
      }
      if (next > ordinal) {
        return false;
      }
    }
    return false;
  }

  private static int getRandomGraphLevel(double ml, SplittableRandom random) {
    double randDouble;
    do {
      randDouble = random.nextDouble(); // avoid 0 value, as log(0) is undefined
    } while (randDouble == 0.0);
    return ((int) (-log(randDouble) * ml));
  }

  /** Configuration class for GraphInitializer */
  public static class GraphInitializerConfig {
    private final Map<Integer, Integer> oldToNewOrdinalMap;
    private final HnswGraph initializerGraph;

    /**
     * Contains config information for initializing graph from another graph
     *
     * @param initializerGraph graph that will be used to initialize another graph
     * @param oldToNewOrdinalMap Map from ordinals in initializer graph to new graph
     */
    public GraphInitializerConfig(
        HnswGraph initializerGraph, Map<Integer, Integer> oldToNewOrdinalMap) {
      this.initializerGraph = initializerGraph;
      this.oldToNewOrdinalMap = oldToNewOrdinalMap;
    }
  }
}
