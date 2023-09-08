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
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyper-parameters.
 */
public final class HnswGraphBuilder {

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  /** Default random seed for level generation * */
  private static final long DEFAULT_RAND_SEED = 42;

  /** A name for the HNSW component for the info-stream * */
  public static final String HNSW_COMPONENT = "HNSW";

  /** Random seed for level generation; public to expose for testing * */
  public static long randSeed = DEFAULT_RAND_SEED;

  private final int M; // max number of connections on upper layers
  private final double ml;
  private final NeighborArray scratch;

  private final SplittableRandom random;
  private final RandomVectorScorerSupplier scorerSupplier;
  private final HnswGraphSearcher graphSearcher;
  private final GraphBuilderKnnCollector entryCandidates; // for upper levels of graph search
  private final GraphBuilderKnnCollector
      beamCandidates; // for levels of graph where we add the node

  final OnHeapHnswGraph hnsw;

  private InfoStream infoStream = InfoStream.getDefault();

  private final Set<Integer> initializedNodes;

  public static HnswGraphBuilder create(
      RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth, long seed)
      throws IOException {
    return new HnswGraphBuilder(scorerSupplier, M, beamWidth, seed);
  }

  public static HnswGraphBuilder create(
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      Map<Integer, Integer> oldToNewOrdinalMap)
      throws IOException {
    HnswGraphBuilder hnswGraphBuilder = new HnswGraphBuilder(scorerSupplier, M, beamWidth, seed);
    hnswGraphBuilder.initializeFromGraph(initializerGraph, oldToNewOrdinalMap);
    return hnswGraphBuilder;
  }

  /**
   * Reads all the vectors from vector values, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param scorerSupplier a supplier to create vector scorer from ordinals.
   * @param M – graph fanout parameter used to calculate the maximum number of connections a node
   *     can have – M on upper layers, and M * 2 on the lowest level.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   */
  private HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth, long seed)
      throws IOException {
    if (M <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.M = M;
    this.scorerSupplier =
        Objects.requireNonNull(scorerSupplier, "scorer supplier must not be null");
    // normalization factor for level generation; currently not configurable
    this.ml = M == 1 ? 1 : 1 / Math.log(1.0 * M);
    this.random = new SplittableRandom(seed);
    this.hnsw = new OnHeapHnswGraph(M);
    this.graphSearcher =
        new HnswGraphSearcher(
            new NeighborQueue(beamWidth, true), new FixedBitSet(this.getGraph().size()));
    // in scratch we store candidates in reverse order: worse candidates are first
    scratch = new NeighborArray(Math.max(beamWidth, M + 1), false);
    entryCandidates = new GraphBuilderKnnCollector(1);
    beamCandidates = new GraphBuilderKnnCollector(beamWidth);
    this.initializedNodes = new HashSet<>();
  }

  /**
   * Adds all nodes to the graph up to the provided {@code maxOrd}.
   *
   * @param maxOrd The maximum ordinal of the nodes to be added.
   */
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + maxOrd + " vectors");
    }
    addVectors(maxOrd);
    return hnsw;
  }

  /**
   * Initializes the graph of this builder. Transfers the nodes and their neighbors from the
   * initializer graph into the graph being produced by this builder, mapping ordinals from the
   * initializer graph to their new ordinals in this builder's graph. The builder's graph must be
   * empty before calling this method.
   *
   * @param initializerGraph graph used for initialization
   * @param oldToNewOrdinalMap map for converting from ordinals in the initializerGraph to this
   *     builder's graph
   */
  private void initializeFromGraph(
      HnswGraph initializerGraph, Map<Integer, Integer> oldToNewOrdinalMap) throws IOException {
    assert hnsw.size() == 0;
    for (int level = 0; level < initializerGraph.numLevels(); level++) {
      HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);

      while (it.hasNext()) {
        int oldOrd = it.nextInt();
        int newOrd = oldToNewOrdinalMap.get(oldOrd);

        hnsw.addNode(level, newOrd);

        if (level == 0) {
          initializedNodes.add(newOrd);
        }

        NeighborArray newNeighbors = this.hnsw.getNeighbors(level, newOrd);
        initializerGraph.seek(level, oldOrd);
        for (int oldNeighbor = initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = initializerGraph.nextNeighbor()) {
          int newNeighbor = oldToNewOrdinalMap.get(oldNeighbor);
          // we will compute these scores later when we need to pop out the non-diverse nodes
          newNeighbors.addOutOfOrder(newNeighbor, Float.NaN);
        }
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

  private void addVectors(int maxOrd) throws IOException {
    long start = System.nanoTime(), t = start;
    for (int node = 0; node < maxOrd; node++) {
      if (initializedNodes.contains(node)) {
        continue;
      }
      addGraphNode(node);
      if ((node % 10000 == 0) && infoStream.isEnabled(HNSW_COMPONENT)) {
        t = printGraphBuildStatus(node, start, t);
      }
    }
  }

  /** Inserts a doc with vector value to the graph */
  public void addGraphNode(int node) throws IOException {
    RandomVectorScorer scorer = scorerSupplier.scorer(node);
    final int nodeLevel = getRandomGraphLevel(ml, random);
    int curMaxLevel = hnsw.numLevels() - 1;

    // If entrynode is -1, then this should finish without adding neighbors
    if (hnsw.entryNode() == -1) {
      for (int level = nodeLevel; level >= 0; level--) {
        hnsw.addNode(level, node);
      }
      return;
    }
    int[] eps = new int[] {hnsw.entryNode()};

    // if a node introduces new levels to the graph, add this new node on new levels
    for (int level = nodeLevel; level > curMaxLevel; level--) {
      hnsw.addNode(level, node);
    }

    // for levels > nodeLevel search with topk = 1
    GraphBuilderKnnCollector candidates = entryCandidates;
    for (int level = curMaxLevel; level > nodeLevel; level--) {
      candidates.clear();
      graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
      eps = new int[] {candidates.popNode()};
    }
    // for levels <= nodeLevel search with topk = beamWidth, and add connections
    candidates = beamCandidates;
    for (int level = Math.min(nodeLevel, curMaxLevel); level >= 0; level--) {
      candidates.clear();
      graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
      eps = candidates.popUntilNearestKNodes();
      hnsw.addNode(level, node);
      addDiverseNeighbors(level, node, candidates);
    }
  }

  private long printGraphBuildStatus(int node, long start, long t) {
    long now = System.nanoTime();
    infoStream.message(
        HNSW_COMPONENT,
        String.format(
            Locale.ROOT,
            "built %d in %d/%d ms",
            node,
            TimeUnit.NANOSECONDS.toMillis(now - t),
            TimeUnit.NANOSECONDS.toMillis(now - start)));
    return now;
  }

  private void addDiverseNeighbors(int level, int node, GraphBuilderKnnCollector candidates)
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
      NeighborArray nbrsOfNbr = hnsw.getNeighbors(level, nbr);
      nbrsOfNbr.addOutOfOrder(node, neighbors.score[i]);
      if (nbrsOfNbr.size() > maxConnOnLevel) {
        int indexToRemove = findWorstNonDiverse(nbrsOfNbr, nbr);
        nbrsOfNbr.removeIndex(indexToRemove);
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
        neighbors.addInOrder(cNode, cScore);
      }
    }
  }

  private void popToScratch(GraphBuilderKnnCollector candidates) {
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float maxSimilarity = candidates.minimumScore();
      scratch.addInOrder(candidates.popNode(), maxSimilarity);
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
    RandomVectorScorer scorer = scorerSupplier.scorer(candidate);
    for (int i = 0; i < neighbors.size(); i++) {
      float neighborSimilarity = scorer.score(neighbors.node[i]);
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
  private int findWorstNonDiverse(NeighborArray neighbors, int nodeOrd) throws IOException {
    RandomVectorScorer scorer = scorerSupplier.scorer(nodeOrd);
    int[] uncheckedIndexes = neighbors.sort(scorer);
    if (uncheckedIndexes == null) {
      // all nodes are checked, we will directly return the most distant one
      return neighbors.size() - 1;
    }
    int uncheckedCursor = uncheckedIndexes.length - 1;
    for (int i = neighbors.size() - 1; i > 0; i--) {
      if (uncheckedCursor < 0) {
        // no unchecked node left
        break;
      }
      if (isWorstNonDiverse(i, neighbors, uncheckedIndexes, uncheckedCursor)) {
        return i;
      }
      if (i == uncheckedIndexes[uncheckedCursor]) {
        uncheckedCursor--;
      }
    }
    return neighbors.size() - 1;
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, NeighborArray neighbors, int[] uncheckedIndexes, int uncheckedCursor)
      throws IOException {
    float minAcceptedSimilarity = neighbors.score[candidateIndex];
    RandomVectorScorer scorer = scorerSupplier.scorer(neighbors.node[candidateIndex]);
    if (candidateIndex == uncheckedIndexes[uncheckedCursor]) {
      // the candidate itself is unchecked
      for (int i = candidateIndex - 1; i >= 0; i--) {
        float neighborSimilarity = scorer.score(neighbors.node[i]);
        // candidate node is too similar to node i given its score relative to the base node
        if (neighborSimilarity >= minAcceptedSimilarity) {
          return true;
        }
      }
    } else {
      // else we just need to make sure candidate does not violate diversity with the (newly
      // inserted) unchecked nodes
      assert candidateIndex > uncheckedIndexes[uncheckedCursor];
      for (int i = uncheckedCursor; i >= 0; i--) {
        float neighborSimilarity = scorer.score(neighbors.node[uncheckedIndexes[i]]);
        // candidate node is too similar to node i given its score relative to the base node
        if (neighborSimilarity >= minAcceptedSimilarity) {
          return true;
        }
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

  /**
   * A restricted, specialized knnCollector that can be used when building a graph.
   *
   * <p>Does not support TopDocs
   */
  public static final class GraphBuilderKnnCollector implements KnnCollector {
    private final NeighborQueue queue;
    private final int k;
    private long visitedCount;
    /**
     * @param k the number of neighbors to collect
     */
    public GraphBuilderKnnCollector(int k) {
      this.queue = new NeighborQueue(k, false);
      this.k = k;
    }

    public int size() {
      return queue.size();
    }

    public int popNode() {
      return queue.pop();
    }

    public int[] popUntilNearestKNodes() {
      while (size() > k()) {
        queue.pop();
      }
      return queue.nodes();
    }

    float minimumScore() {
      return queue.topScore();
    }

    public void clear() {
      this.queue.clear();
      this.visitedCount = 0;
    }

    @Override
    public boolean earlyTerminated() {
      return false;
    }

    @Override
    public void incVisitedCount(int count) {
      this.visitedCount += count;
    }

    @Override
    public long visitedCount() {
      return visitedCount;
    }

    @Override
    public long visitLimit() {
      return Long.MAX_VALUE;
    }

    @Override
    public int k() {
      return k;
    }

    @Override
    public boolean collect(int docId, float similarity) {
      return queue.insertWithOverflow(docId, similarity);
    }

    @Override
    public float minCompetitiveSimilarity() {
      return queue.size() >= k() ? queue.topScore() : Float.NEGATIVE_INFINITY;
    }

    @Override
    public TopDocs topDocs() {
      throw new IllegalArgumentException();
    }
  }
}
