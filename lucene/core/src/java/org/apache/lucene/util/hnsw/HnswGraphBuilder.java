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

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
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
public class HnswGraphBuilder implements HnswBuilder {

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

  private final SplittableRandom random;
  private final RandomVectorScorerSupplier scorerSupplier;
  private final HnswGraphSearcher graphSearcher;
  private final GraphBuilderKnnCollector entryCandidates; // for upper levels of graph search
  private final GraphBuilderKnnCollector
      beamCandidates; // for levels of graph where we add the node

  protected final OnHeapHnswGraph hnsw;
  protected final HnswLock hnswLock;

  private InfoStream infoStream = InfoStream.getDefault();
  private boolean frozen;

  public static HnswGraphBuilder create(
      RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth, long seed)
      throws IOException {
    return new HnswGraphBuilder(scorerSupplier, M, beamWidth, seed, -1);
  }

  public static HnswGraphBuilder create(
      RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth, long seed, int graphSize)
      throws IOException {
    return new HnswGraphBuilder(scorerSupplier, M, beamWidth, seed, graphSize);
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
   * @param graphSize size of graph, if unknown, pass in -1
   */
  protected HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth, long seed, int graphSize)
      throws IOException {
    this(scorerSupplier, M, beamWidth, seed, new OnHeapHnswGraph(M, graphSize));
  }

  protected HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      long seed,
      OnHeapHnswGraph hnsw)
      throws IOException {
    this(
        scorerSupplier,
        M,
        beamWidth,
        seed,
        hnsw,
        null,
        new HnswGraphSearcher(new NeighborQueue(beamWidth, true), new FixedBitSet(hnsw.size())));
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
   * @param hnsw the graph to build, can be previously initialized
   */
  protected HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      long seed,
      OnHeapHnswGraph hnsw,
      HnswLock hnswLock,
      HnswGraphSearcher graphSearcher)
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
    this.hnsw = hnsw;
    this.hnswLock = hnswLock;
    this.graphSearcher = graphSearcher;
    entryCandidates = new GraphBuilderKnnCollector(1);
    beamCandidates = new GraphBuilderKnnCollector(beamWidth);
  }

  @Override
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("This HnswGraphBuilder is frozen and cannot be updated");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + maxOrd + " vectors");
    }
    addVectors(maxOrd);
    return getCompletedGraph();
  }

  @Override
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  @Override
  public OnHeapHnswGraph getCompletedGraph() {
    frozen = true;
    return getGraph();
  }

  @Override
  public OnHeapHnswGraph getGraph() {
    return hnsw;
  }

  /** add vectors in range [minOrd, maxOrd) */
  protected void addVectors(int minOrd, int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("This HnswGraphBuilder is frozen and cannot be updated");
    }
    long start = System.nanoTime(), t = start;
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "addVectors [" + minOrd + " " + maxOrd + ")");
    }
    for (int node = minOrd; node < maxOrd; node++) {
      addGraphNode(node);
      if ((node % 10000 == 0) && infoStream.isEnabled(HNSW_COMPONENT)) {
        t = printGraphBuildStatus(node, start, t);
      }
    }
  }

  private void addVectors(int maxOrd) throws IOException {
    addVectors(0, maxOrd);
  }

  @Override
  public void addGraphNode(int node) throws IOException {
    /*
    Note: this implementation is thread safe when graph size is fixed (e.g. when merging)
    The process of adding a node is roughly:
    1. Add the node to all level from top to the bottom, but do not connect it to any other node,
       nor try to promote itself to an entry node before the connection is done. (Unless the graph is empty
       and this is the first node, in that case we set the entry node and return)
    2. Do the search from top to bottom, remember all the possible neighbours on each level the node
       is on.
    3. Add the neighbor to the node from bottom to top level, when adding the neighbour,
       we always add all the outgoing links first before adding incoming link such that
       when a search visits this node, it can always find a way out
    4. If the node has level that is less or equal to graph level, then we're done here.
       If the node has level larger than graph level, then we need to promote the node
       as the entry node. If, while we add the node to the graph, the entry node has changed
       (which means the graph level has changed as well), we need to reinsert the node
       to the newly introduced levels (repeating step 2,3 for new levels) and again try to
       promote the node to entry node.
    */
    if (frozen) {
      throw new IllegalStateException("Graph builder is already frozen");
    }
    RandomVectorScorer scorer = scorerSupplier.scorer(node);
    final int nodeLevel = getRandomGraphLevel(ml, random);
    // first add nodes to all levels
    for (int level = nodeLevel; level >= 0; level--) {
      hnsw.addNode(level, node);
    }
    // then promote itself as entry node if entry node is not set
    if (hnsw.trySetNewEntryNode(node, nodeLevel)) {
      return;
    }
    // if the entry node is already set, then we have to do all connections first before we can
    // promote ourselves as entry node

    int lowestUnsetLevel = 0;
    int curMaxLevel;
    do {
      curMaxLevel = hnsw.numLevels() - 1;
      // NOTE: the entry node and max level may not be paired, but because we get the level first
      // we ensure that the entry node we get later will always exist on the curMaxLevel
      int[] eps = new int[] {hnsw.entryNode()};

      // we first do the search from top to bottom
      // for levels > nodeLevel search with topk = 1
      GraphBuilderKnnCollector candidates = entryCandidates;
      for (int level = curMaxLevel; level > nodeLevel; level--) {
        candidates.clear();
        graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
        eps[0] = candidates.popNode();
      }

      // for levels <= nodeLevel search with topk = beamWidth, and add connections
      candidates = beamCandidates;
      NeighborArray[] scratchPerLevel =
          new NeighborArray[Math.min(nodeLevel, curMaxLevel) - lowestUnsetLevel + 1];
      for (int i = scratchPerLevel.length - 1; i >= 0; i--) {
        int level = i + lowestUnsetLevel;
        candidates.clear();
        graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
        eps = candidates.popUntilNearestKNodes();
        scratchPerLevel[i] = new NeighborArray(Math.max(beamCandidates.k(), M + 1), false);
        popToScratch(candidates, scratchPerLevel[i]);
      }

      // then do connections from bottom up
      for (int i = 0; i < scratchPerLevel.length; i++) {
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i]); // baseline : similar to false, false, false below.
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], false, false, false, false); // baseline_equivalent
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], false, false, false, true); // baseline with remove other half
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], true, false, false, false); // exp-1 extendCandidates = true
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], false, true, false, false); // exp-2 with keep-pruned =true
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], false, true, true, false); // exp-3 with keep pruned till half max-conn
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], false, true, true, true); // exp-3 with keep pruned till half max-conn and remove other half
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], true, true, false, false);
//        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], true, true, true, false);

        // new heuristic
//        addDiverseNeighborsNewHeuristic(i + lowestUnsetLevel, node, scratchPerLevel[i], false, false); // new heuristic without remove other half
//        addDiverseNeighborsNewHeuristic(i + lowestUnsetLevel, node, scratchPerLevel[i], true, false); // new heuristic with remove other half
        addDiverseNeighborsNewHeuristic(i + lowestUnsetLevel, node, scratchPerLevel[i], true, true); // new heuristic with honour-max-conn true
      }
      lowestUnsetLevel += scratchPerLevel.length;
      assert lowestUnsetLevel == Math.min(nodeLevel, curMaxLevel) + 1;
      if (lowestUnsetLevel > nodeLevel) {
        return;
      }
      assert lowestUnsetLevel == curMaxLevel + 1 && nodeLevel > curMaxLevel;
      if (hnsw.tryPromoteNewEntryNode(node, nodeLevel, curMaxLevel)) {
        return;
      }
      if (hnsw.numLevels() == curMaxLevel + 1) {
        // This should never happen if all the calculations are correct
        throw new IllegalStateException(
            "We're not able to promote node "
                + node
                + " at level "
                + nodeLevel
                + " as entry node. But the max graph level "
                + curMaxLevel
                + " has not changed while we are inserting the node.");
      }
    } while (true);
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

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(NeighborArray neighbors, int level, RandomVectorScorer scorer, boolean removeRandom) throws IOException {
    int[] uncheckedIndexes = neighbors.sort(scorer); // what does 'unchecked node' means? What check? What
    int maxCommonConnectionCount = 0;
    int maxcccIndex = -1;
    int maxConnectionIndex = -1;
    int maxConnectionCount = 2; // has atleast 2 connections
    for (int i = neighbors.size() - 1; i >= 0 ; i--) {
      int currNode = neighbors.nodes()[i];
      float currNodeScore = neighbors.scores()[i];
      NeighborArray currNodeNeighbours = hnsw.getNeighbors(level, currNode);
      NeighborArray commonNeighbours = findCommon(neighbors, currNodeNeighbours);

      if (commonNeighbours.size() > maxCommonConnectionCount) {
        maxCommonConnectionCount = commonNeighbours.size();
        maxcccIndex = i;
      }
      if (currNodeNeighbours.size() >= maxConnectionCount) {
        maxConnectionCount = currNodeNeighbours.size();
        maxConnectionIndex = i;
      }

      for (int j = 0; j < commonNeighbours.size(); j++) {
        if (commonNeighbours.scores()[j] > currNodeScore) {
          return i; // currNode is non-diverse as it's score with another neighbour is higher
        }
      }
    }
    if (maxcccIndex != -1) {
      return maxcccIndex;
    } else if (maxConnectionIndex != -1) {
      return maxConnectionIndex;
    } else {
      if (removeRandom == false) {
        return -1;
      } else {
        return random.nextInt(neighbors.size());
      }
    }
  }

  public int addAndEnsureConnectedDiversity(int newNode, float newScore, int nodeId, NeighborArray neighbours,
                                                      int level, int maxConnOnLevel, boolean removeRandom)
          throws IOException {
    neighbours.addOutOfOrder(newNode, newScore);
    if (neighbours.size() <= maxConnOnLevel) {
      return -1; // none removed
    }
    RandomVectorScorer scorer = scorerSupplier.scorer(nodeId);
    int indexToRemove = findWorstNonDiverse(neighbours, level, scorer, removeRandom);
    if (indexToRemove != -1) {
      int nodeRemoved = neighbours.nodes()[indexToRemove];
      neighbours.removeIndex(indexToRemove);
      return nodeRemoved;
    } else {
      if (removeRandom == true) {
        throw new IllegalStateException("If remove random is set we should not have got -1 for remove index.");
      } else {
        return -1; // no diverse found hence not removed
      }
    }
  }

  private NeighborArray findCommon(NeighborArray neighbors, NeighborArray currNodeNeighbours) {
    NeighborArray common = new NeighborArray(Math.max(currNodeNeighbours.size(), neighbors.size()), true);
    for (int i = 0; i < neighbors.size(); i++) {
      for (int j = 0; j < currNodeNeighbours.size(); j++) {
        if (neighbors.nodes()[i] == currNodeNeighbours.nodes()[j]) {
          common.addOutOfOrder(currNodeNeighbours.nodes()[j], currNodeNeighbours.scores()[j]);
        }
      }
    }

    return common;
  }


  private void addDiverseNeighborsNewHeuristic(int level, int incomingNode, NeighborArray candidates, boolean removeOtherHalf,
                                               boolean honourMaxConn)
          throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, incomingNode);
    assert neighbors.size() == 0; // new node

    int maxConnOnLevel = level == 0 ? M * 2 : M;

    boolean[] mask = selectAll(neighbors, candidates, maxConnOnLevel);
    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    int size = candidates.size();
    for (int i = 0; i < size; i++) {
      if (mask[i] == false) {
        continue;
      }

      int nbr = candidates.nodes()[i];
      int nodeRemoved = -1;
      NeighborArray nbrsOfNbr = hnsw.getNeighbors(level, nbr);
      nbrsOfNbr.rwlock.writeLock().lock();
      try {
         nodeRemoved = addAndEnsureConnectedDiversity(incomingNode, candidates.scores()[i], nbr, nbrsOfNbr, level, maxConnOnLevel, honourMaxConn);
      } finally {
        nbrsOfNbr.rwlock.writeLock().unlock();
      }

      if (removeOtherHalf && nodeRemoved != -1) {
        // it is fine to remove it even from current node as we are iterating over candidates and not over neighbours
        // Also, there is no chance of deadlock as we are not holding more than one lock at a time.
        removeOtherHalf(nodeRemoved, nbr, level);
      }
    }

  }

  private boolean[] selectAll( NeighborArray neighbors, NeighborArray candidates, int maxConnOnLevel) {
    boolean[] selected = new boolean[candidates.size()];

    for (int i = candidates.size() - 1; neighbors.size() < maxConnOnLevel && i >= 0; i--) {
      int cNode = candidates.nodes()[i];
      float cScore = candidates.scores()[i];
      selected[i] = true;
      neighbors.addInOrder(cNode, cScore);
    }

    return selected;
  }

  private void removeOtherHalf(int fromNode, int nbr, int level) {
    NeighborArray neighbors = hnsw.getNeighbors(level, fromNode);
    neighbors.rwlock.writeLock().lock();
    neighbors.removeNode(nbr);
    neighbors.rwlock.writeLock().unlock();
  }


  private void addDiverseNeighbors(int level, int node, NeighborArray candidates,
                                        boolean extendCandidates, boolean keepPrunedConnections,
                                   boolean keepHalfPrunedConnection, boolean removeOtherHalf) throws IOException {
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    assert neighbors.size() == 0; // new node

    int maxConnOnLevel = level == 0 ? M * 2 : M;
    NeighborArray originalCandidates = candidates;
    RandomVectorScorer scorer = scorerSupplier.scorer(node);

    if (extendCandidates) {
      HashSet<Integer> addedNodes = new HashSet<>();

      for (int i = 0; i < originalCandidates.size(); i++) {
        int cand = originalCandidates.nodes()[i];
        addedNodes.add(cand);
        addAllNeighbours(cand, level, addedNodes);
      }

      candidates = new NeighborArray(addedNodes.size(), originalCandidates.isScoresDescOrder());
      for (int cand : addedNodes) {
        candidates.addOutOfOrder(cand, Float.NaN);
      }

      candidates.sort(scorer);
    }


    boolean[] selected = selectAndLinkDiverse(neighbors, candidates, maxConnOnLevel);

    if (keepPrunedConnections) {
      int maskedIndex = selected.length - 1; // start from end as the highest scoring candidates are at the end
      int maxConn = maxConnOnLevel;
      if (keepHalfPrunedConnection) {
        maxConn = maxConnOnLevel/2;
      }
      while (neighbors.size() < maxConn && maskedIndex >= 0) {
        if (selected[maskedIndex] == false) {
          neighbors.addOutOfOrder(candidates.nodes()[maskedIndex], candidates.scores()[maskedIndex]);
          selected[maskedIndex] = true;
        }
        maskedIndex--;
      }
      neighbors.sort(scorer);
    }

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    // NOTE: here we're using candidates and mask but not the neighbour array because once we have
    // added incoming link there will be possibilities of this node being discovered and neighbour
    // array being modified. So using local candidates and mask is a safer option.
    int nodeRemoved = -1;
    for (int i = 0; i < candidates.size(); i++) {
      if (selected[i] == false) {
        continue;
      }
      int nbr = candidates.nodes()[i];
      NeighborArray nbrsOfNbr = hnsw.getNeighbors(level, nbr);
      nbrsOfNbr.rwlock.writeLock().lock();
      try {
         nodeRemoved = nbrsOfNbr.addAndEnsureDiversity(node, candidates.scores()[i], nbr, scorerSupplier);
      } finally {
        nbrsOfNbr.rwlock.writeLock().unlock();
      }

      if (removeOtherHalf && nodeRemoved != -1) {
        removeOtherHalf(nodeRemoved, nbr, level);
      }
    }
  }

  /**
   * Adds all neighbours of `node` into the `addedNodes` set
   *
   * @param node
   * @param addedNodes
   */
  private void addAllNeighbours(int node, int level, HashSet<Integer> addedNodes) {
    NeighborArray neighbors = hnsw.getNeighbors(level, node);

    // TODO: I think we need to guard with lock here as the `neighbors` array might get modified while we are iterating.
    for (int i = 0; i < neighbors.size(); i++) {
      addedNodes.add(neighbors.nodes()[i]);
    }
  }

  private void addDiverseNeighbors(int level, int node, NeighborArray candidates)
      throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    assert neighbors.size() == 0; // new node
    int maxConnOnLevel = level == 0 ? M * 2 : M;
    boolean[] mask = selectAndLinkDiverse(neighbors, candidates, maxConnOnLevel);

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    // NOTE: here we're using candidates and mask but not the neighbour array because once we have
    // added incoming link there will be possibilities of this node being discovered and neighbour
    // array being modified. So using local candidates and mask is a safer option.
    for (int i = 0; i < candidates.size(); i++) {
      if (mask[i] == false) {
        continue;
      }
      int nbr = candidates.nodes()[i];
      if (hnswLock != null) {
        try (HnswLock.LockedRow rowLock = hnswLock.write(level, nbr)) {
          NeighborArray nbrsOfNbr = rowLock.row;
          nbrsOfNbr.addAndEnsureDiversity(node, candidates.scores()[i], nbr, scorerSupplier);
        }
      } else {
        NeighborArray nbrsOfNbr = hnsw.getNeighbors(level, nbr);
        nbrsOfNbr.addAndEnsureDiversity(node, candidates.scores()[i], nbr, scorerSupplier);
      }
    }
  }

  /**
   * This method will select neighbors to add and return a mask telling the caller which candidates
   * are selected
   */
  private boolean[] selectAndLinkDiverse(
      NeighborArray neighbors, NeighborArray candidates, int maxConnOnLevel) throws IOException {
    boolean[] selected = new boolean[candidates.size()];
    return selectAndLinkDiverse(neighbors, candidates, maxConnOnLevel, selected);
  }
  private boolean[] selectAndLinkDiverse(
      NeighborArray neighbors, NeighborArray candidates, int maxConnOnLevel, boolean[] selected) throws IOException {
    // Select the best maxConnOnLevel neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConnOnLevel && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.nodes()[i];
      float cScore = candidates.scores()[i];
      assert cNode <= hnsw.maxNodeId();
      if (diversityCheck(cNode, cScore, neighbors)) {
        selected[i] = true;
        // here we don't need to lock, because there's no incoming link so no others is able to
        // discover this node such that no others will modify this neighbor array as well
        neighbors.addInOrder(cNode, cScore);
      }
    }
    return selected;
  }

  private static void popToScratch(GraphBuilderKnnCollector candidates, NeighborArray scratch) {
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
      float neighborSimilarity = scorer.score(neighbors.nodes()[i]);
      if (neighborSimilarity >= score) {
        return false;
      }
    }
    return true;
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
