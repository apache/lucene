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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.hnsw.HnswUtil.Component;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyper-parameters.
 *
 * <p>Thread-safety: This class is NOT thread safe, it cannot be shared across threads, however, it
 * IS safe for multiple HnswGraphBuilder to build the same graph, if the graph's size is known in
 * the beginning (like when doing merge)
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
  @SuppressWarnings("NonFinalStaticField")
  public static long randSeed = DEFAULT_RAND_SEED;

  private static final int MAX_BULK_SCORE_NODES = 8;

  protected final int M; // max number of connections on upper layers
  private final double ml;

  private final int[] bulkScoreNodes; // for bulk scoring
  private final float[] bulkScores; // for bulk scoring
  private final SplittableRandom random;
  protected final UpdateableRandomVectorScorer scorer;
  protected final HnswGraphSearcher graphSearcher;
  private final GraphBuilderKnnCollector entryCandidates; // for upper levels of graph search
  protected final GraphBuilderKnnCollector
      beamCandidates; // for levels of graph where we add the node
  private final GraphBuilderKnnCollector beamCandidates0;

  protected final OnHeapHnswGraph hnsw;
  protected final HnswLock hnswLock;

  protected InfoStream infoStream = InfoStream.getDefault();
  protected boolean frozen;

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
    this(scorerSupplier, beamWidth, seed, new OnHeapHnswGraph(M, graphSize));
  }

  protected HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier, int beamWidth, long seed, OnHeapHnswGraph hnsw)
      throws IOException {
    this(
        scorerSupplier,
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
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   * @param hnsw the graph to build, can be previously initialized
   */
  protected HnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      OnHeapHnswGraph hnsw,
      HnswLock hnswLock,
      HnswGraphSearcher graphSearcher)
      throws IOException {
    if (hnsw.maxConn() <= 0) {
      throw new IllegalArgumentException("M (max connections) must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.M = hnsw.maxConn();
    this.scorer =
        Objects.requireNonNull(scorerSupplier, "scorer supplier must not be null").scorer();
    // normalization factor for level generation; currently not configurable
    this.ml = M == 1 ? 1 : 1 / Math.log(1.0 * M);
    this.random = new SplittableRandom(seed);
    this.hnsw = hnsw;
    this.hnswLock = hnswLock;
    this.graphSearcher = graphSearcher;
    // pick a number that keeps us from scoring TOO much for diversity checking
    // but enough to take advantage of bulk scoring
    this.bulkScoreNodes = new int[MAX_BULK_SCORE_NODES];
    this.bulkScores = new float[MAX_BULK_SCORE_NODES];
    entryCandidates = new GraphBuilderKnnCollector(1);
    beamCandidates = new GraphBuilderKnnCollector(beamWidth);
    beamCandidates0 = new GraphBuilderKnnCollector(Math.min(beamWidth / 2, M * 3));
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
  public OnHeapHnswGraph getCompletedGraph() throws IOException {
    if (!frozen) {
      finish();
    }
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

  /**
   * Note: this implementation is thread safe when the graph size is fixed (e.g. when merging) The
   * process of adding a node is roughly: 1. Add the node to all levels from top to the bottom, but
   * do not connect it to any other node, nor try to promote itself to an entry node before the
   * connection is done. (Unless the graph is empty and this is the first node, in that case we set
   * the entry node and return) 2. Do the search from top to bottom, remember all the possible
   * neighbours on each level the node is on. 3. Add the neighbor to the node from bottom to top
   * level. When adding the neighbour, we always add all the outgoing links first before adding an
   * incoming link such that when a search visits this node, it can always find a way out 4. If the
   * node has a level that is less or equal to the graph's max level, then we're done here. If the
   * node has a level larger than the graph's max level, then we need to promote the node as the
   * entry node. If, while we add the node to the graph, the entry node has changed (which means the
   * graph level has changed as well), we need to reinsert the node to the newly introduced levels
   * (repeating step 2,3 for new levels) and again try to promote the node to entry node.
   *
   * @param eps0 If specified, we will use it as the entry points of search on level 0, is useful
   *     when you have some prior knowledge, e.g. in {@link MergingHnswGraphBuilder}
   */
  private void addGraphNodeInternal(int node, UpdateableRandomVectorScorer scorer, IntHashSet eps0)
      throws IOException {
    if (frozen) {
      throw new IllegalStateException("Graph builder is already frozen");
    }
    final int nodeLevel = getRandomGraphLevel(ml, random);
    // first add nodes to all levels
    for (int level = nodeLevel; level >= 0; level--) {
      hnsw.addNode(level, node);
    }
    // then promote itself as entry node if entry node is not set (this is the first ever node of
    // the graph)
    if (hnsw.trySetNewEntryNode(node, nodeLevel)) {
      return;
    }
    // if the entry node is already set, then we have to do all connections first before we can
    // promote ourselves as entry node

    int lowestUnsetLevel = 0;
    int curMaxLevel;
    do {
      curMaxLevel = hnsw.numLevels() - 1;
      // NOTE: the entry node and max level are not retrieved synchronously, which could lead to a
      // situation where
      //  the entry node's level is different from the graph's max level, but because we get the
      // level first,
      // we ensure that the entry node we get later will always exist on the curMaxLevel
      // e.g., curMaxLevel <= entryNode.level
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
        if (level == 0 && eps0 != null && eps0.size() > 0) {
          eps = eps0.toArray();
          candidates = beamCandidates0;
        }
        graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
        eps = candidates.popUntilNearestKNodes();
        scratchPerLevel[i] = new NeighborArray(Math.max(candidates.k(), M + 1), false);
        popToScratch(candidates, scratchPerLevel[i]);
      }

      // then do connections from bottom up
      for (int i = 0; i < scratchPerLevel.length; i++) {
        addDiverseNeighbors(i + lowestUnsetLevel, node, scratchPerLevel[i], scorer, false);
      }
      lowestUnsetLevel += scratchPerLevel.length;
      assert lowestUnsetLevel == Math.min(nodeLevel, curMaxLevel) + 1;
      if (lowestUnsetLevel == nodeLevel + 1) {
        // we have already set all the levels we need for this node
        return;
      }
      assert lowestUnsetLevel == curMaxLevel + 1 && nodeLevel > curMaxLevel;
      // The node's level is higher than the graph's max level, so we need to
      // try to promote this node as the graph's entry node
      if (hnsw.tryPromoteNewEntryNode(node, nodeLevel, curMaxLevel)) {
        return;
      }
      // If we're not able to promote, it means the graph must have already changed
      // and has a new max level and some other entry node
      if (hnsw.numLevels() == curMaxLevel + 1) {
        // This is an impossible situation, if happens, then something above is
        // not hold
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

  @Override
  public void addGraphNode(int node) throws IOException {
    scorer.setScoringOrdinal(node);
    addGraphNodeInternal(node, scorer, null);
  }

  @Override
  public void addGraphNode(int node, IntHashSet eps0) throws IOException {
    scorer.setScoringOrdinal(node);
    addGraphNodeInternal(node, scorer, eps0);
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

  void addDiverseNeighbors(
      int level,
      int node,
      NeighborArray candidates,
      UpdateableRandomVectorScorer scorer,
      boolean isLinkRepair)
      throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    int maxConnOnLevel = level == 0 ? M * 2 : M;
    boolean[] mask =
        selectAndLinkDiverse(node, neighbors, candidates, maxConnOnLevel, scorer, isLinkRepair);

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
        Lock lock = hnswLock.write(level, nbr);
        try {
          updateNeighbor(
              getGraph().getNeighbors(level, nbr),
              node,
              candidates.getScores(i),
              nbr,
              scorer,
              isLinkRepair);
        } finally {
          lock.unlock();
        }
      } else {
        updateNeighbor(
            hnsw.getNeighbors(level, nbr),
            node,
            candidates.getScores(i),
            nbr,
            scorer,
            isLinkRepair);
      }
    }
  }

  private void updateNeighbor(
      NeighborArray nbrsOfNbr,
      int node,
      float score,
      int nbr,
      UpdateableRandomVectorScorer scorer,
      boolean isLinkRepair)
      throws IOException {
    // Only check for duplicates during link repair to avoid performance overhead during normal
    // construction
    if (isLinkRepair) {
      for (int j = 0; j < nbrsOfNbr.size(); j++) {
        if (nbrsOfNbr.nodes()[j] == node) return;
      }
    }
    nbrsOfNbr.addAndEnsureDiversity(node, score, nbr, scorer);
  }

  /**
   * This method will select neighbors to add and return a mask telling the caller which candidates
   * are selected
   */
  private boolean[] selectAndLinkDiverse(
      int node,
      NeighborArray neighbors,
      NeighborArray candidates,
      int maxConnOnLevel,
      UpdateableRandomVectorScorer scorer,
      boolean isLinkRepair)
      throws IOException {
    boolean[] mask = new boolean[candidates.size()];
    // Select the best maxConnOnLevel neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConnOnLevel && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.nodes()[i];
      if (node == cNode) {
        continue;
      }
      float cScore = candidates.getScores(i);
      assert cNode <= hnsw.maxNodeId();
      scorer.setScoringOrdinal(cNode);
      if (diversityCheck(cScore, neighbors, scorer)) {
        mask[i] = true;
        // here we don't need to lock, because there's no incoming link so no others is able to
        // discover this node such that no others will modify this neighbor array as well
        if (isLinkRepair) {
          neighbors.addOutOfOrder(cNode, cScore);
        } else {
          neighbors.addInOrder(cNode, cScore);
        }
      }
    }
    return mask;
  }

  static void popToScratch(GraphBuilderKnnCollector candidates, NeighborArray scratch) {
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
   * @param score the score of the new candidate and node n, to be compared with scores of the
   *     candidate and n's neighbors
   * @param neighbors the neighbors selected so far
   * @return whether the candidate is diverse given the existing neighbors
   */
  private boolean diversityCheck(float score, NeighborArray neighbors, RandomVectorScorer scorer)
      throws IOException {
    final int bulkScoreChunk = Math.min((neighbors.size() + 1) / 2, bulkScoreNodes.length);
    for (int scored = 0; scored < neighbors.size(); scored += bulkScoreChunk) {
      int chunkSize = Math.min(bulkScoreChunk, neighbors.size() - scored);
      System.arraycopy(neighbors.nodes(), scored, bulkScoreNodes, 0, chunkSize);
      if (scorer.bulkScore(bulkScoreNodes, bulkScores, chunkSize) >= score) {
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

  void finish() throws IOException {
    // System.out.println("finish " + frozen);
    // TODO: Connect components can be exceptionally expensive, disabling
    //  see: https://github.com/apache/lucene/issues/14214
    // connectComponents();
    frozen = true;
  }

  @SuppressWarnings("unused")
  private void connectComponents() throws IOException {
    long start = System.nanoTime();
    for (int level = 0; level < hnsw.numLevels(); level++) {
      if (connectComponents(level) == false) {
        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(HNSW_COMPONENT, "connectComponents failed on level " + level);
        }
      }
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT, "connectComponents " + (System.nanoTime() - start) / 1_000_000 + " ms");
    }
  }

  private boolean connectComponents(int level) throws IOException {
    FixedBitSet notFullyConnected = new FixedBitSet(hnsw.size());
    int maxConn = M;
    if (level == 0) {
      maxConn *= 2;
    }
    List<Component> components = HnswUtil.components(hnsw, level, notFullyConnected, maxConn);
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT, "connect " + components.size() + " components on level=" + level);
    }
    // System.out.println("HnswGraphBuilder. level=" + level + ": " + components);
    boolean result = true;
    if (components.size() > 1) {
      // connect other components to the largest one
      Component c0 = components.stream().max(Comparator.comparingInt(Component::size)).get();
      if (c0.start() == NO_MORE_DOCS) {
        // the component is already fully connected - no room for new connections
        return false;
      }
      // try for more connections? We only do one since otherwise they may become full
      // while linking
      GraphBuilderKnnCollector beam = new GraphBuilderKnnCollector(2);
      int[] eps = new int[1];
      for (Component c : components) {
        if (c != c0) {
          if (c.start() == NO_MORE_DOCS) {
            continue;
          }
          if (infoStream.isEnabled(HNSW_COMPONENT)) {
            infoStream.message(HNSW_COMPONENT, "connect component " + c + " to " + c0);
          }

          beam.clear();
          eps[0] = c0.start();
          scorer.setScoringOrdinal(c.start());
          // find the closest node in the largest component to the lowest-numbered node in this
          // component that has room to make a connection
          graphSearcher.searchLevel(beam, scorer, level, eps, hnsw, notFullyConnected);
          boolean linked = false;
          while (beam.size() > 0) {
            int c0node = beam.popNode();
            if (c0node == c.start() || notFullyConnected.get(c0node) == false) {
              continue;
            }
            float score = beam.minimumScore();
            assert notFullyConnected.get(c0node);
            // link the nodes
            // System.out.println("link " + c0 + "." + c0node + " to " + c + "." + c.start());
            link(level, c0node, c.start(), score, notFullyConnected);
            linked = true;
            if (infoStream.isEnabled(HNSW_COMPONENT)) {
              infoStream.message(HNSW_COMPONENT, "connected ok " + c0node + " -> " + c.start());
            }
          }
          if (!linked) {
            if (infoStream.isEnabled(HNSW_COMPONENT)) {
              infoStream.message(HNSW_COMPONENT, "not connected; no free nodes found");
            }
            result = false;
          }
        }
      }
    }
    return result;
  }

  // Try to link two nodes bidirectionally; the forward connection will always be made.
  // Update notFullyConnected.
  private void link(int level, int n0, int n1, float score, FixedBitSet notFullyConnected) {
    NeighborArray nbr0 = hnsw.getNeighbors(level, n0);
    NeighborArray nbr1 = hnsw.getNeighbors(level, n1);
    // must subtract 1 here since the nodes array is one larger than the configured
    // max neighbors (M / 2M).
    // We should have taken care of this check by searching for not-full nodes
    int maxConn = nbr0.maxSize() - 1;
    assert notFullyConnected.get(n0);
    assert nbr0.size() < maxConn : "node " + n0 + " is full, has " + nbr0.size() + " friends";
    nbr0.addOutOfOrder(n1, score);
    if (nbr0.size() == maxConn) {
      notFullyConnected.clear(n0);
    }
    if (nbr1.size() < maxConn) {
      nbr1.addOutOfOrder(n0, score);
      if (nbr1.size() == maxConn) {
        notFullyConnected.clear(n1);
      }
    }
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

    public float minimumScore() {
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

    @Override
    public KnnSearchStrategy getSearchStrategy() {
      return null;
    }
  }
}
