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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.IORunnable;

/**
 * This creates a graph builder that is initialized with the provided HnswGraph. This is useful for
 * merging HnswGraphs from multiple segments.
 *
 * <p>The builder performs the following operations:
 *
 * <ul>
 *   <li>Copies the graph structure from the initializer graph with ordinal remapping
 *   <li>Identifies and repairs disconnected nodes (nodes that lost a portion of their neighbors due
 *       to deletions)
 *   <li>Rebalances the graph hierarchy to maintain proper level distribution according to the HNSW
 *       probabilistic model
 *   <li>Allows incremental addition of new nodes while preserving initialized nodes
 * </ul>
 *
 * <p><b>Disconnected Node Detection:</b> A node that lost neighbors this merge is flagged for
 * repair if it either kept less than {@link #DISCONNECTED_NODE_FACTOR} of its neighbors from the
 * source graph, or fell below {@link #CUMULATIVE_DEGREE_FLOOR_FACTOR} of the level's connection
 * budget. The first catches a large loss in a single merge; the second catches slow decay across
 * many merges.
 *
 * @lucene.experimental
 */
public final class InitializedHnswGraphBuilder extends HnswGraphBuilder {

  /**
   * Tracks which nodes have already been initialized from the source graph. These nodes will be
   * skipped during subsequent {@link #addGraphNode(int)} calls to avoid duplicate processing.
   */
  private final BitSet initializedNodes;

  /**
   * Maps each level in the graph hierarchy to the list of node ordinals present at that level. Used
   * during graph rebalancing to identify candidates for promotion to higher levels.
   */
  private IntArrayList[] levelToNodes;

  /**
   * The threshold factor for determining if a node is disconnected. A node is considered
   * disconnected if its new neighbor count is less than {@code (old neighbor count *
   * DISCONNECTED_NODE_FACTOR)}.
   *
   * <p>This helps identify nodes that have lost a significant portion of their neighbors (typically
   * due to document deletions) and need additional connections to maintain graph connectivity and
   * search performance.
   */
  private final double DISCONNECTED_NODE_FACTOR = 0.85;

  /**
   * The cumulative disconnection threshold. A node is disconnected when its out-degree falls below
   * this fraction of the level's connection budget ({@code M} on upper levels, {@code 2*M} at level
   * 0). Because the budget is fixed, the check holds a node to the same target on every merge, so
   * it catches out-degree that drifts down gradually across many merges rather than in a single
   * one.
   */
  private final double CUMULATIVE_DEGREE_FLOOR_FACTOR = 0.5;

  // Tracks if the graph has deletes
  private boolean hasDeletes = false;

  /**
   * Creates an initialized HNSW graph builder from an existing graph.
   *
   * <p>This factory method constructs a new graph builder, initializes it with the structure from
   * the provided graph (applying ordinal remapping), and returns the builder ready for additional
   * operations.
   *
   * @param scorerSupplier provides vector similarity scoring for graph operations
   * @param beamWidth the search beam width for finding neighbors during graph construction
   * @param seed random seed for level assignment and node promotion during rebalancing
   * @param initializerGraph the source graph to copy structure from
   * @param newOrdMap maps old ordinals in the initializer graph to new ordinals in the merged
   *     graph; -1 indicates a deleted document that should be skipped
   * @param initializedNodes bit set marking which nodes are already initialized (can be null if not
   *     tracking)
   * @param totalNumberOfVectors the total number of vectors in the merged graph (used for
   *     pre-allocation)
   * @param abortCheck optional check polled during graph construction, i.e. while copying,
   *     repairing and rebalancing the initializer graph and before each node insertion; may throw
   *     {@link org.apache.lucene.index.MergePolicy.MergeAbortedException} to abort the build when
   *     the surrounding merge has been aborted, or null
   * @return a new builder initialized with the provided graph structure
   * @throws IOException if an I/O error occurs during graph initialization
   */
  public static InitializedHnswGraphBuilder fromGraph(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      int[] newOrdMap,
      BitSet initializedNodes,
      int totalNumberOfVectors,
      IORunnable abortCheck)
      throws IOException {

    InitializedHnswGraphBuilder builder =
        new InitializedHnswGraphBuilder(
            scorerSupplier,
            beamWidth,
            seed,
            new OnHeapHnswGraph(initializerGraph.maxConn(), totalNumberOfVectors),
            initializedNodes);

    if (abortCheck != null) {
      builder.setAbortCheck(abortCheck);
    }
    builder.initializeFromGraph(initializerGraph, newOrdMap);
    return builder;
  }

  /**
   * Same as {@link #fromGraph(RandomVectorScorerSupplier, int, long, HnswGraph, int[], BitSet, int,
   * IORunnable)} but without an abort check.
   */
  public static InitializedHnswGraphBuilder fromGraph(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      int[] newOrdMap,
      BitSet initializedNodes,
      int totalNumberOfVectors)
      throws IOException {
    return fromGraph(
        scorerSupplier,
        beamWidth,
        seed,
        initializerGraph,
        newOrdMap,
        initializedNodes,
        totalNumberOfVectors,
        null);
  }

  /**
   * Convenience method to create a fully initialized on-heap HNSW graph without tracking
   * initialized nodes. This is useful when you just need the resulting graph structure without
   * planning to add additional nodes incrementally.
   *
   * @param initializerGraph the source graph to copy structure from
   * @param newOrdMap maps old ordinals to new ordinals; -1 indicates deleted documents
   * @param totalNumberOfVectors the total number of vectors in the merged graph
   * @param beamWidth the search beam width for graph construction
   * @param scorerSupplier provides vector similarity scoring
   * @param abortCheck optional check polled during graph construction, i.e. while copying,
   *     repairing and rebalancing the initializer graph and before each node insertion; may throw
   *     {@link org.apache.lucene.index.MergePolicy.MergeAbortedException} to abort the build when
   *     the surrounding merge has been aborted, or null
   * @return a fully initialized on-heap HNSW graph
   * @throws IOException if an I/O error occurs during graph initialization
   */
  public static OnHeapHnswGraph initGraph(
      HnswGraph initializerGraph,
      int[] newOrdMap,
      int totalNumberOfVectors,
      int beamWidth,
      RandomVectorScorerSupplier scorerSupplier,
      IORunnable abortCheck)
      throws IOException {

    InitializedHnswGraphBuilder builder =
        fromGraph(
            scorerSupplier,
            beamWidth,
            randSeed,
            initializerGraph,
            newOrdMap,
            null,
            totalNumberOfVectors,
            abortCheck);
    return builder.getGraph();
  }

  /**
   * Same as {@link #initGraph(HnswGraph, int[], int, int, RandomVectorScorerSupplier, IORunnable)}
   * but without an abort check.
   */
  public static OnHeapHnswGraph initGraph(
      HnswGraph initializerGraph,
      int[] newOrdMap,
      int totalNumberOfVectors,
      int beamWidth,
      RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    return initGraph(
        initializerGraph, newOrdMap, totalNumberOfVectors, beamWidth, scorerSupplier, null);
  }

  private InitializedHnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      OnHeapHnswGraph initializedGraph,
      BitSet initializedNodes)
      throws IOException {
    super(scorerSupplier, beamWidth, seed, initializedGraph);
    this.initializedNodes = initializedNodes;
  }

  /**
   * Initializes the graph from the provided initializer graph through a three-phase process:
   *
   * <ol>
   *   <li>Copy the graph structure with ordinal remapping, identifying disconnected nodes
   *   <li>If deletions occurred, repair disconnected nodes by finding additional neighbors
   *   <li>If deletions occurred, rebalance the graph hierarchy to maintain proper level
   *       distribution
   * </ol>
   *
   * @param initializerGraph the source graph to copy from
   * @param newOrdMap ordinal mapping from old to new ordinals
   * @throws IOException if an I/O error occurs during initialization
   */
  private void initializeFromGraph(HnswGraph initializerGraph, int[] newOrdMap) throws IOException {
    hasDeletes = false;
    // Phase 1: Copy structure and identify nodes that lost too many neighbors
    Map<Integer, List<Integer>> disconnectedNodesByLevel =
        copyGraphStructure(initializerGraph, newOrdMap);

    // Repair graph if it has deletes
    if (hasDeletes) {
      // Phase 2: Repair nodes with insufficient connections
      repairDisconnectedNodes(disconnectedNodesByLevel, initializerGraph.numLevels());

      // Phase 3: Rebalance graph to maintain proper level distribution
      rebalanceGraph();
    }
  }

  /**
   * Copies the graph structure from the initializer graph, applying ordinal remapping and
   * identifying nodes that have lost neighbors.
   *
   * <p>Deleted neighbors show up as -1 in newOrdMap and are dropped. A node that loses any this way
   * is flagged for repair when it crosses one of the two thresholds ({@link
   * #DISCONNECTED_NODE_FACTOR} and {@link #CUMULATIVE_DEGREE_FLOOR_FACTOR}); flagged nodes are
   * repaired afterwards.
   *
   * <p><b>Example:</b> at level 0 with {@code M=16} (budget {@code 2*M = 32}), a node that kept 16
   * of 20 neighbors is flagged by the per-merge check ({@code 16/20 = 0.8 <
   * DISCONNECTED_NODE_FACTOR = 0.85}); a node that has drifted to 15 neighbors is flagged by the
   * cumulative check ({@code 15 < 32 * CUMULATIVE_DEGREE_FLOOR_FACTOR = 16}) even if it lost only
   * one this merge.
   *
   * @param initializerGraph the source graph to copy from
   * @param newOrdMap maps old ordinals to new ordinals; -1 indicates deleted documents
   * @return map of level to list of disconnected node ordinals at that level
   * @throws IOException if an I/O error occurs during graph traversal
   */
  private Map<Integer, List<Integer>> copyGraphStructure(
      HnswGraph initializerGraph, int[] newOrdMap) throws IOException {
    int numLevels = initializerGraph.numLevels();
    levelToNodes = new IntArrayList[numLevels];
    Map<Integer, List<Integer>> disconnectedNodesByLevel = new HashMap<>(numLevels);

    for (int level = numLevels - 1; level >= 0; level--) {
      levelToNodes[level] = new IntArrayList();
      List<Integer> disconnectedNodes = new ArrayList<>();
      HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);

      while (it.hasNext()) {
        maybeAbort();
        int oldOrd = it.nextInt();
        int newOrd = newOrdMap[oldOrd];

        // Skip deleted documents (mapped to -1)
        if (newOrd == -1) {
          hasDeletes = true;
          continue;
        }

        hnsw.addNode(level, newOrd);
        levelToNodes[level].add(newOrd);
        hnsw.trySetNewEntryNode(newOrd, level);
        scorer.setScoringOrdinal(newOrd);

        // Copy neighbors
        NeighborArray newNeighbors = hnsw.getNeighbors(level, newOrd);
        initializerGraph.seek(level, oldOrd);
        int oldNeighbourCount = 0;
        for (int oldNeighbor = initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = initializerGraph.nextNeighbor()) {
          oldNeighbourCount++;
          int newNeighbor = newOrdMap[oldNeighbor];

          // Only add neighbors that weren't deleted
          if (newNeighbor != -1) {
            newNeighbors.addOutOfOrder(newNeighbor, Float.NaN);
          }
        }

        // Flag a node that lost neighbors and crossed either disconnection threshold.
        int maxConnOnLevel = newNeighbors.maxSize() - 1; // M on upper levels, 2*M on level 0
        if (newNeighbors.size() < oldNeighbourCount * DISCONNECTED_NODE_FACTOR
            || (newNeighbors.size() < oldNeighbourCount
                && newNeighbors.size() < maxConnOnLevel * CUMULATIVE_DEGREE_FLOOR_FACTOR)) {
          disconnectedNodes.add(newOrd);
        }
      }
      disconnectedNodesByLevel.put(level, disconnectedNodes);
    }
    return disconnectedNodesByLevel;
  }

  /**
   * Repairs disconnected nodes at all levels by finding additional neighbors to restore
   * connectivity.
   *
   * @param disconnectedNodesByLevel map of level to disconnected nodes at that level
   * @param numLevels total number of levels in the graph hierarchy
   * @throws IOException if an I/O error occurs during repair operations
   */
  private void repairDisconnectedNodes(
      Map<Integer, List<Integer>> disconnectedNodesByLevel, int numLevels) throws IOException {
    for (int level = numLevels - 1; level >= 0; level--) {
      fixDisconnectedNodes(disconnectedNodesByLevel.get(level), level, scorer);
    }
  }

  /**
   * Fixes disconnected nodes at a specific level by performing graph searches from their existing
   * neighbors to find additional connections.
   *
   * <p>For each disconnected node:
   *
   * <ol>
   *   <li>Use existing neighbors as entry points for graph search
   *   <li>Search the level to find candidate neighbors
   *   <li>Add diverse neighbors using the HNSW heuristic selection algorithm
   * </ol>
   *
   * <p>If a node has no neighbors at all, it cannot be repaired at this level and will rely on the
   * rebalancing phase.
   *
   * @param disconnectedNodes list of node ordinals that need additional neighbors
   * @param level the level at which to repair connections
   * @param scorer vector similarity scorer for distance calculations
   * @throws IOException if an I/O error occurs during search operations
   */
  private void fixDisconnectedNodes(
      List<Integer> disconnectedNodes, int level, UpdateableRandomVectorScorer scorer)
      throws IOException {
    if (disconnectedNodes.isEmpty()) return;

    int beamWidth = beamCandidates.k();
    GraphBuilderKnnCollector candidates = new GraphBuilderKnnCollector(beamWidth);
    NeighborArray scratchArray = new NeighborArray(beamWidth, false);

    for (int node : disconnectedNodes) {
      maybeAbort();
      scorer.setScoringOrdinal(node);
      NeighborArray existingNeighbors = hnsw.getNeighbors(level, node);

      // Only repair if node has at least one neighbor to use as entry point
      if (existingNeighbors.size() > 0) {
        // Use all existing neighbors as entry points for search
        int[] entryPoints = new int[existingNeighbors.size()];
        System.arraycopy(existingNeighbors.nodes(), 0, entryPoints, 0, existingNeighbors.size());

        // Search from entry points to find candidate neighbors
        graphSearcher.searchLevel(candidates, scorer, level, entryPoints, hnsw, null);
        popToScratch(candidates, scratchArray);

        // Add diverse neighbors using HNSW heuristic (prunes similar neighbors)
        addDiverseNeighbors(level, node, scratchArray, scorer, true);
      } else {
        // Node has no nighbors, add connections from scratch
        addConnections(node, level, scorer);
      }

      // Clear for next iteration
      scratchArray.clear();
      candidates.clear();
    }
  }

  /**
   * Rebalances the graph hierarchy by promoting nodes from lower levels to higher levels to
   * maintain the expected exponential decay in level sizes according to the HNSW probabilistic
   * model.
   *
   * <p>The expected number of nodes at each level follows the formula: <br>
   * {@code maxNodesAtLevel = totalNodes * (1/M)^level}
   *
   * <p>For each level that has fewer nodes than expected, this method randomly promotes nodes from
   * the level below with probability 1/M until the target count is reached.
   *
   * <p>This rebalancing is necessary during merging graph where deletions may have disrupted the
   * proper hierarchical distribution, which could degrade semantic matches quality.
   *
   * @throws IOException if an I/O error occurs during node promotion
   */
  private void rebalanceGraph() throws IOException {
    SplittableRandom random = new SplittableRandom();
    int size = hnsw.size();
    double invMaxConn = 1.0 / M;

    // Process each level starting from level 1 (level 0 always contains all nodes)
    for (int level = 1; ; level++) {

      // Calculate expected number of nodes at this level
      int maxNodesAtLevel = (int) (size * Math.pow(invMaxConn, level));
      if (maxNodesAtLevel <= 0) break; // Stop when expected nodes drops to zero

      int currentNodesAtLevel = 0;

      // Expand levelToNodes array if we need to create new levels
      if (level >= levelToNodes.length) {
        levelToNodes = ArrayUtil.growExact(levelToNodes, level + 1);
        levelToNodes[level] = new IntArrayList();
      } else {
        currentNodesAtLevel = levelToNodes[level].size();
      }

      // Skip if this level already has enough nodes
      if (currentNodesAtLevel >= maxNodesAtLevel) continue;

      // Randomly promote nodes from the level below
      Iterator<IntCursor> it = levelToNodes[level - 1].iterator();

      while (it.hasNext() && currentNodesAtLevel < maxNodesAtLevel) {
        int node = it.next().value;

        // Promote with probability 1/M, matching HNSW's level assignment distribution
        if (random.nextDouble() < invMaxConn && !hnsw.nodeExistAtLevel(level, node)) {
          maybeAbort();
          scorer.setScoringOrdinal(node);
          hnsw.addNode(level, node);

          // If this is the first node at this level, try to make it the entry point
          if (currentNodesAtLevel == 0) {
            hnsw.tryPromoteNewEntryNode(node, level, hnsw.numLevels() - 1);
          } else {
            // Add connections for non-first nodes
            addConnections(node, level, scorer);
          }

          levelToNodes[level].add(node);
          currentNodesAtLevel++;
        }
      }
    }
  }

  /**
   * Adds connections for an existing node at a specific level in the graph hierarchy.
   *
   * <p>The process involves:
   *
   * <ol>
   *   <li>Navigate down from the top level to find the closest node at the target level
   *   <li>Perform a full search at the target level to find neighbors
   *   <li>Add diverse neighbors using the HNSW heuristic selection
   * </ol>
   *
   * @param node the node ordinal to add connections for
   * @param targetLevel the level to add connections at
   * @param scorer vector similarity scorer for distance calculations
   * @throws IOException if an I/O error occurs during search or neighbor addition
   */
  private void addConnections(int node, int targetLevel, UpdateableRandomVectorScorer scorer)
      throws IOException {

    int beamWidth = beamCandidates.k();
    GraphBuilderKnnCollector candidates = new GraphBuilderKnnCollector(beamWidth);
    int[] eps = {hnsw.entryNode()};

    // Navigate down from top to target level, greedily moving toward the new node
    for (int level = hnsw.numLevels() - 1; level > targetLevel; level--) {
      graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
      eps[0] = candidates.popNode();
      candidates.clear();
    }

    // Perform full search at target level to find neighbors
    graphSearcher.searchLevel(candidates, scorer, targetLevel, eps, hnsw, null);

    NeighborArray scratchArray = new NeighborArray(beamWidth, false);
    popToScratch(candidates, scratchArray);

    // Add diverse neighbors and establish bidirectional connections
    addDiverseNeighbors(targetLevel, node, scratchArray, scorer, true);
  }

  @Override
  public void addGraphNode(int node) throws IOException {
    if (initializedNodes.get(node)) {
      return;
    }
    super.addGraphNode(node);
  }
}
