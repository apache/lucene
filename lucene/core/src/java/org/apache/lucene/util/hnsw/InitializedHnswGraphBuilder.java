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

/**
 * This creates a graph builder that is initialized with the provided HnswGraph. This is useful for
 * merging HnswGraphs from multiple segments.
 *
 * @lucene.experimental
 */
public final class InitializedHnswGraphBuilder extends HnswGraphBuilder {

  private final BitSet initializedNodes;

  private IntArrayList[] levelToNodes;

  public static InitializedHnswGraphBuilder fromGraph(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      int[] newOrdMap,
      BitSet initializedNodes,
      int totalNumberOfVectors)
      throws IOException {

    InitializedHnswGraphBuilder builder =
        new InitializedHnswGraphBuilder(
            scorerSupplier,
            beamWidth,
            seed,
            new OnHeapHnswGraph(initializerGraph.maxConn(), totalNumberOfVectors),
            initializedNodes);

    builder.initializeFromGraph(initializerGraph, newOrdMap);
    return builder;
  }

  public static OnHeapHnswGraph initGraph(
      HnswGraph initializerGraph,
      int[] newOrdMap,
      int totalNumberOfVectors,
      int beamWidth,
      RandomVectorScorerSupplier scorerSupplier)
      throws IOException {

    InitializedHnswGraphBuilder builder =
        fromGraph(
            scorerSupplier,
            beamWidth,
            randSeed,
            initializerGraph,
            newOrdMap,
            null,
            totalNumberOfVectors);
    return builder.getGraph();
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

  private void initializeFromGraph(HnswGraph initializerGraph, int[] newOrdMap) throws IOException {
    Map<Integer, List<Integer>> disconnectedNodesByLevel =
        copyGraphStructure(initializerGraph, newOrdMap);
    repairDisconnectedNodes(disconnectedNodesByLevel, initializerGraph.numLevels());

    if (!disconnectedNodesByLevel.get(0).isEmpty()) {
      rebalanceGraph();
    }
  }

  private Map<Integer, List<Integer>> copyGraphStructure(
      HnswGraph initializerGraph, int[] newOrdMap) throws IOException {
    int numLevels = initializerGraph.numLevels();
    levelToNodes = new IntArrayList[numLevels];
    Map<Integer, List<Integer>> disconnectedNodesByLevel = new HashMap<>(numLevels);

    for (int level = numLevels - 1; level >= 0; level--) {
      levelToNodes[level] = new IntArrayList();
      List<Integer> disconnectedNodes = new ArrayList<>();
      HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);
      int disconnectedNodethreshold = level == 0 ? M : M / 2;

      while (it.hasNext()) {
        int oldOrd = it.nextInt();
        int newOrd = newOrdMap[oldOrd];
        if (newOrd == -1) {
          continue;
        }
        hnsw.addNode(level, newOrd);
        levelToNodes[level].add(newOrd);
        hnsw.trySetNewEntryNode(newOrd, level);
        scorer.setScoringOrdinal(newOrd);
        NeighborArray newNeighbors = hnsw.getNeighbors(level, newOrd);
        initializerGraph.seek(level, oldOrd);
        for (int oldNeighbor = initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = initializerGraph.nextNeighbor()) {
          int newNeighbor = newOrdMap[oldNeighbor];
          if (newNeighbor != -1) {
            newNeighbors.addOutOfOrder(newNeighbor, scorer.score(newNeighbor));
          }
        }

        if (newNeighbors.size() < disconnectedNodethreshold) {
          disconnectedNodes.add(newOrd);
        }
      }
      disconnectedNodesByLevel.put(level, disconnectedNodes);
    }
    return disconnectedNodesByLevel;
  }

  private void repairDisconnectedNodes(
      Map<Integer, List<Integer>> disconnectedNodesByLevel, int numLevels) throws IOException {
    for (int level = numLevels - 1; level >= 0; level--) {
      fixDisconnectedNodes(disconnectedNodesByLevel.get(level), level, scorer);
    }
  }

  private void fixDisconnectedNodes(
      List<Integer> disconnectedNodes, int level, UpdateableRandomVectorScorer scorer)
      throws IOException {
    if (disconnectedNodes.isEmpty()) return;

    int beamWidth = beamCandidates.k();
    GraphBuilderKnnCollector candidates = new GraphBuilderKnnCollector(beamWidth);
    NeighborArray scratchArray = new NeighborArray(beamWidth, false);

    for (int node : disconnectedNodes) {
      scorer.setScoringOrdinal(node);
      NeighborArray existingNeighbors = hnsw.getNeighbors(level, node);

      if (existingNeighbors.size() > 0) {
        int[] entryPoints = new int[existingNeighbors.size()];
        System.arraycopy(existingNeighbors.nodes(), 0, entryPoints, 0, existingNeighbors.size());
        graphSearcher.searchLevel(candidates, scorer, level, entryPoints, hnsw, null);
        popToScratch(candidates, scratchArray);
        addDiverseNeighbors(level, node, scratchArray, scorer, true);
      }

      scratchArray.clear();
      candidates.clear();
    }
  }

  private void rebalanceGraph() throws IOException {
    SplittableRandom random = new SplittableRandom();
    int size = hnsw.size();

    for (int level = 1; ; level++) {
      int maxNodesAtLevel = (int) (size * Math.pow(ml, level));
      if (maxNodesAtLevel <= 0) break;

      int currentNodesAtLevel = 0;
      if (level >= levelToNodes.length) {
        levelToNodes = ArrayUtil.growExact(levelToNodes, level + 1);
        levelToNodes[level] = new IntArrayList();
      } else {
        currentNodesAtLevel = levelToNodes[level].size();
      }

      if (currentNodesAtLevel >= maxNodesAtLevel) continue;

      // Fetch nodes from below level and randomly select nodes to promote them
      Iterator<IntCursor> it = levelToNodes[level - 1].iterator();

      while (it.hasNext() && currentNodesAtLevel < maxNodesAtLevel) {
        int node = it.next().value;
        scorer.setScoringOrdinal(node);
        if (random.nextDouble() < ml && !hnsw.nodeExistAtLevel(level, node)) {
          addNodeToLevel(node, level, scorer, currentNodesAtLevel++);
          levelToNodes[level].add(node);
        }
      }
    }
  }

  private void addNodeToLevel(
      int node, int targetLevel, UpdateableRandomVectorScorer scorer, int currentNodes)
      throws IOException {
    hnsw.addNode(targetLevel, node);
    if (currentNodes == 0) {
      hnsw.tryPromoteNewEntryNode(node, targetLevel, hnsw.numLevels() - 1);
      return;
    }

    GraphBuilderKnnCollector candidates = new GraphBuilderKnnCollector(beamCandidates.k());
    int[] eps = {hnsw.entryNode()};

    // Search from top to target level
    for (int level = hnsw.numLevels() - 1; level > targetLevel; level--) {
      graphSearcher.searchLevel(candidates, scorer, level, eps, hnsw, null);
      eps[0] = candidates.popNode();
      candidates.clear();
    }

    graphSearcher.searchLevel(candidates, scorer, targetLevel, eps, hnsw, null);

    NeighborArray scratchArray = new NeighborArray(beamCandidates.k(), false);
    popToScratch(candidates, scratchArray);

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
