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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BitSet;

/**
 * A graph builder used during segments' merging.
 *
 * <p>This builder uses a smart algorithm to merge multiple graphs into a single graph. The
 * algorithm is based on the idea if we know where we want to insert a node, we have a good idea of
 * where we want to insert its neighbours.
 *
 * <p>The algorithm is based on the following steps: 1. Get all graphs that don't have deletions and
 * sort them by size. 2. Copy the largest graph to the new graph (gL). 3. For each remaining small
 * graph (gS) - Find the nodes that best cover gS: join set `j`. These nodes will be inserted into
 * gL as usual: by searching gL to find the best candidates: `w` to which connect the nodes. - For
 * each remaining node in gS: - we do NOT do search in gL. Instead, we form `w` by union of the
 * node's neighbors' in Gs and the node's neighbors' neighbors' in gL.
 *
 * <p>We expect the size of join set `j` to be small, 1/5-1/2 of size gS. And for the rest of the
 * nodes of gS, we expect savings by not doing extra search in gL.
 *
 * @lucene.experimental
 */
public final class MergingHnswGraphBuilder extends HnswGraphBuilder {
  private final HnswGraph[] graphs;
  private final int[][] ordMaps;
  private final BitSet initializedNodes;

  private MergingHnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      OnHeapHnswGraph initializedGraph,
      HnswGraph[] graphs,
      int[][] ordMaps,
      BitSet initializedNodes)
      throws IOException {
    super(scorerSupplier, beamWidth, seed, initializedGraph);
    this.graphs = graphs;
    this.ordMaps = ordMaps;
    this.initializedNodes = initializedNodes;
  }

  /**
   * Create a new HnswGraphBuilder that is initialized with the provided HnswGraph.
   *
   * @param scorerSupplier the scorer to use for vectors
   * @param beamWidth the number of nodes to explore in the search
   * @param seed the seed for the random number generator
   * @param totalNumberOfVectors the total number of vectors in the new graph, this should include
   *     all vectors expected to be added to the graph in the future
   * @return a new HnswGraphBuilder that is initialized with the provided HnswGraph
   * @throws IOException when reading the graph fails
   */
  public static MergingHnswGraphBuilder fromGraphs(
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      long seed,
      HnswGraph[] graphs,
      int[][] ordMaps,
      int totalNumberOfVectors,
      BitSet initializedNodes)
      throws IOException {
    OnHeapHnswGraph graph =
        InitializedHnswGraphBuilder.initGraph(graphs[0], ordMaps[0], totalNumberOfVectors);
    return new MergingHnswGraphBuilder(
        scorerSupplier, beamWidth, seed, graph, graphs, ordMaps, initializedNodes);
  }

  @Override
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("This HnswGraphBuilder is frozen and cannot be updated");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          "build graph from merging " + graphs.length + " graphs of " + maxOrd + " vectors");
    }
    for (int i = 1; i < graphs.length; i++) {
      updateGraph(graphs[i], ordMaps[i]);
    }

    if (initializedNodes != null) {
      for (int node = 0; node < maxOrd; node++) {
        if (initializedNodes.get(node) == false) {
          addGraphNode(node);
        }
      }
    }

    return getCompletedGraph();
  }

  /** Merge the smaller graph into the larger graph. */
  private void updateGraph(HnswGraph gS, int[] ordMapS) throws IOException {
    HnswGraph.NodesIterator it = gS.getNodesOnLevel(0);
    List<List<Integer>> nodesNs = new ArrayList<>(it.size());
    // load graph into heap
    int i = 0;
    while (it.hasNext()) {
      int v = it.nextInt();
      assert (v == i);
      gS.seek(0, v);
      List<Integer> ns = new ArrayList<>();
      for (int u = gS.nextNeighbor(); u != NO_MORE_DOCS; u = gS.nextNeighbor()) {
        ns.add(u);
      }
      nodesNs.add(ns);
      i++;
    }

    long start = System.nanoTime();
    Set<Integer> j = UpdateGraphsUtils.computeJoinSet(nodesNs);
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      long now = System.nanoTime();
      infoStream.message(
          HNSW_COMPONENT,
          String.format(
              Locale.ROOT,
              "built join set of size [%d] from graph size of [%d] in [%d] ms",
              j.size(),
              gS.size(),
              TimeUnit.NANOSECONDS.toMillis(now - start)));
    }

    // for nodes that in the join set, add them directly to the graph
    for (int node : j) {
      addGraphNode(ordMapS[node]);
    }

    // for each other node not from j set:
    // form the candidate set for a node
    // by joining the node's neighbours in gS with
    // the node's neighbours' neighbours in gL
    for (int node = 0; node < it.size; node++) {
      if (j.contains(node)) {
        continue;
      }
      Set<Integer> w = new HashSet<>();
      List<Integer> ns = nodesNs.get(node);
      for (int u : ns) {
        int newu = ordMapS[u];
        // if new ordinal of u > new ordinal of node, then it doesn't exist in gL yet
        // so we can't add it as a neighbour of node
        if (newu < ordMapS[node]) {
          w.add(newu);
        }
        if (j.contains(u)) {
          // add u's neigbhours from the large graph
          NeighborArray nsuArray = hnsw.getNeighbors(0, newu);
          int[] nsu = nsuArray.nodes();
          int nsuSize = nsuArray.size();
          for (int k = 0; k < nsuSize; k++) {
            w.add(nsu[k]);
          }
        }
      }
      addGraphNodeWithCandidates(ordMapS[node], w);
    }
  }
}
