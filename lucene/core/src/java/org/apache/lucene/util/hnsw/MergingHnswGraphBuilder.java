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

import java.io.IOException;
import org.apache.lucene.util.BitSet;

/**
 * A graph builder that is used during segments' merging. It enables efficient merging of graphs
 * using {@link UpdateGraphsUtils#joinSetGraphMerge(HnswGraph, HnswGraph, HnswGraphSearcher, int[], HnswBuilder)}.} method.
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
   * @param graphs the graphs to merge
   * @param ordMaps the ordinal maps for the graphs
   * @param totalNumberOfVectors the total number of vectors in the new graph, this should include
   *     all vectors expected to be added to the graph in the future
   * @param initializedNodes the nodes will be initialized through the merging, if null, all nodes
   *     should be already initialized after {@link UpdateGraphsUtils#joinSetGraphMerge(HnswGraph, HnswGraph, HnswGraphSearcher, int[], HnswBuilder)} being called
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
      String graphSizes = "";
      for (HnswGraph g : graphs) {
        graphSizes += g.size() + " ";
      }
      infoStream.message(
          HNSW_COMPONENT,
          "build graph from merging "
              + graphs.length
              + " graphs of "
              + maxOrd
              + " vectors, graph sizes:"
              + graphSizes);
    }
    // this searcher is created just to satisfy the call signature, shouldn't be used for any actual search functionality
    HnswGraphSearcher graphSearcher = new HnswGraphSearcher(null, null);
    for (int i = 1; i < graphs.length; i++) {
      UpdateGraphsUtils.joinSetGraphMerge(graphs[i], hnsw, graphSearcher, ordMaps[i], this);
    }

    // TODO: optimize to iterate only over unset bits in initializedNodes
    if (initializedNodes != null) {
      for (int node = 0; node < maxOrd; node++) {
        if (initializedNodes.get(node) == false) {
          addGraphNode(node);
        }
      }
    }

    return getCompletedGraph();
  }

}
