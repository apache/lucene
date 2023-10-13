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
import org.apache.lucene.util.BitSet;

/**
 * This creates a graph builder that is initialized with the provided HnswGraph. This is useful for
 * merging HnswGraphs from multiple segments.
 *
 * @lucene.internal
 */
public final class InitializedHnswGraphBuilder extends HnswGraphBuilder {

  /**
   * Create a new HnswGraphBuilder that is initialized with the provided HnswGraph.
   *
   * @param scorerSupplier
   * @param M
   * @param beamWidth
   * @param seed
   * @param initializerGraph
   * @param newOrdOffset the offset to add to the ord of each node in the initializerGraph
   * @param initializedNodes a bitset of nodes that are already initialized in the initializerGraph
   * @return
   * @throws IOException
   */
  public static InitializedHnswGraphBuilder fromGraph(
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      int newOrdOffset,
      BitSet initializedNodes)
      throws IOException {
    OnHeapHnswGraph hnsw = new OnHeapHnswGraph(M);
    for (int level = 0; level < initializerGraph.numLevels(); level++) {
      HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);
      while (it.hasNext()) {
        int oldOrd = it.nextInt();
        int newOrd = oldOrd + newOrdOffset;
        hnsw.addNode(level, newOrd);
        NeighborArray newNeighbors = hnsw.getNeighbors(level, newOrd);
        initializerGraph.seek(level, oldOrd);
        for (int oldNeighbor = initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = initializerGraph.nextNeighbor()) {
          int newNeighbor = oldNeighbor + newOrdOffset;
          // we will compute these scores later when we need to pop out the non-diverse nodes
          newNeighbors.addOutOfOrder(newNeighbor, Float.NaN);
        }
      }
    }
    return new InitializedHnswGraphBuilder(
        scorerSupplier, M, beamWidth, seed, hnsw, initializedNodes);
  }

  private final BitSet initializedNodes;

  public InitializedHnswGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      long seed,
      OnHeapHnswGraph initializedGraph,
      BitSet initializedNodes)
      throws IOException {
    super(scorerSupplier, M, beamWidth, seed, initializedGraph);
    this.initializedNodes = initializedNodes;
  }

  @Override
  public void addGraphNode(int node) throws IOException {
    if (initializedNodes.get(node)) {
      return;
    }
    super.addGraphNode(node);
  }
}
