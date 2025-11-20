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

import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.LongHeap;

/**
 * Utility class for updating a big graph with smaller graphs. This is used during merging of
 * segments containing HNSW graphs.
 *
 * @lucene.internal
 */
public class UpdateGraphsUtils {

  /**
   * Find nodes in the graph that best cover the graph. This is reminiscent of an edge cover
   * problem. Here rather than choosing edges we pick nodes and increment a count at their
   * neighbours.
   *
   * @return a set of nodes that best cover the graph
   */
  public static IntHashSet computeJoinSet(HnswGraph graph) throws IOException {
    int k; // coverage for the current node
    int size = graph.size();
    LongHeap heap = new LongHeap(size);
    IntHashSet j = new IntHashSet();
    boolean[] stale = new boolean[size];
    short[] counts = new short[size];
    long gExit = 0L;
    for (int v = 0; v < size; v++) {
      graph.seek(0, v);
      int degree = graph.neighborCount();
      k = degree < 9 ? 2 : Math.ceilDiv(degree, 4);
      gExit += k;
      int gain = k + degree;
      heap.push(encode(gain, v));
    }

    long gTot = 0L;
    while (gTot < gExit && heap.size() > 0) {
      long el = heap.pop();
      int gain = decodeValue1(el);
      int v = decodeValue2(el);
      graph.seek(0, v);
      int degree = graph.neighborCount();
      int[] ns = new int[degree];
      int i = 0;
      for (int u = graph.nextNeighbor(); u != NO_MORE_DOCS; u = graph.nextNeighbor()) {
        ns[i++] = u;
      }
      k = degree < 9 ? 2 : Math.ceilDiv(degree, 4);
      if (stale[v]) { // if stale, recalculate gain
        int newGain = Math.max(0, k - counts[v]);
        for (int u : ns) {
          if (counts[u] < k && j.contains(u) == false) {
            newGain += 1;
          }
        }
        if (newGain > 0) {
          heap.push(encode(newGain, v));
          stale[v] = false;
        }
      } else {
        j.add(v);
        gTot += gain;
        boolean markNeighboursStale = counts[v] < k;
        for (int u : ns) {
          if (markNeighboursStale) {
            stale[u] = true;
          }
          if (counts[u] < (k - 1)) {
            // make neighbours of u stale
            graph.seek(0, u);
            for (int uu = graph.nextNeighbor(); uu != NO_MORE_DOCS; uu = graph.nextNeighbor()) {
              stale[uu] = true;
            }
          }
          counts[u] += 1;
        }
      }
    }
    return j;
  }

  private static long encode(int value1, int value2) {
    return (((long) -value1) << 32) | (value2 & 0xFFFFFFFFL);
  }

  private static int decodeValue1(long encoded) {
    return (int) -(encoded >> 32);
  }

  private static int decodeValue2(long encoded) {
    return (int) (encoded & 0xFFFFFFFFL);
  }

    /**
     * <p> This method uses a smart algorithm to merge multiple graphs into a single graph. The
     * algorithm is based on the idea that if we know where we want to insert a node, we have a good
     * idea of where we want to insert its neighbors.
     *
     * <p>The algorithm is based on the following steps:
     *
     * <ul>
     *   <li>Get all graphs that don't have deletions and sort them by size desc.
     *   <li>Copy the largest graph to the new graph (gL).
     *   <li>For each remaining small graph (gS):
     *       <ul>
     *         <li>Find the nodes that best cover gS: join set `j`. These nodes will be inserted into gL
     *             as usual: by searching gL to find the best candidates `w` to which connect the nodes.
     *         <li>For each remaining node in gS:
     *             <ul>
     *               <li>We provide eps to search in gL. We form `eps` by the union of the node's
     *                   neighbors in gS and the node's neighbors' neighbors in gL. We also limit
     *                   beamWidth (efConstruction to M*3)
     *             </ul>
     *       </ul>
     * </ul>
     *
     * <p>We expect the size of join set `j` to be small, around 1/5 to 1/2 of the size of gS. For the
     * rest of the nodes in gS, we expect savings by performing lighter searches in gL.
     *
     * Thread-safety: the thread safety is provided by the {@code graphSearcher} and {@code graphBuilder}, if both
     * are thread-safe. Then the whole operation is thread-safe, providing no other thread is working on the same
     * segment
     */
    public static void joinSetGraphMerge(HnswGraph sourceGraph, HnswGraph destGraph, HnswGraphSearcher graphSearcher ,int[] oldToNewOrd, HnswBuilder graphBuilder) throws IOException {
      int size = sourceGraph.size();
      IntHashSet j = computeJoinSet(sourceGraph);

      // for nodes that in the join set, add them directly to the graph
      for (IntCursor node : j) {
        graphBuilder.addGraphNode(oldToNewOrd[node.value]);
      }

      // for each node outside of j set:
      // form the entry points set for the node
      // by joining the node's neighbours in gS with
      // the node's neighbours' neighbours in gL
      for (int u = 0; u < size; u++) {
        if (j.contains(u)) {
          continue;
        }
        IntHashSet eps = new IntHashSet();
        sourceGraph.seek(0, u);
        for (int v = sourceGraph.nextNeighbor(); v != NO_MORE_DOCS; v = sourceGraph.nextNeighbor()) {
          // if u's neighbour v is in the join set, or already added to gL (v < u),
          // then we add v's neighbours from gL to the candidate list
          if (v < u || j.contains(v)) {
            int newv = oldToNewOrd[v];
            eps.add(newv);
            graphSearcher.graphSeek(destGraph, 0, newv);
            int friendOrd;
            while ((friendOrd = graphSearcher.graphNextNeighbor(destGraph)) != NO_MORE_DOCS) {
              eps.add(friendOrd);
            }
          }
        }
        graphBuilder.addGraphNode(oldToNewOrd[u], eps);
      }
    }
}
