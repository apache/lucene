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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * Worker-time entry points from completed same-segment neighbors during concurrent HNSW merge.
 *
 * <p>Join-set ords are not pre-marked. Only nodes already copied into the base graph ({@code
 * initializedNodes}) and nodes that have finished {@code addGraphNode} are completed.
 */
final class CompletedNeighborEps {

  private final int[] sourceGraphIndex;
  private final int[] sourceOrdinal;
  private final KnnVectorsReader[] readers;
  private final String fieldName;
  private final int[][] ordMaps;
  // FixedBitSet is not concurrent; workers mark/read completed ords in parallel.
  private final AtomicLongArray completed;

  private OnHeapHnswGraph outputGraph;
  private HnswLock hnswLock;

  CompletedNeighborEps(int maxOrd, int[][] ordMaps, KnnVectorsReader[] readers, String fieldName) {
    this.ordMaps = ordMaps;
    this.readers = readers;
    this.fieldName = fieldName;
    this.sourceGraphIndex = new int[maxOrd];
    this.sourceOrdinal = new int[maxOrd];
    Arrays.fill(sourceGraphIndex, -1);
    Arrays.fill(sourceOrdinal, -1);
    for (int i = 0; i < ordMaps.length; i++) {
      int[] ordMap = ordMaps[i];
      for (int sourceOrd = 0; sourceOrd < ordMap.length; sourceOrd++) {
        int mergedOrd = ordMap[sourceOrd];
        if (mergedOrd != -1) {
          sourceGraphIndex[mergedOrd] = i;
          sourceOrdinal[mergedOrd] = sourceOrd;
        }
      }
    }
    this.completed = new AtomicLongArray(FixedBitSet.bits2words(maxOrd));
  }

  void bind(OnHeapHnswGraph graph, HnswLock lock) {
    this.outputGraph = graph;
    this.hnswLock = lock;
  }

  HnswGraph[] newSourceGraphs() throws IOException {
    HnswGraph[] graphs = new HnswGraph[readers.length];
    for (int i = 0; i < readers.length; i++) {
      graphs[i] = ((HnswGraphProvider) readers[i]).getGraph(fieldName);
    }
    return graphs;
  }

  IntHashSet getEps(int mergedOrd, HnswGraph[] threadPrivateGraphs) throws IOException {
    if (mergedOrd < 0 || mergedOrd >= sourceGraphIndex.length) {
      return null;
    }
    int graphIdx = sourceGraphIndex[mergedOrd];
    if (graphIdx < 0) {
      return null;
    }
    if (outputGraph == null) {
      throw new IllegalStateException("bind must be called before getEps");
    }
    int sourceOrd = sourceOrdinal[mergedOrd];
    int[] ordMap = ordMaps[graphIdx];
    HnswGraph source = threadPrivateGraphs[graphIdx];
    source.seek(0, sourceOrd);
    IntHashSet eps = new IntHashSet();
    for (int v = source.nextNeighbor(); v != NO_MORE_DOCS; v = source.nextNeighbor()) {
      if (v < 0 || v >= ordMap.length) {
        continue;
      }
      int mergedV = ordMap[v];
      if (mergedV == -1) {
        continue;
      }
      if (isCompleted(mergedV) == false) {
        continue;
      }
      eps.add(mergedV);
      int[] hops;
      int hopSize;
      Lock lock = hnswLock.read(0, mergedV);
      try {
        NeighborArray neighbors = outputGraph.getNeighbors(0, mergedV);
        hopSize = neighbors.size();
        hops = new int[hopSize];
        System.arraycopy(neighbors.nodes(), 0, hops, 0, hopSize);
      } finally {
        lock.unlock();
      }
      for (int i = 0; i < hopSize; i++) {
        eps.add(hops[i]);
      }
    }
    return eps;
  }

  void markCompleted(int node) {
    if (node < 0 || node >= sourceGraphIndex.length) {
      return;
    }
    int word = node >> 6;
    long bit = 1L << node;
    while (true) {
      long current = completed.get(word);
      if ((current & bit) != 0) {
        return;
      }
      if (completed.compareAndSet(word, current, current | bit)) {
        return;
      }
    }
  }

  boolean isCompleted(int node) {
    if (node < 0 || node >= sourceGraphIndex.length) {
      return false;
    }
    long word = completed.get(node >> 6);
    return (word & (1L << node)) != 0;
  }
}
