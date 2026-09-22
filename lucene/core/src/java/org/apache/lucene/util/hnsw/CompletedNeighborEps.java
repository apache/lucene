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
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.FixedBitSet;

/**
 * Worker-time entry points from completed same-segment neighbors during concurrent HNSW merge.
 *
 * <p>Work identity is leftover {@code (graphIdx, sourceOrd)}. Join-set source ords are not
 * pre-marked completed. Cheap-path gate {@code F} is separate from the eps cloud: 2-hop and
 * copied-base nodes are not anchors. Leftover join-set ords are always full-beam; the 1-hop ∩ F
 * gate applies only to leftover rest.
 */
final class CompletedNeighborEps {

  static final int MIN_ANCHORS = 1;

  private final int maxOrd;
  private final KnnVectorsReader[] readers;
  private final String fieldName;
  private final int[][] ordMaps;
  // FixedBitSet is not concurrent; workers mark/read completed ords in parallel.
  private final AtomicLongArray completed;
  // Nodes inserted with full beam (set F). Gate ≠ eps cloud: only leftover L0 1-hop in F count.
  private final AtomicLongArray fullBeamInserted;
  private final AtomicLong leftoverNodes = new AtomicLong();
  private final AtomicLong cheapInserts = new AtomicLong();
  private final IntHashSet[] joinSets;
  private final int[] leftoverGraphIdx;
  private final int[] leftoverSourceOrd;
  private final int joinSetWorkCount;

  private OnHeapHnswGraph outputGraph;
  private HnswLock hnswLock;

  CompletedNeighborEps(int maxOrd, int[][] ordMaps, KnnVectorsReader[] readers, String fieldName)
      throws IOException {
    this.maxOrd = maxOrd;
    this.ordMaps = ordMaps;
    this.readers = readers;
    this.fieldName = fieldName;
    int words = FixedBitSet.bits2words(maxOrd);
    this.completed = new AtomicLongArray(words);
    this.fullBeamInserted = new AtomicLongArray(words);
    HnswGraph[] sources = newSourceGraphs();
    this.joinSets = new IntHashSet[readers.length];
    int[][] joinNodes = new int[readers.length][];
    boolean[] skipGraph = new boolean[readers.length];
    int jCount = 0;
    int restCount = 0;
    for (int g = 0; g < readers.length; g++) {
      HnswGraph source = sources[g];
      int[] ordMap = ordMaps[g];
      if (source == null || source.size() == 0) {
        joinSets[g] = new IntHashSet();
        joinNodes[g] = new int[0];
        skipGraph[g] = true;
        continue;
      }
      IntHashSet join = UpdateGraphsUtils.computeJoinSet(source);
      joinSets[g] = join;
      int[] nodes = join.toArray();
      Arrays.sort(nodes);
      joinNodes[g] = nodes;
      for (int sourceOrd : nodes) {
        if (sourceOrd >= 0 && sourceOrd < ordMap.length && ordMap[sourceOrd] != -1) {
          jCount++;
        }
      }
      for (int sourceOrd = 0; sourceOrd < ordMap.length; sourceOrd++) {
        if (ordMap[sourceOrd] != -1 && join.contains(sourceOrd) == false) {
          restCount++;
        }
      }
    }
    this.joinSetWorkCount = jCount;
    this.leftoverGraphIdx = new int[jCount + restCount];
    this.leftoverSourceOrd = new int[jCount + restCount];
    int w = 0;
    for (int g = 0; g < readers.length; g++) {
      int[] ordMap = ordMaps[g];
      for (int sourceOrd : joinNodes[g]) {
        if (sourceOrd >= 0 && sourceOrd < ordMap.length && ordMap[sourceOrd] != -1) {
          leftoverGraphIdx[w] = g;
          leftoverSourceOrd[w] = sourceOrd;
          w++;
        }
      }
    }
    for (int g = 0; g < readers.length; g++) {
      if (skipGraph[g]) {
        continue;
      }
      IntHashSet join = joinSets[g];
      int[] ordMap = ordMaps[g];
      for (int sourceOrd = 0; sourceOrd < ordMap.length; sourceOrd++) {
        if (ordMap[sourceOrd] != -1 && join.contains(sourceOrd) == false) {
          leftoverGraphIdx[w] = g;
          leftoverSourceOrd[w] = sourceOrd;
          w++;
        }
      }
    }
    if (w != leftoverGraphIdx.length) {
      throw new IllegalStateException("leftover work size " + w + " != " + leftoverGraphIdx.length);
    }
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

  int leftoverWorkCount() {
    return leftoverGraphIdx.length;
  }

  int leftoverGraphIdx(int i) {
    return leftoverGraphIdx[i];
  }

  int leftoverSourceOrd(int i) {
    return leftoverSourceOrd[i];
  }

  int joinSetWorkCount() {
    return joinSetWorkCount;
  }

  int mergedOrd(int graphIdx, int sourceOrd) {
    return ordMaps[graphIdx][sourceOrd];
  }

  IntHashSet getEps(int graphIdx, int sourceOrd, HnswGraph[] threadPrivateGraphs)
      throws IOException {
    if (graphIdx < 0 || graphIdx >= ordMaps.length) {
      return null;
    }
    int[] ordMap = ordMaps[graphIdx];
    if (sourceOrd < 0 || sourceOrd >= ordMap.length || ordMap[sourceOrd] == -1) {
      return null;
    }
    if (outputGraph == null) {
      throw new IllegalStateException("bind must be called before getEps");
    }
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

  IntHashSet collectLeftoverNeighbors(int graphIdx, int sourceOrd, HnswGraph[] threadPrivateGraphs)
      throws IOException {
    if (graphIdx < 0 || graphIdx >= ordMaps.length) {
      return null;
    }
    int[] ordMap = ordMaps[graphIdx];
    if (sourceOrd < 0 || sourceOrd >= ordMap.length || ordMap[sourceOrd] == -1) {
      return null;
    }
    HnswGraph source = threadPrivateGraphs[graphIdx];
    source.seek(0, sourceOrd);
    IntHashSet leftover = new IntHashSet();
    for (int v = source.nextNeighbor(); v != NO_MORE_DOCS; v = source.nextNeighbor()) {
      if (v < 0 || v >= ordMap.length) {
        continue;
      }
      int mergedV = ordMap[v];
      if (mergedV == -1) {
        continue;
      }
      leftover.add(mergedV);
    }
    return leftover;
  }

  void markCompleted(int node) {
    setBit(completed, node);
  }

  boolean isCompleted(int node) {
    return bitIsSet(completed, node);
  }

  void markFullBeam(int node) {
    setBit(fullBeamInserted, node);
  }

  void recordCheap() {
    leftoverNodes.incrementAndGet();
    cheapInserts.incrementAndGet();
  }

  void recordFullBeam() {
    leftoverNodes.incrementAndGet();
  }

  int fullBeamCardinality() {
    int n = 0;
    for (int i = 0; i < fullBeamInserted.length(); i++) {
      n += Long.bitCount(fullBeamInserted.get(i));
    }
    return n;
  }

  String formatStats() {
    long leftover = leftoverNodes.get();
    long cheap = cheapInserts.get();
    double cheapPct = leftover == 0 ? 0.0 : 100.0 * cheap / leftover;
    return String.format(
        Locale.ROOT,
        "leftover=%d cheap=%d (%.1f%%) |F|=%d",
        leftover,
        cheap,
        cheapPct,
        fullBeamCardinality());
  }

  boolean isFullBeam(int node) {
    return bitIsSet(fullBeamInserted, node);
  }

  boolean isJoinSet(int graphIdx, int sourceOrd) {
    if (graphIdx < 0 || graphIdx >= joinSets.length) {
      return false;
    }
    return joinSets[graphIdx].contains(sourceOrd);
  }

  boolean hasFullBeamAnchor(int graphIdx, int sourceOrd, HnswGraph[] threadPrivateGraphs)
      throws IOException {
    return leftoverFullBeamOneHopCount(graphIdx, sourceOrd, threadPrivateGraphs) >= MIN_ANCHORS;
  }

  int leftoverFullBeamOneHopCount(int graphIdx, int sourceOrd, HnswGraph[] threadPrivateGraphs)
      throws IOException {
    if (graphIdx < 0 || graphIdx >= ordMaps.length) {
      return 0;
    }
    int[] ordMap = ordMaps[graphIdx];
    if (sourceOrd < 0 || sourceOrd >= ordMap.length || ordMap[sourceOrd] == -1) {
      return 0;
    }
    HnswGraph source = threadPrivateGraphs[graphIdx];
    source.seek(0, sourceOrd);
    int anchors = 0;
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
      if (isFullBeam(mergedV) == false) {
        continue;
      }
      anchors++;
    }
    return anchors;
  }

  private void setBit(AtomicLongArray bits, int node) {
    if (node < 0 || node >= maxOrd) {
      return;
    }
    int word = node >> 6;
    long bit = 1L << node;
    while (true) {
      long current = bits.get(word);
      if ((current & bit) != 0) {
        return;
      }
      if (bits.compareAndSet(word, current, current | bit)) {
        return;
      }
    }
  }

  private boolean bitIsSet(AtomicLongArray bits, int node) {
    if (node < 0 || node >= maxOrd) {
      return false;
    }
    long word = bits.get(node >> 6);
    return (word & (1L << node)) != 0;
  }
}
