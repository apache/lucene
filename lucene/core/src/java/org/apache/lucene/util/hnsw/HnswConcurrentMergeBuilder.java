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
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.HNSW_COMPONENT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.InfoStream;

/**
 * A graph builder that manages multiple workers, it only supports adding the whole graph all at
 * once. It will spawn a thread for each worker and the workers will pick the work in batches.
 */
public class HnswConcurrentMergeBuilder implements HnswBuilder {

  private static final int DEFAULT_BATCH_SIZE =
      2048; // number of vectors the worker handles sequentially at one batch

  private final TaskExecutor taskExecutor;
  private final ConcurrentMergeWorker[] workers;
  private final HnswLock hnswLock;
  private final CompletedNeighborEps epsHelper;
  private final OnHeapHnswGraph hnsw;
  private final BitSet initializedNodes;
  private InfoStream infoStream = InfoStream.getDefault();
  private boolean frozen;

  public HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      OnHeapHnswGraph hnsw,
      BitSet initializedNodes)
      throws IOException {
    this(taskExecutor, numWorker, scorerSupplier, beamWidth, hnsw, initializedNodes, null);
  }

  HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      OnHeapHnswGraph hnsw,
      BitSet initializedNodes,
      CompletedNeighborEps epsHelper)
      throws IOException {
    this.taskExecutor = taskExecutor;
    this.epsHelper = epsHelper;
    this.hnsw = hnsw;
    this.initializedNodes = initializedNodes;
    AtomicInteger workProgress = new AtomicInteger(0);
    workers = new ConcurrentMergeWorker[numWorker];
    hnswLock = new HnswLock();
    if (epsHelper != null) {
      epsHelper.bind(hnsw, hnswLock);
    }
    for (int i = 0; i < numWorker; i++) {
      workers[i] =
          new ConcurrentMergeWorker(
              scorerSupplier.copy(),
              beamWidth,
              HnswGraphBuilder.randSeed,
              hnsw,
              hnswLock,
              initializedNodes,
              workProgress,
              epsHelper);
    }
  }

  @Override
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("graph has already been built");
    }
    long mergeStartTimeNs = System.nanoTime();
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          "build graph from " + maxOrd + " vectors, with " + workers.length + " workers");
    }
    AtomicLong cumulativeWorkTimeNs = new AtomicLong();
    for (ConcurrentMergeWorker worker : workers) {
      worker.setMergeStartTimeNs(mergeStartTimeNs);
      worker.setCumulativeWorkTimeNs(cumulativeWorkTimeNs);
      worker.workTimeNs = cumulativeWorkTimeNs;
    }
    if (epsHelper != null) {
      preAddLeftoverL0();
    }
    List<Callable<Void>> futures = new ArrayList<>();
    for (int i = 0; i < workers.length; i++) {
      int finalI = i;
      futures.add(
          () -> {
            workers[finalI].run(maxOrd);
            return null;
          });
    }
    taskExecutor.invokeAll(futures);
    if (epsHelper != null) {
      workers[0].insertHoles(maxOrd);
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      double wallClockMs = (System.nanoTime() - mergeStartTimeNs) / 1_000_000.0;
      double totalWorkerMs = cumulativeWorkTimeNs.get() / 1_000_000.0;
      double effectiveConcurrency = wallClockMs > 0 ? totalWorkerMs / wallClockMs : 0;
      infoStream.message(
          HNSW_COMPONENT,
          String.format(
              Locale.ROOT,
              "merge completed: %d vectors, %.2f ms wall clock, %.2f ms cumulative worker time, %.2fx effective concurrency",
              maxOrd,
              wallClockMs,
              totalWorkerMs,
              effectiveConcurrency));
      if (epsHelper != null) {
        infoStream.message(HNSW_COMPONENT, epsHelper.formatStats());
      }
    }
    return getCompletedGraph();
  }

  @Override
  public void addGraphNode(int node) throws IOException {
    throw new UnsupportedOperationException("This builder is for merge only");
  }

  @Override
  public void addGraphNode(int node, IntHashSet eps) throws IOException {
    throw new UnsupportedOperationException("This builder is for merge only");
  }

  @Override
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
    for (HnswBuilder worker : workers) {
      worker.setInfoStream(infoStream);
    }
  }

  @Override
  public void setAbortCheck(IORunnable abortCheck) {
    for (HnswBuilder worker : workers) {
      worker.setAbortCheck(abortCheck);
    }
  }

  @Override
  public OnHeapHnswGraph getCompletedGraph() throws IOException {
    if (frozen == false) {
      // should already have been called in build(), but just in case
      finish();
      frozen = true;
    }
    return getGraph();
  }

  private void finish() throws IOException {
    workers[0].finish();
  }

  @Override
  public OnHeapHnswGraph getGraph() {
    return workers[0].getGraph();
  }

  private void preAddLeftoverL0() {
    int n = epsHelper.leftoverWorkCount();
    for (int i = 0; i < n; i++) {
      int node = epsHelper.mergedOrd(epsHelper.leftoverGraphIdx(i), epsHelper.leftoverSourceOrd(i));
      if (initializedNodes != null && initializedNodes.get(node)) {
        continue;
      }
      hnsw.addNode(0, node);
    }
  }

  /* test only for now */
  void setBatchSize(int newSize) {
    for (ConcurrentMergeWorker worker : workers) {
      worker.batchSize = newSize;
    }
  }

  private static final class ConcurrentMergeWorker extends HnswGraphBuilder {

    /**
     * A common AtomicInteger shared among all workers, used for tracking what's the next vector to
     * be added to the graph.
     */
    private final AtomicInteger workProgress;

    private final BitSet initializedNodes;
    private final CompletedNeighborEps epsHelper;
    private final HnswGraph[] sourceGraphs;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private AtomicLong workTimeNs;

    private ConcurrentMergeWorker(
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        long seed,
        OnHeapHnswGraph hnsw,
        HnswLock hnswLock,
        BitSet initializedNodes,
        AtomicInteger workProgress,
        CompletedNeighborEps epsHelper)
        throws IOException {
      super(
          scorerSupplier,
          beamWidth,
          seed,
          hnsw,
          hnswLock,
          new MergeSearcher(
              new NeighborQueue(beamWidth, true), hnswLock, new FixedBitSet(hnsw.maxNodeId() + 1)));
      this.workProgress = workProgress;
      this.initializedNodes = initializedNodes;
      this.epsHelper = epsHelper;
      this.sourceGraphs = epsHelper == null ? null : epsHelper.newSourceGraphs();
    }

    private void run(int maxOrd) throws IOException {
      int workCount = epsHelper == null ? maxOrd : epsHelper.leftoverWorkCount();
      int start = getStartPos(workCount);
      int end;
      while (start != -1) {
        end = Math.min(workCount, start + batchSize);
        if (epsHelper == null) {
          addVectors(start, end);
        } else {
          addLeftoverWork(start, end);
        }
        start = getStartPos(workCount);
      }
    }

    /** Reserve the work by atomically increment the {@link #workProgress} */
    private int getStartPos(int workCount) {
      int start = workProgress.getAndAdd(batchSize);
      if (start < workCount) {
        return start;
      } else {
        return -1;
      }
    }

    private void addLeftoverWork(int startIdx, int endIdx) throws IOException {
      long startNs = System.nanoTime();
      for (int i = startIdx; i < endIdx; i++) {
        int graphIdx = epsHelper.leftoverGraphIdx(i);
        int sourceOrd = epsHelper.leftoverSourceOrd(i);
        addLeftoverNode(graphIdx, sourceOrd);
      }
      long chunkedElapsedNs = System.nanoTime() - startNs;
      if (workTimeNs != null) {
        workTimeNs.addAndGet(chunkedElapsedNs);
      }
      if (infoStream.isEnabled(HNSW_COMPONENT)) {
        infoStream.message(
            HNSW_COMPONENT,
            String.format(
                Locale.ROOT,
                "addVectors [%d %d): %d vectors in %.2f ms",
                startIdx,
                endIdx,
                endIdx - startIdx,
                chunkedElapsedNs / 1_000_000.0));
      }
    }

    private void addLeftoverNode(int graphIdx, int sourceOrd) throws IOException {
      int node = epsHelper.mergedOrd(graphIdx, sourceOrd);
      if (initializedNodes != null && initializedNodes.get(node)) {
        return;
      }
      if (epsHelper.isJoinSet(graphIdx, sourceOrd)) {
        super.addGraphNode(node);
        epsHelper.markFullBeam(node);
        epsHelper.recordFullBeam();
      } else if (epsHelper.hasFullBeamAnchor(graphIdx, sourceOrd, sourceGraphs)) {
        IntHashSet eps = epsHelper.getEps(graphIdx, sourceOrd, sourceGraphs);
        IntHashSet leftoverC =
            epsHelper.collectLeftoverNeighbors(graphIdx, sourceOrd, sourceGraphs);
        epsHelper.recordCheap();
        super.addGraphNode(node, eps, leftoverC);
      } else {
        super.addGraphNode(node);
        epsHelper.markFullBeam(node);
        epsHelper.recordFullBeam();
      }
      epsHelper.markCompleted(node);
    }

    private void insertHoles(int maxOrd) throws IOException {
      long startNs = System.nanoTime();
      for (int node = 0; node < maxOrd; node++) {
        if (initializedNodes != null && initializedNodes.get(node)) {
          continue;
        }
        if (epsHelper.isCompleted(node)) {
          continue;
        }
        super.addGraphNode(node);
        epsHelper.markCompleted(node);
      }
      long elapsedNs = System.nanoTime() - startNs;
      if (workTimeNs != null) {
        workTimeNs.addAndGet(elapsedNs);
      }
    }

    @Override
    public void addGraphNode(int node) throws IOException {
      if (initializedNodes != null && initializedNodes.get(node)) {
        return;
      }
      super.addGraphNode(node);
      if (epsHelper != null) {
        epsHelper.markCompleted(node);
      }
    }

    @Override
    public void addGraphNode(int node, IntHashSet eps) throws IOException {
      if (initializedNodes != null && initializedNodes.get(node)) {
        return;
      }
      super.addGraphNode(node, eps);
      if (epsHelper != null) {
        epsHelper.markCompleted(node);
      }
    }
  }

  /**
   * This searcher will obtain the lock and make a copy of neighborArray when seeking the graph such
   * that concurrent modification of the graph will not impact the search
   */
  private static class MergeSearcher extends HnswGraphSearcher {
    private final HnswLock hnswLock;
    private int[] nodeBuffer;
    private int upto;
    private int size;

    private MergeSearcher(NeighborQueue candidates, HnswLock hnswLock, BitSet visited) {
      super(candidates, visited);
      this.hnswLock = hnswLock;
    }

    @Override
    void graphSeek(HnswGraph graph, int level, int targetNode) {
      Lock lock = hnswLock.read(level, targetNode);
      try {
        NeighborArray neighborArray = ((OnHeapHnswGraph) graph).getNeighbors(level, targetNode);
        if (nodeBuffer == null || nodeBuffer.length < neighborArray.size()) {
          nodeBuffer = new int[neighborArray.size()];
        }
        size = neighborArray.size();
        System.arraycopy(neighborArray.nodes(), 0, nodeBuffer, 0, size);
      } finally {
        lock.unlock();
      }
      upto = -1;
    }

    @Override
    int graphNextNeighbor(HnswGraph graph) {
      if (++upto < size) {
        return nodeBuffer[upto];
      }
      return NO_MORE_DOCS;
    }
  }
}
