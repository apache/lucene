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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.InfoStream;

/**
 * A graph builder that manages multiple workers, it only supports adding the whole graph all at
 * once. It will spawn a thread for each worker and the workers will pick the work in batches.
 *
 * <p>When given graphs to join, the workers first join them into the graph the way {@link
 * MergingHnswGraphBuilder} does, then add the remaining nodes.
 */
public class HnswConcurrentMergeBuilder implements HnswBuilder {

  private static final int DEFAULT_BATCH_SIZE =
      2048; // number of vectors the worker handles sequentially at one batch

  // number of batches each worker should get, at least, in a phase of a join
  private static final int MIN_JOIN_BATCHES = 16;

  private final TaskExecutor taskExecutor;
  private final ConcurrentMergeWorker[] workers;
  private final HnswLock hnswLock;
  private final HnswGraph[] graphs;
  private final int[][] ordMaps;
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
    this(
        taskExecutor,
        numWorker,
        scorerSupplier,
        beamWidth,
        hnsw,
        initializedNodes,
        new HnswGraph[0],
        new int[0][]);
  }

  /**
   * @param initializedNodes the nodes that must not be added to the graph as new nodes, either
   *     because they are already in it or because they belong to one of {@code graphs}; must not be
   *     null if {@code graphs} is not empty
   * @param graphs graphs without deletions whose nodes are not in {@code hnsw} yet; they are joined
   *     into it with the join set approach of {@link MergingHnswGraphBuilder} before the nodes that
   *     are not in {@code initializedNodes} are added
   * @param ordMaps for each of {@code graphs}, the mapping from its ordinals to ordinals of {@code
   *     hnsw}
   */
  public HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      OnHeapHnswGraph hnsw,
      BitSet initializedNodes,
      HnswGraph[] graphs,
      int[][] ordMaps)
      throws IOException {
    if (graphs.length != ordMaps.length) {
      throw new IllegalArgumentException(
          "got " + graphs.length + " graphs but " + ordMaps.length + " ordinal maps");
    }
    if (graphs.length > 0 && initializedNodes == null) {
      throw new IllegalArgumentException(
          "initializedNodes must mark the nodes of the graphs to join");
    }
    this.graphs = graphs;
    this.ordMaps = ordMaps;
    this.taskExecutor = taskExecutor;
    AtomicInteger workProgress = new AtomicInteger(0);
    workers = new ConcurrentMergeWorker[numWorker];
    hnswLock = new HnswLock();
    for (int i = 0; i < numWorker; i++) {
      workers[i] =
          new ConcurrentMergeWorker(
              scorerSupplier.copy(),
              beamWidth,
              HnswGraphBuilder.randSeed,
              hnsw,
              hnswLock,
              initializedNodes,
              workProgress);
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
    }
    if (graphs.length > 0) {
      joinGraphs(cumulativeWorkTimeNs);
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
    }
    return getCompletedGraph();
  }

  /**
   * Joins {@link #graphs} into the graph like {@link MergingHnswGraphBuilder} does, but with all
   * workers. Graphs are joined one after the other: adding the join sets of all graphs before the
   * other nodes of any of them builds a noticeably worse graph. Since the join sets only depend on
   * the graphs being joined, they are all computed beforehand, one task per graph.
   */
  private void joinGraphs(AtomicLong cumulativeWorkTimeNs) throws IOException {
    long startNs = System.nanoTime();
    List<Callable<JoinPlan>> planTasks = new ArrayList<>(graphs.length);
    for (int g = 0; g < graphs.length; g++) {
      HnswGraph graph = graphs[g];
      int[] ordMap = ordMaps[g];
      planTasks.add(() -> JoinPlan.create(graph, ordMap));
    }
    JoinPlan[] plans = taskExecutor.invokeAll(planTasks).toArray(JoinPlan[]::new);
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          String.format(
              Locale.ROOT,
              "computed the join sets of %d graphs in %.2f ms",
              plans.length,
              (System.nanoTime() - startNs) / 1_000_000.0));
    }

    for (int g = 0; g < plans.length; g++) {
      JoinPlan plan = plans[g];
      plans[g] = null; // not needed once joined
      runInBatches(
          plan.joinSetNodes.length,
          cumulativeWorkTimeNs,
          (worker, start, end) -> {
            for (int i = start; i < end; i++) {
              worker.addJoinSetNode(plan.joinSetNodes[i]);
            }
          });
      runInBatches(
          plan.otherNodes.length,
          cumulativeWorkTimeNs,
          (worker, start, end) -> {
            for (int i = start; i < end; i++) {
              worker.addJoinedNode(
                  plan.otherNodes[i],
                  plan.joinNeighbors,
                  plan.joinNeighborsStart[i],
                  plan.joinNeighborsStart[i + 1]);
            }
          });
    }
  }

  /**
   * The nodes a join adds to the graph, as ordinals of the graph.
   *
   * @param joinSetNodes the join set of the graph being joined
   * @param otherNodes the other nodes of the graph being joined
   * @param joinNeighborsStart the neighbors in the join set of {@code otherNodes[i]} are {@code
   *     joinNeighbors[joinNeighborsStart[i]:joinNeighborsStart[i + 1]]}
   * @param joinNeighbors see {@code joinNeighborsStart}
   */
  private record JoinPlan(
      int[] joinSetNodes, int[] otherNodes, int[] joinNeighborsStart, int[] joinNeighbors) {

    static JoinPlan create(HnswGraph graph, int[] ordMap) throws IOException {
      IntHashSet j = UpdateGraphsUtils.computeJoinSet(graph);
      // sort for stability
      int[] joinSetNodes = j.toArray();
      Arrays.sort(joinSetNodes);
      for (int i = 0; i < joinSetNodes.length; i++) {
        joinSetNodes[i] = ordMap[joinSetNodes[i]];
      }
      int[] otherNodes = new int[graph.size() - joinSetNodes.length];
      int[] joinNeighborsStart = new int[otherNodes.length + 1];
      IntArrayList joinNeighbors = new IntArrayList();
      int i = 0;
      for (int u = 0; u < graph.size(); u++) {
        if (j.contains(u)) {
          continue;
        }
        graph.seek(0, u);
        for (int v = graph.nextNeighbor(); v != NO_MORE_DOCS; v = graph.nextNeighbor()) {
          if (j.contains(v)) {
            joinNeighbors.add(ordMap[v]);
          }
        }
        otherNodes[i++] = ordMap[u];
        joinNeighborsStart[i] = joinNeighbors.size();
      }
      assert i == otherNodes.length;
      return new JoinPlan(joinSetNodes, otherNodes, joinNeighborsStart, joinNeighbors.toArray());
    }
  }

  /**
   * Splits [0, count) into batches that the workers pick concurrently, and returns once all batches
   * are done.
   */
  private void runInBatches(int count, AtomicLong cumulativeWorkTimeNs, BatchTask task)
      throws IOException {
    // The phases of a join are short, so a worker that picks the last full-size batch would keep
    // the others idle for most of a phase. Smaller batches keep all workers busy until the end.
    int batchSize =
        Math.max(1, Math.min(workers[0].batchSize, count / (workers.length * MIN_JOIN_BATCHES)));
    AtomicInteger progress = new AtomicInteger(0);
    List<Callable<Void>> futures = new ArrayList<>();
    for (ConcurrentMergeWorker worker : workers) {
      futures.add(
          () -> {
            long startNs = System.nanoTime();
            for (int start = progress.getAndAdd(batchSize);
                start < count;
                start = progress.getAndAdd(batchSize)) {
              task.run(worker, start, Math.min(count, start + batchSize));
            }
            cumulativeWorkTimeNs.addAndGet(System.nanoTime() - startNs);
            return null;
          });
    }
    taskExecutor.invokeAll(futures);
  }

  @FunctionalInterface
  private interface BatchTask {
    void run(ConcurrentMergeWorker worker, int start, int end) throws IOException;
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
    private int batchSize = DEFAULT_BATCH_SIZE;

    /** The entry points of the search on level 0 for {@link #addJoinedNode}, reused */
    private final IntHashSet eps = new IntHashSet();

    private ConcurrentMergeWorker(
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        long seed,
        OnHeapHnswGraph hnsw,
        HnswLock hnswLock,
        BitSet initializedNodes,
        AtomicInteger workProgress)
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
    }

    /**
     * This method first try to "reserve" part of work by calling {@link #getStartPos(int)} and then
     * calling {@link #addVectors(int, int)} to actually add the nodes to the graph. By doing this
     * we are able to dynamically allocate the work to multiple workers and try to make all of them
     * finishing around the same time.
     */
    private void run(int maxOrd) throws IOException {
      int start = getStartPos(maxOrd);
      int end;
      while (start != -1) {
        end = Math.min(maxOrd, start + batchSize);
        addVectors(start, end);
        start = getStartPos(maxOrd);
      }
    }

    /** Reserve the work by atomically increment the {@link #workProgress} */
    private int getStartPos(int maxOrd) {
      int start = workProgress.getAndAdd(batchSize);
      if (start < maxOrd) {
        return start;
      } else {
        return -1;
      }
    }

    @Override
    public void addGraphNode(int node) throws IOException {
      if (initializedNodes != null && initializedNodes.get(node)) {
        return;
      }
      super.addGraphNode(node);
    }

    /** Adds a node of the join set of a graph being joined, with a full search. */
    private void addJoinSetNode(int node) throws IOException {
      // skip the initializedNodes check: the node is marked since it belongs to a graph being
      // joined
      super.addGraphNode(node);
    }

    /**
     * Adds a node of a graph being joined that is not in its join set, searching level 0 from the
     * node's neighbors in the join set, {@code joinNeighbors[from:to]}, and from their neighbors.
     */
    private void addJoinedNode(int node, int[] joinNeighbors, int from, int to) throws IOException {
      eps.clear();
      for (int i = from; i < to; i++) {
        int joinNeighbor = joinNeighbors[i];
        eps.add(joinNeighbor);
        Lock lock = hnswLock.read(0, joinNeighbor);
        try {
          NeighborArray neighbors = hnsw.getNeighbors(0, joinNeighbor);
          for (int n = 0; n < neighbors.size(); n++) {
            eps.add(neighbors.nodes()[n]);
          }
        } finally {
          lock.unlock();
        }
      }
      addGraphNode(node, eps);
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
