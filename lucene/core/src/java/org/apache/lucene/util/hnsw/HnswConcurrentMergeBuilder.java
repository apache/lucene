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
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntsRef;

/**
 * A graph builder that manages multiple workers, it only supports adding the whole graph all at
 * once. It will spawn a thread for each worker and the workers will pick the work in batches.
 *
 * <p>When given graphs to join, the workers first join them into the graph the way {@link
 * MergingHnswGraphBuilder} does, then add the remaining nodes.
 */
public class HnswConcurrentMergeBuilder implements HnswBuilder {

  // Number of vectors the worker handles sequentially at one batch.
  private static final int DEFAULT_BATCH_SIZE = 2048;

  // Number of disconnected nodes a repair worker claims at a time.
  private static final int REPAIR_BATCH_SIZE = 64;

  // number of batches each worker should get, at least, in a phase of a join
  private static final int MIN_JOIN_BATCHES = 16;

  private final TaskExecutor taskExecutor;
  private final ConcurrentMergeWorker[] workers;
  private final HnswLock hnswLock;
  private final List<IOSupplier<HnswGraph>> graphs;
  private final int[][] ordMaps;
  private final InitializedHnswGraphBuilder.PrunedGraph prunedGraph;
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
        null,
        List.of(),
        new int[0][]);
  }

  /**
   * Creates a builder that, when the pruned source graph had deletes, repairs its disconnected
   * nodes across the worker pool and rebalances it before inserting the remaining vectors.
   *
   * @param prunedGraph the deferred repair state, or null when the reused graph had no deletes
   */
  HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      OnHeapHnswGraph hnsw,
      BitSet initializedNodes,
      InitializedHnswGraphBuilder.PrunedGraph prunedGraph)
      throws IOException {
    this(
        taskExecutor,
        numWorker,
        scorerSupplier,
        beamWidth,
        hnsw,
        initializedNodes,
        prunedGraph,
        List.of(),
        new int[0][]);
  }

  /**
   * Creates a builder that first repairs the pruned source graph if it had deletes, then joins
   * {@code graphs} into it, then inserts the remaining vectors.
   *
   * @param initializedNodes the nodes that must not be added to the graph as new nodes, either
   *     because they are already in it or because they belong to one of {@code graphs}; must not be
   *     null if {@code graphs} is not empty
   * @param prunedGraph the deferred repair state, or null when the reused graph had no deletes
   * @param graphs graphs without deletions whose nodes are not in {@code hnsw} yet; they are joined
   *     into it with the join set approach of {@link MergingHnswGraphBuilder} before the nodes that
   *     are not in {@code initializedNodes} are added. Each supplier must return a new instance on
   *     every call, since every worker reads a graph through its own instance.
   * @param ordMaps for each of {@code graphs}, the mapping from its ordinals to ordinals of {@code
   *     hnsw}
   */
  HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      OnHeapHnswGraph hnsw,
      BitSet initializedNodes,
      InitializedHnswGraphBuilder.PrunedGraph prunedGraph,
      List<IOSupplier<HnswGraph>> graphs,
      int[][] ordMaps)
      throws IOException {
    if (graphs.size() != ordMaps.length) {
      throw new IllegalArgumentException(
          "got " + graphs.size() + " graphs but " + ordMaps.length + " ordinal maps");
    }
    if (graphs.isEmpty() == false && initializedNodes == null) {
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
    this.prunedGraph = prunedGraph;
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
    // The join plans only read the graphs being joined, so they are computed while the repaired
    // base graph is rebalanced, which runs on a single thread.
    List<Callable<JoinPlan>> tasks = new ArrayList<>(graphs.size() + 1);
    for (int g = 0; g < graphs.size(); g++) {
      IOSupplier<HnswGraph> graph = graphs.get(g);
      int[] ordMap = ordMaps[g];
      tasks.add(() -> JoinPlan.create(graph.get(), ordMap));
    }
    int repairedNodes = 0;
    long repairNs = 0;
    AtomicLong rebalanceNs = new AtomicLong();
    if (prunedGraph != null) {
      long repairStartNs = System.nanoTime();
      repairedNodes = repairDisconnectedNodes();
      repairNs = System.nanoTime() - repairStartNs;
      tasks.add(
          () -> {
            long rebalanceStartNs = System.nanoTime();
            prunedGraph.builder().rebalanceGraph();
            rebalanceNs.set(System.nanoTime() - rebalanceStartNs);
            return null;
          });
    }
    long plansStartNs = System.nanoTime();
    JoinPlan[] plans =
        tasks.isEmpty()
            ? new JoinPlan[0]
            : taskExecutor.invokeAll(tasks).subList(0, graphs.size()).toArray(JoinPlan[]::new);
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      if (prunedGraph != null) {
        infoStream.message(
            HNSW_COMPONENT,
            String.format(
                Locale.ROOT,
                "repaired reused graph: %d nodes in %.2f ms with %d workers, %.2f ms rebalance",
                repairedNodes,
                repairNs / 1_000_000.0,
                workers.length,
                rebalanceNs.get() / 1_000_000.0));
      }
      if (plans.length > 0) {
        infoStream.message(
            HNSW_COMPONENT,
            String.format(
                Locale.ROOT,
                "computed the join sets of %d graphs in %.2f ms",
                plans.length,
                (System.nanoTime() - plansStartNs) / 1_000_000.0));
      }
    }
    // join the other graphs into the base graph once it is repaired, as MergingHnswGraphBuilder
    // does after InitializedHnswGraphBuilder.initGraph
    if (plans.length > 0) {
      joinGraphs(plans, maxOrd, cumulativeWorkTimeNs);
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
   * other nodes of any of them builds a noticeably worse graph.
   */
  private void joinGraphs(JoinPlan[] plans, int maxOrd, AtomicLong cumulativeWorkTimeNs)
      throws IOException {
    // the nodes of the joined graphs that are fully inserted, and so usable as entry points
    InsertedNodes inserted = new InsertedNodes(maxOrd);
    for (int g = 0; g < plans.length; g++) {
      JoinPlan plan = plans[g];
      plans[g] = null; // not needed once joined
      FixedBitSet joinSet = plan.joinSet;
      int size = joinSet.length();
      runInBatches(
          size,
          cumulativeWorkTimeNs,
          (worker, start, end) -> {
            for (int u = joinSet.nextSetBit(start, end);
                u != NO_MORE_DOCS;
                u = u + 1 < end ? joinSet.nextSetBit(u + 1, end) : NO_MORE_DOCS) {
              worker.addJoinSetNode(plan.ordMap[u], inserted);
            }
          });
      for (ConcurrentMergeWorker worker : workers) {
        worker.joinedGraph = graphs.get(g).get();
      }
      runInBatches(
          size,
          cumulativeWorkTimeNs,
          (worker, start, end) -> {
            for (int u = start; u < end; u++) {
              if (joinSet.get(u) == false) {
                worker.addJoinedNode(plan.ordMap, u, inserted);
              }
            }
          });
      for (ConcurrentMergeWorker worker : workers) {
        worker.joinedGraph = null;
      }
    }
  }

  /**
   * How a graph is joined.
   *
   * @param ordMap maps ordinals of the graph being joined to ordinals of the merged graph
   * @param joinSet the join set of the graph being joined; its other nodes are the clear bits
   */
  private record JoinPlan(int[] ordMap, FixedBitSet joinSet) {

    static JoinPlan create(HnswGraph graph, int[] ordMap) throws IOException {
      FixedBitSet joinSet = new FixedBitSet(graph.size());
      for (IntCursor node : UpdateGraphsUtils.computeJoinSet(graph)) {
        joinSet.set(node.value);
      }
      return new JoinPlan(ordMap, joinSet);
    }
  }

  /**
   * A set of nodes that workers add to concurrently. Adding a node happens-before any read that
   * finds it.
   */
  private static final class InsertedNodes {
    private final AtomicLongArray words;

    InsertedNodes(int maxOrd) {
      words = new AtomicLongArray(FixedBitSet.bits2words(maxOrd));
    }

    void add(int node) {
      words.accumulateAndGet(node >> 6, 1L << node, (a, b) -> a | b);
    }

    boolean contains(int node) {
      return (words.get(node >> 6) & (1L << node)) != 0;
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

  /**
   * Repairs the pruned graph's disconnected nodes across the worker pool, one level at a time from
   * the top down.
   *
   * @return the number of nodes repaired, counting a node once per level it was repaired on
   */
  private int repairDisconnectedNodes() throws IOException {
    int repairedNodes = 0;
    for (int level = prunedGraph.numLevels() - 1; level >= 0; level--) {
      IntsRef disconnectedNodes = prunedGraph.disconnectedNodesByLevel()[level];
      if (disconnectedNodes == null) {
        continue;
      }
      int total = disconnectedNodes.length;
      repairedNodes += total;
      // Use at most one task per worker and no more tasks than repair batches; each task
      // dynamically claims batches below.
      int taskCount = Math.min(workers.length, Math.ceilDiv(total, REPAIR_BATCH_SIZE));
      AtomicInteger repairProgress = new AtomicInteger(0);
      int repairLevel = level;
      List<Callable<Void>> tasks = new ArrayList<>(taskCount);
      for (int t = 0; t < taskCount; t++) {
        ConcurrentMergeWorker worker = workers[t];
        tasks.add(
            () -> {
              int from;
              while ((from = repairProgress.getAndAdd(REPAIR_BATCH_SIZE)) < total) {
                int length = Math.min(REPAIR_BATCH_SIZE, total - from);
                worker.fixDisconnectedNodes(
                    new IntsRef(disconnectedNodes.ints, disconnectedNodes.offset + from, length),
                    repairLevel,
                    worker.scorer);
              }
              return null;
            });
      }
      // Finish every repair task at this level before repairing the next lower level, because
      // addConnections descends through the already-repaired upper levels.
      taskExecutor.invokeAll(tasks);
    }
    return repairedNodes;
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

    /** This worker's own instance of the graph being joined, for {@link #addJoinedNode} */
    private HnswGraph joinedGraph;

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
    private void addJoinSetNode(int node, InsertedNodes inserted) throws IOException {
      // skip the initializedNodes check: the node is marked since it belongs to a graph being
      // joined
      super.addGraphNode(node);
      inserted.add(node);
    }

    /**
     * Adds a node of a graph being joined that is not in its join set. Like {@link
     * MergingHnswGraphBuilder}, the search on level 0 enters from the node's neighbors in that
     * graph that are already inserted, which includes its join set neighbors, and from their
     * neighbors in the merged graph.
     *
     * @param ordMap maps ordinals of the graph being joined to ordinals of the merged graph
     * @param u the node, as an ordinal of the graph being joined
     */
    private void addJoinedNode(int[] ordMap, int u, InsertedNodes inserted) throws IOException {
      eps.clear();
      joinedGraph.seek(0, u);
      for (int v = joinedGraph.nextNeighbor(); v != NO_MORE_DOCS; v = joinedGraph.nextNeighbor()) {
        int neighbor = ordMap[v];
        if (inserted.contains(neighbor) == false) {
          continue;
        }
        eps.add(neighbor);
        Lock lock = hnswLock.read(0, neighbor);
        try {
          NeighborArray neighbors = hnsw.getNeighbors(0, neighbor);
          for (int n = 0; n < neighbors.size(); n++) {
            eps.add(neighbors.nodes()[n]);
          }
        } finally {
          lock.unlock();
        }
      }
      int node = ordMap[u];
      addGraphNode(node, eps);
      inserted.add(node);
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
