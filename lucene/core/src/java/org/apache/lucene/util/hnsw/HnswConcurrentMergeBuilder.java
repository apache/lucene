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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * A graph builder that manages multiple workers, it only supports adding the whole graph all at
 * once. It will spawn a thread for each worker and the workers will pick the work in batches.
 */
public class HnswConcurrentMergeBuilder implements HnswBuilder {

  private static final int QUEUE_SIZE = 10000;

  private final TaskExecutor taskExecutor;
  private final ConcurrentMergeWorker[] workers;
  private final ConcurrentMergeWorker localWorker;
  private final ArrayBlockingQueue<AddNodeRequest> workQueue;
  private final AtomicBoolean finished;
  private final HnswLock hnswLock;
  private final GraphMergeContext graphMergeContext;
  private final OnHeapHnswGraph hnsw;
  private final int beamWidth;
  private InfoStream infoStream = InfoStream.getDefault();
  private boolean frozen;

  public HnswConcurrentMergeBuilder(
      TaskExecutor taskExecutor,
      int numWorker,
      RandomVectorScorerSupplier scorerSupplier,
      int beamWidth,
      int M,
      GraphMergeContext graphMergeContext)
      throws IOException {
    hnsw = initGraph(M, graphMergeContext);
    this.beamWidth = beamWidth;
    this.taskExecutor = taskExecutor;
    workers = new ConcurrentMergeWorker[numWorker];
    workQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    finished = new AtomicBoolean(false);
    this.graphMergeContext = graphMergeContext;
    hnswLock = new HnswLock();
    for (int i = 0; i < numWorker; i++) {
      workers[i] =
          new ConcurrentMergeWorker(
              scorerSupplier.copy(),
              beamWidth,
              HnswGraphBuilder.randSeed,
              hnsw,
              hnswLock,
              workQueue,
              finished
          );
    }
    localWorker = new ConcurrentMergeWorker(
        scorerSupplier.copy(),
        beamWidth,
        HnswGraphBuilder.randSeed,
        hnsw,
        hnswLock,
        workQueue,
        finished
    );
  }

  private static OnHeapHnswGraph initGraph(int M, GraphMergeContext graphMergeContext) throws IOException {
    if (graphMergeContext.initGraphs() == null || graphMergeContext.initGraphs().length == 0) {
      return new OnHeapHnswGraph(M, graphMergeContext.maxOrd());
    }
    return InitializedHnswGraphBuilder.initGraph(graphMergeContext.initGraphs()[0], graphMergeContext.oldToNewOrdinalMaps()[0], graphMergeContext.maxOrd());
  }

  @Override
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("graph has already been built");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          "build graph from " + maxOrd + " vectors, with " + workers.length + " workers");
    }
    List<Callable<Void>> futures = new ArrayList<>();
    for (int i = 0; i < workers.length; i++) {
      int finalI = i;
      futures.add(
          () -> {
            workers[finalI].run();
            return null;
          });
    }
    Future<List<Void>> future = taskExecutor.asyncInvokeAll(futures);

    HnswGraphSearcher graphSearcher = new MergeSearcher(
        new NeighborQueue(beamWidth, true), hnswLock, new FixedBitSet(hnsw.maxNodeId() + 1));

    if (graphMergeContext.initGraphs() != null) {
      for (int graphToWork = 1; graphToWork < graphMergeContext.initGraphs().length; graphToWork++) {
        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(
              HNSW_COMPONENT,
              "Starting join set merge for graph " + graphToWork);
        }

        HnswGraph sourceGraph = graphMergeContext.initGraphs()[graphToWork];
        int[] oldToNewOrd = graphMergeContext.oldToNewOrdinalMaps()[graphToWork];
        int size = sourceGraph.size();
        IntHashSet j = UpdateGraphsUtils.computeJoinSet(sourceGraph);

        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(
              HNSW_COMPONENT,
              "Done join set computation for graph " + graphToWork);
        }

        // for nodes that in the join set, add them directly to the graph
        for (IntCursor node : j) {
          addGraphNode(oldToNewOrd[node.value]);
        }

        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(
              HNSW_COMPONENT,
              "Done adding join set nodes for graph " + graphToWork + ", draining work queue");
        }

        drainWorkQueue();

        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(
              HNSW_COMPONENT,
              "Done draining work queue for graph " + graphToWork);
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
            // if u's neighbour v is in the join set
            // then we add v's neighbours from gL to the candidate list
            if (j.contains(v)) {
              int newv = oldToNewOrd[v];
              eps.add(newv);
              graphSearcher.graphSeek(hnsw, 0, newv);
              int friendOrd;
              while ((friendOrd = graphSearcher.graphNextNeighbor(hnsw)) != NO_MORE_DOCS) {
                eps.add(friendOrd);
              }
            }
          }
          addGraphNode(oldToNewOrd[u], eps);
        }

        // finished

        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          infoStream.message(
              HNSW_COMPONENT,
              "Done join set merge for graph " + graphToWork);
        }
      }
    }

    if (graphMergeContext.allInitialized() == false) {
      for (int node = 0; node < maxOrd; node++) {
        if (graphMergeContext.initializedNodes() == null || graphMergeContext.initializedNodes().get(node) == false) {
          addGraphNode(node);
        }
      }
    }

    drainWorkQueue();
    finished.set(true);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return getCompletedGraph();
  }

  @Override
  public void addGraphNode(int node) throws IOException {
    AddNodeRequest req = new AddNodeRequest(node, null);
    if (workQueue.offer(req) == false) {
      // queue full, then handle using the main thread
      localWorker.handleAddNodeRequest(req);
    }
  }

  @Override
  public void addGraphNode(int node, IntHashSet eps) throws IOException {
    AddNodeRequest req = new AddNodeRequest(node, eps);
    if (workQueue.offer(req) == false) {
      // queue full, then handle using the main thread
      localWorker.handleAddNodeRequest(req);
    }
  }

  private void drainWorkQueue() throws IOException {
    AddNodeRequest req = workQueue.poll();
    while (req != null) {
      localWorker.handleAddNodeRequest(req);
      req = workQueue.poll();
    }
  }

  @Override
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
    for (HnswBuilder worker : workers) {
      worker.setInfoStream(infoStream);
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
    return hnsw;
  }

  private static final class ConcurrentMergeWorker extends HnswGraphBuilder {

    private final ArrayBlockingQueue<AddNodeRequest> workingQueue;
    private final AtomicBoolean finished;

    private ConcurrentMergeWorker(
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        long seed,
        OnHeapHnswGraph hnsw,
        HnswLock hnswLock,
        ArrayBlockingQueue<AddNodeRequest> workingQueue,
        AtomicBoolean finished
        )
        throws IOException {
      super(
          scorerSupplier,
          beamWidth,
          seed,
          hnsw,
          hnswLock,
          new MergeSearcher(
              new NeighborQueue(beamWidth, true), hnswLock, new FixedBitSet(hnsw.maxNodeId() + 1)));
      this.finished = finished;
      this.workingQueue = workingQueue;
    }

    private void handleAddNodeRequest(AddNodeRequest req) throws IOException {
      if (req.eps == null) {
        addGraphNode(req.ord);
      } else {
        addGraphNode(req.ord, req.eps);
      }
    }

    /**
     * TODO: add javadoc
     */
    private void run() throws IOException {
      while (finished.get() == false) {
        try {
          AddNodeRequest req = workingQueue.poll(1, TimeUnit.SECONDS);
          while (req != null) {
            handleAddNodeRequest(req);
            req = workingQueue.poll(1, TimeUnit.SECONDS);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

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

  private record AddNodeRequest(int ord, IntHashSet eps){}
}
