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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.VectorUtil;

/**
 * Builds an HNSW graph without any graph search during construction, following PiPNN (Rubel, Wen,
 * Dhulipala, Gottesbüren, Jayaram, Łącki: "Search-Free Graph Construction for Nearest Neighbor
 * Search", KDD 2026).
 *
 * <p>Level 0 is built in three search-free passes:
 *
 * <ol>
 *   <li>{@link PipnnPartitioner randomized ball carving} splits the vectors into small overlapping
 *       leaves;
 *   <li>inside every leaf all pairs are scored and each vector's nearest in-leaf neighbors become
 *       candidate edges in both directions;
 *   <li>candidates stream into a per-node {@link PipnnCandidateReservoir reservoir} that keeps a
 *       bounded, direction-diverse subset, and a final pass selects the neighbors from the
 *       survivors with the HNSW diversity heuristic (generalized with {@link PipnnParams#alpha()}).
 * </ol>
 *
 * <p>The upper levels are then built with the regular HNSW insertion over the (few) nodes that are
 * promoted, so the resulting {@link OnHeapHnswGraph} has exactly the shape {@link
 * HnswGraphSearcher} expects and no reader or format change is required.
 *
 * <p>The build is deterministic for a given seed and parameters even when it runs on several
 * threads: the partition only depends on the seed and the reservoirs are history independent.
 *
 * <p>Unlike {@link HnswGraphBuilder}, the whole set of vectors must be known up front: {@link
 * #addGraphNode} is not supported.
 *
 * @lucene.experimental
 */
public final class PipnnGraphBuilder extends HnswGraphBuilder {

  /** Points assigned to leaders per unit of parallel work in the top level partitioning step. */
  private static final int ASSIGN_CHUNK = 1024;

  /** Nodes pruned per unit of parallel work. */
  private static final int PRUNE_CHUNK = 4096;

  private static final int MAX_BULK_SCORE_NODES = 8;

  /**
   * Leaf vectors scored against the rest of the leaf per blocked dot product call. 32 vectors of
   * 1024 dimensions are 128 KB, which stays in the L2 cache while the rest of the leaf streams by.
   */
  private static final int LEAF_ROW_BLOCK = 32;

  /** Points scored against all leaders in one kernel call during the top level assignment. */
  private static final int ASSIGN_BLOCK = 32;

  private final RandomVectorScorerSupplier scorerSupplier;
  private final KnnVectorValues vectors;
  private final VectorSimilarityFunction similarityFunction;
  private final PipnnParams params;
  private final int beamWidth;
  private final PipnnPartitioner partitioner;
  private final TaskExecutor taskExecutor;
  private final int numWorkers;
  private final long seed;
  private final int graphSize;
  private final int maxConn0;
  private final int reservoirSize;
  private final double ml;

  /**
   * Creates a builder for a graph over {@code graphSize} vectors.
   *
   * @param scorerSupplier scorer over the vectors, by ordinal
   * @param vectors the vectors, used to compute the direction hashes; must cover the same ordinals
   *     as the scorer
   * @param similarityFunction the similarity the scorer implements, used to apply {@link
   *     PipnnParams#alpha()} in the right domain; may be null, in which case alpha is treated as 1
   * @param M max connections per node on the upper levels, level 0 gets {@code 2 * M}
   * @param beamWidth beam width used for the upper levels
   * @param seed random seed; the build is reproducible for a given seed
   * @param graphSize number of vectors, which is also the only value accepted by {@link #build}
   * @param params PiPNN parameters
   * @param taskExecutor executor to run the search-free passes in parallel, or null to run them on
   *     the calling thread
   * @param numWorkers number of parallel workers when {@code taskExecutor} is not null
   */
  public static PipnnGraphBuilder create(
      RandomVectorScorerSupplier scorerSupplier,
      KnnVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      int graphSize,
      PipnnParams params,
      TaskExecutor taskExecutor,
      int numWorkers)
      throws IOException {
    return new PipnnGraphBuilder(
        scorerSupplier,
        vectors,
        similarityFunction,
        M,
        beamWidth,
        seed,
        graphSize,
        params,
        taskExecutor,
        numWorkers);
  }

  private PipnnGraphBuilder(
      RandomVectorScorerSupplier scorerSupplier,
      KnnVectorValues vectors,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      int graphSize,
      PipnnParams params,
      TaskExecutor taskExecutor,
      int numWorkers)
      throws IOException {
    super(scorerSupplier, M, beamWidth, seed, graphSize);
    if (graphSize < 0) {
      throw new IllegalArgumentException("graphSize must be known, got " + graphSize);
    }
    if (taskExecutor != null && numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be positive, got " + numWorkers);
    }
    this.scorerSupplier = scorerSupplier;
    this.vectors = vectors;
    this.similarityFunction = similarityFunction;
    this.params = params;
    this.beamWidth = beamWidth;
    this.partitioner =
        new PipnnPartitioner(
            params.leafSize(), params.fanouts(), params.maxLeaders(), params.leaderFraction());
    this.taskExecutor = taskExecutor;
    this.numWorkers = taskExecutor == null ? 1 : numWorkers;
    this.seed = seed;
    this.graphSize = graphSize;
    this.maxConn0 = 2 * M;
    this.reservoirSize =
        params.reservoirSize() == 0
            ? Math.min(2 * maxConn0, Byte.MAX_VALUE)
            : params.reservoirSize();
    this.ml = M == 1 ? 1 : 1 / Math.log(1.0 * M);
  }

  @Override
  public OnHeapHnswGraph build(int maxOrd) throws IOException {
    if (frozen) {
      throw new IllegalStateException("This PipnnGraphBuilder is frozen and cannot be updated");
    }
    if (maxOrd != graphSize) {
      throw new IllegalArgumentException(
          "maxOrd must equal the graph size " + graphSize + ", got " + maxOrd);
    }
    long startNs = System.nanoTime();
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          "PiPNN build graph from "
              + maxOrd
              + " vectors with "
              + numWorkers
              + " workers, "
              + params);
    }
    if (maxOrd > 0) {
      byte[] levels = addNodesOnAllLevels(maxOrd);
      HnswLock lock = numWorkers > 1 ? new HnswLock() : null;
      UpdateableRandomVectorScorer[] workerScorers = createWorkerScorers();

      long phaseNs = System.nanoTime();
      PipnnSketches sketches =
          params.hashBits() == 0
              ? null
              : PipnnSketches.compute(
                  vectors, maxOrd, params.hashBits(), seed + 1, taskExecutor, numWorkers);
      logPhase("sketches", phaseNs);

      phaseNs = System.nanoTime();
      PipnnCandidateReservoir reservoir =
          new PipnnCandidateReservoir(maxOrd, reservoirSize, sketches != null);
      collectCandidates(maxOrd, reservoir, sketches, lock, workerScorers);
      logPhase("candidates", phaseNs);

      phaseNs = System.nanoTime();
      pruneLevel0(maxOrd, reservoir, sketches, lock, workerScorers);
      logPhase("prune", phaseNs);

      phaseNs = System.nanoTime();
      buildUpperLevels(maxOrd, levels);
      logPhase("upper levels", phaseNs);
    }
    OnHeapHnswGraph graph = getCompletedGraph();
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          String.format(
              Locale.ROOT,
              "PiPNN build completed: %d vectors in %.2f ms",
              maxOrd,
              (System.nanoTime() - startNs) / 1_000_000.0));
    }
    return graph;
  }

  @Override
  public void addGraphNode(int node) {
    throw new UnsupportedOperationException("PipnnGraphBuilder builds the whole graph at once");
  }

  @Override
  public void addGraphNode(int node, IntHashSet eps) {
    throw new UnsupportedOperationException("PipnnGraphBuilder builds the whole graph at once");
  }

  /**
   * Draws the level of every node with the same distribution as {@link HnswGraphBuilder} and
   * registers the nodes on all their levels, top down as {@link OnHeapHnswGraph#addNode} requires.
   */
  private byte[] addNodesOnAllLevels(int maxOrd) {
    SplittableRandom random = new SplittableRandom(seed);
    byte[] levels = new byte[maxOrd];
    for (int node = 0; node < maxOrd; node++) {
      int level = Math.min(getRandomGraphLevel(ml, random), Byte.MAX_VALUE);
      levels[node] = (byte) level;
      for (int l = level; l >= 0; l--) {
        hnsw.addNode(l, node);
      }
    }
    return levels;
  }

  private UpdateableRandomVectorScorer[] createWorkerScorers() throws IOException {
    UpdateableRandomVectorScorer[] scorers = new UpdateableRandomVectorScorer[numWorkers];
    scorers[0] = scorer;
    for (int i = 1; i < numWorkers; i++) {
      scorers[i] = scorerSupplier.copy().scorer();
    }
    return scorers;
  }

  /** Partitions the vectors and feeds every leaf's nearest-neighbor edges into the reservoir. */
  private void collectCandidates(
      int maxOrd,
      PipnnCandidateReservoir reservoir,
      PipnnSketches sketches,
      HnswLock lock,
      UpdateableRandomVectorScorer[] workerScorers)
      throws IOException {
    int[] all = new int[maxOrd];
    for (int i = 0; i < maxOrd; i++) {
      all[i] = i;
    }
    if (maxOrd <= params.leafSize()) {
      new LeafProcessor(workerScorers[0], reservoir, sketches, lock).accept(all, maxOrd);
      return;
    }
    // Top level of the partition: assign points to leaders in parallel chunks.
    final int numLeaders = partitioner.numLeaders(maxOrd);
    final int fanout = partitioner.fanout(0, numLeaders);
    final int[] leaders =
        PipnnPartitioner.sampleLeaders(all, maxOrd, numLeaders, new SplittableRandom(seed + 2));
    final int[] assignments = new int[Math.multiplyExact(maxOrd, fanout)];
    final AtomicInteger nextPoint = new AtomicInteger();
    long assignNs = System.nanoTime();
    final GatheredVectors leaderVectors = GatheredVectors.create(this, numLeaders);
    if (leaderVectors != null) {
      leaderVectors.gather(leaders, 0, numLeaders);
    }
    runWorkers(
        workerScorers,
        scorer -> {
          LeaderAssigner assigner =
              leaderVectors == null ? null : new LeaderAssigner(leaderVectors, numLeaders, fanout);
          float[] scores = assigner == null ? new float[numLeaders] : null;
          int from;
          while ((from = nextPoint.getAndAdd(ASSIGN_CHUNK)) < maxOrd) {
            maybeAbort();
            int to = Math.min(maxOrd, from + ASSIGN_CHUNK);
            if (assigner != null) {
              assigner.assign(all, from, to, numLeaders, fanout, assignments, from * fanout);
            } else {
              PipnnPartitioner.assign(
                  all, from, to, leaders, fanout, scorer, assignments, from * fanout, scores);
            }
          }
        });
    logPhase("  top level assign", assignNs);
    final PipnnPartitioner.Clusters clusters =
        PipnnPartitioner.toClusters(all, maxOrd, numLeaders, fanout, assignments);
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      int largest = 0;
      for (int c = 0; c < clusters.count(); c++) {
        largest = Math.max(largest, clusters.size(c));
      }
      infoStream.message(
          HNSW_COMPONENT,
          "PiPNN top level: " + clusters.count() + " clusters, largest " + largest + " points");
    }
    // Carve every top level cluster recursively on its own worker.
    final long clusterSeed = new SplittableRandom(seed + 3).nextLong();
    final AtomicInteger nextCluster = new AtomicInteger();
    runWorkers(
        workerScorers,
        scorer -> {
          LeafProcessor leafProcessor = new LeafProcessor(scorer, reservoir, sketches, lock);
          int cluster;
          while ((cluster = nextCluster.getAndIncrement()) < clusters.count()) {
            maybeAbort();
            int size = clusters.size(cluster);
            if (size == 0) {
              continue;
            }
            int[] members =
                ArrayUtil.copyOfSubArray(
                    clusters.members(),
                    clusters.offsets()[cluster],
                    clusters.offsets()[cluster + 1]);
            partitioner.partition(members, size, 1, clusterSeed + cluster, scorer, leafProcessor);
          }
        });
  }

  /**
   * Selects the level 0 neighbors of every node in two rounds. The first round prunes each node's
   * own candidates with the diversity heuristic. The second round adds the reverse edges: a node
   * accepts every node that selected it while it has room, and only prunes when its list overflows,
   * which is how {@link HnswGraphBuilder} fills neighbor lists as well. In-links are gathered
   * through a second {@link PipnnCandidateReservoir} so the result stays independent of scheduling.
   */
  private void pruneLevel0(
      int maxOrd,
      PipnnCandidateReservoir forward,
      PipnnSketches sketches,
      HnswLock lock,
      UpdateableRandomVectorScorer[] workerScorers)
      throws IOException {
    forEachNodeChunk(
        maxOrd,
        workerScorers,
        scorer -> {
          Pruner pruner = new Pruner(scorer, forward.capacity());
          return node -> pruner.selectForward(node, forward);
        });
    PipnnCandidateReservoir reverse =
        new PipnnCandidateReservoir(maxOrd, forward.capacity(), sketches != null);
    forEachNodeChunk(
        maxOrd,
        workerScorers,
        _ ->
            node -> {
              NeighborArray neighbors = hnsw.getNeighbors(0, node);
              for (int i = 0; i < neighbors.size(); i++) {
                int neighbor = neighbors.nodes()[i];
                int hash = sketches == null ? 0 : sketches.hash(neighbor, node);
                offer(reverse, lock, neighbor, node, hash, neighbors.getScores(i));
              }
            });
    forEachNodeChunk(
        maxOrd,
        workerScorers,
        scorer -> {
          Pruner pruner = new Pruner(scorer, forward.capacity());
          return node -> pruner.addReverse(node, reverse);
        });
  }

  private static void offer(
      PipnnCandidateReservoir reservoir,
      HnswLock lock,
      int node,
      int candidate,
      int hash,
      float score) {
    if (lock == null) {
      reservoir.offer(node, candidate, hash, score);
      return;
    }
    Lock l = lock.write(0, node);
    try {
      reservoir.offer(node, candidate, hash, score);
    } finally {
      l.unlock();
    }
  }

  @FunctionalInterface
  private interface NodeTask {
    void run(int node) throws IOException;
  }

  @FunctionalInterface
  private interface NodeTaskFactory {
    NodeTask create(UpdateableRandomVectorScorer scorer);
  }

  /** Runs a per-node task over all nodes, in chunks claimed dynamically by the workers. */
  private void forEachNodeChunk(
      int maxOrd, UpdateableRandomVectorScorer[] workerScorers, NodeTaskFactory factory)
      throws IOException {
    final AtomicInteger nextNode = new AtomicInteger();
    runWorkers(
        workerScorers,
        scorer -> {
          NodeTask task = factory.create(scorer);
          int from;
          while ((from = nextNode.getAndAdd(PRUNE_CHUNK)) < maxOrd) {
            maybeAbort();
            int to = Math.min(maxOrd, from + PRUNE_CHUNK);
            for (int node = from; node < to; node++) {
              task.run(node);
            }
          }
        });
  }

  /**
   * Builds the levels above 0 with the regular HNSW insertion, restricted to the promoted nodes.
   * With an executor the promoted nodes are inserted concurrently, the way {@link
   * HnswConcurrentMergeBuilder} does, so the upper levels then depend on the scheduling while level
   * 0 does not.
   */
  private void buildUpperLevels(int maxOrd, byte[] levels) throws IOException {
    int[] promoted = new int[maxOrd];
    int promotedCount = 0;
    for (int node = 0; node < maxOrd; node++) {
      if (levels[node] > 0) {
        promoted[promotedCount++] = node;
      }
    }
    final int count = promotedCount;
    if (count == 0) {
      hnsw.trySetNewEntryNode(0, 0);
      return;
    }
    if (taskExecutor == null || count < 2 * UPPER_LEVEL_CHUNK) {
      for (int i = 0; i < count; i++) {
        int node = promoted[i];
        maybeAbort();
        scorer.setScoringOrdinal(node);
        connectNode(node, levels[node], 1, scorer, null);
      }
      return;
    }
    final HnswLock lock = new HnswLock();
    final AtomicInteger next = new AtomicInteger();
    List<Callable<Void>> callables = new ArrayList<>(numWorkers);
    for (int w = 0; w < numWorkers; w++) {
      callables.add(
          () -> {
            UpperLevelWorker worker = new UpperLevelWorker(lock);
            int from;
            while ((from = next.getAndAdd(UPPER_LEVEL_CHUNK)) < count) {
              maybeAbort();
              int to = Math.min(count, from + UPPER_LEVEL_CHUNK);
              for (int i = from; i < to; i++) {
                worker.connect(promoted[i], levels[promoted[i]]);
              }
            }
            return null;
          });
    }
    taskExecutor.invokeAll(callables);
  }

  /** Promoted nodes inserted per unit of parallel work in the upper levels. */
  private static final int UPPER_LEVEL_CHUNK = 64;

  /**
   * A builder over the shared graph with its own scorer and search state, inserting nodes on the
   * levels above 0 under the shared {@link HnswLock}.
   */
  private final class UpperLevelWorker extends HnswGraphBuilder {
    UpperLevelWorker(HnswLock lock) throws IOException {
      super(
          // own copy: scorer suppliers need not be thread safe across their scorers
          PipnnGraphBuilder.this.scorerSupplier.copy(),
          PipnnGraphBuilder.this.beamWidth,
          PipnnGraphBuilder.this.seed,
          PipnnGraphBuilder.this.hnsw,
          lock,
          new HnswConcurrentMergeBuilder.MergeSearcher(
              new NeighborQueue(PipnnGraphBuilder.this.beamWidth, true),
              lock,
              new FixedBitSet(PipnnGraphBuilder.this.hnsw.maxNodeId() + 1)));
    }

    void connect(int node, int level) throws IOException {
      scorer.setScoringOrdinal(node);
      connectNode(node, level, 1, scorer, null);
    }
  }

  @Override
  void finish() throws IOException {
    if (hnsw.size() > 0) {
      long startNs = System.nanoTime();
      boolean connected = connectComponents(0);
      if (infoStream.isEnabled(HNSW_COMPONENT)) {
        infoStream.message(
            HNSW_COMPONENT,
            String.format(
                Locale.ROOT,
                "PiPNN connectComponents on level 0: %s in %.2f ms",
                connected ? "ok" : "some components left unconnected",
                (System.nanoTime() - startNs) / 1_000_000.0));
      }
    }
    frozen = true;
  }

  private void logPhase(String phase, long startNs) {
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(
          HNSW_COMPONENT,
          String.format(
              Locale.ROOT,
              "PiPNN %s: %.2f ms",
              phase,
              (System.nanoTime() - startNs) / 1_000_000.0));
    }
  }

  @FunctionalInterface
  private interface WorkerTask {
    void run(UpdateableRandomVectorScorer scorer) throws IOException;
  }

  private void runWorkers(UpdateableRandomVectorScorer[] workerScorers, WorkerTask task)
      throws IOException {
    if (taskExecutor == null) {
      task.run(workerScorers[0]);
      return;
    }
    List<Callable<Void>> callables = new ArrayList<>(numWorkers);
    for (int i = 0; i < numWorkers; i++) {
      final int worker = i;
      callables.add(
          () -> {
            task.run(workerScorers[worker]);
            return null;
          });
    }
    taskExecutor.invokeAll(callables);
  }

  /**
   * Vectors gathered into one contiguous padded float array so that many pairs can be scored at
   * once with {@link VectorUtil#dotProducts}, together with the per vector terms that turn a raw
   * dot product into the configured similarity's score. Only float vectors are supported; other
   * encodings go through the scorer instead.
   */
  private final class GatheredVectors {
    private final FloatVectorValues floatVectors;
    final int dim;
    // distance between consecutive vectors, padded off a multiple of 1024 floats (4 KB) so the
    // rows of a tile do not map to the same cache sets
    final int stride;
    float[] vectors;
    private float[] norms;

    /** Returns null when the encoding cannot be gathered. */
    static GatheredVectors create(PipnnGraphBuilder builder, int capacity) throws IOException {
      KnnVectorValues v = builder.vectors;
      if (v.getEncoding() == VectorEncoding.FLOAT32) {
        return builder.new GatheredVectors(((FloatVectorValues) v).copy(), capacity);
      }
      return null;
    }

    private GatheredVectors(FloatVectorValues floatVectors, int capacity) throws IOException {
      this.floatVectors = floatVectors;
      this.dim = PipnnGraphBuilder.this.vectors.dimension();
      this.stride = dim % 1024 == 0 ? dim + 16 : dim;
      ensureCapacity(capacity);
    }

    void ensureCapacity(int size) {
      if (vectors != null && size * stride <= vectors.length) {
        return;
      }
      vectors = new float[size * stride];
      norms = new float[size];
    }

    /** Gathers the vectors of {@code ords[from, to)} into rows {@code 0, to - from}. */
    void gather(int[] ords, int from, int to) throws IOException {
      int size = to - from;
      ensureCapacity(size);
      boolean needNorms =
          similarityFunction == VectorSimilarityFunction.EUCLIDEAN
              || similarityFunction == VectorSimilarityFunction.COSINE;
      for (int i = 0; i < size; i++) {
        float[] vector = floatVectors.vectorValue(ords[from + i]);
        System.arraycopy(vector, 0, vectors, i * stride, dim);
        if (needNorms) {
          norms[i] = VectorUtil.dotProduct(vector, vector);
        }
      }
    }

    /**
     * Turns the raw dot product of this set's row {@code i} and {@code other}'s row {@code j} into
     * the score the configured similarity function would return.
     */
    float score(int i, GatheredVectors other, int j, float dot) {
      switch (similarityFunction) {
        case DOT_PRODUCT:
          return VectorUtil.normalizeToUnitInterval(dot);
        case MAXIMUM_INNER_PRODUCT:
          return VectorUtil.scaleMaxInnerProductScore(dot);
        case COSINE:
          return VectorUtil.normalizeToUnitInterval(
              (float) (dot / Math.sqrt((double) norms[i] * (double) other.norms[j])));
        case EUCLIDEAN:
          float squareDistance = Math.max(0, norms[i] + other.norms[j] - 2 * dot);
          return VectorUtil.normalizeDistanceToUnitInterval(squareDistance);
      }
      throw new AssertionError("unknown similarity " + similarityFunction);
    }
  }

  /**
   * Assigns points to their nearest leaders with the blocked kernel: the leaders are gathered once
   * and every block of points is scored against all of them in one call, so a panel of leaders is
   * reused from cache across the block instead of being streamed once per point.
   */
  private final class LeaderAssigner {
    private final GatheredVectors leaders;
    private final GatheredVectors points;
    private final float[] block;
    private final float[] scores;
    private final int[] best;
    private final float[] bestScores;

    LeaderAssigner(GatheredVectors leaders, int numLeaders, int fanout) throws IOException {
      this.leaders = leaders;
      this.points = GatheredVectors.create(PipnnGraphBuilder.this, ASSIGN_BLOCK);
      this.block = new float[ASSIGN_BLOCK * numLeaders];
      this.scores = new float[numLeaders];
      this.best = new int[fanout];
      this.bestScores = new float[fanout];
    }

    /** Same contract as {@link PipnnPartitioner#assign}. */
    void assign(
        int[] ords, int from, int to, int numLeaders, int fanout, int[] assignments, int offset)
        throws IOException {
      int out = offset;
      for (int b0 = from; b0 < to; b0 += ASSIGN_BLOCK) {
        int rows = Math.min(ASSIGN_BLOCK, to - b0);
        points.gather(ords, b0, b0 + rows);
        VectorUtil.dotProducts(
            points.vectors,
            0,
            rows,
            leaders.vectors,
            0,
            numLeaders,
            points.dim,
            points.stride,
            block);
        for (int r = 0; r < rows; r++) {
          int rowOffset = r * numLeaders;
          for (int l = 0; l < numLeaders; l++) {
            scores[l] = points.score(r, leaders, l, block[rowOffset + l]);
          }
          PipnnPartitioner.selectTop(scores, numLeaders, fanout, best, bestScores);
          System.arraycopy(best, 0, assignments, out, fanout);
          out += fanout;
        }
      }
    }
  }

  /**
   * Scores all pairs of a leaf and offers every node's nearest in-leaf neighbors to the reservoir,
   * in both directions.
   */
  private final class LeafProcessor implements PipnnPartitioner.LeafConsumer {
    private final UpdateableRandomVectorScorer scorer;
    private final PipnnCandidateReservoir reservoir;
    private final PipnnSketches sketches;
    private final HnswLock lock;
    private final int k;
    private final float[] scores;
    private final int[] tail;
    // per member of the current leaf: its best k in-leaf neighbors (leaf positions), sorted by
    // descending score, laid out as k consecutive slots
    private int[] best;
    private float[] bestScores;
    // the leaf's vectors gathered for the blocked kernel; null for encodings that go through the
    // scorer
    private final GatheredVectors leaf;
    private float[] block;

    LeafProcessor(
        UpdateableRandomVectorScorer scorer,
        PipnnCandidateReservoir reservoir,
        PipnnSketches sketches,
        HnswLock lock)
        throws IOException {
      this.scorer = scorer;
      this.reservoir = reservoir;
      this.sketches = sketches;
      this.lock = lock;
      this.k = params.leafNeighbors();
      this.scores = new float[params.leafSize()];
      this.tail = new int[params.leafSize()];
      this.best = new int[params.leafSize() * k];
      this.bestScores = new float[params.leafSize() * k];
      this.leaf = GatheredVectors.create(PipnnGraphBuilder.this, params.leafSize());
      this.block = leaf == null ? null : new float[LEAF_ROW_BLOCK * params.leafSize()];
    }

    /**
     * Every similarity function is symmetric, so each pair is scored once: member {@code i} is
     * scored against the members after it and the score feeds the top-k of both endpoints.
     */
    @Override
    public void accept(int[] members, int size) throws IOException {
      int kk = Math.min(k, size - 1);
      if (kk <= 0) {
        return;
      }
      if (size * k > best.length) {
        best = new int[size * k];
        bestScores = new float[size * k];
      }
      Arrays.fill(bestScores, 0, size * k, Float.NEGATIVE_INFINITY);
      if (leaf != null) {
        scoreFloatPairs(members, size);
      } else {
        scorePairs(members, size);
      }
      for (int i = 0; i < size; i++) {
        int node = members[i];
        int base = i * k;
        for (int j = 0; j < kk; j++) {
          int neighbor = members[best[base + j]];
          float score = bestScores[base + j];
          offer(reservoir, lock, node, neighbor, hash(node, neighbor), score);
          offer(reservoir, lock, neighbor, node, hash(neighbor, node), score);
        }
      }
    }

    /** Scores the pairs through the scorer, one member against all later members at a time. */
    private void scorePairs(int[] members, int size) throws IOException {
      for (int i = 0; i < size - 1; i++) {
        int n = size - i - 1;
        System.arraycopy(members, i + 1, tail, 0, n);
        scorer.setScoringOrdinal(members[i]);
        scorer.bulkScore(tail, scores, n);
        for (int j = 0; j < n; j++) {
          float score = scores[j];
          insertBest(i, i + 1 + j, score);
          insertBest(i + 1 + j, i, score);
        }
      }
    }

    /**
     * Gathers the leaf's vectors and scores the pairs with {@link VectorUtil#dotProducts}, in row
     * blocks small enough to stay in cache while they are scored against the rest of the leaf.
     */
    private void scoreFloatPairs(int[] members, int size) throws IOException {
      leaf.gather(members, 0, size);
      if (LEAF_ROW_BLOCK * size > block.length) {
        block = new float[LEAF_ROW_BLOCK * size];
      }
      float[] vecs = leaf.vectors;
      int stride = leaf.stride;
      for (int r0 = 0; r0 < size - 1; r0 += LEAF_ROW_BLOCK) {
        int rows = Math.min(LEAF_ROW_BLOCK, size - r0);
        int cols = size - r0;
        VectorUtil.dotProducts(
            vecs, r0 * stride, rows, vecs, r0 * stride, cols, leaf.dim, stride, block);
        for (int r = 0; r < rows; r++) {
          int i = r0 + r;
          int rowOffset = r * cols - r0;
          for (int j = i + 1; j < size; j++) {
            float score = leaf.score(i, leaf, j, block[rowOffset + j]);
            insertBest(i, j, score);
            insertBest(j, i, score);
          }
        }
      }
    }

    /**
     * Inserts {@code other} into the sorted top-k of leaf member {@code i} if it is competitive.
     */
    private void insertBest(int i, int other, float score) {
      int base = i * k;
      int pos = base + k - 1;
      if (score <= bestScores[pos]) {
        return;
      }
      while (pos > base && bestScores[pos - 1] < score) {
        bestScores[pos] = bestScores[pos - 1];
        best[pos] = best[pos - 1];
        pos--;
      }
      bestScores[pos] = score;
      best[pos] = other;
    }

    private int hash(int from, int to) {
      return sketches == null ? 0 : sketches.hash(from, to);
    }
  }

  /** Turns a node's reservoirs into its level 0 neighbor list. */
  private final class Pruner {
    private final UpdateableRandomVectorScorer scorer;
    private final int[] candidates;
    private final float[] candidateScores;
    private final int[] selected;
    private final int[] bulkScoreNodes = new int[MAX_BULK_SCORE_NODES];
    private final float[] bulkScores = new float[MAX_BULK_SCORE_NODES];

    Pruner(UpdateableRandomVectorScorer scorer, int capacity) {
      this.scorer = scorer;
      this.candidates = new int[capacity];
      this.candidateScores = new float[capacity];
      this.selected = new int[maxConn0];
    }

    /** Fills the node's empty neighbor list with the diverse subset of its own candidates. */
    void selectForward(int node, PipnnCandidateReservoir reservoir) throws IOException {
      int count = loadSorted(node, reservoir);
      NeighborArray neighbors = hnsw.getNeighbors(0, node);
      int numSelected = 0;
      for (int i = 0; i < count && numSelected < maxConn0; i++) {
        int candidate = candidates[i];
        float score = candidateScores[i];
        scorer.setScoringOrdinal(candidate);
        if (isDiverse(pruneThreshold(score), numSelected)) {
          neighbors.addInOrder(candidate, score);
          selected[numSelected++] = candidate;
        }
      }
    }

    /**
     * Adds the nodes that selected this node as a neighbor, pruning with the diversity heuristic
     * only once the list is full, like {@link NeighborArray#addAndEnsureDiversity}.
     */
    void addReverse(int node, PipnnCandidateReservoir reservoir) throws IOException {
      int count = loadSorted(node, reservoir);
      NeighborArray neighbors = hnsw.getNeighbors(0, node);
      for (int i = 0; i < count; i++) {
        int candidate = candidates[i];
        if (contains(neighbors, candidate) == false) {
          neighbors.addAndEnsureDiversity(candidate, candidateScores[i], node, scorer);
        }
      }
    }

    /** Loads and scores the node's candidates, sorted by descending similarity. */
    private int loadSorted(int node, PipnnCandidateReservoir reservoir) throws IOException {
      int count = reservoir.candidates(node, candidates);
      if (count == 0) {
        return 0;
      }
      scorer.setScoringOrdinal(node);
      scorer.bulkScore(candidates, candidateScores, count);
      sortByScoreDescending(candidates, candidateScores, count);
      return count;
    }

    /**
     * Returns true if the current scoring ordinal is not closer than {@code threshold} to any of
     * the already selected neighbors.
     */
    private boolean isDiverse(float threshold, int numSelected) throws IOException {
      for (int scored = 0; scored < numSelected; scored += MAX_BULK_SCORE_NODES) {
        int chunk = Math.min(MAX_BULK_SCORE_NODES, numSelected - scored);
        System.arraycopy(selected, scored, bulkScoreNodes, 0, chunk);
        if (scorer.bulkScore(bulkScoreNodes, bulkScores, chunk) >= threshold) {
          return false;
        }
      }
      return true;
    }
  }

  private static boolean contains(NeighborArray neighbors, int node) {
    int[] nodes = neighbors.nodes();
    for (int i = 0; i < neighbors.size(); i++) {
      if (nodes[i] == node) {
        return true;
      }
    }
    return false;
  }

  /**
   * Translates the alpha pruning rule, {@code alpha * d(selected, candidate) <= d(node, candidate)}
   * in distance space, into a similarity threshold: the candidate is pruned if its similarity to a
   * selected neighbor is at least the returned value. With {@code alpha == 1} this is the plain
   * HNSW heuristic, independent of the similarity function.
   */
  float pruneThreshold(float scoreToNode) {
    float alpha = params.alpha();
    if (alpha == 1f || similarityFunction == null) {
      return scoreToNode;
    }
    switch (similarityFunction) {
      case EUCLIDEAN:
        {
          // score = 1 / (1 + d^2)  =>  d^2 = 1 / score - 1
          double d2 = 1.0 / scoreToNode - 1.0;
          return (float) (1.0 / (1.0 + d2 / ((double) alpha * alpha)));
        }
      case DOT_PRODUCT:
      case COSINE:
        {
          // score = (1 + dot) / 2  =>  1 - dot = 2 * (1 - score), which is a distance
          return (float) (1.0 - (1.0 - scoreToNode) / alpha);
        }
      case MAXIMUM_INNER_PRODUCT:
      default:
        // no distance interpretation; fall back to the standard heuristic
        return scoreToNode;
    }
  }

  /** Insertion sort of two parallel arrays by descending score; the arrays are small. */
  static void sortByScoreDescending(int[] nodes, float[] scores, int count) {
    for (int i = 1; i < count; i++) {
      int node = nodes[i];
      float score = scores[i];
      int j = i - 1;
      while (j >= 0 && scores[j] < score) {
        nodes[j + 1] = nodes[j];
        scores[j + 1] = scores[j];
        j--;
      }
      nodes[j + 1] = node;
      scores[j + 1] = score;
    }
  }
}
