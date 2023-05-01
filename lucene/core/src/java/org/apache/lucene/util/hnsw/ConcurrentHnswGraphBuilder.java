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

import static java.lang.Math.log;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.AtomicBitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.hnsw.ConcurrentOnHeapHnswGraph.NodeAtLevel;

/**
 * Builder for Concurrent HNSW graph. See {@link HnswGraph} for a high level overview, and the
 * comments to `addGraphNode` for details on the concurrent building approach.
 *
 * @param <T> the type of vector
 */
public final class ConcurrentHnswGraphBuilder<T> {

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  /** A name for the HNSW component for the info-stream * */
  public static final String HNSW_COMPONENT = "HNSW";

  private final int beamWidth;
  private final double ml;
  private final ThreadLocal<NeighborArray> scratchNeighbors;

  private final VectorSimilarityFunction similarityFunction;
  private final boolean parallelBuild;
  private final VectorEncoding vectorEncoding;
  private final RandomAccessVectorValues<T> vectors;
  private final ThreadLocal<HnswGraphSearcher<T>> graphSearcher;

  final ConcurrentOnHeapHnswGraph hnsw;
  private final ConcurrentSkipListSet<NodeAtLevel> insertionsInProgress =
      new ConcurrentSkipListSet<>();

  private InfoStream infoStream = InfoStream.getDefault();

  // we need two sources of vectors in order to perform diversity check comparisons without
  // colliding
  private final RandomAccessVectorValues<T> vectorsCopy;
  private final AtomicBitSet initializedNodes;

  /**
   * This factory matches HnswGraphBuilder's signature for convenience. "_seed" is ignored since the
   * Concurrent classes use ThreadLocalRandom. Building will be done in parallel.
   */
  public static <T> ConcurrentHnswGraphBuilder<T> create(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long _seed)
      throws IOException {
    return new ConcurrentHnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth, true);
  }

  /** This is the "native" factory for ConcurrentHnswGraphBuilder. */
  public static <T> ConcurrentHnswGraphBuilder<T> create(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      boolean parallelize)
      throws IOException {
    return new ConcurrentHnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth, parallelize);
  }

  /**
   * Reads all the vectors from vector values, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param vectors the vectors whose relations are represented by the graph - must provide a
   *     different view over those vectors than the one used to add via addGraphNode.
   * @param M – graph fanout parameter used to calculate the maximum number of connections a node
   *     can have – M on upper layers, and M * 2 on the lowest level.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param parallelize use multiple threads to build the graph.
   */
  private ConcurrentHnswGraphBuilder(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      boolean parallelize)
      throws IOException {
    this.vectors = vectors;
    this.vectorsCopy = vectors.copy();
    this.vectorEncoding = Objects.requireNonNull(vectorEncoding);
    this.similarityFunction = Objects.requireNonNull(similarityFunction);
    this.parallelBuild = parallelize;
    if (M <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.beamWidth = beamWidth;
    // normalization factor for level generation; currently not configurable
    this.ml = M == 1 ? 1 : 1 / Math.log(1.0 * M);
    this.hnsw = new ConcurrentOnHeapHnswGraph(M);
    this.graphSearcher =
        ThreadLocal.withInitial(
            () -> {
              return new HnswGraphSearcher<>(
                  vectorEncoding,
                  similarityFunction,
                  new NeighborQueue(beamWidth, true),
                  new FixedBitSet(this.vectors.size()));
            });
    // in scratch we store candidates in reverse order: worse candidates are first
    scratchNeighbors =
        ThreadLocal.withInitial(() -> new NeighborArray(Math.max(beamWidth, M + 1), false));
    this.initializedNodes = new AtomicBitSet(vectors.size());
  }

  /**
   * Reads all the vectors from two copies of a {@link RandomAccessVectorValues}. Providing two
   * copies enables efficient retrieval without extra data copying, while avoiding collision of the
   * returned values.
   *
   * @param vectorsToAdd the vectors for which to build a nearest neighbors graph. Must be an
   *     independent accessor for the vectors
   */
  public ConcurrentOnHeapHnswGraph build(RandomAccessVectorValues<T> vectorsToAdd)
      throws IOException {
    if (vectorsToAdd == this.vectors) {
      throw new IllegalArgumentException(
          "Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + vectorsToAdd.size() + " vectors");
    }
    addVectors(vectorsToAdd);
    return hnsw;
  }

  private void addVectors(RandomAccessVectorValues<T> vectorsToAdd) throws IOException {
    var stream = IntStream.range(0, vectorsToAdd.size());
    if (parallelBuild) { // TODO
      stream = stream.parallel();
    }
    stream.forEach(
        node -> {
          try {
            addGraphNode(node, vectorsToAdd);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Set info-stream to output debugging information * */
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  public ConcurrentOnHeapHnswGraph getGraph() {
    return hnsw;
  }

  /**
   * Inserts a doc with vector value to the graph.
   *
   * <p>To allow correctness under concurrency, we track in-progress updates in a
   * ConcurrentSkipListSet. After adding ourselves, we take a snapshot of this set, and consider all
   * other in-progress updates as neighbor candidates (subject to normal level constraints).
   */
  public void addGraphNode(int node, T value) throws IOException {
    if (initializedNodes.getAndSet(node)) {
      return; // already initialized
    }

    // do this before adding to in-progress, so a concurrent writer checking
    // the in-progress set doesn't have to worry about uninitialized neighbor sets
    final int nodeLevel = getRandomGraphLevel(ml);
    for (int level = nodeLevel; level >= 0; level--) {
      hnsw.addNode(level, node);
    }

    var progressMarker = new NodeAtLevel(nodeLevel, node);
    insertionsInProgress.add(progressMarker);
    var inProgressBefore = insertionsInProgress.clone();
    try {
      NeighborQueue candidates;
      int curMaxLevel = hnsw.numLevels() - 1;

      // find ANN of the new node by searching the graph
      int ep = hnsw.entryNode();
      int[] eps = ep >= 0 ? new int[] {ep} : new int[0];
      // for levels > nodeLevel search with topk = 1
      for (int level = curMaxLevel; level > nodeLevel; level--) {
        candidates = graphSearcher.get().searchLevel(value, 1, level, eps, vectors, hnsw.getView());
        eps = new int[] {candidates.pop()};
      }
      // for levels <= nodeLevel search with topk = beamWidth, and add connections
      for (int level = Math.min(nodeLevel, curMaxLevel); level >= 0; level--) {
        // find best candidates at this level with a beam search
        candidates =
            graphSearcher.get().searchLevel(value, beamWidth, level, eps, vectors, hnsw.getView());
        // any nodes that are being added concurrently at this level are also candidates
        for (var concurrentCandidate : inProgressBefore) {
          if (concurrentCandidate.level < level || concurrentCandidate == progressMarker) {
            continue;
          }
          float score = scoreBetween(value, vectors.vectorValue(concurrentCandidate.node));
          candidates.add(concurrentCandidate.node, score);
          if (candidates.size() > beamWidth) {
            candidates.pop();
          }
        }
        // update entry points and neighbors with these candidates
        eps = candidates.nodes();
        addDiverseNeighbors(level, node, candidates);
      }

      // update entry node last, once everything is wired together
      hnsw.maybeUpdateEntryNode(nodeLevel, node);
    } finally {
      insertionsInProgress.remove(progressMarker);
    }
  }

  public void addGraphNode(int node, RandomAccessVectorValues<T> values) throws IOException {
    addGraphNode(node, values.vectorValue(node));
  }

  private long printGraphBuildStatus(int node, long start, long t) {
    long now = System.nanoTime();
    infoStream.message(
        HNSW_COMPONENT,
        String.format(
            Locale.ROOT,
            "built %d in %d/%d ms",
            node,
            TimeUnit.NANOSECONDS.toMillis(now - t),
            TimeUnit.NANOSECONDS.toMillis(now - start)));
    return now;
  }

  private void addDiverseNeighbors(int level, int newNode, NeighborQueue candidates) {
    // Add links from new node -> candidates.
    // See ConcurrentNeighborSet for an explanation of "diverse."
    var neighbors = hnsw.getNeighbors(level, newNode);
    var scratch = popToScratch(candidates);
    neighbors.insertDiverse(scratch, this::scoreBetween);

    // Add links from candidates -> new node (again applying diversity heuristic)
    neighbors.forEach(
        (nbr, nbrScore) -> {
          var nbrNbr = hnsw.getNeighbors(level, nbr);
          nbrNbr.insert(newNode, nbrScore, this::scoreBetween);
        });
  }

  private NeighborArray popToScratch(NeighborQueue candidates) {
    var scratch = this.scratchNeighbors.get();
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float maxSimilarity = candidates.topScore();
      scratch.add(candidates.pop(), maxSimilarity);
    }
    return scratch;
  }

  private float scoreBetween(int i, int j) {
    try {
      final T v1 = vectors.vectorValue(i);
      final T v2 = vectors.vectorValue(j);
      return scoreBetween(v1, v2);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private float scoreBetween(T v1, T v2) {
    return switch (vectorEncoding) {
      case BYTE -> similarityFunction.compare((byte[]) v1, (byte[]) v2);
      case FLOAT32 -> similarityFunction.compare((float[]) v1, (float[]) v2);
    };
  }

  private static int getRandomGraphLevel(double ml) {
    double randDouble;
    do {
      randDouble =
          ThreadLocalRandom.current().nextDouble(); // avoid 0 value, as log(0) is undefined
    } while (randDouble == 0.0);
    return ((int) (-log(randDouble) * ml));
  }
}
