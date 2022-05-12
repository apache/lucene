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
import java.util.SplittableRandom;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyperparameters.
 */
public final class HnswGraphBuilder {

  /** Default random seed for level generation * */
  private static final long DEFAULT_RAND_SEED = 42;
  /** A name for the HNSW component for the info-stream * */
  public static final String HNSW_COMPONENT = "HNSW";

  /** Random seed for level generation; public to expose for testing * */
  public static long randSeed = DEFAULT_RAND_SEED;

  private final int maxConn;
  private final int beamWidth;
  private final double ml;
  private final NeighborArray scratch;

  private final VectorSimilarityFunction similarityFunction;
  private final RandomAccessVectorValues vectorValues;
  private final SplittableRandom random;
  private final BoundsChecker bound;
  private final HnswGraphSearcher graphSearcher;

  final OnHeapHnswGraph hnsw;

  private InfoStream infoStream = InfoStream.getDefault();

  // we need two sources of vectors in order to perform diversity check comparisons without
  // colliding
  private RandomAccessVectorValues buildVectors;

  /**
   * Reads all the vectors from a VectorValues, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param vectors the vectors whose relations are represented by the graph - must provide a
   *     different view over those vectors than the one used to add via addGraphNode.
   * @param maxConn the number of connections to make when adding a new graph node; roughly speaking
   *     the graph fanout.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   */
  public HnswGraphBuilder(
      RandomAccessVectorValuesProducer vectors,
      VectorSimilarityFunction similarityFunction,
      int maxConn,
      int beamWidth,
      long seed)
      throws IOException {
    vectorValues = vectors.randomAccess();
    buildVectors = vectors.randomAccess();
    this.similarityFunction = Objects.requireNonNull(similarityFunction);
    if (maxConn <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    // normalization factor for level generation; currently not configurable
    this.ml = 1 / Math.log(1.0 * maxConn);
    this.random = new SplittableRandom(seed);
    int levelOfFirstNode = getRandomGraphLevel(ml, random);
    this.hnsw = new OnHeapHnswGraph(maxConn, levelOfFirstNode, similarityFunction.reversed);
    this.graphSearcher =
        new HnswGraphSearcher(
            similarityFunction,
            new NeighborQueue(beamWidth, similarityFunction.reversed == false),
            new FixedBitSet(vectorValues.size()));
    bound = BoundsChecker.create(similarityFunction.reversed);
    // in scratch we store candidates in reverse order: worse candidates are first
    scratch = new NeighborArray(Math.max(beamWidth, maxConn + 1), similarityFunction.reversed);
  }

  /**
   * Reads all the vectors from two copies of a random access VectorValues. Providing two copies
   * enables efficient retrieval without extra data copying, while avoiding collision of the
   * returned values.
   *
   * @param vectors the vectors for which to build a nearest neighbors graph. Must be an independet
   *     accessor for the vectors
   */
  public OnHeapHnswGraph build(RandomAccessVectorValues vectors) throws IOException {
    if (vectors == vectorValues) {
      throw new IllegalArgumentException(
          "Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + vectors.size() + " vectors");
    }
    long start = System.nanoTime(), t = start;
    // start at node 1! node 0 is added implicitly, in the constructor
    for (int node = 1; node < vectors.size(); node++) {
      addGraphNode(node, vectors.vectorValue(node));
      if ((node % 10000 == 0) && infoStream.isEnabled(HNSW_COMPONENT)) {
        t = printGraphBuildStatus(node, start, t);
      }
    }
    return hnsw;
  }

  /** Set info-stream to output debugging information * */
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  /** Inserts a doc with vector value to the graph */
  void addGraphNode(int node, float[] value) throws IOException {
    NeighborQueue candidates;
    final int nodeLevel = getRandomGraphLevel(ml, random);
    int curMaxLevel = hnsw.numLevels() - 1;
    int[] eps = new int[] {hnsw.entryNode()};

    // if a node introduces new levels to the graph, add this new node on new levels
    for (int level = nodeLevel; level > curMaxLevel; level--) {
      hnsw.addNode(level, node);
    }

    // for levels > nodeLevel search with topk = 1
    for (int level = curMaxLevel; level > nodeLevel; level--) {
      candidates = graphSearcher.searchLevel(value, 1, level, eps, vectorValues, hnsw);
      eps = new int[] {candidates.pop()};
    }
    // for levels <= nodeLevel search with topk = beamWidth, and add connections
    for (int level = Math.min(nodeLevel, curMaxLevel); level >= 0; level--) {
      candidates = graphSearcher.searchLevel(value, beamWidth, level, eps, vectorValues, hnsw);
      eps = candidates.nodes();
      hnsw.addNode(level, node);
      addDiverseNeighbors(level, node, candidates);
    }
  }

  private long printGraphBuildStatus(int node, long start, long t) {
    long now = System.nanoTime();
    infoStream.message(
        HNSW_COMPONENT,
        String.format(
            Locale.ROOT,
            "built %d in %d/%d ms",
            node,
            ((now - t) / 1_000_000),
            ((now - start) / 1_000_000)));
    return now;
  }

  private void addDiverseNeighbors(int level, int node, NeighborQueue candidates)
      throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    assert neighbors.size() == 0; // new node
    popToScratch(candidates);
    selectAndLinkDiverse(neighbors, scratch);

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    int size = neighbors.size();
    for (int i = 0; i < size; i++) {
      int nbr = neighbors.node[i];
      NeighborArray nbrNbr = hnsw.getNeighbors(level, nbr);
      nbrNbr.insertSorted(node, neighbors.score[i]);
      if (nbrNbr.size() > maxConn) {
        int indexToRemove = findWorstNonDiverse(nbrNbr);
        nbrNbr.removeIndex(indexToRemove);
      }
    }
  }

  private void selectAndLinkDiverse(NeighborArray neighbors, NeighborArray candidates)
      throws IOException {
    // Select the best maxConn neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConn && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      assert cNode < hnsw.size();
      if (diversityCheck(vectorValues.vectorValue(cNode), cScore, neighbors, buildVectors)) {
        neighbors.add(cNode, cScore);
      }
    }
  }

  private void popToScratch(NeighborQueue candidates) {
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float score = candidates.topScore();
      scratch.add(candidates.pop(), score);
    }
  }

  /**
   * @param candidate the vector of a new candidate neighbor of a node n
   * @param score the score of the new candidate and node n, to be compared with scores of the
   *     candidate and n's neighbors
   * @param neighbors the neighbors selected so far
   * @param vectorValues source of values used for making comparisons between candidate and existing
   *     neighbors
   * @return whether the candidate is diverse given the existing neighbors
   */
  private boolean diversityCheck(
      float[] candidate,
      float score,
      NeighborArray neighbors,
      RandomAccessVectorValues vectorValues)
      throws IOException {
    bound.set(score);
    for (int i = 0; i < neighbors.size(); i++) {
      float diversityCheck =
          similarityFunction.compare(candidate, vectorValues.vectorValue(neighbors.node[i]));
      if (bound.check(diversityCheck) == false) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(NeighborArray neighbors) throws IOException {
    for (int i = neighbors.size() - 1; i > 0; i--) {
      int cNode = neighbors.node[i];
      float[] cVector = vectorValues.vectorValue(cNode);
      bound.set(neighbors.score[i]);
      // check the candidate against its better-scoring neighbors
      for (int j = i - 1; j >= 0; j--) {
        float diversityCheck =
            similarityFunction.compare(cVector, buildVectors.vectorValue(neighbors.node[j]));
        // node i is too similar to node j given its score relative to the base node
        if (bound.check(diversityCheck) == false) {
          return i;
        }
      }
    }
    return neighbors.size() - 1;
  }

  private static int getRandomGraphLevel(double ml, SplittableRandom random) {
    double randDouble;
    do {
      randDouble = random.nextDouble(); // avoid 0 value, as log(0) is undefined
    } while (randDouble == 0.0);
    return ((int) (-log(randDouble) * ml));
  }
}
