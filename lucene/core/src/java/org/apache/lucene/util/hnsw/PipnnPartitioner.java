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
import java.util.SplittableRandom;
import org.apache.lucene.util.ArrayUtil;

/**
 * Randomized ball carving: recursively partitions a point set into overlapping clusters of at most
 * {@code leafSize} points, as used by PiPNN (Rubel et al., KDD 2026) to find candidate neighbors
 * without any graph search.
 *
 * <p>Each step samples a small set of random <i>leaders</i> from the point set and assigns every
 * point to its {@code fanout} nearest leaders. Fanout greater than one makes clusters overlap, so a
 * point's true near neighbors have several chances to end up in a common leaf; the product of the
 * per-depth fanouts is the number of leaves each point belongs to.
 *
 * <p>All randomness is derived from the seed passed to each call, so the partition is reproducible
 * irrespective of how the work is scheduled across threads.
 *
 * @lucene.internal
 */
final class PipnnPartitioner {

  /** Receives the leaves of the partition. */
  interface LeafConsumer {
    /**
     * @param members node ids in the leaf; only the first {@code size} entries are valid and the
     *     array may be reused by the caller after this method returns
     */
    void accept(int[] members, int size) throws IOException;
  }

  /**
   * Clusters in compressed sparse row form: cluster {@code i} is {@code members[offsets[i],
   * offsets[i + 1])}.
   */
  record Clusters(int[] offsets, int[] members) {
    int count() {
      return offsets.length - 1;
    }

    int size(int cluster) {
      return offsets[cluster + 1] - offsets[cluster];
    }
  }

  /** A recursion deeper than this means the carving stopped making progress. */
  private static final int MAX_DEPTH = 16;

  /**
   * Lower bound on the leaders sampled in one step (when the point set is at least that large).
   * With very few leaders the clusters degenerate into random halves of the point set and lose
   * locality.
   */
  static final int MIN_LEADERS = 8;

  private final int leafSize;
  private final int[] fanouts;
  private final int maxLeaders;
  private final double leaderFraction;

  /**
   * @param leafSize maximum number of points in a leaf
   * @param fanouts number of nearest leaders a point is assigned to at each recursion depth; depths
   *     beyond the array use a fanout of one
   * @param maxLeaders maximum number of leaders sampled in one carving step
   * @param leaderFraction fraction of the point set sampled as leaders, before capping
   */
  PipnnPartitioner(int leafSize, int[] fanouts, int maxLeaders, double leaderFraction) {
    if (leafSize < 2) {
      throw new IllegalArgumentException("leafSize must be at least 2, got " + leafSize);
    }
    if (maxLeaders < 2) {
      throw new IllegalArgumentException("maxLeaders must be at least 2, got " + maxLeaders);
    }
    if (leaderFraction <= 0 || leaderFraction > 1) {
      throw new IllegalArgumentException("leaderFraction must be in (0, 1], got " + leaderFraction);
    }
    for (int fanout : fanouts) {
      if (fanout < 1) {
        throw new IllegalArgumentException("fanouts must be positive, got " + fanout);
      }
    }
    this.leafSize = leafSize;
    this.fanouts = fanouts.clone();
    this.maxLeaders = maxLeaders;
    this.leaderFraction = leaderFraction;
  }

  int leafSize() {
    return leafSize;
  }

  /** Number of leaders to sample when carving a set of {@code size} points. */
  int numLeaders(int size) {
    int leaders = (int) Math.ceil(leaderFraction * size);
    leaders = Math.max(Math.min(MIN_LEADERS, maxLeaders), Math.min(leaders, maxLeaders));
    return Math.min(leaders, size);
  }

  /** Number of leaders each point is assigned to at the given depth. */
  int fanout(int depth, int numLeaders) {
    int fanout = depth < fanouts.length ? fanouts[depth] : 1;
    return Math.min(fanout, numLeaders);
  }

  /**
   * Recursively partitions {@code points[0, size)} and passes every leaf to the consumer. This is
   * the single-threaded entry point; {@link PipnnGraphBuilder} parallelizes the top-level step and
   * calls this on each top-level cluster.
   */
  void partition(
      int[] points,
      int size,
      int depth,
      long seed,
      UpdateableRandomVectorScorer scorer,
      LeafConsumer consumer)
      throws IOException {
    if (size <= leafSize) {
      consumer.accept(points, size);
      return;
    }
    if (depth >= MAX_DEPTH) {
      // the carving is not separating these points (e.g. many duplicates); fall back to fixed-size
      // chunks rather than recursing forever
      emitChunks(points, size, consumer);
      return;
    }
    SplittableRandom random = new SplittableRandom(seed);
    int numLeaders = numLeaders(size);
    int fanout = fanout(depth, numLeaders);
    int[] leaders = sampleLeaders(points, size, numLeaders, random);
    int[] assignments = new int[Math.multiplyExact(size, fanout)];
    float[] scores = new float[numLeaders];
    assign(points, 0, size, leaders, fanout, scorer, assignments, 0, scores);
    Clusters clusters = toClusters(points, size, numLeaders, fanout, assignments);
    for (int c = 0; c < clusters.count(); c++) {
      int clusterSize = clusters.size(c);
      if (clusterSize == 0) {
        continue;
      }
      int[] cluster =
          ArrayUtil.copyOfSubArray(
              clusters.members(), clusters.offsets()[c], clusters.offsets()[c + 1]);
      if (clusterSize == size) {
        // every point chose this leader: carving cannot shrink the set any further
        emitChunks(cluster, clusterSize, consumer);
        continue;
      }
      partition(cluster, clusterSize, depth + 1, random.nextLong(), scorer, consumer);
    }
  }

  /** Samples {@code count} distinct entries of {@code points[0, size)}. */
  static int[] sampleLeaders(int[] points, int size, int count, SplittableRandom random) {
    assert count <= size;
    int[] copy = ArrayUtil.copyOfSubArray(points, 0, size);
    int[] leaders = new int[count];
    // partial Fisher-Yates shuffle
    for (int i = 0; i < count; i++) {
      int j = i + random.nextInt(size - i);
      leaders[i] = copy[j];
      copy[j] = copy[i];
    }
    return leaders;
  }

  /**
   * Assigns every point in {@code points[from, to)} to its {@code fanout} most similar leaders,
   * writing the leader indexes (positions in {@code leaders}) to {@code assignments} starting at
   * {@code assignmentsOffset}, {@code fanout} per point.
   *
   * @param scores scratch array of at least {@code leaders.length} floats
   */
  static void assign(
      int[] points,
      int from,
      int to,
      int[] leaders,
      int fanout,
      UpdateableRandomVectorScorer scorer,
      int[] assignments,
      int assignmentsOffset,
      float[] scores)
      throws IOException {
    int[] best = new int[fanout];
    float[] bestScores = new float[fanout];
    int out = assignmentsOffset;
    for (int i = from; i < to; i++) {
      scorer.setScoringOrdinal(points[i]);
      scorer.bulkScore(leaders, scores, leaders.length);
      selectTop(scores, leaders.length, fanout, best, bestScores);
      System.arraycopy(best, 0, assignments, out, fanout);
      out += fanout;
    }
  }

  /**
   * Finds the indexes of the {@code k} largest values in {@code scores[0, size)}, in descending
   * order of score, with ties broken towards the lower index.
   */
  static void selectTop(float[] scores, int size, int k, int[] best, float[] bestScores) {
    assert k <= size;
    int found = 0;
    for (int i = 0; i < size; i++) {
      float score = scores[i];
      if (found == k && score <= bestScores[found - 1]) {
        continue;
      }
      // insertion into the sorted prefix
      int pos = found == k ? k - 1 : found;
      while (pos > 0 && bestScores[pos - 1] < score) {
        bestScores[pos] = bestScores[pos - 1];
        best[pos] = best[pos - 1];
        pos--;
      }
      bestScores[pos] = score;
      best[pos] = i;
      if (found < k) {
        found++;
      }
    }
  }

  /** Groups points by assigned leader. */
  static Clusters toClusters(
      int[] points, int size, int numLeaders, int fanout, int[] assignments) {
    int[] offsets = new int[numLeaders + 1];
    for (int i = 0; i < size * fanout; i++) {
      offsets[assignments[i] + 1]++;
    }
    for (int c = 0; c < numLeaders; c++) {
      offsets[c + 1] += offsets[c];
    }
    int[] members = new int[offsets[numLeaders]];
    int[] fill = ArrayUtil.copyOfSubArray(offsets, 0, numLeaders);
    for (int i = 0; i < size; i++) {
      for (int f = 0; f < fanout; f++) {
        int leader = assignments[i * fanout + f];
        members[fill[leader]++] = points[i];
      }
    }
    return new Clusters(offsets, members);
  }

  private void emitChunks(int[] points, int size, LeafConsumer consumer) throws IOException {
    for (int start = 0; start < size; start += leafSize) {
      int length = Math.min(leafSize, size - start);
      consumer.accept(ArrayUtil.copyOfSubArray(points, start, start + length), length);
    }
  }
}
