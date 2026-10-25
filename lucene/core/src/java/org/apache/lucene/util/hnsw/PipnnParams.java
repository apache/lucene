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

import java.util.Arrays;

/**
 * Tuning parameters of {@link PipnnGraphBuilder}.
 *
 * @param leafSize maximum number of vectors per leaf; every pair of vectors in a leaf is scored, so
 *     this bounds both the quality and the cost of the candidate generation
 * @param fanouts number of nearest leaders each vector is assigned to at each depth of the
 *     partition; the product is the number of leaves a vector belongs to
 * @param maxLeaders maximum number of leaders sampled in one partitioning step
 * @param leaderFraction fraction of a point set sampled as leaders, before capping at {@code
 *     maxLeaders}
 * @param leafNeighbors number of nearest neighbors within a leaf that become candidate edges (in
 *     both directions)
 * @param reservoirSize number of candidate slots per node, or 0 to use twice the level 0 degree
 * @param hashBits number of bits of the direction hash used to diversify the candidate reservoir,
 *     or 0 to keep the plain closest candidates
 * @param alpha pruning factor of the final neighbor selection: a candidate is dropped if it is more
 *     than {@code alpha} times closer to an already selected neighbor than to the node. {@code 1}
 *     is the standard HNSW heuristic; larger values keep longer edges
 * @lucene.experimental
 */
public record PipnnParams(
    int leafSize,
    int[] fanouts,
    int maxLeaders,
    double leaderFraction,
    int leafNeighbors,
    int reservoirSize,
    int hashBits,
    float alpha) {

  /** Defaults following the reference implementation of the paper. */
  public static final PipnnParams DEFAULT =
      new PipnnParams(1024, new int[] {10, 3}, 1000, 0.005, 2, 0, 12, 1.0f);

  public PipnnParams {
    if (leafSize < 2) {
      throw new IllegalArgumentException("leafSize must be at least 2, got " + leafSize);
    }
    if (fanouts.length == 0) {
      throw new IllegalArgumentException("fanouts must not be empty");
    }
    if (leafNeighbors < 1) {
      throw new IllegalArgumentException("leafNeighbors must be positive, got " + leafNeighbors);
    }
    if (reservoirSize < 0 || reservoirSize > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("reservoirSize must be in [0, 127], got " + reservoirSize);
    }
    if (hashBits < 0 || hashBits > 16) {
      throw new IllegalArgumentException("hashBits must be in [0, 16], got " + hashBits);
    }
    if (alpha < 1 || Float.isNaN(alpha)) {
      throw new IllegalArgumentException("alpha must be at least 1, got " + alpha);
    }
    fanouts = fanouts.clone();
  }

  /** Number of leaves each vector is a member of. */
  public int replication() {
    int replication = 1;
    for (int fanout : fanouts) {
      replication = Math.multiplyExact(replication, fanout);
    }
    return replication;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipnnParams other
        && leafSize == other.leafSize
        && Arrays.equals(fanouts, other.fanouts)
        && maxLeaders == other.maxLeaders
        && leaderFraction == other.leaderFraction
        && leafNeighbors == other.leafNeighbors
        && reservoirSize == other.reservoirSize
        && hashBits == other.hashBits
        && alpha == other.alpha;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(
        new Object[] {
          leafSize,
          Arrays.hashCode(fanouts),
          maxLeaders,
          leaderFraction,
          leafNeighbors,
          reservoirSize,
          hashBits,
          alpha
        });
  }

  @Override
  public String toString() {
    return "PipnnParams(leafSize="
        + leafSize
        + ", fanouts="
        + Arrays.toString(fanouts)
        + ", maxLeaders="
        + maxLeaders
        + ", leaderFraction="
        + leaderFraction
        + ", leafNeighbors="
        + leafNeighbors
        + ", reservoirSize="
        + reservoirSize
        + ", hashBits="
        + hashBits
        + ", alpha="
        + alpha
        + ")";
  }
}
