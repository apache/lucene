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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Fixed-capacity candidate reservoirs for every node of a graph under construction, implementing
 * the <i>HashPrune</i> step of PiPNN (Rubel et al., "Search-Free Graph Construction for Nearest
 * Neighbor Search", KDD 2026).
 *
 * <p>Each node owns {@code capacity} slots. A candidate neighbor is inserted together with a small
 * hash of the direction from the node to the candidate: candidates sharing a hash bucket compete
 * and only the closest one is kept, so the slots fill with candidates that point in different
 * directions. Once the slots are full the farthest entry is evicted when a closer one arrives.
 *
 * <p>The final contents are history independent: for a given candidate multiset the surviving set
 * is the set of the {@code capacity} closest per-bucket winners, regardless of insertion order.
 * Ties on the (quantized) score are broken by node id so that this also holds for equal scores.
 * This is what makes it safe to feed the reservoirs from many threads, as long as updates to one
 * node's slots are serialized by the caller.
 *
 * <p>Slots are packed into a single {@code long} per slot: the candidate node id in the high 32
 * bits, the hash in the next 16 bits, and the score as the upper 16 bits of its {@code float} (a
 * bfloat16) in the low 16 bits. Exact scores are recomputed by the caller when the survivors are
 * pruned, so the quantization only affects which candidate wins a bucket when two are nearly
 * equally close.
 *
 * @lucene.internal
 */
final class PipnnCandidateReservoir implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(PipnnCandidateReservoir.class);

  private static final long EMPTY = -1L;

  private final int capacity;
  private final boolean hashed;
  private final long[] slots;
  private final byte[] counts;

  /**
   * @param numNodes number of nodes in the graph
   * @param capacity number of slots per node, at most 127
   * @param hashed whether candidates compete per hash bucket; if false the hash is ignored and the
   *     reservoir simply keeps the closest distinct candidates
   */
  PipnnCandidateReservoir(int numNodes, int capacity, boolean hashed) {
    if (capacity <= 0 || capacity > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("capacity must be in [1, 127], got " + capacity);
    }
    this.capacity = capacity;
    this.hashed = hashed;
    this.slots = new long[Math.multiplyExact(numNodes, capacity)];
    Arrays.fill(slots, EMPTY);
    this.counts = new byte[numNodes];
  }

  int capacity() {
    return capacity;
  }

  /** Number of candidates currently stored for the node. */
  int size(int node) {
    return counts[node];
  }

  /**
   * Offers {@code candidate} as a neighbor of {@code node}.
   *
   * @param node the node whose reservoir is updated
   * @param candidate the candidate neighbor
   * @param hash hash of the direction from node to candidate, only its low 16 bits are used
   * @param score similarity between node and candidate, higher is closer
   * @return true if the candidate was stored (possibly displacing another candidate)
   */
  boolean offer(int node, int candidate, int hash, float score) {
    final long packed = pack(candidate, hash, score);
    final int base = node * capacity;
    final int count = counts[node];
    for (int i = 0; i < count; i++) {
      long slot = slots[base + i];
      if (sameBucket(slot, packed)) {
        if (closer(packed, slot)) {
          slots[base + i] = packed;
          return true;
        }
        return false;
      }
    }
    if (count < capacity) {
      slots[base + count] = packed;
      counts[node] = (byte) (count + 1);
      return true;
    }
    int farthest = base;
    for (int i = base + 1; i < base + count; i++) {
      if (closer(slots[farthest], slots[i])) {
        farthest = i;
      }
    }
    if (closer(packed, slots[farthest])) {
      slots[farthest] = packed;
      return true;
    }
    return false;
  }

  /**
   * Copies the candidate node ids stored for {@code node} into {@code dest}, which must have room
   * for {@link #capacity()} entries.
   *
   * @return the number of candidates copied
   */
  int candidates(int node, int[] dest) {
    final int base = node * capacity;
    final int count = counts[node];
    for (int i = 0; i < count; i++) {
      dest[i] = nodeOf(slots[base + i]);
    }
    return count;
  }

  private boolean sameBucket(long a, long b) {
    return hashed ? hashOf(a) == hashOf(b) : nodeOf(a) == nodeOf(b);
  }

  /**
   * Compares two packed slots: returns true if {@code a} is strictly closer than {@code b}. Equal
   * quantized scores are ordered by node id so that the comparison is a total order.
   */
  static boolean closer(long a, long b) {
    int sa = scoreBitsOf(a);
    int sb = scoreBitsOf(b);
    if (sa != sb) {
      // compare as the floats they represent: shift back to a float bit pattern
      return Float.intBitsToFloat(sa << 16) > Float.intBitsToFloat(sb << 16);
    }
    return nodeOf(a) < nodeOf(b);
  }

  static long pack(int node, int hash, float score) {
    // upper 16 bits of the float: sign, exponent and 7 bits of mantissa
    int scoreBits = Float.floatToIntBits(score) >>> 16;
    return ((long) node << 32) | ((long) (hash & 0xFFFF) << 16) | scoreBits;
  }

  static int nodeOf(long slot) {
    return (int) (slot >>> 32);
  }

  static int hashOf(long slot) {
    return (int) ((slot >>> 16) & 0xFFFF);
  }

  static int scoreBitsOf(long slot) {
    return (int) (slot & 0xFFFF);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(slots) + RamUsageEstimator.sizeOf(counts);
  }
}
