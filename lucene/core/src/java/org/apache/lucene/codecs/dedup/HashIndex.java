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

package org.apache.lucene.codecs.dedup;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Compact open-addressed hash table mapping {@code 64-bit hash → int ord}.
 *
 * <p>Designed for the dedup writer's intern path: callers probe by hash, then byte-verify against
 * stored vectors externally. The table itself stores only {@code (hash, ord)} pairs in two parallel
 * primitive arrays, growing as load factor exceeds {@code MAX_LOAD_FACTOR}.
 *
 * <p>Slot-empty is encoded as {@code hash == DedupHash.EMPTY} (== 0). {@link DedupHash#hash}
 * substitutes a non-zero value if the underlying hasher produces 0.
 *
 * <p>Probing follows linear probing for cache locality.
 */
final class HashIndex {

  /** {@code DedupHash.EMPTY} re-exported for the writer. */
  static final long EMPTY = DedupHash.EMPTY;

  /** Initial capacity (must be a power of two). */
  private static final int INITIAL_CAPACITY = 16;

  /** Load factor at which the table doubles. */
  private static final double MAX_LOAD_FACTOR = 0.6;

  private long[] hashes;
  private int[] ords;
  private int mask;
  private int size;
  private int growThreshold;

  HashIndex() {
    allocate(INITIAL_CAPACITY);
  }

  private void allocate(int capacity) {
    hashes = new long[capacity];
    ords = new int[capacity];
    mask = capacity - 1;
    growThreshold = (int) (capacity * MAX_LOAD_FACTOR);
  }

  /** Returns the starting probe slot for the given hash. */
  int probeStart(long h) {
    return (int) (h & mask);
  }

  /** Returns the next probe slot after {@code slot}. */
  int probeNext(int slot) {
    return (slot + 1) & mask;
  }

  /** Hash stored at {@code slot}, or {@link #EMPTY} if the slot is free. */
  long hashAt(int slot) {
    return hashes[slot];
  }

  /** Ord stored at {@code slot}. Caller must have checked {@link #hashAt} != EMPTY first. */
  int ordAt(int slot) {
    return ords[slot];
  }

  /**
   * Insert {@code (h, ord)} at the given empty slot. {@code slot} must have been confirmed empty
   * via {@link #hashAt(int)}.
   */
  void put(int slot, long h, int ord) {
    assert hashes[slot] == EMPTY;
    hashes[slot] = h;
    ords[slot] = ord;
    size++;
    if (size > growThreshold) {
      grow();
    }
  }

  private void grow() {
    long[] oldHashes = hashes;
    int[] oldOrds = ords;
    allocate(oldHashes.length * 2);
    size = 0;
    for (int i = 0; i < oldHashes.length; i++) {
      long h = oldHashes[i];
      if (h != EMPTY) {
        int slot = probeStart(h);
        while (hashes[slot] != EMPTY) {
          slot = probeNext(slot);
        }
        hashes[slot] = h;
        ords[slot] = oldOrds[i];
        size++;
      }
    }
  }

  int size() {
    return size;
  }

  long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(HashIndex.class)
        + RamUsageEstimator.sizeOf(hashes)
        + RamUsageEstimator.sizeOf(ords);
  }
}
