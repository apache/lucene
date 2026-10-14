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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * The visited set of a graph builder's searcher. A builder searches the graph once for each level
 * of each node it inserts, and each search starts with a cleared set, so clearing a {@link
 * FixedBitSet} sized to the whole graph would write {@code numNodes / 8} bytes several times for
 * every insertion. This set records the bits that it sets and clears only those, unless so many
 * were set that clearing every word is cheaper. Sets smaller than {@link #MIN_TRACKED_BITS} record
 * nothing and are cleared in full, because there a full clear costs less than the recording.
 */
final class TrackingVisitedBitSet extends BitSet {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(TrackingVisitedBitSet.class);

  /** Sets with fewer bits are cleared in full, as a {@link FixedBitSet} is. */
  static final int MIN_TRACKED_BITS = 1 << 21;

  private FixedBitSet bits;
  private int[] touched = new int[256];
  private int count;
  // more bits set than this, and clearing every word is cheaper than clearing them one by one
  private int limit;
  private boolean overflow;
  private final int minTrackedBits;
  private boolean track;

  TrackingVisitedBitSet(int numBits) {
    this(numBits, MIN_TRACKED_BITS);
  }

  TrackingVisitedBitSet(int numBits, int minTrackedBits) {
    bits = new FixedBitSet(numBits);
    limit = limitFor(numBits);
    this.minTrackedBits = minTrackedBits;
    track = numBits >= minTrackedBits;
  }

  private static int limitFor(int numBits) {
    return Math.max(256, FixedBitSet.bits2words(numBits) / 16);
  }

  /** Clears the set, and grows it first if it holds fewer than {@code capacity} bits. */
  void ensureCapacityAndClear(int capacity) {
    if (bits.length() < capacity) {
      bits = FixedBitSet.ensureCapacityAndClear(bits, capacity);
      limit = limitFor(bits.length());
      track = bits.length() >= minTrackedBits;
      count = 0;
      overflow = false;
    } else {
      clear();
    }
  }

  private void record(int i) {
    if (overflow) {
      return;
    }
    if (count == limit) {
      overflow = true;
      return;
    }
    if (count == touched.length) {
      touched = ArrayUtil.grow(touched, count + 1);
    }
    touched[count++] = i;
  }

  @Override
  public void set(int i) {
    if (bits.getAndSet(i) == false && track) {
      record(i);
    }
  }

  @Override
  public boolean getAndSet(int i) {
    boolean wasSet = bits.getAndSet(i);
    if (wasSet == false && track) {
      record(i);
    }
    return wasSet;
  }

  @Override
  public void clear() {
    if (overflow || track == false) {
      bits.clear();
    } else {
      for (int k = 0; k < count; k++) {
        bits.clear(touched[k]);
      }
    }
    count = 0;
    overflow = false;
  }

  @Override
  public void clear(int i) {
    // the index may stay in the touched list; clearing it again later is harmless
    bits.clear(i);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    bits.clear(startIndex, endIndex);
  }

  @Override
  public boolean get(int index) {
    return bits.get(index);
  }

  @Override
  public int length() {
    return bits.length();
  }

  @Override
  public int cardinality() {
    return bits.cardinality();
  }

  @Override
  public int approximateCardinality() {
    return bits.approximateCardinality();
  }

  @Override
  public int prevSetBit(int index) {
    return bits.prevSetBit(index);
  }

  @Override
  public int nextSetBit(int start, int end) {
    return bits.nextSetBit(start, end);
  }

  @Override
  public int nextClearBit(int start, int upperBound) {
    return bits.nextClearBit(start, upperBound);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + bits.ramBytesUsed() + RamUsageEstimator.sizeOf(touched);
  }
}
