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
package org.apache.lucene.util;

/**
 * A frequency tracker backed by {@code N} independent {@link FrequencyTrackingRingBuffer} stripes.
 * The stripe for a given key is chosen deterministically by mixing the key bits, so the per-stripe
 * frequency of a key equals its global frequency (a key always lands in the same stripe).
 *
 * <p>Each stripe carries its own monitor lock, so concurrent {@link #add(int)} and {@link
 * #frequency(int)} calls on keys that hash to different stripes do not contend. The total recent
 * history capacity is {@code numStripes * historyPerStripe}, and the number of stripes must be a
 * power of two.
 *
 * <p>This is intended as a drop-in replacement for {@link FrequencyTrackingRingBuffer} in hot
 * admission paths where a single monitor lock has become a contention point. The eviction behavior
 * is per-stripe LRU on hash codes — semantically a refinement of the single-buffer behavior, not a
 * change to it.
 *
 * @lucene.internal
 */
public final class StripedFrequencyTracker implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(StripedFrequencyTracker.class);

  private final FrequencyTrackingRingBuffer[] stripes;
  private final int mask;

  /**
   * Create a new striped tracker.
   *
   * @param numStripes number of independent stripes; must be a power of two and at least 1
   * @param historyPerStripe per-stripe history capacity; must be at least 2
   * @param sentinel initial sentinel value for every stripe
   */
  public StripedFrequencyTracker(int numStripes, int historyPerStripe, int sentinel) {
    if (numStripes < 1 || Integer.bitCount(numStripes) != 1) {
      throw new IllegalArgumentException(
          "numStripes must be a positive power of two, got: " + numStripes);
    }
    if (historyPerStripe < 2) {
      throw new IllegalArgumentException(
          "historyPerStripe must be at least 2, got: " + historyPerStripe);
    }
    this.stripes = new FrequencyTrackingRingBuffer[numStripes];
    for (int i = 0; i < numStripes; i++) {
      this.stripes[i] = new FrequencyTrackingRingBuffer(historyPerStripe, sentinel);
    }
    this.mask = numStripes - 1;
  }

  private int stripeOf(int hashCode) {
    int h = hashCode ^ (hashCode >>> 16);
    return h & mask;
  }

  /** Add a hash code to its stripe, potentially evicting the oldest entry in that stripe. */
  public void add(int hashCode) {
    FrequencyTrackingRingBuffer stripe = stripes[stripeOf(hashCode)];
    synchronized (stripe) {
      stripe.add(hashCode);
    }
  }

  /** Returns the frequency of {@code hashCode} in its stripe. */
  public int frequency(int hashCode) {
    FrequencyTrackingRingBuffer stripe = stripes[stripeOf(hashCode)];
    synchronized (stripe) {
      return stripe.frequency(hashCode);
    }
  }

  /** Number of stripes in this tracker. */
  public int numStripes() {
    return stripes.length;
  }

  @Override
  public long ramBytesUsed() {
    long bytes = BASE_RAM_BYTES_USED;
    bytes += RamUsageEstimator.shallowSizeOf(stripes);
    for (FrequencyTrackingRingBuffer stripe : stripes) {
      bytes += stripe.ramBytesUsed();
    }
    return bytes;
  }
}
