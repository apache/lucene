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

import java.util.Arrays;

/**
 * Tracks the frequency of recently added integers, typically the hash codes of recently used items,
 * over a bounded history.
 *
 * <p>The history is split into independent stripes, each guarded by its own monitor, so that
 * concurrent callers adding different keys do not contend on a single lock. A key always maps to
 * the same stripe, so its frequency within its stripe is its frequency in the history. Stripes hold
 * 32 to 64 entries (a single stripe is used for histories smaller than 64, and the history is split
 * into at most 1024 stripes, so stripes grow larger beyond 65536 entries), and a key only competes
 * for history slots with the keys of its own stripe. For instance a history of 256 entries is split
 * into 8 stripes of 32 entries.
 *
 * <p>This class is thread-safe: {@link #add} may be called concurrently with {@link #add} and
 * {@link #frequency}.
 *
 * @lucene.internal
 */
public final class StripedFrequencyTracker implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(StripedFrequencyTracker.class);

  /**
   * Maximum number of stripes the history is split into. Stripes are sized so that they hold
   * between {@link #MIN_HISTORY_PER_STRIPE} and twice as many entries (one more when the history
   * size does not divide evenly), which keeps the linear scan in {@link Stripe#frequency} short for
   * any history size up to {@code MAX_STRIPES * 64}.
   */
  private static final int MAX_STRIPES = 1024;

  /**
   * Minimum number of entries per stripe. A key can never appear more often in its stripe than the
   * stripe has entries, so this must stay comfortably above the frequency thresholds that callers
   * act upon.
   */
  private static final int MIN_HISTORY_PER_STRIPE = 32;

  private final Stripe[] stripes;

  private final int stripeShift;

  /**
   * Create a new tracker over a history of {@code historySize} entries. The history initially
   * contains {@code historySize} times the {@code sentinel} value.
   *
   * @param historySize the total number of recent entries to track, must be at least 2
   * @param sentinel the value the history is initially filled with
   */
  public StripedFrequencyTracker(int historySize, int sentinel) {
    if (historySize < 2) {
      throw new IllegalArgumentException("historySize must be at least 2, got: " + historySize);
    }
    int numStripes = 1;
    while (numStripes < MAX_STRIPES && historySize / (numStripes << 1) >= MIN_HISTORY_PER_STRIPE) {
      numStripes <<= 1;
    }
    this.stripes = new Stripe[numStripes];
    for (int i = 0; i < numStripes; ++i) {
      // spread the remainder so that the total history size is preserved
      int size = historySize / numStripes + (i < historySize % numStripes ? 1 : 0);
      stripes[i] = new Stripe(size, sentinel);
    }
    // the stripe is picked from the high bits of a mixed hash, numStripes == 1 maps to shift 32
    this.stripeShift = 32 - Integer.numberOfTrailingZeros(numStripes);
  }

  /** Add {@code key} to the history, evicting the oldest entry of its stripe. */
  public void add(int key) {
    stripeFor(key).add(key);
  }

  /**
   * Return the number of times {@code key} occurs in the history. This does not take a lock and may
   * be off by one when racing with a concurrent {@link #add} on the same stripe.
   */
  public int frequency(int key) {
    return stripeFor(key).frequency(key);
  }

  /** Return the number of stripes the history is split into. */
  int numStripes() {
    return stripes.length;
  }

  /** Return the number of entries of the stripe at index {@code stripe}. */
  int stripeSize(int stripe) {
    return stripes[stripe].buffer.length;
  }

  private Stripe stripeFor(int key) {
    if (stripes.length == 1) {
      return stripes[0];
    }
    // Fibonacci hashing: use the high bits so the stripe choice is independent of the low bits
    return stripes[(key * 0x9E3779B9) >>> stripeShift];
  }

  @Override
  public long ramBytesUsed() {
    long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(stripes);
    for (Stripe stripe : stripes) {
      bytes += stripe.ramBytesUsed();
    }
    return bytes;
  }

  /**
   * A ring buffer of recently added keys. Stripes are small (typically 32 entries), so the
   * frequency of a key is computed by scanning the buffer, which is faster than maintaining a
   * frequency hash table on every add and keeps {@link #add} constant-time.
   */
  private static final class Stripe implements Accountable {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(Stripe.class);

    private final int[] buffer;
    private int position;

    Stripe(int size, int sentinel) {
      buffer = new int[size];
      Arrays.fill(buffer, sentinel);
    }

    synchronized void add(int key) {
      final int[] buffer = this.buffer;
      int position = this.position;
      buffer[position] = key;
      if (++position == buffer.length) {
        position = 0;
      }
      this.position = position;
    }

    /**
     * Counts occurrences of {@code key} in this stripe. This intentionally reads the buffer without
     * holding the lock: int reads cannot tear, and a concurrent {@link #add} can at worst make the
     * count off by one, which is harmless for a caching heuristic and keeps the read path (called
     * on every cache miss) free of lock traffic.
     */
    int frequency(int key) {
      int frequency = 0;
      for (int value : buffer) {
        // branch-free so that the JIT can unroll and vectorize the loop
        frequency += value == key ? 1 : 0;
      }
      return frequency;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(buffer);
    }
  }
}
