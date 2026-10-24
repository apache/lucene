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
package org.apache.lucene.store;

import static org.apache.lucene.store.MemorySegmentIndexInput.MAX_PREFETCH_CHECK_INTERVAL;
import static org.apache.lucene.store.MemorySegmentIndexInput.shouldCheckResidency;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitUtil;

/** Tests the throttling of residency checks in {@link MemorySegmentIndexInput#prefetch}. */
public class TestMemorySegmentPrefetchThrottle extends LuceneTestCase {

  /** The folding of the counter relies on the interval being a power of two. */
  public void testIntervalIsPowerOfTwo() {
    assertTrue(MAX_PREFETCH_CHECK_INTERVAL > 0);
    assertTrue(BitUtil.isZeroOrPowerOfTwo(MAX_PREFETCH_CHECK_INTERVAL));
  }

  /** From a fresh counter, checks happen at zero and at powers of two, and nothing is folded. */
  public void testInitialBackoff() {
    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 2 * MAX_PREFETCH_CHECK_INTERVAL; i++) {
      assertEquals("call " + i, BitUtil.isZeroOrPowerOfTwo(i), shouldCheckResidency(counter));
      assertEquals(i + 1, counter.get());
    }
  }

  /**
   * After the initial backoff, checks happen exactly once per MAX_PREFETCH_CHECK_INTERVAL calls.
   */
  public void testChecksAreCapped() {
    AtomicInteger counter = new AtomicInteger();
    int lastCheck = -1;
    for (int i = 0; i < 100 * MAX_PREFETCH_CHECK_INTERVAL; i++) {
      boolean check = shouldCheckResidency(counter);
      assertTrue(
          "counter grew to " + counter.get(), counter.get() < 4 * MAX_PREFETCH_CHECK_INTERVAL);
      if (i > MAX_PREFETCH_CHECK_INTERVAL) {
        assertEquals("call " + i, i - lastCheck == MAX_PREFETCH_CHECK_INTERVAL, check);
      }
      if (check) {
        lastCheck = i;
      }
    }
  }

  /** A cache miss resets the counter to zero, which restarts the exponential backoff. */
  public void testResetRestartsBackoff() {
    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 5 * MAX_PREFETCH_CHECK_INTERVAL; i++) {
      shouldCheckResidency(counter);
    }
    counter.set(0);
    for (int i = 0; i < 2 * MAX_PREFETCH_CHECK_INTERVAL; i++) {
      assertEquals("call " + i, BitUtil.isZeroOrPowerOfTwo(i), shouldCheckResidency(counter));
    }
  }

  /**
   * Whatever large power of two the counter reaches, a check folds it back into the bounded range.
   */
  public void testLargeCounterIsFolded() {
    AtomicInteger counter = new AtomicInteger();
    for (int power = 2 * MAX_PREFETCH_CHECK_INTERVAL; power > 0; power <<= 1) {
      counter.set(power);
      assertTrue("power " + power, shouldCheckResidency(counter));
      // power + 1 is the value after this call's increment, kept modulo the interval.
      assertEquals("power " + power, MAX_PREFETCH_CHECK_INTERVAL + 1, counter.get());
    }
  }

  /** Values that are not zero or a power of two never check and never fold. */
  public void testNonPowerOfTwoDoesNotCheckOrFold() {
    AtomicInteger counter = new AtomicInteger();
    for (int value : new int[] {3, 1000, 2047, 2049, 5000, 1_000_000}) {
      counter.set(value);
      assertFalse("value " + value, shouldCheckResidency(counter));
      assertEquals("value " + value, value + 1, counter.get());
    }
  }
}
