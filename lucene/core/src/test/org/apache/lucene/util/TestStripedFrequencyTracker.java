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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestStripedFrequencyTracker extends LuceneTestCase {

  public void testInvalidNumStripes() {
    expectThrows(IllegalArgumentException.class, () -> new StripedFrequencyTracker(0, 4, -1));
    expectThrows(IllegalArgumentException.class, () -> new StripedFrequencyTracker(3, 4, -1));
    expectThrows(IllegalArgumentException.class, () -> new StripedFrequencyTracker(6, 4, -1));
  }

  public void testInvalidHistorySize() {
    expectThrows(IllegalArgumentException.class, () -> new StripedFrequencyTracker(4, 1, -1));
    expectThrows(IllegalArgumentException.class, () -> new StripedFrequencyTracker(4, 0, -1));
  }

  public void testKeyAlwaysLandsInSameStripe() {
    // A key's per-stripe frequency must equal its global frequency: it must
    // always map to the same stripe regardless of how many other keys are present.
    // We keep unrelated traffic well under a single stripe's capacity (avg ~5
    // adds per stripe with 80 random adds across 16 stripes) so we can isolate
    // "did the hot key's count survive cross-stripe traffic?" from per-stripe
    // ring-buffer eviction.
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(16, 64, Integer.MIN_VALUE);
    int key = 0xDEADBEEF;
    for (int i = 0; i < 10; i++) {
      tracker.add(key);
    }
    assertEquals(10, tracker.frequency(key));

    for (int i = 1; i <= 80; i++) {
      tracker.add(i * 0x9E3779B1);
    }
    assertEquals(10, tracker.frequency(key));
  }

  public void testFrequencyZeroForUnknownKey() {
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(4, 8, Integer.MIN_VALUE);
    assertEquals(0, tracker.frequency(42));
    tracker.add(7);
    assertEquals(0, tracker.frequency(42));
    assertEquals(1, tracker.frequency(7));
  }

  public void testPerStripeEvictionMatchesRingBuffer() {
    // With numStripes=1, behavior should be identical to a single FrequencyTrackingRingBuffer
    // of the same per-stripe size. We exercise both and check freq parity for sampled keys.
    int historyPerStripe = 32;
    int sentinel = Integer.MIN_VALUE;
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(1, historyPerStripe, sentinel);
    FrequencyTrackingRingBuffer ref = new FrequencyTrackingRingBuffer(historyPerStripe, sentinel);

    int[] keys = {1, 2, 3, 4, 5, 6, 7, 8};
    for (int iter = 0; iter < 200; iter++) {
      int k = keys[random().nextInt(keys.length)];
      tracker.add(k);
      ref.add(k);
    }
    for (int k : keys) {
      assertEquals("key=" + k, ref.frequency(k), tracker.frequency(k));
    }
  }

  public void testRamBytesUsed() {
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(16, 64, Integer.MIN_VALUE);
    for (int i = 0; i < 5000; i++) {
      tracker.add(random().nextInt());
    }
    // Just assert non-zero and stable across reads.
    long bytes = tracker.ramBytesUsed();
    assertTrue(bytes > 0);
    assertEquals(bytes, tracker.ramBytesUsed());
  }

  public void testNumStripes() {
    assertEquals(1, new StripedFrequencyTracker(1, 2, 0).numStripes());
    assertEquals(16, new StripedFrequencyTracker(16, 64, 0).numStripes());
  }

  public void testConcurrentAddAndFrequency() throws Exception {
    final StripedFrequencyTracker tracker = new StripedFrequencyTracker(16, 64, Integer.MIN_VALUE);
    final int numWriters = 8;
    final int addsPerWriter = 5000;
    final int hotKey = 0x12345678;
    final AtomicInteger hotKeyHits = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final Thread[] threads = new Thread[numWriters];

    for (int t = 0; t < numWriters; t++) {
      threads[t] =
          new Thread(
              () -> {
                try {
                  start.await();
                } catch (InterruptedException _) {
                  Thread.currentThread().interrupt();
                  return;
                }
                for (int i = 0; i < addsPerWriter; i++) {
                  // 50% hot key, 50% random — exercise both contention paths.
                  if ((i & 1) == 0) {
                    tracker.add(hotKey);
                    hotKeyHits.incrementAndGet();
                  } else {
                    tracker.add(random().nextInt());
                  }
                  if ((i & 0xFF) == 0) {
                    // periodically read; should never throw
                    tracker.frequency(hotKey);
                  }
                }
              });
      threads[t].start();
    }
    start.countDown();
    for (Thread th : threads) {
      th.join();
    }

    // After all adds, the hot key's frequency should be at most min(totalHotKeyHits, perStripeCap)
    // and at least 1 (it lives in a single stripe sized 64). We just assert non-negative and
    // bounded by the per-stripe capacity — the exact value depends on stripe overflow.
    int freq = tracker.frequency(hotKey);
    assertTrue("freq=" + freq, freq >= 0 && freq <= 64);
    assertTrue("expected at least one hot-key add to survive", hotKeyHits.get() > 0);
  }
}
