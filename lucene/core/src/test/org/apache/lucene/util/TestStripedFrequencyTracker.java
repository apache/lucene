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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;

public class TestStripedFrequencyTracker extends LuceneTestCase {

  private static final int SENTINEL = Integer.MIN_VALUE;

  public void testInvalidHistorySize() {
    for (int historySize : new int[] {1, 0, -1, Integer.MIN_VALUE}) {
      IllegalArgumentException e =
          expectThrows(
              IllegalArgumentException.class,
              () -> new StripedFrequencyTracker(historySize, SENTINEL));
      assertEquals("historySize must be at least 2, got: " + historySize, e.getMessage());
    }
  }

  public void testStripeLayout() {
    for (int historySize :
        new int[] {
          2,
          31,
          32,
          63,
          64,
          100,
          127,
          128,
          256,
          1000,
          1024,
          65535,
          65536,
          100000,
          TestUtil.nextInt(random(), 2, 200_000)
        }) {
      StripedFrequencyTracker tracker = new StripedFrequencyTracker(historySize, SENTINEL);
      int numStripes = tracker.numStripes();
      String msg = "historySize=" + historySize + ", numStripes=" + numStripes;
      assertTrue(msg, numStripes >= 1 && numStripes <= 1024);
      assertEquals(msg, 0, numStripes & (numStripes - 1)); // power of two
      if (historySize < 64) {
        assertEquals(msg, 1, numStripes);
      }
      long total = 0;
      for (int i = 0; i < numStripes; ++i) {
        int size = tracker.stripeSize(i);
        total += size;
        if (numStripes > 1) {
          assertTrue(msg + ", stripeSize=" + size, size >= 32);
          assertTrue(msg + ", stripeSize=" + size, size >= historySize / numStripes);
          // floor(historySize / numStripes) is at most 63, plus one when spreading the remainder;
          // only the 1024 stripe cap lets stripes grow larger
          int maxSize = Math.max(64, (historySize + 1023) / 1024);
          assertTrue(msg + ", stripeSize=" + size, size <= maxSize);
        }
      }
      assertEquals(msg, historySize, total);
    }
    // the default history of the query caching policy
    assertEquals(8, new StripedFrequencyTracker(256, SENTINEL).numStripes());
  }

  public void testInitialFrequency() {
    int historySize = TestUtil.nextInt(random(), 2, 5000);
    int sentinel = random().nextInt();
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(historySize, sentinel);
    int sentinelFrequency = tracker.frequency(sentinel);
    assertTrue(sentinelFrequency >= 1 && sentinelFrequency <= historySize);
    int key = sentinel + 1;
    assertEquals(0, tracker.frequency(key));
    tracker.add(key);
    assertEquals(1, tracker.frequency(key));
  }

  public void testEviction() {
    for (int historySize : new int[] {2, 31, 256, TestUtil.nextInt(random(), 2, 5000)}) {
      StripedFrequencyTracker tracker = new StripedFrequencyTracker(historySize, SENTINEL);
      int key = 42;
      int uses = Math.min(10, historySize);
      for (int i = 0; i < uses; ++i) {
        tracker.add(key);
      }
      assertEquals("historySize=" + historySize, uses, tracker.frequency(key));
      // flooding with other keys eventually pushes the key out of its stripe
      for (int i = 0; i < 100_000; ++i) {
        tracker.add(1000 + i);
      }
      assertEquals("historySize=" + historySize, 0, tracker.frequency(key));
    }
  }

  public void testFrequencyNeverExceedsAdds() {
    StripedFrequencyTracker tracker =
        new StripedFrequencyTracker(TestUtil.nextInt(random(), 2, 2000), SENTINEL);
    int numKeys = TestUtil.nextInt(random(), 1, 100);
    int[] adds = new int[numKeys];
    for (int i = 0; i < 10_000; ++i) {
      int key = random().nextInt(numKeys);
      tracker.add(key);
      adds[key]++;
      assertTrue(tracker.frequency(key) <= adds[key]);
    }
  }

  public void testRamBytesUsed() {
    for (int historySize :
        new int[] {2, 31, 64, 256, 100000, TestUtil.nextInt(random(), 2, 10000)}) {
      StripedFrequencyTracker tracker = new StripedFrequencyTracker(historySize, SENTINEL);
      for (int i = 0; i < 1000; ++i) {
        tracker.add(random().nextInt());
      }
      // the estimate accounts for every object that RamUsageTester finds by walking the object
      // graph (the tracker, the stripes array, each stripe and its buffer), so they match exactly
      assertEquals(
          "historySize=" + historySize, RamUsageTester.ramUsed(tracker), tracker.ramBytesUsed());
    }
  }

  public void testConcurrentUse() throws Exception {
    int historySize = TestUtil.nextInt(random(), 2, 1000);
    StripedFrequencyTracker tracker = new StripedFrequencyTracker(historySize, SENTINEL);
    int numThreads = TestUtil.nextInt(random(), 2, 8);
    Thread[] threads = new Thread[numThreads];
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < numThreads; ++t) {
      final long seed = random().nextLong();
      threads[t] =
          new Thread(() -> {
            Random r = new Random(seed);
            try {
              start.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            for (int i = 0; i < 10_000; ++i) {
              int key = r.nextInt(64);
              tracker.add(key);
              int freq = tracker.frequency(key);
              assertTrue(freq >= 0 && freq <= historySize);
            }
          });
      threads[t].start();
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    // once all threads are done, each key occurs at most as many times as its stripe has entries
    // and the sum over all keys cannot exceed the total history size
    long total = 0;
    for (int key = 0; key < 64; ++key) {
      total += tracker.frequency(key);
    }
    assertTrue(total > 0 && total <= historySize);
  }
}
