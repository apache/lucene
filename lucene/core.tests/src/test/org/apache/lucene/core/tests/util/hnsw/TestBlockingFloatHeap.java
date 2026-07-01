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

package org.apache.lucene.core.tests.util.hnsw;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;

/**
 * Unit tests for {@link BlockingFloatHeap} with focus on thread-safety and data race conditions.
 */
public class TestBlockingFloatHeap extends LuceneTestCase {
  /**
   * Test the specific data race scenario: multiple threads calling poll() simultaneously when heap
   * is near empty
   */
  @SuppressWarnings("unused")
  public void testDataRaceOnPoll() throws InterruptedException {
    int numThreads = 10;
    for (int iteration = 0; iteration < 10; iteration++) {
      BlockingFloatHeap heap = new BlockingFloatHeap(100);

      // Fill heap with exactly numThreads elements
      for (int i = 0; i < numThreads; i++) {
        heap.offer((float) i);
      }

      AtomicInteger successfulPolls = new AtomicInteger(0);
      AtomicInteger failedPolls = new AtomicInteger(0);
      CyclicBarrier barrier = new CyclicBarrier(numThreads);

      Thread[] threads = new Thread[numThreads];

      // All threads try to poll simultaneously
      for (int i = 0; i < numThreads; i++) {
        threads[i] =
            new Thread(
                () -> {
                  try {
                    barrier.await(); // Ensure all threads start simultaneously
                    try {
                      heap.poll();
                      successfulPolls.incrementAndGet();
                    } catch (IllegalStateException ignored) {
                      failedPolls.incrementAndGet();
                    }
                  } catch (InterruptedException | BrokenBarrierException ignored) {
                    Thread.currentThread().interrupt();
                  }
                });
      }

      for (Thread thread : threads) {
        thread.start();
      }

      for (Thread thread : threads) {
        thread.join();
      }

      // Verify exactly numThreads successful polls and 0 failed
      assertEquals(
          "Iteration " + iteration + ": Expected " + numThreads + " successful polls",
          numThreads,
          successfulPolls.get());
      assertEquals("Iteration " + iteration + ": Expected 0 failed polls", 0, failedPolls.get());
      assertEquals("Iteration " + iteration + ": Heap should be empty", 0, heap.size());
    }
  }
}
