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
package org.apache.lucene.index;

import java.util.concurrent.CountDownLatch;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;

public class TestConcurrentApproximatePriorityQueue extends LuceneTestCase {

  public void testPollFromSameThread() {
    ConcurrentApproximatePriorityQueue<Integer> pq =
        new ConcurrentApproximatePriorityQueue<>(
            TestUtil.nextInt(
                random(),
                ConcurrentApproximatePriorityQueue.MIN_CONCURRENCY,
                ConcurrentApproximatePriorityQueue.MAX_CONCURRENCY));
    pq.add(3, 3);
    pq.add(10, 10);
    pq.add(7, 7);
    assertEquals(Integer.valueOf(10), pq.poll(x -> true));
    assertEquals(Integer.valueOf(7), pq.poll(x -> true));
    assertEquals(Integer.valueOf(3), pq.poll(x -> true));
    assertNull(pq.poll(x -> true));
  }

  public void testPollFromDifferentThread() throws Exception {
    ConcurrentApproximatePriorityQueue<Integer> pq =
        new ConcurrentApproximatePriorityQueue<>(
            TestUtil.nextInt(
                random(),
                ConcurrentApproximatePriorityQueue.MIN_CONCURRENCY,
                ConcurrentApproximatePriorityQueue.MAX_CONCURRENCY));
    pq.add(3, 3);
    pq.add(10, 10);
    pq.add(7, 7);
    Thread t =
        new Thread() {
          @Override
          public void run() {
            assertEquals(Integer.valueOf(10), pq.poll(x -> true));
            assertEquals(Integer.valueOf(7), pq.poll(x -> true));
            assertEquals(Integer.valueOf(3), pq.poll(x -> true));
            assertNull(pq.poll(x -> true));
          }
        };
    t.start();
    t.join();
  }

  public void testCurrentLockIsBusy() throws Exception {
    // This test needs a concurrency of 2 or more.
    ConcurrentApproximatePriorityQueue<Integer> pq =
        new ConcurrentApproximatePriorityQueue<>(
            TestUtil.nextInt(random(), 2, ConcurrentApproximatePriorityQueue.MAX_CONCURRENCY));
    pq.add(3, 3);
    CountDownLatch takeLock = new CountDownLatch(1);
    CountDownLatch releaseLock = new CountDownLatch(1);
    Thread t =
        new Thread() {
          @Override
          public void run() {
            int queueIndex = -1;
            for (int i = 0; i < pq.queues.length; ++i) {
              if (pq.queues[i].isEmpty() == false) {
                queueIndex = i;
                break;
              }
            }
            assertTrue(pq.locks[queueIndex].tryLock());
            takeLock.countDown();
            try {
              releaseLock.await();
            } catch (InterruptedException e) {
              throw new ThreadInterruptedException(e);
            }
            pq.locks[queueIndex].unlock();
          }
        };
    t.start();
    takeLock.await();
    pq.add(1, 1); // The lock is taken so this needs to go to a different queue
    assertEquals(Integer.valueOf(1), pq.poll(x -> true));
    releaseLock.countDown();
    assertEquals(Integer.valueOf(3), pq.poll(x -> true));
    assertNull(pq.poll(x -> true));
  }
}
