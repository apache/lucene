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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;

public class TestLockableConcurrentApproximatePriorityQueue extends LuceneTestCase {

  private static class WeightedLock implements Lock {

    private final Lock lock = new ReentrantLock();
    long weight;

    @Override
    public void lock() {
      lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
      lock.unlock();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

  public void testNeverReturnNullOnNonEmptyQueue() throws Exception {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      final int concurrency = TestUtil.nextInt(random(), 1, 16);
      final LockableConcurrentApproximatePriorityQueue<WeightedLock> queue =
          new LockableConcurrentApproximatePriorityQueue<>(concurrency);
      final int numThreads = TestUtil.nextInt(random(), 2, 16);
      final Thread[] threads = new Thread[numThreads];
      final CountDownLatch startingGun = new CountDownLatch(1);
      for (int t = 0; t < threads.length; ++t) {
        threads[t] =
            new Thread(
                () -> {
                  try {
                    startingGun.await();
                  } catch (InterruptedException e) {
                    throw new ThreadInterruptedException(e);
                  }
                  WeightedLock lock = new WeightedLock();
                  lock.lock();
                  lock.weight++; // Simulate a DWPT whose RAM usage increases
                  queue.addAndUnlock(lock, lock.weight);
                  for (int i = 0; i < 10_000; ++i) {
                    lock = queue.lockAndPoll();
                    assertNotNull(lock);
                    queue.addAndUnlock(lock, lock.hashCode());
                  }
                });
      }
      for (Thread thread : threads) {
        thread.start();
      }
      startingGun.countDown();
      for (Thread thread : threads) {
        thread.join();
      }
    }
  }
}
