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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/** A {@link ConcurrentApproximatePriorityQueue} of {@link Lock} objects. */
final class LockableConcurrentApproximatePriorityQueue<T extends Lock> {

  private final ConcurrentApproximatePriorityQueue<T> queue;
  private final AtomicInteger addAndUnlockCounter = new AtomicInteger();

  LockableConcurrentApproximatePriorityQueue(int concurrency) {
    this.queue = new ConcurrentApproximatePriorityQueue<>(concurrency);
  }

  LockableConcurrentApproximatePriorityQueue() {
    this.queue = new ConcurrentApproximatePriorityQueue<>();
  }

  /**
   * Lock an entry, and poll it from the queue, in that order. If no entry can be found and locked,
   * {@code null} is returned.
   */
  T lockAndPoll() {
    int addAndUnlockCount;
    do {
      addAndUnlockCount = addAndUnlockCounter.get();
      T entry = queue.poll(Lock::tryLock);
      if (entry != null) {
        return entry;
      }
      // If an entry has been added to the queue in the meantime, try again.
    } while (addAndUnlockCount != addAndUnlockCounter.get());

    return null;
  }

  /** Remove an entry from the queue. */
  boolean remove(Object o) {
    return queue.remove(o);
  }

  // Only used for assertions
  boolean contains(Object o) {
    return queue.contains(o);
  }

  /** Add an entry to the queue and unlock it, in that order. */
  void addAndUnlock(T entry, long weight) {
    queue.add(entry, weight);
    entry.unlock();
    addAndUnlockCounter.incrementAndGet();
  }
}
