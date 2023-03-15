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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Concurrent version of {@link ApproximatePriorityQueue}, which trades a bit more of ordering for
 * better concurrency by maintaining 8 sub {@link ApproximatePriorityQueue}s that are locked
 * independently.
 */
final class ConcurrentApproximatePriorityQueue<T> {

  /** Keeping 8 queues should already help a lot compared to a single one. */
  static final int CONCURRENCY = 8;

  private static final int MASK = 0x07;

  final Lock[] locks;
  final ApproximatePriorityQueue<T>[] queues;

  ConcurrentApproximatePriorityQueue() {
    locks = new Lock[CONCURRENCY];
    @SuppressWarnings({"rawtypes", "unchecked"})
    ApproximatePriorityQueue<T>[] queues = new ApproximatePriorityQueue[CONCURRENCY];
    this.queues = queues;
    for (int i = 0; i < CONCURRENCY; ++i) {
      locks[i] = new ReentrantLock();
      queues[i] = new ApproximatePriorityQueue<>();
    }
  }

  void add(T entry, long weight) {
    // Seed the order in which to look at entries based on the current thread. This helps distribute
    // entries across queues and gives a bit of thread affinity between entries and threads, which
    // can't hurt.
    final int threadHash = Thread.currentThread().hashCode();
    for (int i = 0; i < CONCURRENCY; ++i) {
      final int index = (threadHash + i) & MASK;
      final Lock lock = locks[index];
      final ApproximatePriorityQueue<T> queue = queues[index];
      if (lock.tryLock()) {
        try {
          queue.add(entry, weight);
          return;
        } finally {
          lock.unlock();
        }
      }
    }
    final int index = threadHash & MASK;
    final Lock lock = locks[index];
    final ApproximatePriorityQueue<T> queue = queues[index];
    lock.lock();
    try {
      queue.add(entry, weight);
    } finally {
      lock.unlock();
    }
  }

  T poll(Predicate<T> predicate) {
    final int threadHash = Thread.currentThread().hashCode();
    for (int i = 0; i < CONCURRENCY; ++i) {
      final int index = (threadHash + i) & MASK;
      final Lock lock = locks[index];
      final ApproximatePriorityQueue<T> queue = queues[index];
      if (lock.tryLock()) {
        try {
          T entry = queue.poll(predicate);
          if (entry != null) {
            return entry;
          }
        } finally {
          lock.unlock();
        }
      }
    }
    for (int i = 0; i < CONCURRENCY; ++i) {
      final int index = (threadHash + i) & MASK;
      final Lock lock = locks[index];
      final ApproximatePriorityQueue<T> queue = queues[index];
      lock.lock();
      try {
        T entry = queue.poll(predicate);
        if (entry != null) {
          return entry;
        }
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  // Only used for assertions
  boolean contains(Object o) {
    boolean assertionsAreEnabled = false;
    assert assertionsAreEnabled = true;
    if (assertionsAreEnabled == false) {
      throw new AssertionError("contains should only be used for assertions");
    }

    for (int i = 0; i < CONCURRENCY; ++i) {
      final Lock lock = locks[i];
      final ApproximatePriorityQueue<T> queue = queues[i];
      lock.lock();
      try {
        if (queue.contains(o)) {
          return true;
        }
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  boolean remove(Object o) {
    for (int i = 0; i < CONCURRENCY; ++i) {
      final Lock lock = locks[i];
      final ApproximatePriorityQueue<T> queue = queues[i];
      lock.lock();
      try {
        if (queue.remove(o)) {
          return true;
        }
      } finally {
        lock.unlock();
      }
    }
    return false;
  }
}
