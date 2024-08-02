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

package org.apache.lucene.util.hnsw;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provide (read-and-write) locked access to rows of an OnHeapHnswGraph. For use by
 * HnswConcurrentMerger and its HnswGraphBuilders.
 */
class HnswLock {
  private static final int NUM_LOCKS = 512;
  private final ReentrantReadWriteLock[] locks;
  private final OnHeapHnswGraph graph;

  HnswLock(OnHeapHnswGraph graph) {
    this.graph = graph;
    locks = new ReentrantReadWriteLock[NUM_LOCKS];
    for (int i = 0; i < NUM_LOCKS; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  LockedRow read(int level, int node) {
    int lockid = Objects.hash(level, node) % NUM_LOCKS;
    Lock lock = locks[lockid].readLock();
    lock.lock();
    return new LockedRow(graph.getNeighbors(level, node), lock);
  }

  LockedRow write(int level, int node) {
    int lockid = Objects.hash(level, node) % NUM_LOCKS;
    Lock lock = locks[lockid].writeLock();
    lock.lock();
    return new LockedRow(graph.getNeighbors(level, node), lock);
  }

  static class LockedRow implements Closeable {
    final Lock lock;
    final NeighborArray row;

    LockedRow(NeighborArray row, Lock lock) {
      this.lock = lock;
      this.row = row;
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
