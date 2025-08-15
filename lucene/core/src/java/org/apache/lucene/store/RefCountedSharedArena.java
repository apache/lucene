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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reference counted shared Arena.
 *
 * <p>The purpose of this class is to allow a number of mmapped memory segments to be associated
 * with a single underlying arena in order to avoid closing the underlying arena until all segments
 * are closed. Typically, these memory segments belong to the same logical group, e.g. individual
 * files of the same index segment. We do this to avoid the expensive cost of closing a shared
 * Arena.
 *
 * <p>The reference count is increased by {@link #acquire()}, and decreased by {@link #release()}.
 * When the reference count reaches 0, then the underlying arena is closed and the given {@code
 * onClose} runnable is executed. No more references can be acquired.
 *
 * <p>The total number of acquires that can be obtained for the lifetime of an instance of this
 * class is 1024. When the total number of acquires is exhausted, then no more acquires are
 * permitted and {@link #acquire()} returns false. This is independent of the actual number of the
 * ref count.
 */
final class RefCountedSharedArena implements Arena {

  // default maximum permits
  static final int DEFAULT_MAX_PERMITS = 64;

  private static final int CLOSED = 0;
  // minimum value, beyond which permits are exhausted
  private static final int REMAINING_UNIT = 1 << 16;
  // acquire decrement; effectively decrements permits and increments ref count
  private static final int ACQUIRE_DECREMENT = REMAINING_UNIT - 1; // 0xffff

  private final String segmentName;
  private final Runnable onClose;
  private final Arena arena;

  // high 16 bits contain the total remaining acquires; monotonically decreasing
  // low 16 bit contain the current ref count
  private final AtomicInteger state;

  RefCountedSharedArena(String segmentName, Runnable onClose) {
    this(segmentName, onClose, DEFAULT_MAX_PERMITS);
  }

  RefCountedSharedArena(String segmentName, Runnable onClose, int maxPermits) {
    if (validMaxPermits(maxPermits) == false) {
      throw new IllegalArgumentException("invalid max permits: " + maxPermits);
    }
    this.segmentName = segmentName;
    this.onClose = onClose;
    this.arena = Arena.ofShared();
    this.state = new AtomicInteger(maxPermits << 16);
  }

  static boolean validMaxPermits(int v) {
    return v > 0 && v <= 0x7FFF;
  }

  // for debugging
  String getSegmentName() {
    return segmentName;
  }

  /**
   * Returns true if the ref count has been increased. Otherwise, false if there are no remaining
   * acquires.
   */
  boolean acquire() {
    int value;
    while (true) {
      value = state.get();
      if (value < REMAINING_UNIT) {
        return false;
      }
      if (this.state.compareAndSet(value, value - ACQUIRE_DECREMENT)) {
        return true;
      }
    }
  }

  /** Decrements the ref count. */
  void release() {
    int value;
    while (true) {
      value = state.get();
      final int count = value & 0xFFFF;
      if (count == 0) {
        throw new IllegalStateException(value == CLOSED ? "closed" : "nothing to release");
      }
      final int newValue = count == 1 ? CLOSED : value - 1;
      if (this.state.compareAndSet(value, newValue)) {
        if (newValue == CLOSED) {
          onClose.run();
          arena.close();
        }
        return;
      }
    }
  }

  @Override
  public void close() {
    release();
  }

  @Override
  public MemorySegment allocate(long byteSize, long byteAlignment) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MemorySegment.Scope scope() {
    return arena.scope();
  }

  @Override
  public String toString() {
    return "RefCountedArena[segmentName="
        + segmentName
        + ", value="
        + state.get()
        + ", arena="
        + arena
        + "]";
  }
}
