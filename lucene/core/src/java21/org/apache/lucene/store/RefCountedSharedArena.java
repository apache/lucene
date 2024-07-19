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

@SuppressWarnings("preview")
final class RefCountedSharedArena implements Arena {

  static final int OPEN = 0;
  static final int CLOSED = -1;
  static final int LIMIT = 256;

  private final String segmentName;
  private final Runnable onClose;
  private final Arena arena;
  private final AtomicInteger state;

  RefCountedSharedArena(String segmentName, Runnable onClose) {
    this.segmentName = segmentName;
    this.onClose = onClose;
    this.arena = Arena.ofShared();
    this.state = new AtomicInteger(OPEN);
  }

  // for debugging
  String getSegmentName() {
    return segmentName;
  }

  boolean acquire() {
    int value;
    while (true) {
      value = state.get();
      if (value >= LIMIT) {
        return false;
      }
      if (value < OPEN) {
        throw new IllegalStateException("already closed");
      }
      if (state.compareAndSet(value, value + 1)) {
        return true;
      }
    }
  }

  void release() {
    int updatedValue = state.decrementAndGet();
    if (updatedValue < OPEN) {
      throw new IllegalStateException("already closed");
    }
    if (updatedValue == OPEN && state.compareAndSet(OPEN, CLOSED)) {
      onClose.run();
      arena.close();
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
