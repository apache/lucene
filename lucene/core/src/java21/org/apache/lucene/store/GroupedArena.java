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

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("preview")
final class GroupedArena implements Arena {

  private final String scopeId;

  private final ConcurrentHashMap<String, GroupedArena> arenas;

  private final Arena backing;

  private final AtomicInteger refCt;

  static Arena get(String scopeId, ConcurrentHashMap<String, GroupedArena> arenas) {
    Arena ret;
    do {
      boolean[] computed = new boolean[1];
      final GroupedArena template =
          arenas.computeIfAbsent(
              scopeId,
              (s) -> {
                computed[0] = true;
                return new GroupedArena(s, arenas);
              });
      if (computed[0]) {
        return template;
      }
      ret = template.cloneIfActive();
    } while (ret == null); // TODO: will this ever actually loop?
    return ret;
  }

  GroupedArena(String scopeId, ConcurrentHashMap<String, GroupedArena> arenas) {
    this.scopeId = scopeId;
    this.arenas = arenas;
    this.backing = Arena.ofShared();
    this.refCt = new AtomicInteger(1);
  }

  private GroupedArena(GroupedArena template) {
    this.scopeId = template.scopeId;
    this.arenas = template.arenas;
    this.backing = template.backing;
    this.refCt = template.refCt;
  }

  private GroupedArena cloneIfActive() {
    if (refCt.getAndIncrement() > 0) {
      // the usual (always?) case
      return new GroupedArena(this);
    } else {
      // TODO: this should never happen?
      return null;
    }
  }

  @Override
  public void close() {
    int ct = refCt.decrementAndGet();
    if (ct == 0) {
      arenas.remove(scopeId);
      if (refCt.get() == 0) {
        // TODO: this should always be the case? But if it's not, it should be a benign
        //  race condition. Whatever caller incremented `refCt` will close it, and if
        //  anyone tries to open a new arena with the same `scopeId` that we removed
        //  above, they'll simply create a new Arena, and we're no worse off than we
        //  would have been if every Arena was created as a one-off.
        backing.close();
      }
    } else {
      assert ct > 0 : "refCt should never be negative; found " + ct;
    }
  }

  @Override
  public MemorySegment allocate(long byteSize, long byteAlignment) {
    return backing.allocate(byteSize, byteAlignment);
  }

  @Override
  public MemorySegment.Scope scope() {
    return backing.scope();
  }

  @Override
  public MemorySegment allocateUtf8String(String str) {
    return backing.allocateUtf8String(str);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfByte layout, byte value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfChar layout, char value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfShort layout, short value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfInt layout, int value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfFloat layout, float value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfLong layout, long value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(ValueLayout.OfDouble layout, double value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocate(AddressLayout layout, MemorySegment value) {
    return backing.allocate(layout, value);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfByte elementLayout, byte... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfShort elementLayout, short... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfChar elementLayout, char... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfInt elementLayout, int... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfFloat elementLayout, float... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfLong elementLayout, long... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocateArray(ValueLayout.OfDouble elementLayout, double... elements) {
    return backing.allocateArray(elementLayout, elements);
  }

  @Override
  public MemorySegment allocate(MemoryLayout layout) {
    return backing.allocate(layout);
  }

  @Override
  public MemorySegment allocateArray(MemoryLayout elementLayout, long count) {
    return backing.allocateArray(elementLayout, count);
  }

  @Override
  public MemorySegment allocate(long byteSize) {
    return backing.allocate(byteSize);
  }
}
