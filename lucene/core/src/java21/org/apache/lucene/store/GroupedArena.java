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
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexFileNames;

@SuppressWarnings("preview")
final class GroupedArena implements Arena {

  private final String scopeId;

  private final ConcurrentHashMap<String, GroupedArena> arenas;

  private final Arena backing;

  private final AtomicInteger refCt;

  static Arena get(Path p, ConcurrentHashMap<String, GroupedArena> arenas) {
    String filename = p.getFileName().toString();
    String segmentName = IndexFileNames.parseSegmentName(filename);
    if (filename.length() == segmentName.length()) {
      // no segment found; return a 1-off Arena
      // TODO: what if the associated Directory is used for some purpose other than Lucene
      //  index files, and some of the filenames happen to be patterned so that a "segment"
      //  can be parsed from them? That could result in files being spuriously grouped
      //  together in the same arena. This would only practically be a problem if such a
      //  "segment" was never closed (refCt never 0), _and_ enough files associated with that
      //  "segment" were opened and closed (without ever closing the associated Arena)
      //  to exhaust virtual memory space.
      return Arena.ofShared();
    }
    GroupedArena ret;
    // We loop below to protect against the possibility that we get an entry that is
    // in the process of being closed. It is unlikely (perhaps impossible?) that this
    // would happen in practice, based on how Lucene opens IndexInputs. But if we
    // don't check here it's theoretically possible that our return value would be
    // backed by an Arena that is closed (and would throw IllegalStateException).
    // NOTE also: if we admit the possibility that a `Directory` can be used for
    // purposes other than a Lucene index, then we don't have the implicit protection
    // conferred by expected usage patterns, and it becomes all the more important to
    // loop to protect against the possibility of concurrent open/close.
    do {
      ret = arenas.computeIfAbsent(segmentName, (s) -> new GroupedArena(s, arenas));
    } while (ret.refCt.getAndIncrement() < 1);
    return ret;
  }

  GroupedArena(String scopeId, ConcurrentHashMap<String, GroupedArena> arenas) {
    this.scopeId = scopeId;
    this.arenas = arenas;
    this.backing = Arena.ofShared();
    this.refCt = new AtomicInteger(1); // initial reference from `arenas` ConcurrentHashMap
  }

  @Override
  public void close() {
    int ct = refCt.decrementAndGet();
    if (ct == 1) {
      // the only reference remaining is from the ConcurrentHashMap -- this is no longer
      // being used.
      if (refCt.compareAndSet(1, 0)) {
        // If a new IndexInput was opened against this segment while we were in the
        // process of closing, it's possible that the above CAS will return false.
        // See note about looping in `GroupedArena.get(...)`.
        try {
          GroupedArena removed = arenas.remove(scopeId);
          assert removed == this;
        } finally {
          backing.close();
        }
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
