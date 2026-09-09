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
package org.apache.lucene.internal.hppc;

import java.util.Iterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A read-only view of an {@link IntObjectHashMap} that may use dense storage for non-negative keys.
 *
 * <p>This map is intended for field-number maps that are built once with {@link IntObjectHashMap}
 * and then only queried. When the key range is dense enough, values are copied into an {@code
 * Object[]} indexed by key. Otherwise, this class delegates lookups and iteration to the original
 * {@link IntObjectHashMap}.
 *
 * @lucene.internal
 */
@SuppressWarnings("unchecked")
public final class ReadOnlyIntObjectMap<VType>
    implements Iterable<IntObjectHashMap.IntObjectCursor<VType>>, Accountable {

  // Avoid changing the map implementation unless the dense array removes meaningful slack.
  private static final int MIN_SLOT_SAVINGS_PERCENT = 30;

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(ReadOnlyIntObjectMap.class);

  private final IntObjectHashMap<VType> delegate;
  private Object[] denseValues;
  private int denseSize;

  public static <VType> ReadOnlyIntObjectMap<VType> wrap(IntObjectHashMap<VType> map) {
    return wrap(map, MIN_SLOT_SAVINGS_PERCENT);
  }

  public static <VType> ReadOnlyIntObjectMap<VType> wrap(
      IntObjectHashMap<VType> map, int minSlotSavingsPercent) {
    if (minSlotSavingsPercent < 0 || minSlotSavingsPercent > 100) {
      throw new IllegalArgumentException(
          "minSlotSavingsPercent must be in [0, 100]: " + minSlotSavingsPercent);
    }
    int maxKey = -1;
    for (IntObjectHashMap.IntObjectCursor<VType> cursor : map) {
      if (cursor.key < 0 || cursor.value == null) {
        return new ReadOnlyIntObjectMap<>(map);
      }
      maxKey = Math.max(maxKey, cursor.key);
    }
    if (maxKey == Integer.MAX_VALUE) {
      return new ReadOnlyIntObjectMap<>(map);
    }
    int denseSlots = maxKey + 1;
    int savedSlots = map.values.length - denseSlots;
    return savedSlots * 100L >= (long) map.values.length * minSlotSavingsPercent
        ? new ReadOnlyIntObjectMap<>(map, maxKey)
        : new ReadOnlyIntObjectMap<>(map);
  }

  private ReadOnlyIntObjectMap(IntObjectHashMap<VType> delegate) {
    this.delegate = delegate;
  }

  private ReadOnlyIntObjectMap(IntObjectHashMap<? extends VType> map, int maxKey) {
    delegate = null;
    denseValues = new Object[maxKey + 1];
    for (IntObjectHashMap.IntObjectCursor<? extends VType> cursor : map) {
      denseValues[cursor.key] = cursor.value;
    }
    denseSize = map.size();
  }

  boolean usesDenseStorage() {
    return denseValues != null;
  }

  public VType get(int key) {
    if (delegate != null) {
      return delegate.get(key);
    }
    return key >= 0 && key < denseValues.length ? (VType) denseValues[key] : null;
  }

  public VType getOrDefault(int key, VType defaultValue) {
    if (delegate != null) {
      return delegate.getOrDefault(key, defaultValue);
    }
    VType value = get(key);
    return value == null ? defaultValue : value;
  }

  public boolean containsKey(int key) {
    if (delegate != null) {
      return delegate.containsKey(key);
    }
    return get(key) != null;
  }

  public int indexOf(int key) {
    if (delegate != null) {
      return delegate.indexOf(key);
    }
    if (containsKey(key)) {
      return key;
    }
    return key >= 0 ? ~key : -1;
  }

  public boolean indexExists(int index) {
    if (delegate != null) {
      return delegate.indexExists(index);
    }
    return index >= 0 && index < denseValues.length && denseValues[index] != null;
  }

  public VType indexGet(int index) {
    if (delegate != null) {
      return delegate.indexGet(index);
    }
    assert indexExists(index) : "The index must point at an existing key.";
    return (VType) denseValues[index];
  }

  public int size() {
    return delegate != null ? delegate.size() : denseSize;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public void release() {
    if (delegate != null) {
      delegate.release();
      return;
    }
    denseValues = new Object[0];
    denseSize = 0;
  }

  @Override
  public Iterator<IntObjectHashMap.IntObjectCursor<VType>> iterator() {
    if (delegate != null) {
      return delegate.iterator();
    }
    return new EntryIterator();
  }

  public Keys keys() {
    return new Keys();
  }

  public final class Keys implements Iterable<IntCursor> {
    @Override
    public Iterator<IntCursor> iterator() {
      if (delegate != null) {
        return delegate.keys().iterator();
      }
      return new KeysIterator();
    }

    public int size() {
      return ReadOnlyIntObjectMap.this.size();
    }

    public int[] toArray() {
      int[] array = new int[size()];
      int i = 0;
      for (IntCursor cursor : this) {
        array[i++] = cursor.value;
      }
      return array;
    }
  }

  public Values values() {
    return new Values();
  }

  public final class Values implements Iterable<ObjectCursor<VType>> {
    @Override
    public Iterator<ObjectCursor<VType>> iterator() {
      if (delegate != null) {
        return delegate.values().iterator();
      }
      return new ValuesIterator();
    }

    public int size() {
      return ReadOnlyIntObjectMap.this.size();
    }
  }

  @Override
  public long ramBytesUsed() {
    if (delegate != null) {
      return delegate.ramBytesUsed() + BASE_RAM_BYTES_USED;
    }
    long size = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(denseValues);
    for (ObjectCursor<VType> value : values()) {
      size += RamUsageEstimator.sizeOfObject(value.value);
    }
    return size;
  }

  private final class EntryIterator
      extends AbstractIterator<IntObjectHashMap.IntObjectCursor<VType>> {
    private final IntObjectHashMap.IntObjectCursor<VType> cursor =
        new IntObjectHashMap.IntObjectCursor<>();
    private int index;

    @Override
    protected IntObjectHashMap.IntObjectCursor<VType> fetch() {
      while (index < denseValues.length) {
        Object value = denseValues[index];
        if (value != null) {
          cursor.index = index;
          cursor.key = index++;
          cursor.value = (VType) value;
          return cursor;
        }
        index++;
      }
      return done();
    }
  }

  private final class KeysIterator extends AbstractIterator<IntCursor> {
    private final IntCursor cursor = new IntCursor();
    private int index;

    @Override
    protected IntCursor fetch() {
      while (index < denseValues.length) {
        if (denseValues[index] != null) {
          cursor.index = index;
          cursor.value = index++;
          return cursor;
        }
        index++;
      }
      return done();
    }
  }

  private final class ValuesIterator extends AbstractIterator<ObjectCursor<VType>> {
    private final ObjectCursor<VType> cursor = new ObjectCursor<>();
    private int index;

    @Override
    protected ObjectCursor<VType> fetch() {
      while (index < denseValues.length) {
        Object value = denseValues[index];
        if (value != null) {
          cursor.index = index++;
          cursor.value = (VType) value;
          return cursor;
        }
        index++;
      }
      return done();
    }
  }
}
