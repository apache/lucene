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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A read-only dense map of non-negative int keys to non-null object values.
 *
 * <p>This map stores values directly in an {@code Object[]} indexed by key. It is intended for
 * field-number maps that are built once with {@link IntObjectHashMap} and then only queried.
 *
 * @lucene.internal
 */
@SuppressWarnings("unchecked")
public final class ReadOnlyDenseIntObjectMap<VType> extends IntObjectHashMap<VType> {

  // Avoid changing the map implementation unless the dense array removes meaningful slack.
  private static final int MIN_SLOT_SAVINGS_PERCENT = 30;

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(ReadOnlyDenseIntObjectMap.class);

  public static <VType> IntObjectHashMap<VType> maybeWrap(IntObjectHashMap<VType> map) {
    return maybeWrap(map, MIN_SLOT_SAVINGS_PERCENT);
  }

  public static <VType> IntObjectHashMap<VType> maybeWrap(
      IntObjectHashMap<VType> map, int minSlotSavingsPercent) {
    if (minSlotSavingsPercent < 0 || minSlotSavingsPercent > 100) {
      throw new IllegalArgumentException(
          "minSlotSavingsPercent must be in [0, 100]: " + minSlotSavingsPercent);
    }
    int maxKey = -1;
    for (IntObjectCursor<VType> cursor : map) {
      if (cursor.key < 0 || cursor.value == null) {
        return map;
      }
      maxKey = Math.max(maxKey, cursor.key);
    }
    if (maxKey == Integer.MAX_VALUE) {
      return map;
    }
    int denseSlots = maxKey + 1;
    int savedSlots = map.values.length - denseSlots;
    return savedSlots * 100L >= (long) map.values.length * minSlotSavingsPercent
        ? new ReadOnlyDenseIntObjectMap<>(map)
        : map;
  }

  public ReadOnlyDenseIntObjectMap(IntObjectHashMap<? extends VType> map) {
    super(0);

    int maxKey = -1;
    for (IntObjectCursor<? extends VType> cursor : map) {
      if (cursor.key < 0) {
        throw new IllegalArgumentException("Dense int map only supports non-negative keys");
      }
      if (cursor.value == null) {
        throw new IllegalArgumentException("Dense int map does not support null values");
      }
      maxKey = Math.max(maxKey, cursor.key);
    }
    keys = null;
    values = new Object[maxKey + 1];
    for (IntObjectCursor<? extends VType> cursor : map) {
      values[cursor.key] = cursor.value;
    }
    assigned = map.size();
    hasEmptyKey = values.length > 0 && values[0] != null;
  }

  @Override
  public VType put(int key, VType value) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public boolean putIfAbsent(int key, VType value) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public VType remove(int key) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public int putAll(Iterable<? extends IntObjectCursor<? extends VType>> iterable) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public VType indexReplace(int index, VType newValue) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public void indexInsert(int index, int key, VType value) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public VType indexRemove(int index) {
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public VType get(int key) {
    return key >= 0 && key < values.length ? (VType) values[key] : null;
  }

  @Override
  public VType getOrDefault(int key, VType defaultValue) {
    VType value = get(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public boolean containsKey(int key) {
    return get(key) != null;
  }

  @Override
  public int indexOf(int key) {
    if (containsKey(key)) {
      return key;
    }
    return key >= 0 ? ~key : -1;
  }

  @Override
  public boolean indexExists(int index) {
    return index >= 0 && index < values.length && values[index] != null;
  }

  @Override
  public VType indexGet(int index) {
    assert indexExists(index) : "The index must point at an existing key.";
    return (VType) values[index];
  }

  @Override
  public void clear() {
    Arrays.fill(values, null);
    assigned = 0;
    hasEmptyKey = false;
  }

  @Override
  public void release() {
    values = new Object[0];
    assigned = 0;
    hasEmptyKey = false;
  }

  @Override
  public void ensureCapacity(int expectedElements) {
    if (keys == null && assigned == 0 && resizeAt == 0) {
      super.ensureCapacity(expectedElements);
      return;
    }
    throw new UnsupportedOperationException("This map is read-only");
  }

  @Override
  public int size() {
    return assigned;
  }

  @Override
  public boolean isEmpty() {
    return assigned == 0;
  }

  @Override
  public Iterator<IntObjectCursor<VType>> iterator() {
    return new EntryIterator();
  }

  @Override
  public KeysContainer keys() {
    return new DenseKeysContainer();
  }

  @Override
  public ValuesContainer values() {
    return new DenseValuesContainer();
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(values);
    for (ObjectCursor<VType> value : values()) {
      size += RamUsageEstimator.sizeOfObject(value.value);
    }
    return size;
  }

  @Override
  public ReadOnlyDenseIntObjectMap<VType> clone() {
    IntObjectHashMap<VType> map = new IntObjectHashMap<>(size());
    for (IntObjectCursor<VType> cursor : this) {
      map.put(cursor.key, cursor.value);
    }
    return new ReadOnlyDenseIntObjectMap<>(map);
  }

  private final class EntryIterator extends AbstractIterator<IntObjectCursor<VType>> {
    private final IntObjectCursor<VType> cursor = new IntObjectCursor<>();
    private int index;

    @Override
    protected IntObjectCursor<VType> fetch() {
      while (index < values.length) {
        Object value = values[index];
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

  private final class DenseKeysContainer extends KeysContainer {
    @Override
    public Iterator<IntCursor> iterator() {
      return new KeysIterator();
    }
  }

  private final class KeysIterator extends AbstractIterator<IntCursor> {
    private final IntCursor cursor = new IntCursor();
    private int index;

    @Override
    protected IntCursor fetch() {
      while (index < values.length) {
        if (values[index] != null) {
          cursor.index = index;
          cursor.value = index++;
          return cursor;
        }
        index++;
      }
      return done();
    }
  }

  private final class DenseValuesContainer extends ValuesContainer {
    @Override
    public Iterator<ObjectCursor<VType>> iterator() {
      return new ValuesIterator();
    }
  }

  private final class ValuesIterator extends AbstractIterator<ObjectCursor<VType>> {
    private final ObjectCursor<VType> cursor = new ObjectCursor<>();
    private int index;

    @Override
    protected ObjectCursor<VType> fetch() {
      while (index < values.length) {
        Object value = values[index];
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
