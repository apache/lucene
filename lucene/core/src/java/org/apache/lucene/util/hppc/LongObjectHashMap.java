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

package org.apache.lucene.util.hppc;

import static org.apache.lucene.util.hppc.HashContainers.*;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A hash map of <code>long</code> to <code>Object</code>, implemented using open addressing with
 * linear probing for collision resolution. Supports null values.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongObjectHashMap
 *
 * <p>github: https://github.com/carrotsearch/hppc release 0.9.0
 */
@SuppressWarnings("unchecked")
public class LongObjectHashMap<VType>
    implements Iterable<LongObjectHashMap.LongObjectCursor<VType>>, Accountable, Cloneable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(LongObjectHashMap.class);

  /** The array holding keys. */
  public long[] keys;

  /** The array holding values. */
  public Object[] values;

  /**
   * The number of stored keys (assigned key slots), excluding the special "empty" key, if any (use
   * {@link #size()} instead).
   *
   * @see #size()
   */
  protected int assigned;

  /** Mask for slot scans in {@link #keys}. */
  protected int mask;

  /** Expand (rehash) {@link #keys} when {@link #assigned} hits this value. */
  protected int resizeAt;

  /** Special treatment for the "empty slot" key marker. */
  protected boolean hasEmptyKey;

  /** The load factor for {@link #keys}. */
  protected double loadFactor;

  /** Seed used to ensure the hash iteration order is different from an iteration to another. */
  protected int iterationSeed;

  /** New instance with sane defaults. */
  public LongObjectHashMap() {
    this(DEFAULT_EXPECTED_ELEMENTS);
  }

  /**
   * New instance with sane defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause buffer
   *     expansion (inclusive).
   */
  public LongObjectHashMap(int expectedElements) {
    this(expectedElements, DEFAULT_LOAD_FACTOR);
  }

  /**
   * New instance with the provided defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause a rehash
   *     (inclusive).
   * @param loadFactor The load factor for internal buffers. Insane load factors (zero, full
   *     capacity) are rejected by {@link #verifyLoadFactor(double)}.
   */
  public LongObjectHashMap(int expectedElements, double loadFactor) {
    this.loadFactor = verifyLoadFactor(loadFactor);
    iterationSeed = ITERATION_SEED.incrementAndGet();
    ensureCapacity(expectedElements);
  }

  /** Create a hash map from all key-value pairs of another map. */
  public LongObjectHashMap(LongObjectHashMap<VType> map) {
    this(map.size());
    putAll(map);
  }

  public VType put(long key, VType value) {
    assert assigned < mask + 1;

    final int mask = this.mask;
    if (((key) == 0)) {
      VType previousValue = hasEmptyKey ? (VType) values[mask + 1] : null;
      hasEmptyKey = true;
      values[mask + 1] = value;
      return previousValue;
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final VType previousValue = (VType) values[slot];
          values[slot] = value;
          return previousValue;
        }
        slot = (slot + 1) & mask;
      }

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(slot, key, value);
      } else {
        keys[slot] = key;
        values[slot] = value;
      }

      assigned++;
      return null;
    }
  }

  public int putAll(Iterable<? extends LongObjectCursor<? extends VType>> iterable) {
    final int count = size();
    for (LongObjectCursor<? extends VType> c : iterable) {
      put(c.key, c.value);
    }
    return size() - count;
  }

  /**
   * <a href="http://trove4j.sourceforge.net">Trove</a>-inspired API method. An equivalent of the
   * following code:
   *
   * <pre>
   * if (!map.containsKey(key)) map.put(value);
   * </pre>
   *
   * @param key The key of the value to check.
   * @param value The value to put if <code>key</code> does not exist.
   * @return <code>true</code> if <code>key</code> did not exist and <code>value</code> was placed
   *     in the map.
   */
  public boolean putIfAbsent(long key, VType value) {
    int keyIndex = indexOf(key);
    if (!indexExists(keyIndex)) {
      indexInsert(keyIndex, key, value);
      return true;
    } else {
      return false;
    }
  }

  public VType remove(long key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      if (!hasEmptyKey) {
        return null;
      }
      hasEmptyKey = false;
      VType previousValue = (VType) values[mask + 1];
      values[mask + 1] = 0;
      return previousValue;
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final VType previousValue = (VType) values[slot];
          shiftConflictingKeys(slot);
          return previousValue;
        }
        slot = (slot + 1) & mask;
      }

      return null;
    }
  }

  public VType get(long key) {
    if (((key) == 0)) {
      return hasEmptyKey ? (VType) values[mask + 1] : null;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return (VType) values[slot];
        }
        slot = (slot + 1) & mask;
      }

      return null;
    }
  }

  public VType getOrDefault(long key, VType defaultValue) {
    if (((key) == 0)) {
      return hasEmptyKey ? (VType) values[mask + 1] : defaultValue;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return (VType) values[slot];
        }
        slot = (slot + 1) & mask;
      }

      return defaultValue;
    }
  }

  public boolean containsKey(long key) {
    if (((key) == 0)) {
      return hasEmptyKey;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return true;
        }
        slot = (slot + 1) & mask;
      }

      return false;
    }
  }

  public int indexOf(long key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      return hasEmptyKey ? mask + 1 : ~(mask + 1);
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return slot;
        }
        slot = (slot + 1) & mask;
      }

      return ~slot;
    }
  }

  public boolean indexExists(int index) {
    assert index < 0 || (index >= 0 && index <= mask) || (index == mask + 1 && hasEmptyKey);

    return index >= 0;
  }

  public VType indexGet(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    return (VType) values[index];
  }

  public VType indexReplace(int index, VType newValue) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    VType previousValue = (VType) values[index];
    values[index] = newValue;
    return previousValue;
  }

  public void indexInsert(int index, long key, VType value) {
    assert index < 0 : "The index must not point at an existing key.";

    index = ~index;
    if (((key) == 0)) {
      assert index == mask + 1;
      values[index] = value;
      hasEmptyKey = true;
    } else {
      assert ((keys[index]) == 0);

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(index, key, value);
      } else {
        keys[index] = key;
        values[index] = value;
      }

      assigned++;
    }
  }

  public VType indexRemove(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    VType previousValue = (VType) values[index];
    if (index > mask) {
      assert index == mask + 1;
      hasEmptyKey = false;
      values[index] = 0;
    } else {
      shiftConflictingKeys(index);
    }
    return previousValue;
  }

  public void clear() {
    assigned = 0;
    hasEmptyKey = false;

    Arrays.fill(keys, 0);

    /*  */
  }

  public void release() {
    assigned = 0;
    hasEmptyKey = false;

    keys = null;
    values = null;
    ensureCapacity(DEFAULT_EXPECTED_ELEMENTS);
  }

  public int size() {
    return assigned + (hasEmptyKey ? 1 : 0);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public int hashCode() {
    int h = hasEmptyKey ? 0xDEADBEEF : 0;
    for (LongObjectCursor<VType> c : this) {
      h += BitMixer.mix(c.key) + BitMixer.mix(c.value);
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj)
        || (obj != null && getClass() == obj.getClass() && equalElements(getClass().cast(obj)));
  }

  /** Return true if all keys of some other container exist in this container. */
  protected boolean equalElements(LongObjectHashMap<?> other) {
    if (other.size() != size()) {
      return false;
    }

    for (LongObjectCursor<?> c : other) {
      long key = c.key;
      if (!containsKey(key) || !java.util.Objects.equals(c.value, get(key))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Ensure this container can hold at least the given number of keys (entries) without resizing its
   * buffers.
   *
   * @param expectedElements The total number of keys, inclusive.
   */
  public void ensureCapacity(int expectedElements) {
    if (expectedElements > resizeAt || keys == null) {
      final long[] prevKeys = this.keys;
      final VType[] prevValues = (VType[]) this.values;
      allocateBuffers(minBufferSize(expectedElements, loadFactor));
      if (prevKeys != null && !isEmpty()) {
        rehash(prevKeys, prevValues);
      }
    }
  }

  /**
   * Provides the next iteration seed used to build the iteration starting slot and offset
   * increment. This method does not need to be synchronized, what matters is that each thread gets
   * a sequence of varying seeds.
   */
  protected int nextIterationSeed() {
    return iterationSeed = BitMixer.mixPhi(iterationSeed);
  }

  @Override
  public Iterator<LongObjectCursor<VType>> iterator() {
    return new EntryIterator();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(keys) + sizeOfValues();
  }

  private long sizeOfValues() {
    long size = RamUsageEstimator.shallowSizeOf(values);
    for (ObjectCursor<VType> value : values()) {
      size += RamUsageEstimator.sizeOfObject(value);
    }
    return size;
  }

  /** An iterator implementation for {@link #iterator}. */
  private final class EntryIterator extends AbstractIterator<LongObjectCursor<VType>> {
    private final LongObjectCursor<VType> cursor;
    private final int increment;
    private int index;
    private int slot;

    public EntryIterator() {
      cursor = new LongObjectCursor<VType>();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected LongObjectCursor<VType> fetch() {
      final int mask = LongObjectHashMap.this.mask;
      while (index <= mask) {
        long existing;
        index++;
        slot = (slot + increment) & mask;
        if (!((existing = keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.key = existing;
          cursor.value = (VType) values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.key = 0;
        cursor.value = (VType) values[index++];
        return cursor;
      }

      return done();
    }
  }

  /** Returns a specialized view of the keys of this associated container. */
  public KeysContainer keys() {
    return new KeysContainer();
  }

  /** A view of the keys inside this hash map. */
  public final class KeysContainer implements Iterable<LongCursor> {

    @Override
    public Iterator<LongCursor> iterator() {
      return new KeysIterator();
    }

    public int size() {
      return LongObjectHashMap.this.size();
    }

    public long[] toArray() {
      long[] array = new long[size()];
      int i = 0;
      for (LongCursor cursor : this) {
        array[i++] = cursor.value;
      }
      return array;
    }
  }

  /** An iterator over the set of assigned keys. */
  private final class KeysIterator extends AbstractIterator<LongCursor> {
    private final LongCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public KeysIterator() {
      cursor = new LongCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected LongCursor fetch() {
      final int mask = LongObjectHashMap.this.mask;
      while (index <= mask) {
        long existing;
        index++;
        slot = (slot + increment) & mask;
        if (!((existing = keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.value = existing;
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index++;
        cursor.value = 0;
        return cursor;
      }

      return done();
    }
  }

  /**
   * @return Returns a container with all values stored in this map.
   */
  public ValuesContainer values() {
    return new ValuesContainer();
  }

  /** A view over the set of values of this map. */
  public final class ValuesContainer implements Iterable<ObjectCursor<VType>> {

    @Override
    public Iterator<ObjectCursor<VType>> iterator() {
      return new ValuesIterator();
    }

    public int size() {
      return LongObjectHashMap.this.size();
    }

    public VType[] toArray() {
      VType[] array = (VType[]) new Object[size()];
      int i = 0;
      for (ObjectCursor<VType> cursor : this) {
        array[i++] = cursor.value;
      }
      return array;
    }
  }

  /** An iterator over the set of assigned values. */
  private final class ValuesIterator extends AbstractIterator<ObjectCursor<VType>> {
    private final ObjectCursor<VType> cursor;
    private final int increment;
    private int index;
    private int slot;

    public ValuesIterator() {
      cursor = new ObjectCursor<>();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected ObjectCursor<VType> fetch() {
      final int mask = LongObjectHashMap.this.mask;
      while (index <= mask) {
        index++;
        slot = (slot + increment) & mask;
        if (!((keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.value = (VType) values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.value = (VType) values[index++];
        return cursor;
      }

      return done();
    }
  }

  @Override
  public LongObjectHashMap<VType> clone() {
    try {
      /*  */
      LongObjectHashMap<VType> cloned = (LongObjectHashMap<VType>) super.clone();
      cloned.keys = keys.clone();
      cloned.values = values.clone();
      cloned.hasEmptyKey = hasEmptyKey;
      cloned.iterationSeed = ITERATION_SEED.incrementAndGet();
      return cloned;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /** Convert the contents of this map to a human-friendly string. */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("[");

    boolean first = true;
    for (LongObjectCursor<VType> cursor : this) {
      if (!first) {
        buffer.append(", ");
      }
      buffer.append(cursor.key);
      buffer.append("=>");
      buffer.append(cursor.value);
      first = false;
    }
    buffer.append("]");
    return buffer.toString();
  }

  /** Creates a hash map from two index-aligned arrays of key-value pairs. */
  public static <VType> LongObjectHashMap<VType> from(long[] keys, VType[] values) {
    if (keys.length != values.length) {
      throw new IllegalArgumentException(
          "Arrays of keys and values must have an identical length.");
    }

    LongObjectHashMap<VType> map = new LongObjectHashMap<>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }

    return map;
  }

  /**
   * Returns a hash code for the given key.
   *
   * <p>The output from this function should evenly distribute keys across the entire integer range.
   */
  protected int hashKey(long key) {
    assert !((key) == 0); // Handled as a special case (empty slot marker).
    return BitMixer.mixPhi(key);
  }

  /**
   * Validate load factor range and return it. Override and suppress if you need insane load
   * factors.
   */
  protected double verifyLoadFactor(double loadFactor) {
    checkLoadFactor(loadFactor, MIN_LOAD_FACTOR, MAX_LOAD_FACTOR);
    return loadFactor;
  }

  /** Rehash from old buffers to new buffers. */
  protected void rehash(long[] fromKeys, VType[] fromValues) {
    assert fromKeys.length == fromValues.length && checkPowerOfTwo(fromKeys.length - 1);

    // Rehash all stored key/value pairs into the new buffers.
    final long[] keys = this.keys;
    final VType[] values = (VType[]) this.values;
    final int mask = this.mask;
    long existing;

    // Copy the zero element's slot, then rehash everything else.
    int from = fromKeys.length - 1;
    keys[keys.length - 1] = fromKeys[from];
    values[values.length - 1] = fromValues[from];
    while (--from >= 0) {
      if (!((existing = fromKeys[from]) == 0)) {
        int slot = hashKey(existing) & mask;
        while (!((keys[slot]) == 0)) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = existing;
        values[slot] = fromValues[from];
      }
    }
  }

  /**
   * Allocate new internal buffers. This method attempts to allocate and assign internal buffers
   * atomically (either allocations succeed or not).
   */
  protected void allocateBuffers(int arraySize) {
    assert Integer.bitCount(arraySize) == 1;

    // Ensure no change is done if we hit an OOM.
    long[] prevKeys = this.keys;
    VType[] prevValues = (VType[]) this.values;
    try {
      int emptyElementSlot = 1;
      this.keys = (new long[arraySize + emptyElementSlot]);
      this.values = new Object[arraySize + emptyElementSlot];
    } catch (OutOfMemoryError e) {
      this.keys = prevKeys;
      this.values = prevValues;
      throw new BufferAllocationException(
          "Not enough memory to allocate buffers for rehashing: %,d -> %,d",
          e, this.mask + 1, arraySize);
    }

    this.resizeAt = expandAtCount(arraySize, loadFactor);
    this.mask = arraySize - 1;
  }

  /**
   * This method is invoked when there is a new key/ value pair to be inserted into the buffers but
   * there is not enough empty slots to do so.
   *
   * <p>New buffers are allocated. If this succeeds, we know we can proceed with rehashing so we
   * assign the pending element to the previous buffer (possibly violating the invariant of having
   * at least one empty slot) and rehash all keys, substituting new buffers at the end.
   */
  protected void allocateThenInsertThenRehash(int slot, long pendingKey, VType pendingValue) {
    assert assigned == resizeAt && ((keys[slot]) == 0) && !((pendingKey) == 0);

    // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
    final long[] prevKeys = this.keys;
    final VType[] prevValues = (VType[]) this.values;
    allocateBuffers(nextBufferSize(mask + 1, size(), loadFactor));
    assert this.keys.length > prevKeys.length;

    // We have succeeded at allocating new data so insert the pending key/value at
    // the free slot in the old arrays before rehashing.
    prevKeys[slot] = pendingKey;
    prevValues[slot] = pendingValue;

    // Rehash old keys, including the pending key.
    rehash(prevKeys, prevValues);
  }

  /**
   * Shift all the slot-conflicting keys and values allocated to (and including) <code>slot</code>.
   */
  protected void shiftConflictingKeys(int gapSlot) {
    final long[] keys = this.keys;
    final VType[] values = (VType[]) this.values;
    final int mask = this.mask;

    // Perform shifts of conflicting keys to fill in the gap.
    int distance = 0;
    while (true) {
      final int slot = (gapSlot + (++distance)) & mask;
      final long existing = keys[slot];
      if (((existing) == 0)) {
        break;
      }

      final int idealSlot = hashKey(existing);
      final int shift = (slot - idealSlot) & mask;
      if (shift >= distance) {
        // Entry at this position was originally at or before the gap slot.
        // Move the conflict-shifted entry to the gap's position and repeat the procedure
        // for any entries to the right of the current position, treating it
        // as the new gap.
        keys[gapSlot] = existing;
        values[gapSlot] = values[slot];
        gapSlot = slot;
        distance = 0;
      }
    }

    // Mark the last found gap slot without a conflict as empty.
    keys[gapSlot] = 0;
    values[gapSlot] = null;
    assigned--;
  }

  /** Forked from HPPC, holding int index,key and value */
  public static final class LongObjectCursor<VType> {
    /**
     * The current key and value's index in the container this cursor belongs to. The meaning of
     * this index is defined by the container (usually it will be an index in the underlying storage
     * buffer).
     */
    public int index;

    /** The current key. */
    public long key;

    /** The current value. */
    public VType value;

    @Override
    public String toString() {
      return "[cursor, index: " + index + ", key: " + key + ", value: " + value + "]";
    }
  }
}
