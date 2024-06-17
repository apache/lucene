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

import static org.apache.lucene.internal.hppc.HashContainers.DEFAULT_EXPECTED_ELEMENTS;
import static org.apache.lucene.internal.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.apache.lucene.internal.hppc.HashContainers.ITERATION_SEED;
import static org.apache.lucene.internal.hppc.HashContainers.MAX_LOAD_FACTOR;
import static org.apache.lucene.internal.hppc.HashContainers.MIN_LOAD_FACTOR;
import static org.apache.lucene.internal.hppc.HashContainers.checkLoadFactor;
import static org.apache.lucene.internal.hppc.HashContainers.checkPowerOfTwo;
import static org.apache.lucene.internal.hppc.HashContainers.expandAtCount;
import static org.apache.lucene.internal.hppc.HashContainers.iterationIncrement;
import static org.apache.lucene.internal.hppc.HashContainers.minBufferSize;
import static org.apache.lucene.internal.hppc.HashContainers.nextBufferSize;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A hash map of <code>long</code> to <code>float</code>, implemented using open addressing with
 * linear probing for collision resolution.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongFloatHashMap
 *
 * <p>github: https://github.com/carrotsearch/hppc release 0.10.0
 *
 * @lucene.internal
 */
public class LongFloatHashMap
    implements Iterable<LongFloatHashMap.LongFloatCursor>, Accountable, Cloneable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(LongFloatHashMap.class);

  /** The array holding keys. */
  public long[] keys;

  /** The array holding values. */
  public float[] values;

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
  public LongFloatHashMap() {
    this(DEFAULT_EXPECTED_ELEMENTS);
  }

  /**
   * New instance with sane defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause buffer
   *     expansion (inclusive).
   */
  public LongFloatHashMap(int expectedElements) {
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
  public LongFloatHashMap(int expectedElements, double loadFactor) {
    this.loadFactor = verifyLoadFactor(loadFactor);
    iterationSeed = ITERATION_SEED.incrementAndGet();
    ensureCapacity(expectedElements);
  }

  /** Create a hash map from all key-value pairs of another map. */
  public LongFloatHashMap(LongFloatHashMap map) {
    this(map.size());
    putAll(map);
  }

  public float put(long key, float value) {
    assert assigned < mask + 1;

    final int mask = this.mask;
    if (((key) == 0)) {
      float previousValue = hasEmptyKey ? values[mask + 1] : 0;
      hasEmptyKey = true;
      values[mask + 1] = value;
      return previousValue;
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final float previousValue = values[slot];
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
      return 0;
    }
  }

  public int putAll(Iterable<? extends LongFloatCursor> iterable) {
    final int count = size();
    for (LongFloatCursor c : iterable) {
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
  public boolean putIfAbsent(long key, float value) {
    int keyIndex = indexOf(key);
    if (!indexExists(keyIndex)) {
      indexInsert(keyIndex, key, value);
      return true;
    } else {
      return false;
    }
  }

  /**
   * If <code>key</code> exists, <code>putValue</code> is inserted into the map, otherwise any
   * existing value is incremented by <code>additionValue</code>.
   *
   * @param key The key of the value to adjust.
   * @param putValue The value to put if <code>key</code> does not exist.
   * @param incrementValue The value to add to the existing value if <code>key</code> exists.
   * @return Returns the current value associated with <code>key</code> (after changes).
   */
  public float putOrAdd(long key, float putValue, float incrementValue) {
    assert assigned < mask + 1;

    int keyIndex = indexOf(key);
    if (indexExists(keyIndex)) {
      putValue = values[keyIndex] + incrementValue;
      indexReplace(keyIndex, putValue);
    } else {
      indexInsert(keyIndex, key, putValue);
    }
    return putValue;
  }

  /**
   * Adds <code>incrementValue</code> to any existing value for the given <code>key</code> or
   * inserts <code>incrementValue</code> if <code>key</code> did not previously exist.
   *
   * @param key The key of the value to adjust.
   * @param incrementValue The value to put or add to the existing value if <code>key</code> exists.
   * @return Returns the current value associated with <code>key</code> (after changes).
   */
  public float addTo(long key, float incrementValue) {
    return putOrAdd(key, incrementValue, incrementValue);
  }

  public float remove(long key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      if (!hasEmptyKey) {
        return 0;
      }
      hasEmptyKey = false;
      float previousValue = values[mask + 1];
      values[mask + 1] = 0;
      return previousValue;
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final float previousValue = values[slot];
          shiftConflictingKeys(slot);
          return previousValue;
        }
        slot = (slot + 1) & mask;
      }

      return 0;
    }
  }

  public float get(long key) {
    if (((key) == 0)) {
      return hasEmptyKey ? values[mask + 1] : 0;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return values[slot];
        }
        slot = (slot + 1) & mask;
      }

      return 0;
    }
  }

  public float getOrDefault(long key, float defaultValue) {
    if (((key) == 0)) {
      return hasEmptyKey ? values[mask + 1] : defaultValue;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return values[slot];
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
    assert index < 0 || index <= mask || (index == mask + 1 && hasEmptyKey);

    return index >= 0;
  }

  public float indexGet(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    return values[index];
  }

  public float indexReplace(int index, float newValue) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    float previousValue = values[index];
    values[index] = newValue;
    return previousValue;
  }

  public void indexInsert(int index, long key, float value) {
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

  public float indexRemove(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    float previousValue = values[index];
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

    Arrays.fill(keys, 0L);

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
    for (LongFloatCursor c : this) {
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
  protected boolean equalElements(LongFloatHashMap other) {
    if (other.size() != size()) {
      return false;
    }

    for (LongFloatCursor c : other) {
      long key = c.key;
      if (!containsKey(key) || !(Float.floatToIntBits(c.value) == Float.floatToIntBits(get(key)))) {
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
      final float[] prevValues = this.values;
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
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(keys) + RamUsageEstimator.sizeOf(values);
  }

  /** An iterator implementation for {@link #iterator}. */
  private final class EntryIterator extends AbstractIterator<LongFloatCursor> {
    private final LongFloatCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public EntryIterator() {
      cursor = new LongFloatCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected LongFloatCursor fetch() {
      final int mask = LongFloatHashMap.this.mask;
      while (index <= mask) {
        long existing;
        index++;
        slot = (slot + increment) & mask;
        if (!((existing = keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.key = existing;
          cursor.value = values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.key = 0;
        cursor.value = values[index++];
        return cursor;
      }

      return done();
    }
  }

  @Override
  public Iterator<LongFloatCursor> iterator() {
    return new EntryIterator();
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
      return LongFloatHashMap.this.size();
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
      final int mask = LongFloatHashMap.this.mask;
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
  public final class ValuesContainer implements Iterable<FloatCursor> {

    @Override
    public Iterator<FloatCursor> iterator() {
      return new ValuesIterator();
    }

    public int size() {
      return LongFloatHashMap.this.size();
    }

    public float[] toArray() {
      float[] array = new float[size()];
      int i = 0;
      for (FloatCursor cursor : this) {
        array[i++] = cursor.value;
      }
      return array;
    }
  }

  /** An iterator over the set of assigned values. */
  private final class ValuesIterator extends AbstractIterator<FloatCursor> {
    private final FloatCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public ValuesIterator() {
      cursor = new FloatCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected FloatCursor fetch() {
      final int mask = LongFloatHashMap.this.mask;
      while (index <= mask) {
        index++;
        slot = (slot + increment) & mask;
        if (!((keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.value = values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.value = values[index++];
        return cursor;
      }

      return done();
    }
  }

  @Override
  public LongFloatHashMap clone() {
    try {
      /*  */
      LongFloatHashMap cloned = (LongFloatHashMap) super.clone();
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
    for (LongFloatCursor cursor : this) {
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
  public static LongFloatHashMap from(long[] keys, float[] values) {
    if (keys.length != values.length) {
      throw new IllegalArgumentException(
          "Arrays of keys and values must have an identical length.");
    }

    LongFloatHashMap map = new LongFloatHashMap(keys.length);
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
  protected void rehash(long[] fromKeys, float[] fromValues) {
    assert fromKeys.length == fromValues.length && checkPowerOfTwo(fromKeys.length - 1);

    // Rehash all stored key/value pairs into the new buffers.
    final long[] keys = this.keys;
    final float[] values = this.values;
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
    float[] prevValues = this.values;
    try {
      int emptyElementSlot = 1;
      this.keys = (new long[arraySize + emptyElementSlot]);
      this.values = (new float[arraySize + emptyElementSlot]);
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
  protected void allocateThenInsertThenRehash(int slot, long pendingKey, float pendingValue) {
    assert assigned == resizeAt && ((keys[slot]) == 0) && !((pendingKey) == 0);

    // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
    final long[] prevKeys = this.keys;
    final float[] prevValues = this.values;
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
    final float[] values = this.values;
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
    values[gapSlot] = 0;
    assigned--;
  }

  /** Forked from HPPC, holding int index,key and value */
  public static final class LongFloatCursor {
    /**
     * The current key and value's index in the container this cursor belongs to. The meaning of
     * this index is defined by the container (usually it will be an index in the underlying storage
     * buffer).
     */
    public int index;

    /** The current key. */
    public long key;

    /** The current value. */
    public float value;

    @Override
    public String toString() {
      return "[cursor, index: " + index + ", key: " + key + ", value: " + value + "]";
    }
  }
}
