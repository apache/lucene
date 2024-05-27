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
 * A hash set of <code>long</code>s, implemented using open addressing with linear probing for
 * collision resolution.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.LongHashSet
 *
 * <p>github: https://github.com/carrotsearch/hppc release 0.9.0
 */
public class LongHashSet implements Iterable<LongCursor>, Accountable, Cloneable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(LongHashSet.class);

  /** The hash array holding keys. */
  public long[] keys;

  /**
   * The number of stored keys (assigned key slots), excluding the special "empty" key, if any.
   *
   * @see #size()
   * @see #hasEmptyKey
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
  public LongHashSet() {
    this(DEFAULT_EXPECTED_ELEMENTS);
  }

  /**
   * New instance with sane defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause a rehash
   *     (inclusive).
   */
  public LongHashSet(int expectedElements) {
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
  public LongHashSet(int expectedElements, double loadFactor) {
    this.loadFactor = verifyLoadFactor(loadFactor);
    iterationSeed = ITERATION_SEED.incrementAndGet();
    ensureCapacity(expectedElements);
  }

  /** New instance copying elements from another set. */
  public LongHashSet(LongHashSet set) {
    this(set.size());
    addAll(set);
  }

  public boolean add(long key) {
    if (((key) == 0)) {
      assert ((keys[mask + 1]) == 0);
      boolean added = !hasEmptyKey;
      hasEmptyKey = true;
      return added;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          return false;
        }
        slot = (slot + 1) & mask;
      }

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(slot, key);
      } else {
        keys[slot] = key;
      }

      assigned++;
      return true;
    }
  }

  /**
   * Adds all elements from the given list (vararg) to this set.
   *
   * @return Returns the number of elements actually added as a result of this call (not previously
   *     present in the set).
   */
  public final int addAll(long... elements) {
    ensureCapacity(elements.length);
    int count = 0;
    for (long e : elements) {
      if (add(e)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Adds all elements from the given set to this set.
   *
   * @return Returns the number of elements actually added as a result of this call (not previously
   *     present in the set).
   */
  public int addAll(LongHashSet set) {
    ensureCapacity(set.size());
    return addAll((Iterable<? extends LongCursor>) set);
  }

  /**
   * Adds all elements from the given iterable to this set.
   *
   * @return Returns the number of elements actually added as a result of this call (not previously
   *     present in the set).
   */
  public int addAll(Iterable<? extends LongCursor> iterable) {
    int count = 0;
    for (LongCursor cursor : iterable) {
      if (add(cursor.value)) {
        count++;
      }
    }
    return count;
  }

  public long[] toArray() {

    final long[] cloned = (new long[size()]);
    int j = 0;
    if (hasEmptyKey) {
      cloned[j++] = 0L;
    }

    final long[] keys = this.keys;
    int seed = nextIterationSeed();
    int inc = iterationIncrement(seed);
    for (int i = 0, mask = this.mask, slot = seed & mask;
        i <= mask;
        i++, slot = (slot + inc) & mask) {
      long existing;
      if (!((existing = keys[slot]) == 0)) {
        cloned[j++] = existing;
      }
    }

    return cloned;
  }

  /** An alias for the (preferred) {@link #removeAll}. */
  public boolean remove(long key) {
    if (((key) == 0)) {
      boolean hadEmptyKey = hasEmptyKey;
      hasEmptyKey = false;
      return hadEmptyKey;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          shiftConflictingKeys(slot);
          return true;
        }
        slot = (slot + 1) & mask;
      }
      return false;
    }
  }

  /**
   * Removes all keys present in a given container.
   *
   * @return Returns the number of elements actually removed as a result of this call.
   */
  public int removeAll(LongHashSet other) {
    final int before = size();

    // Try to iterate over the smaller set or over the container that isn't implementing
    // efficient contains() lookup.

    if (other.size() >= size()) {
      if (hasEmptyKey && other.contains(0L)) {
        hasEmptyKey = false;
      }

      final long[] keys = this.keys;
      for (int slot = 0, max = this.mask; slot <= max; ) {
        long existing;
        if (!((existing = keys[slot]) == 0) && other.contains(existing)) {
          // Shift, do not increment slot.
          shiftConflictingKeys(slot);
        } else {
          slot++;
        }
      }
    } else {
      for (LongCursor c : other) {
        remove(c.value);
      }
    }

    return before - size();
  }

  public boolean contains(long key) {
    if (((key) == 0)) {
      return hasEmptyKey;
    } else {
      final long[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;
      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          return true;
        }
        slot = (slot + 1) & mask;
      }
      return false;
    }
  }

  public void clear() {
    assigned = 0;
    hasEmptyKey = false;
    Arrays.fill(keys, 0L);
  }

  public void release() {
    assigned = 0;
    hasEmptyKey = false;
    keys = null;
    ensureCapacity(DEFAULT_EXPECTED_ELEMENTS);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Ensure this container can hold at least the given number of elements without resizing its
   * buffers.
   *
   * @param expectedElements The total number of elements, inclusive.
   */
  public void ensureCapacity(int expectedElements) {
    if (expectedElements > resizeAt || keys == null) {
      final long[] prevKeys = this.keys;
      allocateBuffers(minBufferSize(expectedElements, loadFactor));
      if (prevKeys != null && !isEmpty()) {
        rehash(prevKeys);
      }
    }
  }

  public int size() {
    return assigned + (hasEmptyKey ? 1 : 0);
  }

  @Override
  public int hashCode() {
    int h = hasEmptyKey ? 0xDEADBEEF : 0;
    final long[] keys = this.keys;
    for (int slot = mask; slot >= 0; slot--) {
      long existing;
      if (!((existing = keys[slot]) == 0)) {
        h += BitMixer.mix(existing);
      }
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj)
        || (obj != null && getClass() == obj.getClass() && sameKeys(getClass().cast(obj)));
  }

  /** Return true if all keys of some other container exist in this container. */
  private boolean sameKeys(LongHashSet other) {
    if (other.size() != size()) {
      return false;
    }

    for (LongCursor c : other) {
      if (!contains(c.value)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public LongHashSet clone() {
    try {
      /*  */
      LongHashSet cloned = (LongHashSet) super.clone();
      cloned.keys = keys.clone();
      cloned.hasEmptyKey = hasEmptyKey;
      cloned.iterationSeed = ITERATION_SEED.incrementAndGet();
      return cloned;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<LongCursor> iterator() {
    return new EntryIterator();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(keys);
  }

  /**
   * Provides the next iteration seed used to build the iteration starting slot and offset
   * increment. This method does not need to be synchronized, what matters is that each thread gets
   * a sequence of varying seeds.
   */
  protected int nextIterationSeed() {
    return iterationSeed = BitMixer.mixPhi(iterationSeed);
  }

  /** An iterator implementation for {@link #iterator}. */
  protected final class EntryIterator extends AbstractIterator<LongCursor> {
    private final LongCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public EntryIterator() {
      cursor = new LongCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected LongCursor fetch() {
      final int mask = LongHashSet.this.mask;
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
        cursor.value = 0L;
        return cursor;
      }

      return done();
    }
  }

  /**
   * Create a set from a variable number of arguments or an array of <code>long</code>. The elements
   * are copied from the argument to the internal buffer.
   */
  /*  */
  public static LongHashSet from(long... elements) {
    final LongHashSet set = new LongHashSet(elements.length);
    set.addAll(elements);
    return set;
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
   * Returns a logical "index" of a given key that can be used to speed up follow-up logic in
   * certain scenarios (conditional logic).
   *
   * <p>The semantics of "indexes" are not strictly defined. Indexes may (and typically won't be)
   * contiguous.
   *
   * <p>The index is valid only between modifications (it will not be affected by read-only
   * operations).
   *
   * @see #indexExists
   * @see #indexGet
   * @see #indexInsert
   * @see #indexReplace
   * @param key The key to locate in the set.
   * @return A non-negative value of the logical "index" of the key in the set or a negative value
   *     if the key did not exist.
   */
  public int indexOf(long key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      return hasEmptyKey ? mask + 1 : ~(mask + 1);
    } else {
      final long[] keys = this.keys;
      int slot = hashKey(key) & mask;

      long existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((key) == (existing))) {
          return slot;
        }
        slot = (slot + 1) & mask;
      }

      return ~slot;
    }
  }

  /**
   * @see #indexOf
   * @param index The index of a given key, as returned from {@link #indexOf}.
   * @return Returns <code>true</code> if the index corresponds to an existing key or false
   *     otherwise. This is equivalent to checking whether the index is a positive value (existing
   *     keys) or a negative value (non-existing keys).
   */
  public boolean indexExists(int index) {
    assert index < 0 || index <= mask || (index == mask + 1 && hasEmptyKey);

    return index >= 0;
  }

  /**
   * Returns the exact value of the existing key. This method makes sense for sets of objects which
   * define custom key-equality relationship.
   *
   * @see #indexOf
   * @param index The index of an existing key.
   * @return Returns the equivalent key currently stored in the set.
   * @throws AssertionError If assertions are enabled and the index does not correspond to an
   *     existing key.
   */
  public long indexGet(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    return keys[index];
  }

  /**
   * Replaces the existing equivalent key with the given one and returns any previous value stored
   * for that key.
   *
   * @see #indexOf
   * @param index The index of an existing key.
   * @param equivalentKey The key to put in the set as a replacement. Must be equivalent to the key
   *     currently stored at the provided index.
   * @return Returns the previous key stored in the set.
   * @throws AssertionError If assertions are enabled and the index does not correspond to an
   *     existing key.
   */
  public long indexReplace(int index, long equivalentKey) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);
    assert ((keys[index]) == (equivalentKey));

    long previousValue = keys[index];
    keys[index] = equivalentKey;
    return previousValue;
  }

  /**
   * Inserts a key for an index that is not present in the set. This method may help in avoiding
   * double recalculation of the key's hash.
   *
   * @see #indexOf
   * @param index The index of a previously non-existing key, as returned from {@link #indexOf}.
   * @throws AssertionError If assertions are enabled and the index does not correspond to an
   *     existing key.
   */
  public void indexInsert(int index, long key) {
    assert index < 0 : "The index must not point at an existing key.";

    index = ~index;
    if (((key) == 0)) {
      assert index == mask + 1;
      assert ((keys[index]) == 0);
      hasEmptyKey = true;
    } else {
      assert ((keys[index]) == 0);

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(index, key);
      } else {
        keys[index] = key;
      }

      assigned++;
    }
  }

  /**
   * Removes a key at an index previously acquired from {@link #indexOf}.
   *
   * @see #indexOf
   * @param index The index of the key to remove, as returned from {@link #indexOf}.
   * @throws AssertionError If assertions are enabled and the index does not correspond to an
   *     existing key.
   */
  public void indexRemove(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    if (index > mask) {
      hasEmptyKey = false;
    } else {
      shiftConflictingKeys(index);
    }
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
  protected void rehash(long[] fromKeys) {
    assert HashContainers.checkPowerOfTwo(fromKeys.length - 1);

    // Rehash all stored keys into the new buffers.
    final long[] keys = this.keys;
    final int mask = this.mask;
    long existing;
    for (int i = fromKeys.length - 1; --i >= 0; ) {
      if (!((existing = fromKeys[i]) == 0)) {
        int slot = hashKey(existing) & mask;
        while (!((keys[slot]) == 0)) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = existing;
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
    try {
      int emptyElementSlot = 1;
      this.keys = (new long[arraySize + emptyElementSlot]);
    } catch (OutOfMemoryError e) {
      this.keys = prevKeys;
      throw new BufferAllocationException(
          "Not enough memory to allocate buffers for rehashing: %,d -> %,d",
          e, this.keys == null ? 0 : size(), arraySize);
    }

    this.resizeAt = expandAtCount(arraySize, loadFactor);
    this.mask = arraySize - 1;
  }

  /**
   * This method is invoked when there is a new key to be inserted into the buffer but there is not
   * enough empty slots to do so.
   *
   * <p>New buffers are allocated. If this succeeds, we know we can proceed with rehashing so we
   * assign the pending element to the previous buffer (possibly violating the invariant of having
   * at least one empty slot) and rehash all keys, substituting new buffers at the end.
   */
  protected void allocateThenInsertThenRehash(int slot, long pendingKey) {
    assert assigned == resizeAt && ((keys[slot]) == 0) && !((pendingKey) == 0);

    // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
    final long[] prevKeys = this.keys;
    allocateBuffers(nextBufferSize(mask + 1, size(), loadFactor));
    assert this.keys.length > prevKeys.length;

    // We have succeeded at allocating new data so insert the pending key/value at
    // the free slot in the old arrays before rehashing.
    prevKeys[slot] = pendingKey;

    // Rehash old keys, including the pending key.
    rehash(prevKeys);
  }

  /** Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>. */
  protected void shiftConflictingKeys(int gapSlot) {
    final long[] keys = this.keys;
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
        gapSlot = slot;
        distance = 0;
      }
    }

    // Mark the last found gap slot without a conflict as empty.
    keys[gapSlot] = 0L;
    assigned--;
  }
}
