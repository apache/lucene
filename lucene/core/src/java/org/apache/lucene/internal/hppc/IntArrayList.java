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

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An array-backed list of {@code int}.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.IntArrayList
 *
 * <p>github: https://github.com/carrotsearch/hppc release 0.10.0
 *
 * @lucene.internal
 */
public class IntArrayList implements Iterable<IntCursor>, Cloneable, Accountable {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(IntArrayList.class);

  /** An immutable empty buffer (array). */
  public static final int[] EMPTY_ARRAY = new int[0];

  /**
   * Internal array for storing the list. The array may be larger than the current size ({@link
   * #size()}).
   */
  public int[] buffer;

  /** Current number of elements stored in {@link #buffer}. */
  public int elementsCount;

  /** New instance with sane defaults. */
  public IntArrayList() {
    this(DEFAULT_EXPECTED_ELEMENTS);
  }

  /**
   * New instance with sane defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause buffer
   *     expansion (inclusive).
   */
  public IntArrayList(int expectedElements) {
    buffer = new int[expectedElements];
  }

  /** Creates a new list from the elements of another list in its iteration order. */
  public IntArrayList(IntArrayList list) {
    this(list.size());
    addAll(list);
  }

  public void add(int e1) {
    ensureBufferSpace(1);
    buffer[elementsCount++] = e1;
  }

  /** Add all elements from a range of given array to the list. */
  public void add(int[] elements, int start, int length) {
    assert length >= 0 : "Length must be >= 0";

    ensureBufferSpace(length);
    System.arraycopy(elements, start, buffer, elementsCount, length);
    elementsCount += length;
  }

  /**
   * Vararg-signature method for adding elements at the end of the list.
   *
   * <p><b>This method is handy, but costly if used in tight loops (anonymous array passing)</b>
   */
  /*  */
  public final void add(int... elements) {
    add(elements, 0, elements.length);
  }

  /** Adds all elements from another list. */
  public int addAll(IntArrayList list) {
    final int size = list.size();
    ensureBufferSpace(size);

    for (IntCursor cursor : list) {
      add(cursor.value);
    }

    return size;
  }

  /** Adds all elements from another iterable. */
  public int addAll(Iterable<? extends IntCursor> iterable) {
    int size = 0;
    for (IntCursor cursor : iterable) {
      add(cursor.value);
      size++;
    }
    return size;
  }

  public void insert(int index, int e1) {
    assert (index >= 0 && index <= size())
        : "Index " + index + " out of bounds [" + 0 + ", " + size() + "].";

    ensureBufferSpace(1);
    System.arraycopy(buffer, index, buffer, index + 1, elementsCount - index);
    buffer[index] = e1;
    elementsCount++;
  }

  public int get(int index) {
    assert (index >= 0 && index < size())
        : "Index " + index + " out of bounds [" + 0 + ", " + size() + ").";

    return buffer[index];
  }

  public int set(int index, int e1) {
    assert (index >= 0 && index < size())
        : "Index " + index + " out of bounds [" + 0 + ", " + size() + ").";

    final int v = buffer[index];
    buffer[index] = e1;
    return v;
  }

  /** Removes the element at the specified position in this container and returns it. */
  public int removeAt(int index) {
    assert (index >= 0 && index < size())
        : "Index " + index + " out of bounds [" + 0 + ", " + size() + ").";

    final int v = buffer[index];
    System.arraycopy(buffer, index + 1, buffer, index, --elementsCount - index);
    return v;
  }

  /** Removes and returns the last element of this list. */
  public int removeLast() {
    assert !isEmpty() : "List is empty";

    return buffer[--elementsCount];
  }

  /**
   * Removes from this list all the elements with indexes between <code>fromIndex</code>, inclusive,
   * and <code>toIndex</code>, exclusive.
   */
  public void removeRange(int fromIndex, int toIndex) {
    assert (fromIndex >= 0 && fromIndex <= size())
        : "Index " + fromIndex + " out of bounds [" + 0 + ", " + size() + ").";
    assert (toIndex >= 0 && toIndex <= size())
        : "Index " + toIndex + " out of bounds [" + 0 + ", " + size() + "].";
    assert fromIndex <= toIndex : "fromIndex must be <= toIndex: " + fromIndex + ", " + toIndex;

    System.arraycopy(buffer, toIndex, buffer, fromIndex, elementsCount - toIndex);
    final int count = toIndex - fromIndex;
    elementsCount -= count;
  }

  /**
   * Removes the first element that equals <code>e</code>, returning whether an element has been
   * removed.
   */
  public boolean removeElement(int e) {
    return removeFirst(e) != -1;
  }

  /**
   * Removes the first element that equals <code>e1</code>, returning its deleted position or <code>
   * -1</code> if the element was not found.
   */
  public int removeFirst(int e1) {
    final int index = indexOf(e1);
    if (index >= 0) removeAt(index);
    return index;
  }

  /**
   * Removes the last element that equals <code>e1</code>, returning its deleted position or <code>
   * -1</code> if the element was not found.
   */
  public int removeLast(int e1) {
    final int index = lastIndexOf(e1);
    if (index >= 0) removeAt(index);
    return index;
  }

  /**
   * Removes all occurrences of <code>e</code> from this collection.
   *
   * @param e Element to be removed from this collection, if present.
   * @return The number of removed elements as a result of this call.
   */
  public int removeAll(int e) {
    int to = 0;
    for (int from = 0; from < elementsCount; from++) {
      if (((e) == (buffer[from]))) {
        continue;
      }
      if (to != from) {
        buffer[to] = buffer[from];
      }
      to++;
    }
    final int deleted = elementsCount - to;
    this.elementsCount = to;
    return deleted;
  }

  public boolean contains(int e1) {
    return indexOf(e1) >= 0;
  }

  public int indexOf(int e1) {
    for (int i = 0; i < elementsCount; i++) {
      if (((e1) == (buffer[i]))) {
        return i;
      }
    }

    return -1;
  }

  public int lastIndexOf(int e1) {
    for (int i = elementsCount - 1; i >= 0; i--) {
      if (((e1) == (buffer[i]))) {
        return i;
      }
    }

    return -1;
  }

  public boolean isEmpty() {
    return elementsCount == 0;
  }

  /**
   * Ensure this container can hold at least the given number of elements without resizing its
   * buffers.
   *
   * @param expectedElements The total number of elements, inclusive.
   */
  public void ensureCapacity(int expectedElements) {
    if (expectedElements > buffer.length) {
      ensureBufferSpace(expectedElements - size());
    }
  }

  /**
   * Ensures the internal buffer has enough free slots to store <code>expectedAdditions</code>.
   * Increases internal buffer size if needed.
   */
  protected void ensureBufferSpace(int expectedAdditions) {
    if (elementsCount + expectedAdditions > buffer.length) {
      this.buffer = ArrayUtil.grow(buffer, elementsCount + expectedAdditions);
    }
  }

  /**
   * Truncate or expand the list to the new size. If the list is truncated, the buffer will not be
   * reallocated (use {@link #trimToSize()} if you need a truncated buffer), but the truncated
   * values will be reset to the default value (zero). If the list is expanded, the elements beyond
   * the current size are initialized with JVM-defaults (zero or <code>null</code> values).
   */
  public void resize(int newSize) {
    if (newSize <= buffer.length) {
      if (newSize < elementsCount) {
        Arrays.fill(buffer, newSize, elementsCount, 0);
      } else {
        Arrays.fill(buffer, elementsCount, newSize, 0);
      }
    } else {
      ensureCapacity(newSize);
    }
    this.elementsCount = newSize;
  }

  public int size() {
    return elementsCount;
  }

  /** Trim the internal buffer to the current size. */
  public void trimToSize() {
    if (size() != this.buffer.length) {
      this.buffer = toArray();
    }
  }

  /**
   * Sets the number of stored elements to zero. Releases and initializes the internal storage array
   * to default values. To clear the list without cleaning the buffer, simply set the {@link
   * #elementsCount} field to zero.
   */
  public void clear() {
    Arrays.fill(buffer, 0, elementsCount, 0);
    this.elementsCount = 0;
  }

  /** Sets the number of stored elements to zero and releases the internal storage array. */
  public void release() {
    this.buffer = EMPTY_ARRAY;
    this.elementsCount = 0;
  }

  /** The returned array is sized to match exactly the number of elements of the stack. */
  public int[] toArray() {

    return ArrayUtil.copyOfSubArray(buffer, 0, elementsCount);
  }

  /**
   * Clone this object. The returned clone will reuse the same hash function and array resizing
   * strategy.
   */
  @Override
  public IntArrayList clone() {
    try {
      final IntArrayList cloned = (IntArrayList) super.clone();
      cloned.buffer = buffer.clone();
      return cloned;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hashCode() {
    int h = 1, max = elementsCount;
    for (int i = 0; i < max; i++) {
      h = 31 * h + BitMixer.mix(this.buffer[i]);
    }
    return h;
  }

  /**
   * Returns <code>true</code> only if the other object is an instance of the same class and with
   * the same elements.
   */
  @Override
  public boolean equals(Object obj) {
    return (this == obj)
        || (obj != null && getClass() == obj.getClass() && equalElements(getClass().cast(obj)));
  }

  /** Compare index-aligned elements against another {@link IntArrayList}. */
  protected boolean equalElements(IntArrayList other) {
    int max = size();
    if (other.size() != max) {
      return false;
    }

    for (int i = 0; i < max; i++) {
      if (!((get(i)) == (other.get(i)))) {
        return false;
      }
    }

    return true;
  }

  /** Convert the contents of this list to a human-friendly string. */
  @Override
  public String toString() {
    return Arrays.toString(this.toArray());
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(buffer);
  }

  /** Returns a stream of all the elements contained in this list. */
  public IntStream stream() {
    return Arrays.stream(buffer, 0, size());
  }

  /** Sorts the elements in this list and returns this list. */
  public IntArrayList sort() {
    Arrays.sort(buffer, 0, elementsCount);
    return this;
  }

  /** Reverses the elements in this list and returns this list. */
  public IntArrayList reverse() {
    for (int i = 0, mid = elementsCount >> 1, j = elementsCount - 1; i < mid; i++, j--) {
      int tmp = buffer[i];
      buffer[i] = buffer[j];
      buffer[j] = tmp;
    }
    return this;
  }

  /** An iterator implementation for {@link IntArrayList#iterator}. */
  static final class ValueIterator extends AbstractIterator<IntCursor> {
    private final IntCursor cursor;

    private final int[] buffer;
    private final int size;

    public ValueIterator(int[] buffer, int size) {
      this.cursor = new IntCursor();
      this.cursor.index = -1;
      this.size = size;
      this.buffer = buffer;
    }

    @Override
    protected IntCursor fetch() {
      if (cursor.index + 1 == size) return done();

      cursor.value = buffer[++cursor.index];
      return cursor;
    }
  }

  @Override
  public Iterator<IntCursor> iterator() {
    return new ValueIterator(buffer, size());
  }

  /**
   * Create a list from a variable number of arguments or an array of <code>int</code>. The elements
   * are copied from the argument to the internal buffer.
   */
  /*  */
  public static IntArrayList from(int... elements) {
    final IntArrayList list = new IntArrayList(elements.length);
    list.add(elements);
    return list;
  }
}
