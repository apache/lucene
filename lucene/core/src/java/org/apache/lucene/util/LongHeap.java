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
package org.apache.lucene.util;

import java.util.Arrays;

/**
 * A min heap that stores longs; a primitive priority queue that like all priority queues maintains
 * a partial ordering of its elements such that the least element can always be found in constant
 * time. Put()'s and pop()'s require log(size). This heap provides unbounded growth via {@link
 * #push(long)}, and bounded-size insertion based on its initial capacity via {@link
 * #insertWithOverflow(long)}. The heap is a min heap, meaning that the top element is the lowest
 * value of the heap. LongHeap implements 2-ary heap.
 *
 * @lucene.internal
 */
public final class LongHeap {

  private final int initialCapacity;

  private long[] heap;
  private int size = 0;

  /**
   * Constructs a heap with specified size and initializes all elements with the given value.
   *
   * @param size the number of elements to initialize in the heap.
   * @param initialValue the value to fill the heap with.
   */
  public LongHeap(int size, long initialValue) {
    this(size);
    Arrays.fill(heap, 1, size + 1, initialValue);
    this.size = size;
  }

  /**
   * Create an empty priority queue of the configured initial size.
   *
   * @param initialCapacity the initial capacity of the heap
   */
  public LongHeap(int initialCapacity) {
    final int heapSize;
    if (initialCapacity < 1 || initialCapacity >= ArrayUtil.MAX_ARRAY_LENGTH) {
      // Throw exception to prevent confusing OOME:
      throw new IllegalArgumentException(
          "initialCapacity must be > 0 and < "
              + (ArrayUtil.MAX_ARRAY_LENGTH - 1)
              + "; got: "
              + initialCapacity);
    }
    // NOTE: we add +1 because all access to heap is 1-based not 0-based.  heap[0] is unused.
    heapSize = initialCapacity + 1;
    this.initialCapacity = initialCapacity;
    this.heap = new long[heapSize];
  }

  /**
   * Adds a value in log(size) time. Grows unbounded as needed to accommodate new values.
   *
   * @return the new 'top' element in the queue.
   */
  public long push(long element) {
    size++;
    if (size == heap.length) {
      heap = ArrayUtil.grow(heap, (size * 3 + 1) / 2);
    }
    heap[size] = element;
    upHeap(size);
    return heap[1];
  }

  /**
   * Adds a value to an LongHeap in log(size) time. If the number of values would exceed the heap's
   * initialCapacity, the least value is discarded.
   *
   * @return whether the value was added (unless the heap is full, or the new value is less than the
   *     top value)
   */
  public boolean insertWithOverflow(long value) {
    if (size >= initialCapacity) {
      if (value < heap[1]) {
        return false;
      }
      updateTop(value);
      return true;
    }
    push(value);
    return true;
  }

  /**
   * Returns the least element of the LongHeap in constant time. It is up to the caller to verify
   * that the heap is not empty; no checking is done, and if no elements have been added, 0 is
   * returned.
   */
  public long top() {
    return heap[1];
  }

  /**
   * Removes and returns the least element of the PriorityQueue in log(size) time.
   *
   * @throws IllegalStateException if the LongHeap is empty.
   */
  public long pop() {
    if (size > 0) {
      long result = heap[1]; // save first value
      heap[1] = heap[size]; // move last to first
      size--;
      downHeap(1); // adjust heap
      return result;
    } else {
      throw new IllegalStateException("The heap is empty");
    }
  }

  /**
   * Replace the top of the pq with {@code newTop}. Should be called when the top value changes.
   * Still log(n) worst case, but it's at least twice as fast to
   *
   * <pre><code class="language-java">
   * pq.updateTop(value);
   * </code></pre>
   *
   * instead of
   *
   * <pre><code class="language-java">
   * pq.pop();
   * pq.push(value);
   * </code></pre>
   *
   * Calling this method on an empty LongHeap has no visible effect.
   *
   * @param value the new element that is less than the current top.
   * @return the new 'top' element after shuffling the heap.
   */
  public long updateTop(long value) {
    heap[1] = value;
    downHeap(1);
    return heap[1];
  }

  /** Returns the number of elements currently stored in the PriorityQueue. */
  public int size() {
    return size;
  }

  /** Removes all entries from the PriorityQueue. */
  public void clear() {
    size = 0;
  }

  private void upHeap(int origPos) {
    int i = origPos;
    long value = heap[i]; // save bottom value
    int j = i >>> 1;
    while (j > 0 && value < heap[j]) {
      heap[i] = heap[j]; // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = value; // install saved value
  }

  private void downHeap(int i) {
    long value = heap[i]; // save top value
    int j = i << 1; // find smaller child
    int k = j + 1;
    if (k <= size && heap[k] < heap[j]) {
      j = k;
    }
    while (j <= size && heap[j] < value) {
      heap[i] = heap[j]; // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && heap[k] < heap[j]) {
        j = k;
      }
    }
    heap[i] = value; // install saved value
  }

  public void pushAll(LongHeap other) {
    for (int i = 1; i <= other.size; i++) {
      push(other.heap[i]);
    }
  }

  /**
   * Return the element at the ith location in the heap array. Use for iterating over elements when
   * the order doesn't matter. Note that the valid arguments range from [1, size].
   */
  public long get(int i) {
    return heap[i];
  }

  /**
   * This method returns the internal heap array.
   *
   * @lucene.internal
   */
  // pkg-private for testing
  long[] getHeapArray() {
    return heap;
  }
}
