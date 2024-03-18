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

package org.apache.lucene.util.hnsw;

/**
 * A bounded min heap that stores floats. The top element is the lowest value of the heap.
 *
 * <p>A primitive priority queue that maintains a partial ordering of its elements such that the
 * least element can always be found in constant time. Implementation is based on {@link
 * org.apache.lucene.util.LongHeap}
 *
 * @lucene.internal
 */
public final class FloatHeap {
  private final int maxSize;
  private final float[] heap;
  private int size;

  public FloatHeap(int maxSize) {
    this.maxSize = maxSize;
    this.heap = new float[maxSize + 1];
    this.size = 0;
  }

  /**
   * Inserts a value into this heap.
   *
   * <p>If the number of values would exceed the heap's maxSize, the least value is discarded
   *
   * @param value the value to add
   * @return whether the value was added (unless the heap is full, or the new value is less than the
   *     top value)
   */
  public boolean offer(float value) {
    if (size >= maxSize) {
      if (value < heap[1]) {
        return false;
      }
      updateTop(value);
      return true;
    }
    push(value);
    return true;
  }

  public float[] getHeap() {
    float[] result = new float[size];
    System.arraycopy(this.heap, 1, result, 0, size);
    return result;
  }

  /**
   * Removes and returns the head of the heap
   *
   * @return the head of the heap, the smallest value
   * @throws IllegalStateException if the heap is empty
   */
  public float poll() {
    if (size > 0) {
      float result;
      result = heap[1]; // save first value
      heap[1] = heap[size]; // move last to first
      size--;
      downHeap(1); // adjust heap
      return result;
    } else {
      throw new IllegalStateException("The heap is empty");
    }
  }

  /**
   * Retrieves, but does not remove, the head of this heap.
   *
   * @return the head of the heap, the smallest value
   */
  public float peek() {
    return heap[1];
  }

  /**
   * Returns the number of elements in this heap.
   *
   * @return the number of elements in this heap
   */
  public int size() {
    return size;
  }

  public void clear() {
    size = 0;
  }

  private void push(float element) {
    size++;
    heap[size] = element;
    upHeap(size);
  }

  private float updateTop(float value) {
    heap[1] = value;
    downHeap(1);
    return heap[1];
  }

  private void downHeap(int i) {
    float value = heap[i]; // save top value
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

  private void upHeap(int origPos) {
    int i = origPos;
    float value = heap[i]; // save bottom value
    int j = i >>> 1;
    while (j > 0 && value < heap[j]) {
      heap[i] = heap[j]; // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = value; // install saved value
  }
}
