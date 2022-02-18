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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.packed.PackedInts;

/** Sparse structure to collect docIds. The structure will grow until the provided threshold. */
class Buffers {

  private static class Buffer {
    int[] array;
    int length;

    Buffer(int length) {
      this.array = new int[length];
      this.length = 0;
    }

    Buffer(int[] array, int length) {
      this.array = array;
      this.length = length;
    }
  }

  private final List<Buffer> buffers;
  private final int threshold;
  private final int maxDoc;
  private int totalAllocated; // accumulated size of the allocated buffers
  private Buffer current;
  // pkg-private for testing
  final boolean multiValued;

  Buffers(int maxDoc, boolean multiValued) {
    this.maxDoc = maxDoc;
    this.multiValued = multiValued;
    this.buffers = new ArrayList<>();
    // For ridiculously small sets, we'll just use a sorted int[]
    // maxDoc >>> 7 is a good value if you want to save memory, lower values
    // such as maxDoc >>> 11 should provide faster building but at the expense
    // of using a full bitset even for quite sparse data
    this.threshold = maxDoc >>> 7;
  }

  /** add doc to the buffer structure */
  public void addDoc(int doc) {
    current.array[current.length++] = doc;
  }

  /** return true if the buffer can allocate numDocs, otherwise false */
  public boolean ensureBufferCapacity(int numDocs) {
    if ((long) totalAllocated + numDocs > threshold) {
      return false;
    }
    if (buffers.isEmpty()) {
      addBuffer(additionalCapacity(numDocs));
    } else if (current.array.length - current.length >= numDocs) {
      // current buffer is large enough
    } else if (current.length < current.array.length - (current.array.length >>> 3)) {
      // current buffer is less than 7/8 full, resize rather than waste space
      growBuffer(current, additionalCapacity(numDocs));
    } else {
      addBuffer(additionalCapacity(numDocs));
    }
    return true;
  }

  /**
   * Fill the provided BitSet with the current dics. Returns the total number of docs this structure
   * is holding.
   */
  public long toBitSet(BitSet bitSet) {
    long counter = 0;
    for (Buffer buffer : buffers) {
      int[] array = buffer.array;
      int length = buffer.length;
      counter += length;
      for (int i = 0; i < length; ++i) {
        bitSet.set(array[i]);
      }
    }
    return counter;
  }

  /** Build a {@link DocIdSet} with the documents in the data structure. */
  public DocIdSet toDocIdSet() {
    Buffer concatenated = concat(buffers);
    LSBRadixSorter sorter = new LSBRadixSorter();
    sorter.sort(PackedInts.bitsRequired(maxDoc - 1), concatenated.array, concatenated.length);
    final int l;
    if (multiValued) {
      l = dedup(concatenated.array, concatenated.length);
    } else {
      assert noDups(concatenated.array, concatenated.length);
      l = concatenated.length;
    }
    assert l <= concatenated.length;
    concatenated.array[l] = DocIdSetIterator.NO_MORE_DOCS;
    return new IntArrayDocIdSet(concatenated.array, l);
  }

  /**
   * Concatenate the buffers in any order, leaving at least one empty slot in the end NOTE: this
   * method might reuse one of the arrays
   */
  private static Buffer concat(List<Buffer> buffers) {
    int totalLength = 0;
    Buffer largestBuffer = null;
    for (Buffer buffer : buffers) {
      totalLength += buffer.length;
      if (largestBuffer == null || buffer.array.length > largestBuffer.array.length) {
        largestBuffer = buffer;
      }
    }
    if (largestBuffer == null) {
      return new Buffer(1);
    }
    int[] docs = largestBuffer.array;
    if (docs.length < totalLength + 1) {
      docs = ArrayUtil.growExact(docs, totalLength + 1);
    }
    totalLength = largestBuffer.length;
    for (Buffer buffer : buffers) {
      if (buffer != largestBuffer) {
        System.arraycopy(buffer.array, 0, docs, totalLength, buffer.length);
        totalLength += buffer.length;
      }
    }
    return new Buffer(docs, totalLength);
  }

  private static boolean noDups(int[] a, int len) {
    for (int i = 1; i < len; ++i) {
      assert a[i - 1] < a[i];
    }
    return true;
  }

  private Buffer addBuffer(int len) {
    Buffer buffer = new Buffer(len);
    buffers.add(buffer);
    this.current = buffer;
    totalAllocated += buffer.length;
    return buffer;
  }

  private void growBuffer(Buffer buffer, int additionalCapacity) {
    buffer.array = ArrayUtil.growExact(buffer.array, buffer.length + additionalCapacity);
    totalAllocated += additionalCapacity;
  }

  private int additionalCapacity(int numDocs) {
    // exponential growth: the new array has a size equal to the sum of what
    // has been allocated so far
    int c = totalAllocated;
    // but is also >= numDocs + 1 so that we can store the next batch of docs
    // (plus an empty slot so that we are more likely to reuse the array in build())
    c = Math.max(numDocs + 1, c);
    // avoid cold starts
    c = Math.max(32, c);
    // do not go beyond the threshold
    c = Math.min(threshold - totalAllocated, c);
    return c;
  }

  private static int dedup(int[] arr, int length) {
    if (length == 0) {
      return 0;
    }
    int l = 1;
    int previous = arr[0];
    for (int i = 1; i < length; ++i) {
      final int value = arr[i];
      assert value >= previous;
      if (value != previous) {
        arr[l++] = value;
        previous = value;
      }
    }
    return l;
  }
}
