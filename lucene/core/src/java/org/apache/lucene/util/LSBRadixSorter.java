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
import org.apache.lucene.util.packed.PackedInts;

/**
 * A LSB Radix sorter for unsigned int values.
 *
 * @lucene.internal
 */
public final class LSBRadixSorter {

  private static final int INSERTION_SORT_THRESHOLD = 30;
  private static final int HISTOGRAM_SIZE = 256;

  private final int[] histogram = new int[HISTOGRAM_SIZE];
  private int[] buffer = new int[0];

  private static void buildHistogram(int[] array, int len, int[] histogram, int shift) {
    for (int i = 0; i < len; ++i) {
      final int b = (array[i] >>> shift) & 0xFF;
      histogram[b] += 1;
    }
  }

  private static void sumHistogram(int[] histogram) {
    int accum = 0;
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int count = histogram[i];
      histogram[i] = accum;
      accum += count;
    }
  }

  private static void reorder(int[] array, int len, int[] histogram, int shift, int[] dest) {
    for (int i = 0; i < len; ++i) {
      final int v = array[i];
      final int b = (v >>> shift) & 0xFF;
      dest[histogram[b]++] = v;
    }
  }

  private static boolean sort(int[] array, int len, int[] histogram, int shift, int[] dest) {
    Arrays.fill(histogram, 0);
    buildHistogram(array, len, histogram, shift);
    if (histogram[0] == len) {
      return false;
    }
    sumHistogram(histogram);
    reorder(array, len, histogram, shift, dest);
    return true;
  }

  private static void insertionSort(int[] array, int off, int len) {
    for (int i = off + 1, end = off + len; i < end; ++i) {
      for (int j = i; j > off; --j) {
        if (array[j - 1] > array[j]) {
          int tmp = array[j - 1];
          array[j - 1] = array[j];
          array[j] = tmp;
        } else {
          break;
        }
      }
    }
  }

  /** Number of passes needed to sort values of {@code numBits} bits, 8 bits at a time. */
  private static int numPasses(int numBits) {
    return (numBits + 7) >>> 3;
  }

  /**
   * Sort {@code array[0:len]} in place.
   *
   * @param numBits how many bits are required to store any of the values in {@code array[0:len]}.
   *     Pass {@code 32} if unknown.
   */
  public void sort(int numBits, final int[] array, int len) {
    if (len < INSERTION_SORT_THRESHOLD) {
      insertionSort(array, 0, len);
      return;
    }

    // The number of passes is proportional to the number of bits of the values. Values are often
    // clustered in a small part of their range, e.g. doc IDs that match a range query on a
    // timestamp in a segment whose documents were indexed in timestamp order, or in a sorted
    // index. So we sort differences with the minimum value if this saves passes. Values that need
    // 32 bits are unsigned, so this only applies when numBits < 32.
    if (numBits < 32) {
      final int numPasses = numPasses(numBits);
      int min = array[0];
      int max = array[0];
      // The range of values can only grow as we look at more values, so stop as soon as we know
      // that sorting differences would not save passes. This way values that span most of their
      // range only pay for looking at the first few of them.
      for (int from = 1;
          from < len && numPasses(PackedInts.bitsRequired(max - min)) < numPasses; ) {
        final int to = Math.min(len, from + 64);
        for (; from < to; ++from) {
          final int v = array[from];
          min = Math.min(min, v);
          max = Math.max(max, v);
        }
      }
      final int numDeltaBits = PackedInts.bitsRequired(max - min);
      if (numPasses(numDeltaBits) < numPasses) {
        // Subtracting and adding back the minimum in separate loops is cheaper than applying it
        // when computing buckets in every pass.
        for (int i = 0; i < len; ++i) {
          array[i] -= min;
        }
        radixSort(numDeltaBits, array, len);
        for (int i = 0; i < len; ++i) {
          array[i] += min;
        }
        return;
      }
    }
    radixSort(numBits, array, len);
  }

  private void radixSort(int numBits, int[] array, int len) {
    buffer = ArrayUtil.growNoCopy(buffer, len);

    int[] arr = array;

    int[] buf = buffer;

    for (int shift = 0; shift < numBits; shift += 8) {
      if (sort(arr, len, histogram, shift, buf)) {
        // swap arrays
        int[] tmp = arr;
        arr = buf;
        buf = tmp;
      }
    }

    if (array == buf) {
      System.arraycopy(arr, 0, array, 0, len);
    }
  }
}
