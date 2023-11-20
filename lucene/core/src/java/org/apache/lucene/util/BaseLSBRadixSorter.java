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
 * Radix sorter for fixed-length objects. This class sorts based on the least significant byte first
 * and falls back to {@link #insertionSort} when the size to sort is small.
 *
 * <p>This algorithm is stable.
 *
 * @lucene.internal
 */
public abstract class BaseLSBRadixSorter extends Sorter {

  private static final int HISTOGRAM_SIZE = 256;
  private final int[] histogram = new int[HISTOGRAM_SIZE];
  protected int bits = -1;

  protected BaseLSBRadixSorter(int bits) {
    super();
    this.bits = bits;
  }

  @Override
  public void sort(int from, int to) {
    if (to - from < INSERTION_SORT_THRESHOLD) {
      insertionSort(from, to);
    } else {
      radixSort(from, to);
    }
  }

  private void radixSort(int from, int to) {
    for (int shift = 0; shift < bits; shift += 8) {
      if (sort(from, to, histogram, shift)) {
        switchBuffer();
      }
    }
    restore(from, to);
  }

  private boolean sort(int from, int to, int[] histogram, int shift) {
    Arrays.fill(histogram, 0);
    buildHistogram(from, to, histogram, shift);
    if (histogram[0] == to - from) {
      return false;
    }
    sumHistogram(histogram);
    reorder(from, to, histogram, shift);
    return true;
  }

  private void buildHistogram(int from, int to, int[] histogram, int shift) {
    for (int i = from; i < to; ++i) {
      final int b = bucket(i, shift);
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

  protected void reorder(int from, int to, int[] histogram, int shift) {
    for (int i = from; i < to; ++i) {
      final int b = bucket(i, shift);
      save(i, from + histogram[b]++);
    }
  }

  /** Switch the src and dest array */
  protected abstract void switchBuffer();

  /** Get the least significant byte after shift right. */
  protected abstract int bucket(int i, int shift);

  /** Save the i-th value into the j-th position in temporary storage. */
  protected abstract void save(int i, int j);

  /** Restore values between i-th and j-th(excluding) in temporary storage into original storage. */
  protected abstract void restore(int i, int j);
}
