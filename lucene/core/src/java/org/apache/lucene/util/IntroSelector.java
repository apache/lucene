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

import java.util.Comparator;
import java.util.Random;

/**
 * Implementation of the introspective quick select algorithm using Tukey's ninther
 * median-of-medians for pivot selection and Bentley-McIlroy 3-way partitioning. In addition, small
 * ranges are sorted with insertion sort. The introspective protection shuffles the sub-range if the
 * max recursive depth is exceeded.
 *
 * <p>This selection algorithm is fast on most data shapes, especially with low cardinality. It runs
 * in linear time on average.
 *
 * @lucene.internal
 */
public abstract class IntroSelector extends Selector {

  private Random random;

  @Override
  public final void select(int from, int to, int k) {
    checkArgs(from, to, k);
    select(from, to, k, 2 * MathUtil.log2(to - from));
  }

  private void select(int from, int to, int k, int maxDepth) {
    // This code is similar to IntroSorter#sort, adapted to loop on a single partition.

    // Sort small ranges with insertion sort.
    int size;
    while ((size = to - from) > Sorter.INSERTION_SORT_THRESHOLD) {

      if (--maxDepth == -1) {
        // Max recursion depth exceeded: shuffle (only once) and continue.
        shuffle(from, to);
      }

      // Pivot selection based on medians.
      int last = to - 1;
      int mid = (from + last) >>> 1;
      int pivot;
      if (size <= IntroSorter.SINGLE_MEDIAN_THRESHOLD) {
        // Select the pivot with a single median around the middle element.
        // Do not take the median between [from, mid, last] because it hurts performance
        // if the order is descending in conjunction with the 3-way partitioning.
        int range = size >> 2;
        pivot = median(mid - range, mid, mid + range);
      } else {
        // Select the pivot with the Tukey's ninther median of medians.
        int range = size >> 3;
        int doubleRange = range << 1;
        int medianFirst = median(from, from + range, from + doubleRange);
        int medianMiddle = median(mid - range, mid, mid + range);
        int medianLast = median(last - doubleRange, last - range, last);
        pivot = median(medianFirst, medianMiddle, medianLast);
      }

      // Bentley-McIlroy 3-way partitioning.
      setPivot(pivot);
      swap(from, pivot);
      int i = from;
      int j = to;
      int p = from + 1;
      int q = last;
      while (true) {
        int leftCmp, rightCmp;
        while ((leftCmp = comparePivot(++i)) > 0) {}
        while ((rightCmp = comparePivot(--j)) < 0) {}
        if (i >= j) {
          if (i == j && rightCmp == 0) {
            swap(i, p);
          }
          break;
        }
        swap(i, j);
        if (rightCmp == 0) {
          swap(i, p++);
        }
        if (leftCmp == 0) {
          swap(j, q--);
        }
      }
      i = j + 1;
      for (int l = from; l < p; ) {
        swap(l++, j--);
      }
      for (int l = last; l > q; ) {
        swap(l--, i++);
      }

      // Select the partition containing the k-th element.
      if (k <= j) {
        to = j + 1;
      } else if (k >= i) {
        from = i;
      } else {
        return;
      }
    }

    insertionSort(from, to);
  }

  /** Copy of {@code IntroSorter#median}. */
  private int median(int i, int j, int k) {
    if (compare(i, j) < 0) {
      if (compare(j, k) <= 0) {
        return j;
      }
      return compare(i, k) < 0 ? k : i;
    }
    if (compare(j, k) >= 0) {
      return j;
    }
    return compare(i, k) < 0 ? i : k;
  }

  /** Copy of {@link Sorter#insertionSort(int, int)}. */
  void insertionSort(int from, int to) {
    for (int i = from + 1; i < to; ) {
      int current = i++;
      int previous;
      while (compare((previous = current - 1), current) > 0) {
        swap(previous, current);
        if (previous == from) {
          break;
        }
        current = previous;
      }
    }
  }

  /**
   * Shuffles the entries between from (inclusive) and to (exclusive) with Durstenfeld's algorithm.
   */
  private void shuffle(int from, int to) {
    if (this.random == null) {
      this.random = new Random();
    }
    Random random = this.random;
    for (int i = to - from; i > 1; i--) {
      swap(i - 1 + from, random.nextInt(i) + from);
    }
  }

  /**
   * Save the value at slot <code>i</code> so that it can later be used as a pivot, see {@link
   * #comparePivot(int)}.
   */
  protected abstract void setPivot(int i);

  /**
   * Compare the pivot with the slot at <code>j</code>, similarly to {@link #compare(int, int)
   * compare(i, j)}.
   */
  protected abstract int comparePivot(int j);

  /**
   * Compare entries found in slots <code>i</code> and <code>j</code>. The contract for the returned
   * value is the same as {@link Comparator#compare(Object, Object)}.
   */
  protected int compare(int i, int j) {
    setPivot(i);
    return comparePivot(j);
  }
}
