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
import java.util.SplittableRandom;

/**
 * Adaptive selection algorithm based on the introspective quick select algorithm. The quick select
 * algorithm uses an interpolation variant of Tukey's ninther median-of-medians for pivot, and
 * Bentley-McIlroy 3-way partitioning. For the introspective protection, it shuffles the sub-range
 * if the max recursive depth is exceeded.
 *
 * <p>This selection algorithm is fast on most data shapes, especially on nearly sorted data, or
 * when k is close to the boundaries. It runs in linear time on average.
 *
 * @lucene.internal
 */
public abstract class IntroSelector extends Selector {

  // This selector is used repeatedly by the radix selector for sub-ranges of less than
  // 100 entries. This means this selector is also optimized to be fast on small ranges.
  // It uses the variant of medians-of-medians and 3-way partitioning, and finishes the
  // last tiny range (3 entries or less) with a very specialized sort.

  private SplittableRandom random;

  @Override
  public final void select(int from, int to, int k) {
    checkArgs(from, to, k);
    select(from, to, k, 2 * MathUtil.log(to - from, 2));
  }

  // Visible for testing.
  void select(int from, int to, int k, int maxDepth) {
    // This code is inspired from IntroSorter#sort, adapted to loop on a single partition.

    // For efficiency, we must enter the loop with at least 4 entries to be able to skip
    // some boundary tests during the 3-way partitioning.
    int size;
    while ((size = to - from) > 3) {

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
        // Select the pivot with a variant of the Tukey's ninther median of medians.
        // If k is close to the boundaries, select either the lowest or highest median (this variant
        // is inspired from the interpolation search).
        int range = size >> 3;
        int doubleRange = range << 1;
        int medianFirst = median(from, from + range, from + doubleRange);
        int medianMiddle = median(mid - range, mid, mid + range);
        int medianLast = median(last - doubleRange, last - range, last);
        if (k - from < range) {
          // k is close to 'from': select the lowest median.
          pivot = min(medianFirst, medianMiddle, medianLast);
        } else if (to - k <= range) {
          // k is close to 'to': select the highest median.
          pivot = max(medianFirst, medianMiddle, medianLast);
        } else {
          // Otherwise select the median of medians.
          pivot = median(medianFirst, medianMiddle, medianLast);
        }
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

    // Sort the final tiny range (3 entries or less) with a very specialized sort.
    switch (size) {
      case 2:
        if (compare(from, from + 1) > 0) {
          swap(from, from + 1);
        }
        break;
      case 3:
        sort3(from);
        break;
    }
  }

  /** Returns the index of the min element among three elements at provided indices. */
  private int min(int i, int j, int k) {
    if (compare(i, j) <= 0) {
      return compare(i, k) <= 0 ? i : k;
    }
    return compare(j, k) <= 0 ? j : k;
  }

  /** Returns the index of the max element among three elements at provided indices. */
  private int max(int i, int j, int k) {
    if (compare(i, j) <= 0) {
      return compare(j, k) < 0 ? k : j;
    }
    return compare(i, k) < 0 ? k : i;
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

  /**
   * Sorts 3 entries starting at from (inclusive). This specialized method is more efficient than
   * calling {@link Sorter#insertionSort(int, int)}.
   */
  private void sort3(int from) {
    final int mid = from + 1;
    final int last = from + 2;
    if (compare(from, mid) <= 0) {
      if (compare(mid, last) > 0) {
        swap(mid, last);
        if (compare(from, mid) > 0) {
          swap(from, mid);
        }
      }
    } else if (compare(mid, last) >= 0) {
      swap(from, last);
    } else {
      swap(from, mid);
      if (compare(mid, last) > 0) {
        swap(mid, last);
      }
    }
  }

  /**
   * Shuffles the entries between from (inclusive) and to (exclusive) with Durstenfeld's algorithm.
   */
  private void shuffle(int from, int to) {
    if (this.random == null) {
      this.random = new SplittableRandom();
    }
    SplittableRandom random = this.random;
    for (int i = to - 1; i > from; i--) {
      swap(i, random.nextInt(from, i + 1));
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
