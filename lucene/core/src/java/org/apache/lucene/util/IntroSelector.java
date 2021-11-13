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
 * algorithm uses Tukey's ninther median-of-medians for pivot and Bentley-McIlroy 3-way
 * partitioning. For the introspective protection, it shuffles the sub-range if the max recursive
 * depth is exceeded. At anytime during the quick selection loop, if the selected {@code k} comes
 * close to the analyzed range boundaries, then it shortcuts to a top-k selection algorithm.
 *
 * <p>This selection algorithm is fast on most data shapes, especially with low cardinality, or when
 * k is close to the boundaries. It runs in linear time on average.
 *
 * @lucene.internal
 */
public abstract class IntroSelector extends Selector {

  // This selector is used repeatedly by the radix selector for sub-ranges of less than
  // 100 entries. This means this selector is also optimized to be fast on small ranges.
  // It uses the medians-of-medians and 3-way partitioning, it shortcuts to a top-k selection when
  // possible, and finishes the last tiny range (3 entries or less) with a very specialized sort
  // method.

  /** Top-k tuning: how close k must be to the boundaries. */
  private static final int TOP_K_CLOSE = 30;
  /** Top-k tuning: min range size to apply top-k selection. */
  private static final int TOP_K_RANGE = 60;

  private SplittableRandom random;

  @Override
  public final void select(int from, int to, int k) {
    checkArgs(from, to, k);
    select(from, to, k, 2 * MathUtil.log(to - from, 2));
  }

  // Visible for testing.
  void select(int from, int to, int k, int maxDepth) {
    // This code is similar to IntroSorter#sort, adapted to loop on a single partition.

    // For efficiency, we must enter the loop with at least 4 entries to be able to skip
    // some boundary tests during the 3-way partitioning.
    int size;
    while ((size = to - from) > 3) {

      if (k - from < TOP_K_CLOSE && to - k > TOP_K_RANGE) {
        // k is close to 'from' while the range is not too small: speed up with a bottom-k
        // selection.
        selectBottom(from, to, k);
        return;
      } else if (to - k <= TOP_K_CLOSE && k - from >= TOP_K_RANGE) {
        // k is close to 'to' while the range is not too small: speed up with a top-k selection.
        selectTop(from, to, k);
        return;
      }

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

  /** Selects the k-th entry with a bottom-k algorithm, given that k is close to {@code from}. */
  private void selectBottom(int from, int to, int k) {
    assert k >= from && k < to - 1;
    int last = to - 1;
    int bSize = k - from + 1;
    int[] bottom = new int[bSize];

    // Adapt to descending order: swap the first and last k elements if the first elements are
    // greater.
    for (int i = 0; i < bSize && compare(from + i, last - i) > 0; i++) {
      swap(from + i, last - i);
    }

    // Initialize the bottom list with the indexes of the first entries.
    // Determine the bottom-max pivot: the greatest entry in the bottom list.
    int bMax = 0;
    int index;
    setPivot(bottom[0] = index = from);
    while (++index <= k) {
      bottom[index - from] = index;
      if (comparePivot(index) < 0) {
        bMax = index - from;
        setPivot(index);
      }
    }

    // Loop on remaining entries and compare each one with the bottom-max pivot.
    do {
      if (comparePivot(index) > 0) {
        // The entry is less than the bottom-max pivot.
        // Replace the max by the new entry in the bottom list.
        bottom[bMax] = index;
        // Determine the new bottom-max.
        setPivot(bottom[bMax = 0]);
        for (int i = 1; i < bSize; i++) {
          int bIndex;
          if (comparePivot(bIndex = bottom[i]) < 0) {
            bMax = i;
            setPivot(bIndex);
          }
        }
      }
    } while (++index < to);

    // Partially sort the bottom entries, and set the k-th entry with the bottom-max.
    for (int i = 0; i < bSize; i++) {
      int entryIndex = bottom[i];
      if (entryIndex > k) {
        swap(from + i, entryIndex);
      }
    }
    swap(from + bMax, k);
  }

  /** Selects the k-th entry with a top-k algorithm, given that k is close to {@code to}. */
  private void selectTop(int from, int to, int k) {
    assert k > from && k < to;
    int last = to - 1;
    int tSize = to - k;
    int[] top = new int[tSize];

    // Adapt to descending order: swap the first and last k elements if the first elements are
    // greater.
    for (int i = 0; i < tSize && compare(from + i, last - i) > 0; i++) {
      swap(from + i, last - i);
    }

    // Initialize the top list with the indexes of the last entries.
    // Determine the top-min pivot: the least entry in the top list.
    int tMin = 0;
    int index;
    setPivot(top[0] = index = last);
    int tFirst = to - tSize;
    while (--index >= tFirst) {
      top[last - index] = index;
      if (comparePivot(index) > 0) {
        tMin = last - index;
        setPivot(index);
      }
    }

    // Loop on remaining entries and compare each one with the top-min pivot.
    do {
      if (comparePivot(index) < 0) {
        // The entry is greater than the top-min pivot.
        // Replace the min by the new entry in the top list.
        top[tMin] = index;
        // Determine the new top-min.
        setPivot(top[tMin = 0]);
        for (int i = 1; i < tSize; i++) {
          int tIndex;
          if (comparePivot(tIndex = top[i]) > 0) {
            tMin = i;
            setPivot(tIndex);
          }
        }
      }
    } while (--index >= from);

    // Partially sort the top entries, and set the k-th entry with the top-min.
    for (int i = 0; i < tSize; i++) {
      int entryIndex = top[i];
      if (entryIndex < tFirst) {
        swap(last - i, entryIndex);
      }
    }
    swap(last - tMin, k);
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
