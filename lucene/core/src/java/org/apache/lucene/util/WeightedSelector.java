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
public abstract class WeightedSelector {

  // This selector is used repeatedly by the radix selector for sub-ranges of less than
  // 100 entries. This means this selector is also optimized to be fast on small ranges.
  // It uses the variant of medians-of-medians and 3-way partitioning, and finishes the
  // last tiny range (3 entries or less) with a very specialized sort.

  private SplittableRandom random;

  protected abstract long getWeight(int i);

  protected abstract long getValue(int i);

  public final WeightRangeInfo[] select(
      int from,
      int to,
      long rangeTotalValue,
      long beforeTotalValue,
      long rangeWeight,
      long beforeWeight,
      double[] kWeights,
      boolean selectMinimumInQuantiles) {
    WeightRangeInfo[] kIndexResults = new WeightRangeInfo[kWeights.length];
    Arrays.fill(kIndexResults, new WeightRangeInfo(-1, 0, 0));
    checkArgs(rangeWeight, beforeWeight, kWeights);
    select(
        from,
        to,
        rangeTotalValue,
        beforeTotalValue,
        rangeWeight,
        beforeWeight,
        kWeights,
        0,
        kWeights.length,
        kIndexResults,
        selectMinimumInQuantiles,
        true,
        2 * MathUtil.log(to - from, 2));
    return kIndexResults;
  }

  void checkArgs(long rangeWeight, long beforeWeight, double[] kWeights) {
    if (kWeights.length < 1) {
      throw new IllegalArgumentException("There must be at least one k to select, none given");
    }
    Arrays.sort(kWeights);
    if (kWeights[0] < beforeWeight) {
      throw new IllegalArgumentException("All kWeights must be >= beforeWeight");
    }
    if (kWeights[kWeights.length - 1] > beforeWeight + rangeWeight) {
      throw new IllegalArgumentException("All kWeights must be < beforeWeight + rangeWeight");
    }
  }

  // Visible for testing.
  void select(
      int from,
      int to,
      long rangeTotalValue,
      long beforeTotalValue,
      long rangeWeight,
      long beforeWeight,
      double[] kWeights,
      int kFrom,
      int kTo,
      WeightRangeInfo[] kIndexResults,
      boolean selectMinimumInQuantiles,
      boolean fromIsStartOfQuantile,
      int maxDepth) {

    // This code is inspired from IntroSorter#sort, adapted to loop on a single partition.

    // For efficiency, we must enter the loop with at least 4 entries to be able to skip
    // some boundary tests during the 3-way partitioning.
    int size;
    if ((size = to - from) > 3) {

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

        double avgWeight = ((double) rangeWeight) / (to - from);
        double middleWeight = kWeights[(kFrom + kTo - 1) >> 1];
        // Approximate the k we are trying to find by assuming an equal weight amongst values
        int middleK = from + (int) ((middleWeight - beforeWeight) / avgWeight);
        if (middleK - from < range) {
          // k is close to 'from': select the lowest median.
          pivot = min(medianFirst, medianMiddle, medianLast);
        } else if (to - middleK <= range) {
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
      long leftTotalValue = 0;
      long leftWeight = 0;
      long rightTotalValue = 0;
      long rightWeight = 0;
      while (true) {
        int leftCmp, rightCmp;
        while ((leftCmp = comparePivot(++i)) > 0) {
          leftTotalValue += getValue(i);
          leftWeight += getWeight(i);
        }
        while ((rightCmp = comparePivot(--j)) < 0) {
          rightTotalValue += getValue(j);
          rightWeight += getWeight(j);
        }
        if (i >= j) {
          if (i == j && rightCmp == 0) {
            swap(i, p);
          }
          break;
        }
        swap(i, j);
        if (rightCmp == 0) {
          swap(i, p++);
        } else {
          leftTotalValue += getValue(i);
          leftWeight += getWeight(i);
        }
        if (leftCmp == 0) {
          swap(j, q--);
        } else {
          rightTotalValue += getValue(j);
          rightWeight += getWeight(j);
        }
      }
      i = j + 1;
      for (int l = from; l < p; ) {
        swap(l++, j--);
      }
      for (int l = last; l > q; ) {
        swap(l--, i++);
      }
      long leftWeightEnd = beforeWeight + leftWeight;
      long rightWeightStart = beforeWeight + rangeWeight - rightWeight;

      // Select the K weight values contained in the bottom and top partitions.
      int topKFrom = kTo;
      int bottomKTo = kFrom;
      for (int ki = kTo - 1; ki >= kFrom; ki--) {
        if (kWeights[ki] > rightWeightStart) {
          topKFrom = ki;
        }
        if (kWeights[ki] <= leftWeightEnd) {
          bottomKTo = ki + 1;
          break;
        }
      }

      // We create quantiles by finding the maximum value from each quantile.
      // Sometime this lets us find the minimum of the next quantile, but not always.
      // This variable lets us know if the minimum of the top group needs to be found,
      // because a quantile was found at the last value of this partition
      boolean lastPivotIsQuantileEnd = false;

      // Choose the k result indexes for this partition
      if (bottomKTo < topKFrom) {
        lastPivotIsQuantileEnd =
            findKIndexes(
                j + 1,
                i,
                beforeTotalValue + leftTotalValue,
                beforeWeight + leftWeight,
                bottomKTo,
                topKFrom,
                kWeights,
                kIndexResults);
      }

      // Recursively select the relevant k-values from the bottom group, if there are any k-values
      // to select there
      if (bottomKTo > kFrom) {
        select(
            from,
            j + 1,
            leftTotalValue,
            beforeTotalValue,
            leftWeight,
            beforeWeight,
            kWeights,
            kFrom,
            bottomKTo,
            kIndexResults,
            selectMinimumInQuantiles,
            fromIsStartOfQuantile,
            maxDepth);
      } else if (fromIsStartOfQuantile && selectMinimumInQuantiles && from < j) {
        // This means that we don't need to look through the bottom group for the quantile,
        // but we still need to select the minimum (from) value in this bottom range
        selectMinimum(from, j + 1);
      }

      // Recursively select the relevant k-values from the top group, if there are any k-values to
      // select there
      if (topKFrom < kTo) {
        select(
            i,
            to,
            rightTotalValue,
            beforeTotalValue + rangeTotalValue - rightTotalValue,
            rightWeight,
            beforeWeight + rangeWeight - rightWeight,
            kWeights,
            topKFrom,
            kTo,
            kIndexResults,
            selectMinimumInQuantiles,
            lastPivotIsQuantileEnd,
            maxDepth);
      }
    }

    // Sort the final tiny range (3 entries or less) with a very specialized sort.
    switch (size) {
      case 1:
        kIndexResults[kTo - 1] =
            new WeightRangeInfo(
                from, beforeTotalValue + getValue(from), beforeWeight + getWeight(from));
        break;
      case 2:
        if (compare(from, from + 1) > 0) {
          swap(from, from + 1);
        }
        findKIndexes(
            from, from + 2, beforeTotalValue, beforeWeight, kFrom, kTo, kWeights, kIndexResults);
        break;
      case 3:
        sort3(from);
        findKIndexes(
            from, from + 3, beforeTotalValue, beforeWeight, kFrom, kTo, kWeights, kIndexResults);
        break;
    }
  }

  private boolean findKIndexes(
      int from,
      int to,
      long beforeTotalValue,
      long beforeWeight,
      int kFrom,
      int kTo,
      double[] kWeights,
      WeightRangeInfo[] kIndexResults) {
    long runningWeight = beforeWeight;
    long runningTotalValue = beforeTotalValue;
    int kIdx = kFrom;
    for (int listIdx = from; listIdx < to && kIdx < kTo; listIdx++) {
      runningWeight += getWeight(listIdx);
      runningTotalValue += getValue(listIdx);
      // Skip ahead in the weight list if the same value is used for multiple quantiles, we will
      // only record a result index for the last quantile that matches it.
      while (++kIdx < kTo && kWeights[kIdx] <= runningWeight) {}
      if (kWeights[--kIdx] <= runningWeight) {
        kIndexResults[kIdx] = new WeightRangeInfo(listIdx, runningTotalValue, runningWeight);
        // Now that we have recorded the resultIndex for this weight, go to the next weight
        kIdx++;
      }
    }
    // Determine if the last value was used for a WeightRangeBoundary.
    // If so, then we need to select for the next value to get the minimum
    // of the next quantile.
    return kIndexResults[kTo - 1].index() == to - 1;
  }

  /**
   * Find the minimum value in this range, and place it in the {@code from} position.
   *
   * @param from beginning of range (inclusive)
   * @param to end of range (exclusive)
   */
  private void selectMinimum(int from, int to) {
    int minIndex = from;
    for (int index = from + 1; index < to; index++) {
      if (compare(minIndex, index) > 0) {
        minIndex = index;
      }
    }
    if (minIndex != from) {
      swap(minIndex, from);
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

  /** Swap values at slots <code>i</code> and <code>j</code>. */
  protected abstract void swap(int i, int j);

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

  /**
   * Holds information for a returned weight index result
   *
   * @param index the index at which the weight range limit was found
   * @param runningValueSum the sum of values from the start of the list to the end of the range
   *     limit
   * @param runningWeight the sum of weights from the start of the list to the end of the range
   *     limit
   */
  public record WeightRangeInfo(int index, long runningValueSum, long runningWeight) {}
}
