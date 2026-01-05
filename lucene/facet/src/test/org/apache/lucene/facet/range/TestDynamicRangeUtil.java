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
package org.apache.lucene.facet.range;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDynamicRangeUtil extends LuceneTestCase {
  public void testComputeDynamicNumericRangesBasic() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[1000];
    long[] weights = new long[1000];

    long totalWeight = 0;
    for (int i = 0; i < 1000; i++) {
      values[i] = i + 1;
      weights[i] = i;
      totalWeight += i;
    }

    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(501, 125250L, 1L, 501L, 251D));
    expectedRangeInfoList.add(
        new DynamicRangeUtil.DynamicRangeInfo(207, 125028L, 502L, 708L, 605D));
    expectedRangeInfoList.add(
        new DynamicRangeUtil.DynamicRangeInfo(159, 125133L, 709L, 867L, 788D));
    expectedRangeInfoList.add(
        new DynamicRangeUtil.DynamicRangeInfo(133, 124089L, 868L, 1000L, 934D));
    assertDynamicNumericRangeResults(values, weights, 4, totalWeight, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSameValues() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long totalWeight = 0;
    long[] values = new long[100];
    long[] weights = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 50;
      weights[i] = i;
      totalWeight += i;
    }

    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(51, 1275L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(21, 1281L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(16, 1272L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(12, 1122L, 50L, 50L, 50D));

    assertDynamicNumericRangeResults(values, weights, 4, totalWeight, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSameWeights() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[100];
    long[] weights = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
      weights[i] = 50;
    }

    // Supplying only equal weights should return ranges with equal counts - excluding the last
    // range
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 0L, 24L, 12.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 25L, 49L, 37.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 50L, 74L, 62.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 75L, 99L, 87.0D));
    assertDynamicNumericRangeResults(values, weights, 4, 5000L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithOneValue() {
    long[] values = new long[] {50};
    long[] weights = new long[] {1};
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();

    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(1, 1L, 50L, 50L, 50D));
    assertDynamicNumericRangeResults(values, weights, 4, 1, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithOneLargeWeight() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[] {45, 32, 52, 14, 455, 342, 53};
    long[] weights = new long[] {143, 23, 1, 52343, 53, 12, 2534};

    // value 14 has its own bin since the weight is large, and the rest of the values fall in the
    // other bin
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(1, 52343L, 14L, 14L, 14D));
    expectedRangeInfoList.add(
        new DynamicRangeUtil.DynamicRangeInfo(6, 2766L, 32L, 455L, 163.16666666666666D));
    assertDynamicNumericRangeResults(values, weights, 4, 55109, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithLargeTopN() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[] {487, 439, 794, 277};
    long[] weights = new long[] {59, 508, 736, 560};

    // More requested ranges (TopN) than values should return ranges with weights larger than the
    // average weight - excluding the last range
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(1, 560L, 277L, 277L, 277D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(1, 508L, 439L, 439L, 439D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(2, 795L, 487L, 794L, 640.5D));
    assertDynamicNumericRangeResults(values, weights, 42, 1863L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithZeroTopN() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[] {487, 439, 794, 277};
    long[] weights = new long[] {59, 508, 736, 560};

    // Zero requested ranges (TopN) should return a empty list of ranges regardless of inputs
    assertDynamicNumericRangeResults(values, weights, 0, 1863L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSingleTopN() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[] {487, 439, 794, 277};
    long[] weights = new long[] {59, 508, 736, 560};

    // A single requested range (TopN) should return a single range regardless of the weights
    // provided
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(4, 1863L, 277L, 794L, 499.25D));
    assertDynamicNumericRangeResults(values, weights, 1, 1863L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithTwoTopN() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[] {487, 439, 794, 277};
    long[] weights = new long[] {59, 508, 736, 560};

    // Two requested ranges (TopN) should return two ranges where the first range's weight is equal
    // or larger than half of the total weight
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(2, 1068L, 277L, 439L, 358.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(2, 795L, 487L, 794L, 640.5D));
    assertDynamicNumericRangeResults(values, weights, 2, 1863L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSameWeightsShuffled() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values = new long[100];
    long[] weights = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
      weights[i] = 50;
    }

    // Shuffling the values and weights should not change the answer between runs
    // We expect that returned ranges should come in a strict, deterministic order
    // with the same values and weights
    shuffleValuesWeights(values, weights);
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 0L, 24L, 12.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 25L, 49L, 37.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 50L, 74L, 62.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 75L, 99L, 87.0D));
    assertDynamicNumericRangeResults(values, weights, 4, 5000L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSameValuesShuffled() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long totalWeight = 0;
    long[] values = new long[100];
    long[] weights = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 50;
      weights[i] = i;
      totalWeight += i;
    }

    // Shuffling the values and weights should not change the answer between runs
    // We expect that returned ranges should come in a strict, deterministic order
    // with the same values and weights
    shuffleValuesWeights(values, weights);
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(51, 1275L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(21, 1281L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(16, 1272L, 50L, 50L, 50D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(12, 1122L, 50L, 50L, 50D));

    assertDynamicNumericRangeResults(values, weights, 4, totalWeight, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithMisplacedValue() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values =
        new long[] {
          1, 2, 11, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 12, 111, 112, 113, 114, 115
        };
    long[] weights =
        new long[] {
          2, 3, 12, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 13, 112, 113, 114, 115, 116
        };

    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(8, 444L, 1L, 104L, 54.5D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(4, 430L, 105L, 108L, 106.5D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(4, 446L, 109L, 112L, 110.5D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(3, 345L, 113L, 115L, 114.0D));
    assertDynamicNumericRangeResults(values, weights, 4, 1665, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithSameWeightsOutOfOrder() {
    List<DynamicRangeUtil.DynamicRangeInfo> expectedRangeInfoList = new ArrayList<>();
    long[] values =
        new long[] {
          20, 15, 59, 49, 13, 93, 72, 21, 36, 81, 57, 1, 90, 79, 16, 51, 7, 17, 25, 63, 12, 5, 83,
          66, 48, 43, 55, 78, 64, 77, 65, 73, 80, 37, 54, 50, 95, 31, 97, 3, 82, 29, 70, 26, 4, 46,
          34, 67, 87, 0, 30, 19, 41, 85, 84, 89, 8, 10, 22, 28, 6, 23, 88, 40, 33, 44, 18, 27, 69,
          38, 91, 98, 62, 14, 35, 2, 92, 47, 94, 75, 32, 99, 86, 71, 74, 24, 52, 96, 9, 58, 39, 76,
          56, 11, 53, 61, 42, 68, 60, 45
        };
    long[] weights =
        new long[] {
          50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
          50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
          50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
          50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
          50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50
        };

    // This is testComputeDynamicNumericRangesWithSameWeightsShuffled with seed
    // 9AE79D72C8DD56D8 that failed a previous test
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 0L, 24L, 12.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 25L, 49L, 37.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 50L, 74L, 62.0D));
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(25, 1250L, 75L, 99L, 87.0D));
    assertDynamicNumericRangeResults(values, weights, 4, 5000L, expectedRangeInfoList);
  }

  public void testComputeDynamicNumericRangesWithRandomValues() {
    int arraySize = random().nextInt(100);
    long[] values = new long[arraySize];
    long[] weights = new long[arraySize];

    for (int i = 0; i < arraySize; i++) {
      values[i] = random().nextLong(1000);
      weights[i] = random().nextLong(1000);
    }

    int topN = random().nextInt(100);

    long totalWeight = 0;
    for (long weight : weights) {
      totalWeight += weight;
    }

    assertDynamicNumericRangeValidProperties(values, weights, topN, totalWeight);
  }

  /** Implementation of Durstenfeld's algorithm for shuffling values and weights */
  private static void shuffleValuesWeights(long[] values, long[] weights) {
    for (int i = values.length - 1; i > 0; i--) {
      int rdmIdx = random().nextInt(i + 1);
      long tmpValue = values[i];
      long tmpWeight = weights[i];
      values[i] = values[rdmIdx];
      weights[i] = weights[rdmIdx];
      values[rdmIdx] = tmpValue;
      weights[rdmIdx] = tmpWeight;
    }
  }

  private static void assertDynamicNumericRangeValidProperties(
      long[] values, long[] weights, int topN, long totalWeight) {

    List<WeightedPair> sortedPairs = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      long value = values[i];
      long weight = weights[i];
      WeightedPair pair = new WeightedPair(value, weight);
      sortedPairs.add(pair);
    }

    sortedPairs.sort(
        Comparator.comparingLong(WeightedPair::value).thenComparingLong(WeightedPair::weight));

    int len = values.length;

    double rangeWeightTarget = (double) totalWeight / Math.min(topN, len);

    List<DynamicRangeUtil.DynamicRangeInfo> mockDynamicRangeResult =
        DynamicRangeUtil.computeDynamicNumericRanges(
            values, weights, values.length, totalWeight, topN);

    // Zero requested ranges (TopN) should return a empty list of ranges regardless of inputs
    if (topN == 0) {
      assertTrue(mockDynamicRangeResult.size() == 0);
      return; // Early return; do not check anything else
    }

    // Adjacent ranges do not overlap - only adjacent max-min can overlap
    for (int i = 0; i < mockDynamicRangeResult.size() - 1; i++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(i);
      DynamicRangeUtil.DynamicRangeInfo nextRangeInfo = mockDynamicRangeResult.get(i + 1);
      assertTrue(rangeInfo.max() <= nextRangeInfo.min());
    }

    // The count of every range sums to the number of values
    int accuCount = 0;
    for (int i = 0; i < mockDynamicRangeResult.size(); i++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(i);
      int count = rangeInfo.count();
      accuCount += count;
    }
    assertTrue(accuCount == len);

    // The sum of every range weight equals the total weight
    long accuWeight = 0;
    for (int i = 0; i < mockDynamicRangeResult.size(); i++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(i);
      long weight = rangeInfo.weight();
      accuWeight += weight;
    }
    assertTrue(accuWeight == totalWeight);

    // All values appear in atleast one range
    for (int pairOffset = 0, rangeIdx = 0; rangeIdx < mockDynamicRangeResult.size(); rangeIdx++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(rangeIdx);
      int count = rangeInfo.count();
      for (int i = pairOffset; i < pairOffset + count; i++) {
        WeightedPair pair = sortedPairs.get(i);
        long value = pair.value();
        assertTrue(rangeInfo.min() <= value && value <= rangeInfo.max());
      }
      pairOffset += count;
    }

    // The minimum/maximum of each range is actually the smallest/largest value
    for (int pairOffset = 0, rangeIdx = 0; rangeIdx < mockDynamicRangeResult.size(); rangeIdx++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(rangeIdx);
      int count = rangeInfo.count();
      WeightedPair minPair = sortedPairs.get(pairOffset);
      WeightedPair maxPair = sortedPairs.get(pairOffset + count - 1);
      long min = minPair.value();
      long max = maxPair.value();
      assertTrue(rangeInfo.min() == min);
      assertTrue(rangeInfo.max() == max);
      pairOffset += count;
    }

    // Weights of each range is over the rangeWeightTarget - exclude last range
    for (int i = 0; i < mockDynamicRangeResult.size() - 1; i++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(i);
      assertTrue(rangeInfo.weight() >= rangeWeightTarget);
    }

    // Removing the last weight from a range brings it under the rangeWeightTarget - exclude last
    // range
    for (int pairOffset = 0, rangeIdx = 0;
        rangeIdx < mockDynamicRangeResult.size() - 1;
        rangeIdx++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(rangeIdx);
      int count = rangeInfo.count();
      WeightedPair lastPair = sortedPairs.get(pairOffset + count - 1);
      long lastWeight = lastPair.weight();
      pairOffset += count;
      assertTrue(rangeInfo.weight() - lastWeight < rangeWeightTarget);
    }

    // Centroids for each range are correct
    for (int pairOffset = 0, rangeIdx = 0; rangeIdx < mockDynamicRangeResult.size(); rangeIdx++) {
      DynamicRangeUtil.DynamicRangeInfo rangeInfo = mockDynamicRangeResult.get(rangeIdx);
      int count = rangeInfo.count();
      long accuValue = 0;
      for (int i = pairOffset; i < pairOffset + count; i++) {
        WeightedPair pair = sortedPairs.get(i);
        long value = pair.value();
        accuValue += value;
      }
      pairOffset += count;
      assertTrue(rangeInfo.centroid() == ((double) accuValue / count));
    }
  }

  private static void assertDynamicNumericRangeResults(
      long[] values,
      long[] weights,
      int topN,
      long totalWeight,
      List<DynamicRangeUtil.DynamicRangeInfo> expectedDynamicRangeResult) {
    List<DynamicRangeUtil.DynamicRangeInfo> mockDynamicRangeResult =
        DynamicRangeUtil.computeDynamicNumericRanges(
            values, weights, values.length, totalWeight, topN);
    assertEquals(expectedDynamicRangeResult, mockDynamicRangeResult);
  }

  /**
   * Holds parameters of a weighted pair.
   *
   * @param value the value of the pair
   * @param weight the weight of the pair
   */
  private record WeightedPair(long value, long weight) {}
}
