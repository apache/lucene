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

    // value 14 has its own bin since the weight is large, and the rest of values fall the other bin
    expectedRangeInfoList.add(new DynamicRangeUtil.DynamicRangeInfo(1, 52343, 14L, 14L, 14D));
    expectedRangeInfoList.add(
        new DynamicRangeUtil.DynamicRangeInfo(6, 2766, 32L, 455L, 163.16666666666666D));
    assertDynamicNumericRangeResults(values, weights, 4, 55109, expectedRangeInfoList);
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
    assertTrue(compareDynamicRangeResult(mockDynamicRangeResult, expectedDynamicRangeResult));
  }

  private static boolean compareDynamicRangeResult(
      List<DynamicRangeUtil.DynamicRangeInfo> mockResult,
      List<DynamicRangeUtil.DynamicRangeInfo> expectedResult) {
    return mockResult.size() == expectedResult.size() && mockResult.containsAll(expectedResult);
  }
}
