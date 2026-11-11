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
package org.apache.lucene.codecs.bloom;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link FuzzySet} sizing. */
public class TestFuzzySet extends LuceneTestCase {

  private static final float TARGET_FPP = 0.1023f;

  /** A value count small enough that no sizing arithmetic can overflow. */
  private static final int SAFE_VALUE_COUNT = 1_000_000;

  private static double observedFalsePositiveRate(FuzzySet set) {
    for (int i = 0; i < 1000; i++) {
      set.addValue(new BytesRef("present-" + i));
    }
    int maybes = 0;
    for (int i = 0; i < 1000; i++) {
      if (set.contains(new BytesRef("absent-" + i)) == FuzzySet.ContainsResult.MAYBE) {
        maybes++;
      }
    }
    return (double) maybes / 1000;
  }

  private static void assertRejectsAbsentValues(String desc, FuzzySet set) {
    double fpp = observedFalsePositiveRate(set);
    assertTrue(
        desc + " answered MAYBE for " + (fpp * 100) + "% of absent values; set=" + set, fpp < 0.5);
  }

  private static void assertNotSmallerThanSafeBaseline(String desc, FuzzySet set) {
    FuzzySet baseline = FuzzySet.createOptimalSet(SAFE_VALUE_COUNT, TARGET_FPP);
    assertTrue(
        desc
            + " allocated "
            + set.ramBytesUsed()
            + " bytes, less than the "
            + baseline.ramBytesUsed()
            + " bytes used for only "
            + SAFE_VALUE_COUNT
            + " values; set="
            + set,
        set.ramBytesUsed() >= baseline.ramBytesUsed());
  }

  //
  // Test cases checking for integer overflow issues when sizing the bitset.
  //

  public void testCreateOptimalSetDoesNotOverflowWhenDoubling() {
    int valueCount = 226_505_854;
    FuzzySet set = FuzzySet.createOptimalSet(valueCount, TARGET_FPP);
    assertNotSmallerThanSafeBaseline("createOptimalSet(" + valueCount + ")", set);
    assertRejectsAbsentValues("createOptimalSet(" + valueCount + ")", set);
  }

  public void testCreateOptimalSetDoesNotOverflowWhenCastSaturates() {
    int valueCount = Integer.MAX_VALUE;
    FuzzySet set = FuzzySet.createOptimalSet(valueCount, TARGET_FPP);
    assertNotSmallerThanSafeBaseline("createOptimalSet(" + valueCount + ")", set);
    assertRejectsAbsentValues("createOptimalSet(" + valueCount + ")", set);
  }

  public void testCreateOptimalSetSizeIsMonotonic() {
    long previous = 0;
    for (int valueCount = 1 << 10; valueCount <= 1 << 22; valueCount <<= 1) {
      FuzzySet set = FuzzySet.createOptimalSet(valueCount, TARGET_FPP);
      long current = set.ramBytesUsed();
      assertTrue(
          "createOptimalSet("
              + valueCount
              + ") allocated "
              + current
              + " bytes, down from "
              + previous
              + " bytes for half as many values; set="
              + set,
          current >= previous);
      previous = current;
    }
  }

  public void testCreateSetBasedOnMaxMemoryDoesNotOverflow() {
    int maxNumBytes = 1 << 28;
    FuzzySet set = FuzzySet.createSetBasedOnMaxMemory(maxNumBytes);
    FuzzySet baseline = FuzzySet.createSetBasedOnMaxMemory(1 << 20);
    assertTrue(
        "createSetBasedOnMaxMemory("
            + maxNumBytes
            + ") allocated "
            + set.ramBytesUsed()
            + " bytes, less than the "
            + baseline.ramBytesUsed()
            + " bytes used for a 1MB budget; set="
            + set,
        set.ramBytesUsed() >= baseline.ramBytesUsed());
    assertRejectsAbsentValues("createSetBasedOnMaxMemory(" + maxNumBytes + ")", set);
  }

  public void testCreateSetBasedOnMaxMemoryHonorsBudget() {
    for (int maxNumBytes = 1 << 10; maxNumBytes <= 1 << 24; maxNumBytes <<= 1) {
      FuzzySet set = FuzzySet.createSetBasedOnMaxMemory(maxNumBytes);

      // Slack for the object header on the backing long[]
      int ramOverheadSlack = 64;
      assertTrue(
          "createSetBasedOnMaxMemory("
              + maxNumBytes
              + ") allocated "
              + set.ramBytesUsed()
              + " bytes; set="
              + set,
          set.ramBytesUsed() <= maxNumBytes + ramOverheadSlack);
    }
  }

  public void testHashCountIsNeverZero() {
    float targetFpp = 0.9f;
    FuzzySet set = FuzzySet.createOptimalSet(SAFE_VALUE_COUNT, targetFpp);
    assertRejectsAbsentValues("createOptimalSet(" + SAFE_VALUE_COUNT + ", " + targetFpp + ")", set);
  }

  public void testUsableBitSetSizesCanAllBeIncremented() {
    for (int usableBitSetSize : FuzzySet.usableBitSetSizes) {
      assertTrue(
          "usableBitSetSizes entry " + usableBitSetSize + " overflows when incremented",
          usableBitSetSize + 1 > 0);
    }
  }
}
