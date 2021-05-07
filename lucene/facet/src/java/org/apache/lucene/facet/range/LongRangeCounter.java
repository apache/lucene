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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Segment tree for counting numeric ranges. Works for both single- and multi-valued cases (assuming
 * you use it correctly).
 *
 * <p>Usage notes: For counting against a single value field/source, callers should call
 * addSingleValued() for each value and then call finish() after all documents have been processed.
 * The call to finish() will inform the caller how many documents didn't match against any ranges.
 * After finish() has been called, the caller-provided count buffer (passed into the ctor) will be
 * populated with accurate range counts.
 *
 * <p>For counting against a multi-valued field, callers should call startDoc() at the beginning of
 * processing each doc, followed by addMultiValued() for each value, and then endDoc() at the end of
 * the doc. The call to endDoc() will inform the caller if that document matched against any ranges.
 *
 * <p>Note that it's possible to mix single- and multi-valued call patterns. Docs having a single
 * value only need to use addSingleValued(), while docs with multiple values need to use startDoc(),
 * addMultiValued(), endDoc(). The caller should always call finish() at the end to ensure all
 * counts are flushed and final missing docs are identified.
 */
abstract class LongRangeCounter {

  /** accumulated counts for all of the ranges */
  private final int[] countBuffer;

  /**
   * track the last counted leaf so we can skip over ones we've already counted for multi-value doc
   * cases. takes advantage of the fact that values within a given doc are sorted.
   */
  protected int multiValuedDocLastSeenLeaf;

  static LongRangeCounter create(LongRange[] ranges, int[] countBuffer) {
    if (hasOverlappingRanges(ranges)) {
      return new OverlappingLongRangeCounter(ranges, countBuffer);
    } else {
      return new SimpleLongRangeCounter(ranges, countBuffer);
    }
  }

  protected LongRangeCounter(int[] countBuffer) {
    // We'll populate the user-provided count buffer with range counts:
    this.countBuffer = countBuffer;
  }

  /** Start processing a new doc. It's unnecessary to call this for single-value cases. */
  void startMultiValuedDoc() {
    multiValuedDocLastSeenLeaf = -1;
  }

  /**
   * Finish processing a new doc. Returns whether-or-not the document contributed a count to at
   * least one range. It's unnecessary to call this for single-value cases.
   */
  abstract boolean endMultiValuedDoc();

  /** Count a single valued doc */
  abstract void addSingleValued(long v);

  /** Count a multi-valued doc value */
  abstract void addMultiValued(long v);

  /**
   * Finish processing all documents. This will return the number of docs that didn't contribute to
   * any ranges (that weren't already reported when calling endMultiValuedDoc()).
   */
  abstract int finish();

  /** Increment the specified range by one. */
  protected final void increment(int rangeNum) {
    countBuffer[rangeNum]++;
  }

  /** Increment the specified range by the specified count. */
  protected final void increment(int rangeNum, int count) {
    countBuffer[rangeNum] += count;
  }

  /** Number of ranges requested by the caller. */
  protected final int rangeCount() {
    return countBuffer.length;
  }

  /** Determine whether-or-not any requested ranges overlap */
  private static boolean hasOverlappingRanges(LongRange[] ranges) {
    if (ranges.length == 0) {
      return false;
    }

    // Copy before sorting so we don't mess with the caller's original ranges:
    LongRange[] sortedRanges = new LongRange[ranges.length];
    System.arraycopy(ranges, 0, sortedRanges, 0, ranges.length);
    Arrays.sort(sortedRanges, Comparator.comparingLong(r -> r.min));

    long prev = sortedRanges[0].max;
    for (int i = 1; i < sortedRanges.length; i++) {
      if (sortedRanges[i].min <= prev) {
        return true;
      }
      prev = sortedRanges[i].max;
    }

    return false;
  }

  protected static final class InclusiveRange {
    final long start;
    final long end;

    InclusiveRange(long start, long end) {
      assert end >= start;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return start + " to " + end;
    }
  }
}
