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
package org.apache.lucene.sandbox.facet.cutters.ranges;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

/**
 * {@link FacetCutter} for ranges of long values. It's based on LongRangeCounter class.
 *
 * <p>TODO: support "total count" facet ordinal - to be able to return {@link
 * org.apache.lucene.facet.FacetResult#value}
 *
 * @lucene.experimental
 */
public abstract class LongRangeFacetCutter implements FacetCutter {

  final MultiLongValuesSource valuesSource;

  // TODO: refactor - weird that we have both multi and single here.
  final LongValuesSource singleValues;

  // Field faceted by name, whose skip index is used when present, or null when faceting a source.
  final String fieldName;

  final LongRangeAndPos[] sortedRanges;

  final int requestedRangeCount;

  final List<InclusiveRange> elementaryIntervals;

  /** elementary interval boundaries used for efficient counting (bsearch to find interval) */
  final long[] boundaries;

  final int[] pos;

  // Default interval position, when elementary interval is mapped to this interval
  // it is skipped.
  static final int SKIP_INTERVAL_POSITION = -1;

  /** Create {@link FacetCutter} for provided value source and long ranges. */
  static LongRangeFacetCutter createSingleOrMultiValued(
      MultiLongValuesSource longValuesSource,
      LongValuesSource singleLongValuesSource,
      LongRange[] longRanges) {
    return createSingleOrMultiValued(longValuesSource, singleLongValuesSource, longRanges, null);
  }

  /** Same as above, but uses the {@code fieldName} skip index when present. */
  static LongRangeFacetCutter createSingleOrMultiValued(
      MultiLongValuesSource longValuesSource,
      LongValuesSource singleLongValuesSource,
      LongRange[] longRanges,
      String fieldName) {
    if (areOverlappingRanges(longRanges)) {
      return new OverlappingLongRangeFacetCutter(
          longValuesSource, singleLongValuesSource, longRanges, fieldName);
    }
    return new NonOverlappingLongRangeFacetCutter(
        longValuesSource, singleLongValuesSource, longRanges, fieldName);
  }

  public static LongRangeFacetCutter create(
      MultiLongValuesSource longValuesSource, LongRange[] longRanges) {
    return createSingleOrMultiValued(longValuesSource, null, longRanges, null);
  }

  /** Create {@link FacetCutter} for a long field by name, using its skip index when present. */
  public static LongRangeFacetCutter create(String field, LongRange[] longRanges) {
    // Leave the single-valued source null. The skip path reads the field directly, and a
    // multi-valued segment must fall back to the multi-valued leaf cutter.
    return createSingleOrMultiValued(
        MultiLongValuesSource.fromLongField(field), null, longRanges, field);
  }

  // caller handles conversion of Doubles and DoubleRange to Long and LongRange
  // ranges need not be sorted
  LongRangeFacetCutter(
      MultiLongValuesSource longValuesSource,
      LongValuesSource singleLongValuesSource,
      LongRange[] longRanges,
      String fieldName) {
    super();
    valuesSource = longValuesSource;
    if (singleLongValuesSource != null) {
      singleValues = singleLongValuesSource;
    } else {
      singleValues = MultiLongValuesSource.unwrapSingleton(valuesSource);
    }
    this.fieldName = fieldName;

    sortedRanges = new LongRangeAndPos[longRanges.length];
    requestedRangeCount = longRanges.length;

    for (int i = 0; i < longRanges.length; i++) {
      sortedRanges[i] = new LongRangeAndPos(longRanges[i], i);
    }

    Arrays.sort(this.sortedRanges, Comparator.comparingLong(r -> r.range.min));
    elementaryIntervals = buildElementaryIntervals();

    // Keep track of elementary interval boundary ends (for binary search) along with the requested
    // range they map back to (and -1 when they map to a "gap" range in case of ExclusiveRanges):
    boundaries = new long[elementaryIntervals.size()];
    pos = new int[elementaryIntervals.size()];
    Arrays.fill(pos, SKIP_INTERVAL_POSITION);
    int currRange = 0;
    for (int i = 0; i < boundaries.length; i++) {
      boundaries[i] = elementaryIntervals.get(i).end;
      if (currRange < sortedRanges.length) {
        LongRangeAndPos curr = sortedRanges[currRange];
        if (boundaries[i] == curr.range.max) {
          pos[i] = curr.pos;
          currRange++;
        }
      }
    }
  }

  /**
   * Generates non-overlapping intervals that cover requested ranges and gaps in-between. Each
   * elementary range refers to a gap, single requested range, or multiple requested ranges when
   * they overlap.
   */
  abstract List<InclusiveRange> buildElementaryIntervals();

  /**
   * Single-valued {@link NumericDocValues} for {@link #fieldName} in this segment, or null when no
   * field is configured or some doc in this segment has more than one value.
   */
  final NumericDocValues singletonFieldValues(LeafReaderContext context) throws IOException {
    if (fieldName == null) {
      return null;
    }
    return DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), fieldName));
  }

  /** Wraps {@link NumericDocValues} as {@link LongValues}. */
  static LongValues asLongValues(NumericDocValues values) {
    return new LongValues() {
      @Override
      public long longValue() throws IOException {
        return values.longValue();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
      }
    };
  }

  private static boolean areOverlappingRanges(LongRange[] ranges) {
    if (ranges.length == 0) {
      return false;
    }

    // Copy before sorting so we don't mess with the caller's original ranges:
    // TODO: We're going to do this again in the constructor. Can't we come up with a clever way to
    // avoid doing it twice?
    LongRange[] sortedRanges = new LongRange[ranges.length];
    System.arraycopy(ranges, 0, sortedRanges, 0, ranges.length);
    Arrays.sort(sortedRanges, Comparator.comparingLong(r -> r.min));

    long previousMax = sortedRanges[0].max;
    for (int i = 1; i < sortedRanges.length; i++) {
      // Ranges overlap if the next min is <= the previous max (note that LongRange models
      // closed ranges, so equal limit points are considered overlapping):
      if (sortedRanges[i].min <= previousMax) {
        return true;
      }
      previousMax = sortedRanges[i].max;
    }

    return false;
  }

  abstract static class LongRangeMultivaluedLeafFacetCutter implements LeafFacetCutter {
    private final MultiLongValues multiLongValues;
    private final long[] boundaries;
    final int[] pos;
    final IntervalTracker elementaryIntervalTracker;

    // TODO: we need it only for overlapping ranges, should not handle it in advanceExact for
    // exclusive ranges.
    IntervalTracker requestedIntervalTracker;

    LongRangeMultivaluedLeafFacetCutter(MultiLongValues longValues, long[] boundaries, int[] pos) {
      this.multiLongValues = longValues;
      this.boundaries = boundaries;
      this.pos = pos;
      elementaryIntervalTracker = new IntervalTracker.MultiIntervalTracker(boundaries.length);
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (multiLongValues.advanceExact(doc) == false) {
        return false;
      }

      elementaryIntervalTracker.clear();

      if (requestedIntervalTracker != null) {
        requestedIntervalTracker.clear();
      }

      long numValues = multiLongValues.getValueCount();

      int lastIntervalSeen = -1;

      for (int i = 0; i < numValues; i++) {
        lastIntervalSeen = processValue(multiLongValues.nextValue(), lastIntervalSeen);
        assert lastIntervalSeen >= 0 && lastIntervalSeen < boundaries.length;
        elementaryIntervalTracker.set(lastIntervalSeen);
        if (lastIntervalSeen == boundaries.length - 1) {
          // we've already reached the end of all possible intervals for this doc
          break;
        }
      }
      maybeRollUp(requestedIntervalTracker);

      elementaryIntervalTracker.freeze();

      if (requestedIntervalTracker != null) {
        requestedIntervalTracker.freeze();
      }

      return true;
    }

    // Returns the value of the interval v belongs or lastIntervalSeen
    // if no processing is done, it returns the lastIntervalSeen
    private int processValue(long v, int lastIntervalSeen) {
      int lo = 0, hi = boundaries.length - 1;

      if (lastIntervalSeen != -1) {
        // this is the multivalued doc case, we need to set lo correctly
        if (v <= boundaries[lastIntervalSeen]) {
          // we've already counted something for this interval and doc
          // we don't need to process v
          return lastIntervalSeen;
        }

        lo = lastIntervalSeen + 1;
        if (lo == boundaries.length) {
          // we've already counted the last elementary interval. If so, there's nothing
          // else to count for this doc
          // TODO: does it make sense to return something else?
          return lastIntervalSeen;
        }
      }
      int lowerBound = lo;

      while (true) {
        int mid = (lo + hi) >>> 1;
        if (v <= boundaries[mid]) {
          if (mid == lowerBound) {
            return mid;
          } else {
            hi = mid - 1;
          }
        } else if (v > boundaries[mid + 1]) {
          lo = mid + 1;
        } else {
          return mid + 1;
        }
      }
    }

    void maybeRollUp(IntervalTracker rollUpInto) {}
  }

  abstract static class LongRangeSingleValuedLeafFacetCutter implements LeafFacetCutter {
    private final LongValues longValues;
    private final long[] boundaries;
    final int[] pos;
    int elementaryIntervalOrd;

    IntervalTracker requestedIntervalTracker;

    private final DocValuesSkipper skipper;

    // advanceSkipper's decisions for the current block; the fields below hold while doc <=
    // upToInclusive, after which it runs again for the next block.
    private int upToInclusive = -1;
    // Whether every value in the block maps to the single interval upToIntervalOrd.
    private boolean upToSameInterval;
    // Whether every doc in the block has a value.
    private boolean upToDense;
    private int upToIntervalOrd;

    // Interval of the previous doc with a value, for replaying the tracker on a repeat.
    private int previousIntervalOrd = -1;

    LongRangeSingleValuedLeafFacetCutter(LongValues longValues, long[] boundaries, int[] pos) {
      this(longValues, boundaries, pos, null);
    }

    LongRangeSingleValuedLeafFacetCutter(
        LongValues longValues, long[] boundaries, int[] pos, DocValuesSkipper skipper) {
      this.longValues = longValues;
      this.boundaries = boundaries;
      this.pos = pos;
      this.skipper = skipper;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (skipper != null && doc > upToInclusive) {
        advanceSkipper(doc);
      }

      int intervalOrd;
      if (upToSameInterval) {
        // Reuse the cached ordinal, skipping the binary search. A dense block also skips the value
        // lookup, a sparse one still needs advanceExact to know whether this doc has a value.
        if (upToDense == false && longValues.advanceExact(doc) == false) {
          return false;
        }
        intervalOrd = upToIntervalOrd;
      } else if (longValues.advanceExact(doc)) {
        intervalOrd = processValue(longValues.longValue());
      } else {
        return false;
      }

      elementaryIntervalOrd = intervalOrd;
      if (requestedIntervalTracker != null) {
        if (skipper != null && intervalOrd == previousIntervalOrd) {
          // Same interval as the previous doc, so replay its frozen rollup instead of rebuilding.
          requestedIntervalTracker.rewind();
        } else {
          requestedIntervalTracker.clear();
          maybeRollUp(requestedIntervalTracker);
          requestedIntervalTracker.freeze();
          previousIntervalOrd = intervalOrd;
        }
      }

      return true;
    }

    private void advanceSkipper(int doc) throws IOException {
      if (doc > skipper.maxDocID(0)) {
        skipper.advance(doc);
      }
      upToSameInterval = false;

      if (skipper.minDocID(0) > doc) {
        // Corner case which happens if doc doesn't have a value and is between two intervals of the
        // skip index. Fall back to per-doc lookups until the next block.
        upToInclusive = skipper.minDocID(0) - 1;
        return;
      }

      upToInclusive = skipper.maxDocID(0);
      // Climb to the highest level that still maps to a single interval.
      for (int level = 0; level < skipper.numLevels(); ++level) {
        // Long fields store raw values, skipper's min/max maps straight into the boundary space.
        int minInterval = processValue(skipper.minValue(level));
        int maxInterval = processValue(skipper.maxValue(level));
        if (minInterval != maxInterval) {
          break;
        }
        upToInclusive = skipper.maxDocID(level);
        upToSameInterval = true;
        upToIntervalOrd = minInterval;
        int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
        upToDense = skipper.docCount(level) == totalDocsAtLevel;
      }
    }

    // Returns the value of the interval v belongs or lastIntervalSeen
    // if no processing is done, it returns the lastIntervalSeen
    private int processValue(long v) {
      int lo = 0, hi = boundaries.length - 1;

      int lowerBound = lo;

      while (true) {
        int mid = (lo + hi) >>> 1;
        if (v <= boundaries[mid]) {
          if (mid == lowerBound) {
            return mid;
          } else {
            hi = mid - 1;
          }
        } else if (v > boundaries[mid + 1]) {
          lo = mid + 1;
        } else {
          return mid + 1;
        }
      }
    }

    void maybeRollUp(IntervalTracker rollUpInto) {}
  }

  record LongRangeAndPos(LongRange range, int pos) {
    @Override
    public String toString() {
      return "LongRangeAndPos[" + "range=" + range + ", " + "pos=" + pos + ']';
    }
  }

  /**
   * Similar to InclusiveRange from LongRangeCounter.
   *
   * <p>TODO: dedup
   */
  record InclusiveRange(long start, long end) {

    @Override
    public String toString() {
      return start + " to " + end;
    }
  }
}
