package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public abstract class LongRangeFacetCutter extends RangeFacetCutter {

    MultiLongValuesSource valuesSource;
    LongRangeAndPos[] sortedRanges;

    int requestedRangeCount;

    List<InclusiveRange> elementaryIntervals;

    long[] boundaries;
    int[] pos;

    // Temporary callers should ensure that passed in single values sources are wrapped
    // TODO: make a common interface for all ValueSources - Long, Double, Multi
    public static LongRangeFacetCutter create(String field, MultiLongValuesSource longValuesSource, LongRange[] longRanges) {
        if (areOverlappingRanges(longRanges)) {
            return new OverlappingLongRangeFacetCutter(field, longValuesSource, longRanges);
        }
        return new ExclusiveLongRangeFacetCutter(field, longValuesSource, longRanges);
    }

    // caller handles conversion of Doubles and DoubleRange to Long and LongRange
    // ranges need not be sorted
    LongRangeFacetCutter(String field, MultiLongValuesSource longValuesSource, LongRange[] longRanges) {
        super(field);
        valuesSource = longValuesSource;
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
        Arrays.fill(pos, -1);
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

    abstract List<InclusiveRange> buildElementaryIntervals();

    private static boolean areOverlappingRanges(LongRange[] ranges) {
        if (ranges.length == 0) {
            return false;
        }

        // Copy before sorting so we don't mess with the caller's original ranges:
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

    static abstract class LongRangeFacetLeafCutter implements FacetLeafCutter {
        final MultiLongValues multiLongValues;
        final long[] boundaries;
        final int[] pos;

        final int requestedRangeCount;

        int currentDoc = -1;

        IntervalTracker elementaryIntervalTracker;

        IntervalTracker requestedIntervalTracker;


        LongRangeFacetLeafCutter(MultiLongValues longValues, long[] boundaries, int[] pos, int requestedRangeCount) {
            this.multiLongValues = longValues;
            this.boundaries = boundaries;
            this.pos = pos;
            this.requestedRangeCount = requestedRangeCount;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            if (doc < currentDoc) {
                throw new IllegalStateException("doc id going backwards");
            }
            if (doc == currentDoc) {
                return true;
            }
            if (multiLongValues.advanceExact(doc) == false) {
                return false;
            }
            currentDoc = doc;

            long numValues = multiLongValues.getValueCount();

            int lastIntervalSeen = -1;

            if (numValues == 1) {
                elementaryIntervalTracker = new IntervalTracker.SingleIntervalTracker();
                lastIntervalSeen = processValue(multiLongValues.nextValue(), lastIntervalSeen);
                elementaryIntervalTracker.set(lastIntervalSeen);
            } else {
                elementaryIntervalTracker = new IntervalTracker.MultiIntervalTracker(boundaries.length);
                for (int i = 0; i < numValues; i++) {
                    lastIntervalSeen = processValue(multiLongValues.nextValue(), lastIntervalSeen);
                    if (lastIntervalSeen >= 0 && lastIntervalSeen < boundaries.length) {
                        elementaryIntervalTracker.set(lastIntervalSeen);
                    }
                    if (lastIntervalSeen == boundaries.length - 1) {
                        // we've already reached the end of all possible intervals for this doc
                        break;
                    }
                }
            }
            maybeRollUp(requestedIntervalTracker);
            elementaryIntervalTracker.clear();
            requestedIntervalTracker.clear();
            return true;
        }

        // Returns the value of the interval v belongs or lastIntervalSeen
        // if no processing is done, it returns the lastIntervalSeen
        private int processValue(long v, int lastIntervalSeen) {
            int lo = 0, hi = boundaries.length - 1;;

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
                        hi = mid + 1;
                    }
                } else if (v > boundaries[mid+1]) {
                    lo = mid + 1;
                } else {
                    return mid + 1;
                }
            }
        }

        void maybeRollUp(IntervalTracker rollUpInto) {}

    }

    public record LongRangeAndPos(LongRange range, int pos) {
    }

    record InclusiveRange(long start, long end) {
        public String toString() {
            return start + " to " + end;
        }
    }
}
