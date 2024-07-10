package org.apache.lucene.sandbox.facet.ranges;

import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** {@link RangeFacetCutter} for ranges of long values. **/
public abstract class LongRangeFacetCutter extends RangeFacetCutter {

    MultiLongValuesSource valuesSource;
    LongRangeAndPos[] sortedRanges;

    int requestedRangeCount;

    List<InclusiveRange> elementaryIntervals;

    long[] boundaries;
    int[] pos;

    // Default interval position, when elementary interval is mapped to this interval
    // it is skipped.
    static final int SKIP_INTERVAL_POSITION = -1;

    // Temporary callers should ensure that passed in single values sources are wrapped
    // TODO: make a common interface for all ValueSources - Long, Double, Multi
    /** add doc **/
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

            if (elementaryIntervalTracker != null) {
                elementaryIntervalTracker.clear();
            }
            if (requestedIntervalTracker != null) {
                requestedIntervalTracker.clear();
            }

            long numValues = multiLongValues.getValueCount();

            int lastIntervalSeen = -1;

            if (numValues == 1) {
                // TODO: we should clear() interval tracker for doc rather than re-create it,
                //  it requires some refactoring to handle single value and multi value sources separately.
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

            if (elementaryIntervalTracker != null) {
                elementaryIntervalTracker.freeze();
            }
            if (requestedIntervalTracker != null) {
                requestedIntervalTracker.freeze();
            }

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
                        hi = mid - 1;
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


    /** add doc **/
    public static final class LongRangeAndPos {
        private final LongRange range;
        private final int pos;

        /**
         * add doc
         * @param range TODO add doc
         * @param pos   TODO add doc
         */
        public LongRangeAndPos(LongRange range, int pos) {
            this.range = range;
            this.pos = pos;
        }

        /** add doc **/
        public LongRange range() {
            return range;
        }

        /** add doc **/
        public int pos() {
            return pos;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LongRangeAndPos) obj;
            return Objects.equals(this.range, that.range) &&
                    this.pos == that.pos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(range, pos);
        }

        @Override
        public String toString() {
            return "LongRangeAndPos[" +
                    "range=" + range + ", " +
                    "pos=" + pos + ']';
        }

        }

    static final class InclusiveRange {
        private final long start;
        private final long end;

        InclusiveRange(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public String toString() {
                return start + " to " + end;
            }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (InclusiveRange) obj;
            return this.start == that.start &&
                    this.end == that.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }

        }
}