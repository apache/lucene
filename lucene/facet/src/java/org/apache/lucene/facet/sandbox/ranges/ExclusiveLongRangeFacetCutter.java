package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** add doc **/
public class ExclusiveLongRangeFacetCutter extends LongRangeFacetCutter {
    ExclusiveLongRangeFacetCutter(String field, MultiLongValuesSource longValuesSource, LongRange[] longRanges) {
        super(field, longValuesSource, longRanges);
    }

    @Override
    List<InclusiveRange> buildElementaryIntervals() {
        List<InclusiveRange> elementaryIntervals = new ArrayList<>();
        long prev = Long.MIN_VALUE;
        for (LongRangeAndPos range : sortedRanges) {
            if (range.range().min > prev) {
                // add a "gap" range preceding requested range if necessary:
                elementaryIntervals.add(new InclusiveRange(prev, range.range().min - 1));
            }
            // add the requested range:
            elementaryIntervals.add(new InclusiveRange(range.range().min, range.range().max));
            prev = range.range().max + 1;
        }
        if (elementaryIntervals.isEmpty() == false) {
            long lastEnd = elementaryIntervals.get(elementaryIntervals.size() - 1).end();
            if (lastEnd < Long.MAX_VALUE) {
                elementaryIntervals.add(new InclusiveRange(lastEnd + 1, Long.MAX_VALUE));
            }
        } else {
            // If no ranges were requested, create a single entry from MIN_VALUE to MAX_VALUE:
            elementaryIntervals.add(new InclusiveRange(Long.MIN_VALUE, Long.MAX_VALUE));
        }

        return elementaryIntervals;
    }

    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        MultiLongValues values = valuesSource.getValues(context);
        return new ExclusiveLongRangeFacetLeafCutter(values, boundaries, pos, requestedRangeCount);
    }

    static class ExclusiveLongRangeFacetLeafCutter extends LongRangeFacetLeafCutter {

        ExclusiveLongRangeFacetLeafCutter(MultiLongValues longValues, long[] boundaries, int[] pos, int requestedRangeCount) {
            super(longValues, boundaries, pos, requestedRangeCount);
        }

        @Override
        public int nextOrd() throws IOException {
            if (elementaryIntervalTracker == null) {
                return NO_MORE_ORDS;
            }
            int ordinal = elementaryIntervalTracker.nextOrd();
            if (ordinal == NO_MORE_ORDS) {
                return NO_MORE_ORDS;
            }
            return pos[ordinal];
        }
    }
}
