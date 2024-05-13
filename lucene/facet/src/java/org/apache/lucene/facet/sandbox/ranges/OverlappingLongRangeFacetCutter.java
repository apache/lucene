package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
// TODO: copy over class or change modifiers later

import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OverlappingLongRangeFacetCutter extends LongRangeFacetCutter {

    // Uses segment tree optimisation to find all matching ranges for a given value
    // https://blog.mikemccandless.com/2013/12/fast-range-faceting-using-segment-trees.html
    private final LongRangeNode root;

    OverlappingLongRangeFacetCutter(String field, MultiLongValuesSource longValuesSource, LongRange[] longRanges) {
        super(field, longValuesSource, longRanges);

        // Build binary tree on top of intervals:
        root = split(0, elementaryIntervals.size(), elementaryIntervals);

        // Set outputs, so we know which range to output for each node in the tree:
        for (int i = 0; i < sortedRanges.length; i++) {
            root.addOutputs(i, sortedRanges[i]);
        }
    }

    @Override
    List<InclusiveRange> buildElementaryIntervals() {
        // Maps all range inclusive endpoints to int flags; 1
        // = start of interval, 2 = end of interval.  We need to
        // track the start vs end case separately because if a
        // given point is both, then it must be its own
        // elementary interval:
        Map<Long, Integer> endsMap = new HashMap<>();

        endsMap.put(Long.MIN_VALUE, 1);
        endsMap.put(Long.MAX_VALUE, 2);

        for (LongRangeAndPos rangeAndPos : sortedRanges) {
            Integer cur = endsMap.get(rangeAndPos.range().min);
            if (cur == null) {
                endsMap.put(rangeAndPos.range().min, 1);
            } else {
                endsMap.put(rangeAndPos.range().min, cur | 1);
            }
            cur = endsMap.get(rangeAndPos.range().max);
            if (cur == null) {
                endsMap.put(rangeAndPos.range().max, 2);
            } else {
                endsMap.put(rangeAndPos.range().max, cur | 2);
            }
        }

        List<Long> endsList = new ArrayList<>(endsMap.keySet());
        Collections.sort(endsList);

        // Build elementaryIntervals (a 1D Venn diagram):
        List<InclusiveRange> elementaryIntervals = new ArrayList<>();
        int upto = 1;
        long v = endsList.get(0);
        long prev;
        if (endsMap.get(v) == 3) {
            elementaryIntervals.add(new InclusiveRange(v, v));
            prev = v + 1;
        } else {
            prev = v;
        }

        while (upto < endsList.size()) {
            v = endsList.get(upto);
            int flags = endsMap.get(v);
            if (flags == 3) {
                // This point is both an end and a start; we need to
                // separate it:
                if (v > prev) {
                    elementaryIntervals.add(new InclusiveRange(prev, v - 1));
                }
                elementaryIntervals.add(new InclusiveRange(v, v));
                prev = v + 1;
            } else if (flags == 1) {
                // This point is only the start of an interval;
                // attach it to next interval:
                if (v > prev) {
                    elementaryIntervals.add(new InclusiveRange(prev, v - 1));
                }
                prev = v;
            } else {
                assert flags == 2;
                // This point is only the end of an interval; attach
                // it to last interval:
                elementaryIntervals.add(new InclusiveRange(prev, v));
                prev = v + 1;
            }
            upto++;
        }

        return elementaryIntervals;
    }

    private static LongRangeNode split(int start, int end, List<InclusiveRange> elementaryIntervals) {
        if (start == end - 1) {
            // leaf
            InclusiveRange range = elementaryIntervals.get(start);
            return new LongRangeNode(range.start(), range.end(), null, null, start);
        } else {
            int mid = (start + end) >>> 1;
            LongRangeNode left = split(start, mid, elementaryIntervals);
            LongRangeNode right = split(mid, end, elementaryIntervals);
            return new LongRangeNode(left.start(), right.end(), left, right, -1);
        }
    }


    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        MultiLongValues values = valuesSource.getValues(context);
        return new OverlappingRangeFacetLeafCutter(values, boundaries, pos, requestedRangeCount, root);
    }

    static class OverlappingRangeFacetLeafCutter extends LongRangeFacetLeafCutter {

        boolean isSingleValued = false;

        LongRangeNode elementaryIntervalRoot;

        OverlappingRangeFacetLeafCutter(MultiLongValues longValues, long[] boundaries, int[] pos, int requestedRangeCount, LongRangeNode elementaryIntervalRoot) {
            super(longValues, boundaries, pos, requestedRangeCount);
            if (longValues.getValueCount() == 1) {
                isSingleValued = true;
                // TODO: since we want to retrieve values per doc and we want to count an range just once per doc,
                // for single valued docs, we can't roll up after seeing all docs, we need to roll up after processing each doc.
                // Therefore, only one range should be counted ever
                // should we not create the disjoint ranges and only count a range once if the doc appeared in it?
                // like the ExclusiveRangeCounter case. Could we save the effort in roll up for this case?
                requestedIntervalTracker = new IntervalTracker.SingleIntervalTracker();
            } else {
                requestedIntervalTracker = new IntervalTracker.MultiIntervalTracker(requestedRangeCount);
            }
            this.elementaryIntervalRoot = elementaryIntervalRoot;
        }

        @Override
        void maybeRollUp(IntervalTracker rollUpInto) {
            rollUp(elementaryIntervalRoot, -1, rollUpInto);
        }

        // Note: combined rollUpSingleValued and rollUpMultiValued from OverlappingLongRangeCounter into 1 rollUp method
        private int rollUp(LongRangeNode node, int elementaryIntervalUpto, IntervalTracker requestedIntervalsTracker) {
            if (node.left() != null) {
                elementaryIntervalUpto = rollUp(node.left(), elementaryIntervalUpto, requestedIntervalsTracker);
                elementaryIntervalUpto = rollUp(node.right(), elementaryIntervalUpto, requestedIntervalsTracker);
            } else {
                // Leaf
                int intervalIndex = elementaryIntervalUpto;
                if (intervalIndex == -1) {
                    intervalIndex = 0;
                }
                 if (elementaryIntervalTracker.get(intervalIndex)) {
                     elementaryIntervalUpto++;
                 }
            }
            if (elementaryIntervalUpto != -1 && node.outputs() != null) {
                for (int rangeIndex : node.outputs()) {
                    requestedIntervalsTracker.set(rangeIndex);
                }
            };
            return elementaryIntervalUpto;
        }

        @Override
        public int nextOrd() throws IOException {
            if (requestedIntervalTracker == null) {
                return NO_MORE_ORDS;
            }
            return requestedIntervalTracker.nextOrd();
        }
    }

}
