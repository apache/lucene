package org.apache.lucene.facet.sandbox.aggregations;

import org.apache.lucene.facet.sandbox.abstracts.GetOrd;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;

/**
 * {@link OrdToComparable} that compares ordinals by selected aggregation with tie-break by ordinal.
 * TODO: we need wither TwoLongAggregationToComparable to tie break by count, or somehow implement the logic here?
 * TODO: better name?
 */
public class SingleLongAggregationToComparable implements
        OrdToComparable<SingleLongAggregationToComparable.SingleLongAggregationComparable> {

    private final LongAggregationsFacetRecorder longAggregationsFacetRecorder;
    private final int aggregationId;

    public SingleLongAggregationToComparable(LongAggregationsFacetRecorder longAggregationsFacetRecorder,
                                             int aggregationId) {
        this.longAggregationsFacetRecorder = longAggregationsFacetRecorder;
        this.aggregationId = aggregationId;
    }

    @Override
    public SingleLongAggregationComparable getComparable(int ord, SingleLongAggregationComparable reuse) {
        if (reuse == null) {
            return new SingleLongAggregationComparable(ord,
                    longAggregationsFacetRecorder.getAggregation(ord, aggregationId));
        } else {
            reuse.ord = ord;
            reuse.aggregation = longAggregationsFacetRecorder.getAggregation(ord, aggregationId);
        }
        return null;
    }

    public static class SingleLongAggregationComparable implements Comparable<SingleLongAggregationComparable>,
            GetOrd {

        private int ord;
        private long aggregation;

        /**
         * We have to make the class itself public to use it as a parent class type;
         * but we make constructor and attrs private to make sure we only create it in this file.
         */
        private SingleLongAggregationComparable(int ord, long aggregation) {
            this.ord = ord;
            this.aggregation = aggregation;
        }

        @Override
        public int compareTo(SingleLongAggregationComparable o) {
            // TODO: simplify?
            int comp = Long.compare(aggregation, o.aggregation);
            if (comp == 0) {
                comp = Integer.compare(ord, o.ord);
            }
            return comp;
        }

        @Override
        public int getOrd() {
            return ord;
        }
    }
}
