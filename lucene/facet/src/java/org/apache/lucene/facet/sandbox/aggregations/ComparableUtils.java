package org.apache.lucene.facet.sandbox.aggregations;

import org.apache.lucene.facet.sandbox.abstracts.GetOrd;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;

/**
 * Collection of static methods to provide most common comparables for sandbox faceting.
 * You can also use it as an example for creating your own {@link OrdToComparable} to enable
 * custom facets sorting.
 */
public class ComparableUtils {
    private ComparableUtils() {}

    private static class SkeletalGetOrd implements GetOrd {
        int ord;

        @Override
        public int getOrd() {
            return ord;
        }
    }

    /** Result of {@link #countOrdToComparable} method below */
    public static class IntOrdComparable extends SkeletalGetOrd implements Comparable<IntOrdComparable> {
        private IntOrdComparable() {};

        private int rank;

        @Override
        public int compareTo(IntOrdComparable o) {
            int cmp = Integer.compare(rank, o.rank);
            if (cmp == 0) {
                cmp = Integer.compare(o.ord, ord);
            }
            return cmp;
        }
    }

    /**
     * To sort facet ords by count (descending) with ord as a tie-break (ascending) using
     * provided {@link CountRecorder}.
     */
    public static OrdToComparable<IntOrdComparable> countOrdToComparable(CountRecorder recorder) {
        return new OrdToComparable<>() {
            @Override
            public IntOrdComparable getComparable(int ord, IntOrdComparable reuse) {
                if (reuse == null) {
                    reuse = new IntOrdComparable();
                }
                reuse.ord = ord;
                reuse.rank = recorder.getCount(ord);
                return reuse;
            }
        };
    }

    /** Result of {@link #rankCountOrdToComparable} methods below */
    public static class LongIntOrdComparable extends SkeletalGetOrd implements Comparable<LongIntOrdComparable> {
        private LongIntOrdComparable() {};
        private int secondaryRank;
        private long primaryRank;

        @Override
        public int compareTo(LongIntOrdComparable o) {
            int cmp = Long.compare(primaryRank, o.primaryRank);
            if (cmp == 0) {
                cmp = Integer.compare(secondaryRank, o.secondaryRank);
                if (cmp == 0) {
                    cmp = Integer.compare(o.ord, ord);
                }
            }
            return cmp;
        }
    }

    /**
     * To sort facet ords by long aggregation (descending) with tie-break by count (descending)
     * with ord as a tie-break (ascending) using provided {@link CountRecorder} and {@link LongAggregationsFacetRecorder}.
     */
    public static OrdToComparable<LongIntOrdComparable> rankCountOrdToComparable(CountRecorder countRecorder, LongAggregationsFacetRecorder longAggregationsFacetRecorder, int aggregationId) {
        return new OrdToComparable<>() {
            @Override
            public LongIntOrdComparable getComparable(int ord, LongIntOrdComparable reuse) {
                if (reuse == null) {
                    reuse = new LongIntOrdComparable();
                }
                reuse.ord = ord;
                reuse.secondaryRank = countRecorder.getCount(ord);
                reuse.primaryRank = longAggregationsFacetRecorder.getRecordedValue(ord, aggregationId);
                return reuse;
            }
        };
    }
}
