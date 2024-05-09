package org.apache.lucene.facet.sandbox.aggregations;

import org.apache.lucene.facet.sandbox.abstracts.GetOrd;
import org.apache.lucene.facet.sandbox.abstracts.GetRank;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;

/**
 * Comparable that allows sorting by rank (descending) and tie-break by ord (ascending).
 */
public class OrdRank implements Comparable<OrdRank>, GetOrd {

    private int ord;
    private int rank;

    private OrdRank(int ord, int rank) {
        this.ord = ord;
        this.rank = rank;
    }

    @Override
    public int compareTo(OrdRank o) {
        // TODO: ascending descending correct?
        int cmp = Integer.compare(o.rank, rank);
        if (cmp == 0) {
            cmp = Integer.compare(ord, o.ord);
        }
        return cmp;
    }

    @Override
    public int getOrd() {
        return ord;
    }

    /**
     * {@link OrdToComparable} for {@link OrdRank}.
     */
    public static class Comparable implements OrdToComparable<OrdRank> {

        private final GetRank getRank;

        /**
         * Construct.
         */
        public Comparable(GetRank getRank) {
            this.getRank = getRank;
        }

        @Override
        public OrdRank getComparable(int ord, OrdRank reuse) {
            if (reuse == null) {
                return new OrdRank(ord, getRank.getRank(ord));
            } else {
                reuse.ord = ord;
                reuse.rank = getRank.getRank(ord);
                return reuse;
            }
        }
    }

}
