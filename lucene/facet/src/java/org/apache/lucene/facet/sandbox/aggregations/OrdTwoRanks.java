package org.apache.lucene.facet.sandbox.aggregations;

import org.apache.lucene.facet.sandbox.abstracts.GetOrd;
import org.apache.lucene.facet.sandbox.abstracts.GetRank;
import org.apache.lucene.facet.sandbox.abstracts.GetTwoRanks;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;

/**
 * Comparable that allows sorting by rank (descending) and tie-break by ord (ascending).
 */
public class OrdTwoRanks implements Comparable<OrdTwoRanks>, GetOrd {

    private int ord;
    private int rank;
    private long secondRank;

    private OrdTwoRanks(int ord, int rank, long secondRank) {
        this.ord = ord;
        this.rank = rank;
        this.secondRank = secondRank;
    }

    @Override
    public int compareTo(OrdTwoRanks o) {
        // TODO: ascending descending correct?
        int cmp = Long.compare(o.secondRank, secondRank);
        if (cmp == 0) {
            cmp = Integer.compare(o.rank, rank);
        }
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
     * {@link OrdToComparable} for {@link OrdTwoRanks}.
     */
    public static class Comparable implements OrdToComparable<OrdTwoRanks> {

        private final GetTwoRanks getTwoRanks;

        /**
         * Construct.
         */
        public Comparable(GetTwoRanks getTwoRanks) {
            this.getTwoRanks = getTwoRanks;
        }

        @Override
        public OrdTwoRanks getComparable(int ord, OrdTwoRanks reuse) {
            if (reuse == null) {
                return new OrdTwoRanks(ord, getTwoRanks.getRank(ord), getTwoRanks.getSecondRank(ord));
            } else {
                reuse.ord = ord;
                reuse.rank = getTwoRanks.getRank(ord);
                reuse.secondRank = getTwoRanks.getSecondRank(ord);
                return reuse;
            }
        }
    }

}
