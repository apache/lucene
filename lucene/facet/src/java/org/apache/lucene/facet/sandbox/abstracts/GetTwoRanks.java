package org.apache.lucene.facet.sandbox.abstracts;


/**
 * Interface to get second rank
 */
public interface GetTwoRanks extends GetRank {

    /**
     * Get rank for the ordinal.
     */
    long getSecondRank(int ord);
}
