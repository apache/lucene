package org.apache.lucene.facet.sandbox.abstracts;


/**
 * Interface to get count
 */
public interface GetRank {

    /**
     * Get rank for the ordinal.
     */
    int getRank(int ord);
}
