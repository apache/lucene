package org.apache.lucene.facet.sandbox.abstracts;


/**
 * Interface to get count.
 * TODO: rename to GetCount with count method?
 */
public interface GetRank {

    /**
     * Get rank for the ordinal.
     */
    int getRank(int ord);
}
