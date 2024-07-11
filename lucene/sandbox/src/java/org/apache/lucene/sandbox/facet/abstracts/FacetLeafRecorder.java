package org.apache.lucene.sandbox.facet.abstracts;

import java.io.IOException;

/** Facets, that don't contain hits anymore, but */
public interface FacetLeafRecorder {

    /**
     * TODO: collect? accumulate?
     * @param docId
     * @param facetId
     */
    void record(int docId, int facetId) throws IOException;

    /** Finish collection
     * TODO: do we really need it? */
    void finish(FacetLeafCutter cutter);

    // TODO: we need something like get() method to be able to aggregate (reduce)
    //  results from multiple leafs
}

