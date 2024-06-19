package org.apache.lucene.sandbox.facet.abstracts;

import java.io.IOException;

/** Facets, that don't contain hits anymore, but */
public interface FacetLeafRecorder {

    /**
     * TODO: Rename: collect? accumulate?
     * @param docId
     * @param facetId
     */
    void record(int docId, int facetId) throws IOException;
}

