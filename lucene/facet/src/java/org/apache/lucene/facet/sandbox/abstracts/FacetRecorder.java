package org.apache.lucene.facet.sandbox.abstracts;

import org.apache.lucene.index.LeafReaderContext;

/**
 * Registers which payload we need for a field, and then
 * generates per leaf payload class that computes the payload.
 * TODO: do we need FacetRecorderManager similar to CollectorManager, e.g. is getLeafRecorder always thread safe?
 * TODO: we need a method to reduce (merge) results from leafs.
 */
public interface FacetRecorder {

    /**
     * Get leaf recorder.
     */
    FacetLeafRecorder getLeafRecorder(LeafReaderContext context);

    /**
     * Return next collected ordinal, or {@link FacetLeafCutter#NO_MORE_ORDS}
     * TODO: do we really need that here? Should it live somewhere else?
     */
    OrdinalIterator recordedOrds();

    /**
     * Reduce leaf recorder results into this recorder.
     */
    //void reduce();

    /**
     * Rollup
     */
    //void rollup(int fromOrd, int toOrd);
}
