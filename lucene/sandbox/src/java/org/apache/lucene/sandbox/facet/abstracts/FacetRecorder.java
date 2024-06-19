package org.apache.lucene.sandbox.facet.abstracts;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Record data for each facet of each doc.
 * TODO: do we need FacetRecorderManager similar to CollectorManager, e.g. is getLeafRecorder always thread safe?
 *  If we have a Manager-level recorder, then collection within a slice can be done without "syncronized"
 */
public interface FacetRecorder {
    /**
     * Get leaf recorder.
     */
    FacetLeafRecorder getLeafRecorder(LeafReaderContext context) throws IOException;

    /**
     * Return next collected ordinal, or {@link FacetLeafCutter#NO_MORE_ORDS}
     */
    OrdinalIterator recordedOrds();

    /**
     * Reduce leaf recorder results into this recorder.
     * If facetRollup is not null, it also rolls up values.
     *
     * @throws UnsupportedOperationException if facetRollup is not null and {@link FacetRollup#getDimOrdsToRollup()} returns at least one
     *  dimension ord, but this type of record can't be rolled up.
     */
    void reduce(FacetRollup facetRollup) throws IOException;
}
