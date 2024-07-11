package org.apache.lucene.sandbox.facet.abstracts;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Creates {@link FacetLeafCutter} for each leaf.
 * TODO: do we need FacetCutterManager similar to CollectorManager, e.g. is createLeafCutter always thread safe?
 */
public interface FacetCutter {

    /**
     * Get cutter for the leaf.
     */
    FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException;
}
