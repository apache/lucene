package org.apache.lucene.sandbox.facet;

import org.apache.lucene.sandbox.facet.abstracts.FacetCutter;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.Collection;

/**
 * Collector manager for {@link FacetFieldCollector}.
 * Returns the same extension of {@link FacetRecorder} that was used to collect results.
 */

public class FacetFieldCollectorManager<V extends FacetRecorder> implements CollectorManager<FacetFieldCollector, V> {

    private final FacetCutter facetCutter;
    private final V facetRecorder;
    private final FacetRollup facetRollup;

    /**
     * Create collector for a cutter + recorder pair
     */
    public FacetFieldCollectorManager(FacetCutter facetCutter, FacetRollup facetRollup, V facetRecorder) {
        this.facetCutter = facetCutter;
        this.facetRollup = facetRollup;
        this.facetRecorder = facetRecorder;
    }

    @Override
    public FacetFieldCollector newCollector() throws IOException {
        return new FacetFieldCollector(facetCutter, facetRecorder.getSliceRecorder());
    }

    @Override
    public V reduce(Collection<FacetFieldCollector> collectors) throws IOException {
        facetRecorder.reduce(facetRollup);
        return this.facetRecorder;
    }
}
