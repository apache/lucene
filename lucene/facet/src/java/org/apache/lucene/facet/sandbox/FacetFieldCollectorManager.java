package org.apache.lucene.facet.sandbox;

import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
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

    public FacetFieldCollectorManager(FacetCutter facetCutter, V facetRecorder) {
        this.facetCutter = facetCutter;
        this.facetRecorder = facetRecorder;
    }

    @Override
    public FacetFieldCollector newCollector() throws IOException {
        return new FacetFieldCollector(facetCutter, facetRecorder);
    }

    @Override
    public V reduce(Collection<FacetFieldCollector> collectors) throws IOException {
        // TODO: implement
        // TODO: do rollup if needed; but how do we decide that rollup is needed?
        //  Options:
        //  - Do rollup here, so FacetFieldCollectorManager should have a property that says that rollup is required.
        //    Or maybe that's FacetCutter that can have this property?
        //  - Do rollup during OrdIterators chaining phase - but I don't think it's a good idea, as client has to
        //    know when the rollup is required.
        return null;
    }
}
