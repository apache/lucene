package org.apache.lucene.facet.sandbox.taxonomy;


import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Wrapper for another {@link FacetCutter} that skips ords that are not in the set.
 * It assumes the incoming ords are in sorted order (which is the case for example for {@link TaxonomyFacetsCutter}),
 * so it also optimizes with min and max values.
 * TODO: implement!
 */
public class SetFilterFacetCutter implements FacetCutter {
    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        return null;
    }
}
