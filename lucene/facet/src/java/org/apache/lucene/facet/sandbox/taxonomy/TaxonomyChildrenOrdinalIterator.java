package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;

import java.io.IOException;

/**
 * Facets results selector to get top children for selected parent.
 *
 */
public final class TaxonomyChildrenOrdinalIterator implements OrdinalIterator {

    // TODO: do we want to have something like ChainOrdinalIterators to chain multiple iterators?
    //  Or are we find that we chain them manually every time?
    private final OrdinalIterator sourceOrds;
    private final ParallelTaxonomyArrays.IntArray parents;
    private final int parentOrd;

    public TaxonomyChildrenOrdinalIterator(FacetRecorder facetRecorder,
                                           ParallelTaxonomyArrays.IntArray parents,
                                           int parentOrd) {
        this.sourceOrds = facetRecorder.recordedOrds();
        this.parents = parents;
        this.parentOrd = parentOrd;
    }

    @Override
    public int nextOrd() throws IOException {
        for (int nextOrdinal = sourceOrds.nextOrd(); nextOrdinal != NO_MORE_ORDS;) {
            if (parents.get(nextOrdinal) == parentOrd) {
                return nextOrdinal;
            }
            nextOrdinal = sourceOrds.nextOrd();
        }
        return NO_MORE_ORDS;
    }
}
