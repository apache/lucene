package org.apache.lucene.sandbox.facet.taxonomy;

import org.apache.lucene.sandbox.facet.abstracts.OrdLabelBiMap;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;

import java.io.IOException;

/**
 * Facets results selector to get top children for selected parent.
 *
 */
public final class TaxonomyChildrenOrdinalIterator implements OrdinalIterator {

    // TODO: do we want to have something like ChainOrdinalIterators to chain multiple iterators?
    //  Or are we fine with chaining them manually every time?
    private final OrdinalIterator sourceOrds;
    private final ParallelTaxonomyArrays.IntArray parents;
    private final int parentOrd;

    /** Create */
    public TaxonomyChildrenOrdinalIterator(OrdinalIterator sourceOrds,
                                           ParallelTaxonomyArrays.IntArray parents,
                                           int parentOrd) {
        this.sourceOrds = sourceOrds;
        this.parents = parents;
        assert parentOrd != OrdLabelBiMap.INVALID_ORD: "Parent Ordinal is not valid";
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
