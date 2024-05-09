package org.apache.lucene.facet.sandbox.abstracts;

import java.io.IOException;

/**
 * This interface is used to rollup values in {@link FacetRecorder}.
 *
 * Rollup is an optimization for facets types that support hierarchy, if single document belongs
 * to at most one leaf in the hierarchy, we can first record data for the leafs, and then roll up values to parent
 * ordinals.
 */
public interface FacetRollup {

    /**
     * For facets that has hierarchy (levels), return all top level dimension ordinals for current facets cutter.
     */
    OrdinalIterator getDimOrds();

    /**
     * For facets that has hierarchy (levels), get all children ordinals for current ord.
     */
    OrdinalIterator getChildrenOrds(int ord) throws IOException;

}
