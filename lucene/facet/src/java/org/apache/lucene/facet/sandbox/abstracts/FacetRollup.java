package org.apache.lucene.facet.sandbox.abstracts;

import java.io.IOException;

/**
 * This interface is used to rollup values in {@link FacetRecorder}.
 *
 * Rollup is an optimization for facets types that support hierarchy, if single document belongs
 * to at most one leaf in the hierarchy, we can first record data for the leafs, and then roll up values to parent
 * ordinals.
 *
 * TODO: should we use this interface to express facets hierarchy? E.g. add getAllDimOrds method and use it to implement
 *  {@link org.apache.lucene.facet.Facets#getAllDims(int)}. It might be better to have a separate interface for it,
 *  as it probably need more methods that are not required during collection. At the same time, we never need rollup
 *  unless there is hierarchy, so it might make sense to merge the two interfaces?
 */
public interface FacetRollup {

    /**
     * For facets that has hierarchy (levels), return all top level dimension ordinals that require rollup.
     */
    OrdinalIterator getDimOrdsToRollup();

    /**
     * For facets that has hierarchy (levels), get all children ordinals for current ord.
     */
    OrdinalIterator getChildrenOrds(int ord) throws IOException;

}
