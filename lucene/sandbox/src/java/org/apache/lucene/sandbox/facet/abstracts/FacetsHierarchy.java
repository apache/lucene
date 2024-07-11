package org.apache.lucene.sandbox.facet.abstracts;

/**
 * Interface that defines facets hierarchy. Currently used to implement functionality similar to
 * {@link org.apache.lucene.facet.Facets#getAllDims(int)}.
 *
 * TODO: do we really need it? Does anyone use getAllDims?
 *
 * See also {@link FacetRollup}.
 */
public interface FacetsHierarchy {

    /** Get ordinals for all dimensions. */
    OrdinalIterator getAllDimOrds();

}
