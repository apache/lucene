package org.apache.lucene.sandbox.facet.abstracts;

import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * Interface to be implemented to cut current document into facets.
 */
public interface FacetLeafCutter extends OrdinalIterator {
    /** advance to the next doc */
    boolean advanceExact(int doc) throws IOException;
}
