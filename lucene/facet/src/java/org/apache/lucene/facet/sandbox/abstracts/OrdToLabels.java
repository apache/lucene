package org.apache.lucene.facet.sandbox.abstracts;

import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.io.IOException;

/**
 * Label to ord bimap interface.
 * TODO: do we need the bulk methods? Should they have other signature, e.g. OrdinalIterator as inputs?
 * TODO: move FacetLabel out of taxonomy folder to use it for any facets, not just taxonomy?
 * TODO: there is some overlap with {@link org.apache.lucene.facet.taxonomy.writercache.LabelToOrdinal}, can we reuse something?
 */
public interface OrdToLabels {

    /** get label of one ord */
    FacetLabel getLabel(int ordinal) throws IOException;

    /**
     * get labels for multiple ords
     */
    FacetLabel[] getLabels(int[] ordinals) throws IOException;

    /** get ord for one label */
    int getOrd(FacetLabel label) throws IOException;

    /** get ords for multiple labels */
    int[] getOrds(FacetLabel[] labels) throws IOException;
}
