package org.apache.lucene.sandbox.facet.abstracts;

import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.io.IOException;

/**
 * Label to ord bimap interface.
 * TODO: move FacetLabel out of taxonomy folder to use it for any facets, not just taxonomy?
 * TODO: there is some overlap with {@link org.apache.lucene.facet.taxonomy.writercache.LabelToOrdinal}, can we reuse something?
 */
public interface OrdLabelBiMap {

    /** Ordinal to return if facet label doesn't exist in {@link #getOrd(FacetLabel)}
     * and {@link #getOrds(FacetLabel[])} */
    int INVALID_ORD = -1;

    /** get label of one ord
     * TODO: what do we return when ordinal is not valid? */
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
