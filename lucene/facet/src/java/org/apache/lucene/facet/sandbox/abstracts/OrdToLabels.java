package org.apache.lucene.facet.sandbox.abstracts;

import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.io.IOException;

/**
 * Label to ord bimap interface.
 * TODO: do we need the bulk methods? Should they have other signature, e.g. OrdinalIterator as inputs?
 * TODO: do we want to rely on FacetLabel instead of String?
 */
public interface OrdToLabels {

    /** get label of one ord */
    String getLabel(int ordinal) throws IOException;

    /**
     * get labels for multiple ords
     */
    FacetLabel[] getLabels(int[] ordinals) throws IOException;

    /** get ord for one label */
    int getOrd(String label);

    /** get ords for multiple labels */
    int[] getOrds(String[] labels);
}
