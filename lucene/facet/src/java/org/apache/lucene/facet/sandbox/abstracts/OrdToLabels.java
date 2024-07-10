package org.apache.lucene.facet.sandbox.abstracts;

/**
 * Label to ord bimap interface.
 * TODO: do we need the bulk methods? Should they have other signature, e.g. OrdinalIterator as inputs?
 * TODO: do we want to rely on FacetLabel instead of String?
 */
public interface OrdToLabels {
    String getLabel(int ordinal);

    String[] getLabels(int[] ordinals);

    int getOrd(String label);

    int[] getOrds(String[] labels);
}
