package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.sandbox.abstracts.OrdToLabels;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

/**
 * Get labels for taxonomy ordinals;
 * TODO: implement!
 */
public class TaxonomyOrdLabels implements OrdToLabels {

    /** Construct */
    public TaxonomyOrdLabels(TaxonomyReader taxoReader) {
        // TODO
    }

    @Override
    public String getLabel(int ordinal) {
        // TODO
        return null;
    }

    @Override
    public String[] getLabels(int[] ordinals) {
        // TODO: use bulk get labels method
        return new String[0];
    }

    @Override
    public int getOrd(String label) {
        // TODO
        return 0;
    }

    @Override
    public int[] getOrds(String[] labels) {
        // TODO: use bulk get ords method
        return new int[0];
    }
}
