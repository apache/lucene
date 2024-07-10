package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.sandbox.abstracts.OrdToLabels;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.io.IOException;

/**
 * Get labels for taxonomy ordinals;
 * TODO: implement!
 */
public class TaxonomyOrdLabels implements OrdToLabels {

    private final TaxonomyReader taxoReader;

    /** Construct */
    public TaxonomyOrdLabels(TaxonomyReader taxoReader) {
        this.taxoReader = taxoReader;
    }

    @Override
    public FacetLabel getLabel(int ordinal) throws IOException {
        return taxoReader.getPath(ordinal);
    }

    @Override
    public FacetLabel[] getLabels(int[] ordinals) throws IOException {
        return taxoReader.getBulkPath(ordinals.clone()); // Have to clone because getBulkPath shuffles its input array.
    }

    @Override
    public int getOrd(FacetLabel label) {
        // TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int[] getOrds(FacetLabel[] labels) throws IOException {
        return taxoReader.getBulkOrdinals(labels);
    }
}
