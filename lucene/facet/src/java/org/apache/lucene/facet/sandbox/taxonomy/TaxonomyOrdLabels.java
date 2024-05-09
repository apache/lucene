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
    public String getLabel(int ordinal) throws IOException {
        // TODO: well this is not very flexible. Should we consider returning FacetLabel for this method?
        FacetLabel facetLabel = taxoReader.getPath(ordinal);
        return facetLabel.components[facetLabel.length - 1];
    }

    @Override
    public FacetLabel[] getLabels(int[] ordinals) throws IOException {
        return taxoReader.getBulkPath(ordinals.clone()); // Have to clone because getBulkPath shuffles its input array.
    }

    @Override
    public int getOrd(String label) {
        // TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int[] getOrds(String[] labels) {
        // TODO: use bulk get ords method
        throw new UnsupportedOperationException("not yet implemented");
    }
}
