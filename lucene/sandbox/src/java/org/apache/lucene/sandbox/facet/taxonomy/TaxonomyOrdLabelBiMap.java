package org.apache.lucene.sandbox.facet.taxonomy;

import org.apache.lucene.sandbox.facet.abstracts.OrdLabelBiMap;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.io.IOException;

/**
 * Get labels for taxonomy ordinals;
 * TODO: implement!
 */
public class TaxonomyOrdLabelBiMap implements OrdLabelBiMap {

    private final TaxonomyReader taxoReader;

    /** Construct */
    public TaxonomyOrdLabelBiMap(TaxonomyReader taxoReader) {
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
    public int getOrd(FacetLabel label) throws IOException {
        assert INVALID_ORD == TaxonomyReader.INVALID_ORDINAL;
        return taxoReader.getOrdinal(label);
    }

    @Override
    public int[] getOrds(FacetLabel[] labels) throws IOException {
        assert INVALID_ORD == TaxonomyReader.INVALID_ORDINAL;
        return taxoReader.getBulkOrdinals(labels);
    }
}