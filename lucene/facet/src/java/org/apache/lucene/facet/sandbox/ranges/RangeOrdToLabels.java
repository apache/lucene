package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.sandbox.abstracts.OrdToLabels;
import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.io.IOException;

/**TODO: add doc**/
public class RangeOrdToLabels implements OrdToLabels {

    Range[] ranges;

    /**TODO: add doc**/
    public RangeOrdToLabels(Range[] inputRanges) {
        ranges = inputRanges;
    }
    @Override
    public String getLabel(int ordinal) throws IOException {
        if (ordinal >= 0 && ordinal < ranges.length) {
            return ranges[ordinal].label;
        }
        return null;
    }

    @Override
    public FacetLabel[] getLabels(int[] ordinals) throws IOException {
        FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
        for (int i = 0; i < ordinals.length; i++) {
            facetLabels[i] = new FacetLabel("Price", ranges[ordinals[i]].label);
        }
        return facetLabels;
    }

    @Override
    public int getOrd(String label) {
        throw new UnsupportedOperationException("Not yet supported for ranges");
    }

    @Override
    public int[] getOrds(String[] labels) {
        throw new UnsupportedOperationException("Not yet supported for ranges");
    }
}
