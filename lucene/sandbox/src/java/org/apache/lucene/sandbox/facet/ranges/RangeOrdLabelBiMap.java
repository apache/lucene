package org.apache.lucene.sandbox.facet.ranges;

import org.apache.lucene.facet.range.Range;
import org.apache.lucene.sandbox.facet.abstracts.OrdLabelBiMap;
import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.io.IOException;
import java.util.List;

/**
 * {@link OrdLabelBiMap} for ranges.
 */
public class RangeOrdLabelBiMap implements OrdLabelBiMap {

    Range[] ranges;

    /** Constructor that takes array of Range objects as input **/
    public RangeOrdLabelBiMap(Range[] inputRanges) {
        ranges = inputRanges;
    }

    /** Constructor that takes List of Range objects as input **/
    public RangeOrdLabelBiMap(List<? extends Range> inputRanges) {
        ranges = inputRanges.toArray(new Range[0]);
    }
    @Override
    public FacetLabel getLabel(int ordinal) throws IOException {
        if (ordinal >= 0 && ordinal < ranges.length) {
            return new FacetLabel(ranges[ordinal].label);
        }
        return null;
    }

    @Override
    public FacetLabel[] getLabels(int[] ordinals) throws IOException {
        FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
        for (int i = 0; i < ordinals.length; i++) {
            facetLabels[i] = new FacetLabel(ranges[ordinals[i]].label);
        }
        return facetLabels;
    }

    @Override
    public int getOrd(FacetLabel label) {
        throw new UnsupportedOperationException("Not yet supported for ranges");
    }

    @Override
    public int[] getOrds(FacetLabel[] labels) {
        throw new UnsupportedOperationException("Not yet supported for ranges");
    }
}