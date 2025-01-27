package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.ranges.LongRangeFacetCutter;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;
import org.apache.lucene.sandbox.facet.labels.RangeOrdToLabel;

/** {@link FacetBuilder} for long range facets. */
public class LongRangeFacetBuilder extends BaseFacetBuilder<LongRangeFacetBuilder>{
    private final MultiLongValuesSource valueSource;
    private final LongRange[] ranges;
    /**
     * Request long range facets for numeric field by name.
     */
    public LongRangeFacetBuilder(String field,
                                 LongRange... ranges) {
        this(field, MultiLongValuesSource.fromLongField(field), ranges);
    }

    /**
     * Request long range facets for provided {@link MultiLongValuesSource}.
     * @param dimension dimension to return in results to match
     *                  {@link org.apache.lucene.facet.range.LongRangeFacetCounts#getTopChildren(int, String, String...)} results
     * @param valuesSource value source
     * @param ranges ranges
     */
    public LongRangeFacetBuilder(String dimension,
                                 MultiLongValuesSource valuesSource,
                                 LongRange... ranges) {
        super(dimension);
        this.valueSource = valuesSource;
        this.ranges = ranges;
    }

    @Override
    public FacetCutter createFacetCutter() {
        return LongRangeFacetCutter.create(valueSource, ranges);
    }

    @Override
    public Number getOverallValue() {
        // Not currently supported, see TODO item in LongRangeFacetCutter
        return -1;
    }

    @Override
    public OrdToLabel ordToLabel() {
        return new RangeOrdToLabel(ranges);
    }

    @Override
    protected LongRangeFacetBuilder self() {
        return this;
    }
}
