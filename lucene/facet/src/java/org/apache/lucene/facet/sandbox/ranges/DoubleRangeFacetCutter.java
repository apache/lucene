package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.MultiDoubleValuesSource;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

public class DoubleRangeFacetCutter extends RangeFacetCutter {

    LongRangeFacetCutter longRangeFacetCutter;

    MultiDoubleValuesSource multiDoubleValuesSource;
    DoubleRange[] doubleRanges;

    MultiLongValuesSource multiLongValuesSource;

    LongRange[] longRanges;

    DoubleRangeFacetCutter(String field, MultiDoubleValuesSource valuesSource, DoubleRange[] doubleRanges) {
        super(field);
        this.multiDoubleValuesSource = valuesSource;
        this.doubleRanges = doubleRanges;
        this.multiLongValuesSource = multiDoubleValuesSource.toPreciseMultiLongValuesSource();
        this.longRanges = mapDoubleRangesToLongWithPrecision(doubleRanges);
        this.longRangeFacetCutter = LongRangeFacetCutter.create(field, multiLongValuesSource, longRanges);
    }
    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        return longRangeFacetCutter.createLeafCutter(context);
    }
}
