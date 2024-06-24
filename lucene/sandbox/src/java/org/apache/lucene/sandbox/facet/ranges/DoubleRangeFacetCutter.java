package org.apache.lucene.sandbox.facet.ranges;

import org.apache.lucene.facet.MultiDoubleValuesSource;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.sandbox.facet.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;

import java.io.IOException;

/** {@link RangeFacetCutter} for ranges of double values. **/
public class DoubleRangeFacetCutter extends RangeFacetCutter {

    LongRangeFacetCutter longRangeFacetCutter;

    MultiDoubleValuesSource multiDoubleValuesSource;
    DoubleValuesSource singleDoubleValuesSource;
    DoubleRange[] doubleRanges;

    MultiLongValuesSource multiLongValuesSource;
    LongValuesSource singleLongValuesSource;

    LongRange[] longRanges;

    /**
     * Constructor.
     * TODO: maybe explain how it works?
     **/
    public DoubleRangeFacetCutter(String field, MultiDoubleValuesSource valuesSource, DoubleRange[] doubleRanges) {
        super(field);
        this.multiDoubleValuesSource = valuesSource;
        this.singleDoubleValuesSource = MultiDoubleValuesSource.unwrapSingleton(valuesSource);
        this.doubleRanges = doubleRanges;
        if (singleDoubleValuesSource != null) { // TODO: ugly!
            this.singleLongValuesSource = singleDoubleValuesSource.toPreciseLongDoubleValuesSource();
        } else {
            this.multiLongValuesSource = multiDoubleValuesSource.toPreciseMultiLongValuesSource();
        }
        this.longRanges = mapDoubleRangesToLongWithPrecision(doubleRanges);
        this.longRangeFacetCutter = LongRangeFacetCutter.create(field, multiLongValuesSource, singleLongValuesSource, longRanges);
    }
    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        return longRangeFacetCutter.createLeafCutter(context);
    }
}
