package org.apache.lucene.facet.sandbox.ranges;

import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** add doc **/
public abstract class RangeFacetCutter implements FacetCutter {
    // 1. Takes input - DoubleValuesSource or a LongValuesSource
    // The use case we have currently does DoubleRanges + Double Values
    // 2. Takes input - Ranges (LongRange/DoubleRange); the latter is converted to LongRange in existing use
    // cases

    // OrdToLabel vice versa - knows original labels (strings in case of ranges and their index in the Range[])

    // next ord in the leafCutter should give thr ord in the user passed in ranges since that's the one
    // that will be globally changed/updated

    // assuming that the passed in ranges are not sorted will allow us to keep boundaries and rangeNums
    // representation for both cases

    // the leaf cutter has to find all applicable ranges for the value (values in case of a multivalued doc)

    // single valued docs and multi valued docs
    // overlapping ranges or exclusive ranges
    // For multi valued doc, we want to only count a range once per doc (even if multiple values from the doc appeared in the range)
    // For overlapping ranges, we form elementary intervals (which are disjoint sections of the venn diagram)
    // for single valued docs, we can roll up values from elementary ranges to requested ranges after looking at all docs
    // this is because it is guaranteed that the value will be in at most one range for single valued cases
    // for multi-valued docs, we have only ensured that a doc counts once per elementary range
    // for this reason, we roll up after processing each doc, as opposed to the end, so that we know exactly which
    // requested range the doc contributes to and ensure we don't double count
    // For exclusive ranges, we don't need to roll up because elementary ranges are same as requested ranges
    // the only thing we need to ensure even in this case is that for multivalued docs, we ensure that a doc contributes
    // to a range exactly once despite having multiple values in the range

    // Do I need the roll up optimisation ? Yes and no. because we roll up values from elementary ranges to
    // requested ranges to come up with the actual value of the ranges. But, we don't need it in it's current form
    // i.e in we don't have to do it after we finish looking at all docs. We always have to do roll ups after
    // processing a doc (single-valued, multi-valued, exclusive or overlapping)

    String field;

    // TODO: make the constructor also take in requested value sources and ranges
    // Ranges can be done now, we need to make a common interface for ValueSources
    RangeFacetCutter(String field) {
        this.field = field;
    }

    LongRange[] mapDoubleRangesToLongWithPrecision(DoubleRange[] doubleRanges) {
        LongRange[] longRanges = new LongRange[doubleRanges.length];
        for (int i =0; i < longRanges.length; i++) {
            DoubleRange dr = doubleRanges[i];
            longRanges[i] = new LongRange(dr.label,
                    NumericUtils.doubleToSortableLong(dr.min), true,
                    NumericUtils.doubleToSortableLong(dr.max), true);
        }
        return longRanges;
    }

}
