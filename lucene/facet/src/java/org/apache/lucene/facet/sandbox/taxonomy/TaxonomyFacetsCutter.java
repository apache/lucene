package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link FacetCutter} for SortedNumericDocValues field.
 * TODO: can remove "Taxonomy" from the name, it works with other fields too?
 *  Cons: Less obvious for users what to use for taxonomy.
 */

public class TaxonomyFacetsCutter implements FacetCutter {

    private final String indexFieldName;
    private List<TaxonomyLeafFacetCutterMultiValue> leafCutters = new ArrayList<>();// TODO: init when first added?

    TaxonomyFacetsCutter(String indexFieldName) {
        this.indexFieldName = indexFieldName;
    }

    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        SortedNumericDocValues multiValued = context.reader().getSortedNumericDocValues(indexFieldName);
        if (multiValued == null) {
            return null; // TODO: oh not not null!
        }
        TaxonomyLeafFacetCutterMultiValue leafCutter = new TaxonomyLeafFacetCutterMultiValue(multiValued);
        leafCutters.add(leafCutter);
        return leafCutter;

        // TODO: does unwrapping Single valued makes things any faster? We still need to wrap it into FacetLeafCutter
        // NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);
    }

    private static class TaxonomyLeafFacetCutterMultiValue implements FacetLeafCutter {
        private final SortedNumericDocValues multiValued;
        private int ordPerDoc;

        private TaxonomyLeafFacetCutterMultiValue(SortedNumericDocValues multiValued) {
            this.multiValued = multiValued;
        }

        @Override
        public int nextOrd() throws IOException {
            if (ordPerDoc++ < multiValued.docValueCount()) {
                return (int) multiValued.nextValue();
            }
            return FacetLeafCutter.NO_MORE_ORDS;
        }

        @Override
        public void advanceExact(int doc) throws IOException {
            multiValued.advanceExact(doc);
            ordPerDoc = 0;
        }
    }
}
