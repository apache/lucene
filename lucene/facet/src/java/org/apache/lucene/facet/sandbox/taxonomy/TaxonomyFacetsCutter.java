package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sandbox.abstracts.FacetCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetLeafCutter;
import org.apache.lucene.facet.sandbox.abstracts.FacetRecorder;
import org.apache.lucene.facet.sandbox.abstracts.FacetRollup;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link FacetCutter} for SortedNumericDocValues field.
 * TODO: can remove "Taxonomy" from the name, it works with other fields too?
 *  Cons: Less obvious for users what to use for taxonomy.
 */

public class TaxonomyFacetsCutter implements FacetCutter, FacetRollup {

    //private final String indexFieldName;
    private final FacetsConfig facetsConfig;
    private final TaxonomyReader taxoReader;
    private final String indexFieldName;

    // TODO: do we need it for anything?
    //private List<TaxonomyLeafFacetCutterMultiValue> leafCutters = new ArrayList<>();

    private ParallelTaxonomyArrays.IntArray parents;
    private ParallelTaxonomyArrays.IntArray children;
    private ParallelTaxonomyArrays.IntArray siblings;

    /**
     * Create {@link FacetCutter} for taxonomy facets.
     */
    public TaxonomyFacetsCutter(String indexFieldName, FacetsConfig facetsConfig, TaxonomyReader taxoReader) {
        this.facetsConfig = facetsConfig;
        this.indexFieldName = indexFieldName;
        this.taxoReader = taxoReader;
    }

    /**
     * Returns int[] mapping each ordinal to its first child; this is a large array and is computed
     * (and then saved) the first time this method is invoked.
     */
    ParallelTaxonomyArrays.IntArray getChildren() throws IOException {
        if (children == null) {
            children = taxoReader.getParallelTaxonomyArrays().children();
        }
        return children;
    }

    /**
     * Returns int[] mapping each ordinal to its next sibling; this is a large array and is computed
     * (and then saved) the first time this method is invoked.
     */
    ParallelTaxonomyArrays.IntArray getSiblings() throws IOException {
        if (siblings == null) {
            siblings = taxoReader.getParallelTaxonomyArrays().siblings();
        }
        return siblings;
    }

    @Override
    public FacetLeafCutter createLeafCutter(LeafReaderContext context) throws IOException {
        SortedNumericDocValues multiValued = DocValues.getSortedNumeric(context.reader(), indexFieldName);
        if (multiValued == null) {
            return null; // TODO: oh not not null!
        }
        TaxonomyLeafFacetCutterMultiValue leafCutter = new TaxonomyLeafFacetCutterMultiValue(multiValued);
        //leafCutters.add(leafCutter);
        return leafCutter;

        // TODO: does unwrapping Single valued makes things any faster? We still need to wrap it into FacetLeafCutter
        // NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);
    }

    @Override
    public OrdinalIterator getDimOrdsToRollup() {
        // Rollup any necessary dims:
        Iterator<Map.Entry<String, FacetsConfig.DimConfig>> dimensions = facetsConfig.getDimConfigs()
                .entrySet()
                .iterator();
        return new OrdinalIterator() {
            @Override
            public int nextOrd() throws IOException {
                while (dimensions.hasNext()) {
                    Map.Entry<String, FacetsConfig.DimConfig> ent = dimensions.next();
                    String dim = ent.getKey();
                    FacetsConfig.DimConfig ft = ent.getValue();
                    if (ft.hierarchical && ft.multiValued == false && ft.indexFieldName.equals(indexFieldName)) {
                        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
                        // It can be -1 if this field was declared in the
                        // config but never indexed:
                        if (dimRootOrd != TaxonomyReader.INVALID_ORDINAL) {
                            return dimRootOrd;
                        }
                    }
                }
                return NO_MORE_ORDS;
            }
        };
    }

    @Override
    public OrdinalIterator getChildrenOrds(final int parentOrd) throws IOException {
        ParallelTaxonomyArrays.IntArray children = getChildren();
        ParallelTaxonomyArrays.IntArray siblings = getSiblings();
        return new OrdinalIterator() {
            int currentChild = parentOrd;
            @Override
            public int nextOrd() throws IOException {
                // TODO: kinda ugly, can this be simpler?
                if (currentChild == parentOrd) {
                    currentChild = children.get(currentChild);
                } else {
                    currentChild = siblings.get(currentChild);
                }
                if (currentChild != TaxonomyReader.INVALID_ORDINAL) {
                    return currentChild;
                }
                return NO_MORE_ORDS;
            }
        };
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
        public boolean advanceExact(int doc) throws IOException {
            if (multiValued.advanceExact(doc)) {
                ordPerDoc = 0;
                return true;
            };
            return false;
        }
    }
}
