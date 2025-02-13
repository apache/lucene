package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;

import java.io.IOException;

/** {@link FacetBuilder} for taxonomy facets. */
public final class TaxonomyFacetBuilder extends BaseFacetBuilder<TaxonomyFacetBuilder> {
    private final FacetsConfig facetsConfig;
    private final FacetsConfig.DimConfig dimConfig;
    private final TaxonomyReader taxonomyReader;
    private final String indexFieldName;

    // Post-collection vars
    private TaxonomyOrdLabelBiMap taxoOrdLabels;
    private int parentOrd = -1;

    public TaxonomyFacetBuilder(FacetsConfig facetsConfig,
                                TaxonomyReader taxonomyReader,
                                String dimension,
                                String... path) {
        super(dimension, path);
        this.facetsConfig = facetsConfig;
        this.taxonomyReader = taxonomyReader;
        this.dimConfig = facetsConfig.getDimConfig(dimension);
        this.indexFieldName = dimConfig.indexFieldName;
    }

    @Override
    Object collectionKey() {
        return indexFieldName;
    }

    @Override
    FacetCutter createFacetCutter() {
        return new TaxonomyFacetsCutter(indexFieldName, facetsConfig, taxonomyReader);
    }

    private int getParentOrd() throws IOException {
        if (this.parentOrd < 0) {
            FacetLabel parentLabel = new FacetLabel(dimension, path);
            this.parentOrd = ordToLabel().getOrd(parentLabel);
        }
        return this.parentOrd;
    }

    @Override
    OrdinalIterator getMatchingOrdinalIterator() throws IOException {
        return new TaxonomyChildrenOrdinalIterator(
                super.getMatchingOrdinalIterator(),
                taxonomyReader.getParallelTaxonomyArrays().parents(),
                getParentOrd());
    }

    @Override
    Number getOverallValue() throws IOException {
        if (dimConfig.multiValued == false || dimConfig.hierarchical || dimConfig.requireDimCount) {
            return getValue(getParentOrd());
        }
        return -1; // Can't compute
    }

    @Override
    TaxonomyOrdLabelBiMap ordToLabel() {
        if (taxoOrdLabels == null) {
            taxoOrdLabels = new TaxonomyOrdLabelBiMap(taxonomyReader);
        }
        return taxoOrdLabels;
    }

    @Override
    TaxonomyFacetBuilder self() {
        return this;
    }
}
