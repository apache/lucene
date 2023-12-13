package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;

abstract class TaxonomyFacetAssociations extends TaxonomyFacets {
    /**
     * Sole constructor.
     *
     * @param indexFieldName
     * @param taxoReader
     * @param config
     * @param fc
     */
    TaxonomyFacetAssociations(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
        super(indexFieldName, taxoReader, config, fc);
    }

    @Override
    public FacetResult getAllChildren(String dim, String... path) throws IOException {
        return null;
    }

    @Override
    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        return null;
    }

    @Override
    public Number getSpecificValue(String dim, String... path) throws IOException {
        return null;
    }
}
