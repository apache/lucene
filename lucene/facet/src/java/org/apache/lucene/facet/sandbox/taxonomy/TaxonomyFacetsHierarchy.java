package org.apache.lucene.facet.sandbox.taxonomy;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sandbox.abstracts.FacetsHierarchy;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Facets hierarchy implementation for Taxonomy facets.
 * TODO: do we really need it? If we do, should we implement the interface in {@link TaxonomyFacetsCutter} as it already
 * has all required attributes?
 */
public class TaxonomyFacetsHierarchy implements FacetsHierarchy {

    private final FacetsConfig facetsConfig;
    private final TaxonomyReader taxoReader;
    private final String indexFieldName;

    /**
     * Create.
     */
    public TaxonomyFacetsHierarchy(String indexFieldName, FacetsConfig facetsConfig, TaxonomyReader taxoReader) {
        this.facetsConfig = facetsConfig;
        this.indexFieldName = indexFieldName;
        this.taxoReader = taxoReader;
    }
    @Override
    public OrdinalIterator getAllDimOrds() {
        // TODO: TaxonomyFacets.getAllDims uses taxo arrays rather than directly facets config,
        //  should we do it the same way here? Is that faster?
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
                    if (ft.indexFieldName.equals(indexFieldName)) {
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
}
