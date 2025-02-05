package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class to orchestrate {@link FacetBuilder}s to setup and exec collection for multiple facets. */
public final class FacetOrchestrator {
    private List<FacetBuilder> facetBuilders = new ArrayList<>();

    private FacetOrchestrator() {
    }

    public static FacetOrchestrator start() {
        return new FacetOrchestrator();
    }

    public FacetOrchestrator addBuilder(FacetBuilder request) {
        facetBuilders.add(request);
        return this;
    }

    public void collect(Query query, IndexSearcher searcher) throws IOException {
        collect(query, searcher, null);
    }

    //public FacetBuilder addCollector(CollectorManager<C, T>)
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <C extends Collector, T> T collect(Query query,
                                              IndexSearcher searcher, CollectorManager<C, T> mainCollector) throws IOException {
        // Check if we can reuse collectors for some FacetBuilders
        Map<Object, FacetBuilder> buildersUniqueForCollection = new HashMap<>();
        for (FacetBuilder builder : facetBuilders) {
            buildersUniqueForCollection.compute(builder.collectionKey(), (k, v) -> builder.initOrReuseCollector(v));
        }
        List<CollectorManager<? extends Collector, ?>> managers = new ArrayList<>();
        if (mainCollector != null) {
            managers.add(mainCollector);
        }
        for (FacetBuilder c: buildersUniqueForCollection.values()) {
            managers.add(c.getCollectorManager());
        }
        MultiCollectorManager mcm = new MultiCollectorManager(managers.toArray(new CollectorManager[0]));
        Object[] res = searcher.search(query, mcm);
        if (mainCollector != null) {
            return (T) res[0];
        } else {
            return null;
        }
    }

    public void collect(DrillDownQuery query, DrillSideways drillSideways) {
        // TK
    }
}
