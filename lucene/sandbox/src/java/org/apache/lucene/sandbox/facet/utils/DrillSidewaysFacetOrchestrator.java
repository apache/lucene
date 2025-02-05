package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetsConfig;
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

/** Utility class to orchestrate {@link FacetBuilder}s to setup and exec collection
 * for multiple facets using {@link DrillSideways}. */
public final class DrillSidewaysFacetOrchestrator {
    private List<FacetBuilder> drillDownFacetBuilders = new ArrayList<>();
    private Map<String, Integer> dimToIndex = new HashMap<>();
    private Map<Integer, List<FacetBuilder>> drillSidewaysFacetBuilders = new ArrayList<>();

    public DrillSidewaysFacetOrchestrator() {
    }

    public DrillSidewaysFacetOrchestrator addSidewaysBuilder(FacetBuilder facetBuilder, String dim) {
        // TODO: we should fix DrillSideways API, instead of accepting a list ofr drill sideways collectors
        //       it should get a dimension (String) to collector manager map
        //       so that we can be sure that we use the right collector for the right dimension.
        //       but I want to do it in a separate PR as it requires an API change.
        int dimIndex = dimToIndex.computeIfAbsent(dim, (x) -> dimToIndex.size());
        drillSidewaysFacetBuilders.computeIfAbsent(dimIndex, (x) -> new ArrayList<>()).add(facetBuilder);
        return this;
    }

    public DrillSidewaysFacetOrchestrator addBuilder(FacetBuilder facetBuilder) {
        drillDownFacetBuilders.add(facetBuilder);
        return this;
    }

    public void collect(DrillDownQuery query,
                        DrillSideways drillSideways) throws IOException {
        collect(query, drillSideways, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <C extends Collector, T> T collect(DrillDownQuery query,
                                              DrillSideways drillSideways,
                                              CollectorManager<C, T> mainCollector) throws IOException {
        List<CollectorManager<? extends Collector, ?>> drillDownManagers = collectorManagerForBuilders(drillDownFacetBuilders);
        if (mainCollector != null) {
            drillDownManagers.add(mainCollector);
        }
        MultiCollectorManager drillDownManager = new MultiCollectorManager(drillDownManagers.toArray(new CollectorManager[0]));

        List<CollectorManager<? extends Collector, ?>> drillSidewaysManagers = new ArrayList<>();


        Object[] res = searcher.search(query, mcm);
        if (mainCollector != null) {
            return (T) res[0];
        } else {
            return null;
        }
    }

    private <C extends Collector, T> List<CollectorManager<? extends Collector, ?>> collectorManagerForBuilders(List<FacetBuilder> facetBuilders) {
        // Check if we can reuse collectors for some FacetBuilders
        Map<Object, FacetBuilder> buildersUniqueForCollection = new HashMap<>();
        for (FacetBuilder builder : drillDownFacetBuilders) {
            buildersUniqueForCollection.compute(builder.collectionKey(), (k, v) -> builder.initOrReuseCollector(v));
        }
        List<CollectorManager<? extends Collector, ?>> managers = new ArrayList<>();
        for (FacetBuilder c: buildersUniqueForCollection.values()) {
            managers.add(c.getCollectorManager());
        }
        return managers;
    }
}
