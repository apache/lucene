package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MultiCollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class to orchestrate {@link FacetBuilder}s to collect facets using {@link DrillSideways}. */
public final class DrillSidewaysFacetOrchestrator {
    private List<FacetBuilder> drillDownFacetBuilders = new ArrayList<>();
    private Map<String, Integer> dimToIndex = new HashMap<>();
    private Map<Integer, List<FacetBuilder>> drillSidewaysFacetBuilders = new HashMap<>();

    public DrillSidewaysFacetOrchestrator() {
    }

    public DrillSidewaysFacetOrchestrator addDrillSidewaysBuilder(String dim, FacetBuilder facetBuilder) {
        // TODO: this looks fragile as it duplicates index assignment logic from DrillDownQuery.
        //       Instead we can change DrillSideways API to accept a dimension (String) to collector manager map
        //       instead of a list so that we can be sure that we use the right collector for the right dimension.
        //       but I think we should do it in a separate PR as it requires changing existing API.
        int dimIndex = dimToIndex.computeIfAbsent(dim, (x) -> dimToIndex.size());
        drillSidewaysFacetBuilders.computeIfAbsent(dimIndex, (x) -> new ArrayList<>()).add(facetBuilder);
        return this;
    }

    public DrillSidewaysFacetOrchestrator addDrillDownBuilder(FacetBuilder facetBuilder) {
        drillDownFacetBuilders.add(facetBuilder);
        return this;
    }

    public void collect(DrillDownQuery query,
                        DrillSideways drillSideways) throws IOException {
        collect(query, drillSideways, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> T collect(DrillDownQuery query,
                          DrillSideways drillSideways,
                          CollectorManager<? extends Collector, T> mainCollector) throws IOException {
        // drill down
        List<FacetFieldCollectorManager<CountFacetRecorder>> drillDownManagers = collectorManagerForBuilders(drillDownFacetBuilders);
        int i = 0;
        CollectorManager[] managersArray;
        if (mainCollector != null) {
            managersArray = new CollectorManager[drillDownManagers.size() + 1];
            managersArray[i++] = mainCollector;
        } else {
            managersArray = new CollectorManager[drillDownManagers.size()];
        }
        for (FacetFieldCollectorManager<?> m : drillDownManagers) {
            managersArray[i++] = m;
        }
        MultiCollectorManager drillDownManager = new MultiCollectorManager(managersArray);

        // drill sideways
        List<FacetFieldCollectorManager<CountFacetRecorder>> drillSidewaysManagers = new ArrayList<>();
        for (i = 0; i < drillSidewaysFacetBuilders.size(); i++) {
            List<FacetFieldCollectorManager<CountFacetRecorder>> managers = collectorManagerForBuilders(drillSidewaysFacetBuilders.get(i));
            if (managers.size() != 1) {
                throw new IllegalArgumentException("Expected exactly one collector manager per dimension but got " + managers.size());
            }
            drillSidewaysManagers.add(managers.getFirst());
        }

        DrillSideways.Result<Object[], CountFacetRecorder> result = drillSideways.search(query, drillDownManager, drillSidewaysManagers);

        if (mainCollector != null) {
            return (T) result.drillDownResult();
        } else {
            return null;
        }
    }

    private static <C extends Collector, T> List<FacetFieldCollectorManager<CountFacetRecorder>> collectorManagerForBuilders(List<FacetBuilder> facetBuilders) {
        // Check if we can reuse collectors for some FacetBuilders
        Map<Object, FacetBuilder> buildersUniqueForCollection = new HashMap<>();
        for (FacetBuilder builder : facetBuilders) {
            buildersUniqueForCollection.compute(builder.collectionKey(), (k, v) -> builder.initOrReuseCollector(v));
        }
        List<FacetFieldCollectorManager<CountFacetRecorder>> managers = new ArrayList<>();
        for (FacetBuilder c: buildersUniqueForCollection.values()) {
            managers.add(c.getCollectorManager());
        }
        return managers;
    }
}
