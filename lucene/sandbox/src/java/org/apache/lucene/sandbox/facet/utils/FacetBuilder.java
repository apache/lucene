package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;

/**
 * End-to-end (request, collection and results) management of a single facet request.
 * Requires implementation for each facet type (taxonomy, ranges, etc).
 *
 * It's an abstract class, not interface, to define some methods as package private as they
 * should be only called by {@link FacetOrchestrator} or {@link DrillSidewaysFacetOrchestrator}.
 **/
abstract public class FacetBuilder {

    /**
     * If parameter is null, init attrs required for collection phase
     * Otherwise, reuse the attributes from {@link FacetBuilder},
     * so that we can share {@link org.apache.lucene.search.CollectorManager} with it.
     *
     * @return similar if it is not null, otherwise this. */
    abstract FacetBuilder initOrReuseCollector(FacetBuilder similar);


    /** Create {@link org.apache.lucene.search.CollectorManager} for this facet request. */
    abstract FacetFieldCollectorManager<CountFacetRecorder> getCollectorManager();

    /** Unique key for collection time.
     * Multiple facet requests can use the same collector, e.g. when they require
     * counting matches for the same field.
     * <p>
     * Default: this, to use unique collector for this request.
     * */
    Object collectionKey() {
        return this;
    }

    /** Get results for this request. */
    abstract public FacetResult getResult();

}
