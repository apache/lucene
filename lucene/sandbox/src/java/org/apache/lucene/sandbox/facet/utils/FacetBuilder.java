package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;

/**
 * End-to-end (request, collection and results) management of a single facet dimension.
 * Requires implementation for each facet type (taxonomy, ranges, etc)*/
public interface FacetBuilder {

    /** Init attrs required for collection phase, or reuse ones from
     * the argument that uses the same attrs for collection.
     *
     * @return similar if it is not null, otherwise this. */
    FacetBuilder initOrReuseCollector(FacetBuilder similar);


    /** Create {@link org.apache.lucene.search.CollectorManager} for this facet (dimension). */
    FacetFieldCollectorManager<CountFacetRecorder> getCollectorManager();

    /** Uniaue key for collection time.
     * Multiple facet requests can use the same collector, e.g. when they require
     * counting matches for the same field.
     * <p>
     * Default: this, to use unique collector for this request.
     * */
    default Object collectionKey() {
        return this;
    }

    /** Get results for this request. */
    FacetResult getResult();

}
