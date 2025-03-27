package org.apache.lucene.sandbox.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;

import java.util.List;
import java.util.Map;

public abstract class AbstractQueryProfilerBreakdown extends QueryProfilerBreakdown {
    /**
     * Return (or create) contextual profile breakdown instance
     * @param context leaf reader context
     * @return contextual profile breakdown instance
     */
    public abstract QueryProfilerBreakdown context(LeafReaderContext context);

    // To be implemented by ConcurrentQueryProfileBreakdown
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext context) {}

    // To be implemented by ConcurrentQueryProfileBreakdown
    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
