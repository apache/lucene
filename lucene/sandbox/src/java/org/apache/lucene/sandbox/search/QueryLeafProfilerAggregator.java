package org.apache.lucene.sandbox.search;

import java.util.List;

interface QueryLeafProfilerAggregator {
    QueryProfilerResult.AggregationType getAggregationType();
    long getQueryStartTime();
    long getQueryEndTime();
    QueryProfilerTimer getTimer(QueryProfilerTimingType timingType);
    List<AggregatedQueryLeafProfilerResult> getAggregatedQueryLeafProfilerResults();
}
