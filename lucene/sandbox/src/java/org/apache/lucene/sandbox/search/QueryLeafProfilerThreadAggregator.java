package org.apache.lucene.sandbox.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Implementation of QueryLeafProfilerAggregator that aggregates leaf breakdowns at thread level */
class QueryLeafProfilerThreadAggregator implements QueryLeafProfilerAggregator {
  private final ConcurrentMap<Long, QueryLeafProfilerBreakdown> queryThreadBreakdowns;
  private long queryStartTime = Long.MAX_VALUE;
  private long queryEndTime = Long.MIN_VALUE;

  public QueryLeafProfilerThreadAggregator() {
    queryThreadBreakdowns = new ConcurrentHashMap<>();
  }

  @Override
  public QueryProfilerResult.AggregationType getAggregationType() {
    return QueryProfilerResult.AggregationType.THREAD;
  }

  @Override
  public long getQueryStartTime() {
    return queryStartTime;
  }

  @Override
  public long getQueryEndTime() {
    return queryEndTime;
  }

  private QueryLeafProfilerBreakdown getQuerySliceProfilerBreakdown() {
    final long currentThreadId = Thread.currentThread().threadId();
    // See please https://bugs.openjdk.java.net/browse/JDK-8161372
    final QueryLeafProfilerBreakdown profilerBreakdown = queryThreadBreakdowns.get(currentThreadId);

    if (profilerBreakdown != null) {
      return profilerBreakdown;
    }

    return queryThreadBreakdowns.computeIfAbsent(
        currentThreadId, _ -> new QueryLeafProfilerBreakdown());
  }

  @Override
  public QueryProfilerTimer getTimer(QueryProfilerTimingType timingType) {
    assert timingType.isLeafLevel();

    return getQuerySliceProfilerBreakdown().getTimer(timingType);
  }

  @Override
  public List<AggregatedQueryLeafProfilerResult> getAggregatedQueryLeafProfilerResults() {
    final List<AggregatedQueryLeafProfilerResult> sliceProfilerResults = new ArrayList<>();
    for (Long sliceId : queryThreadBreakdowns.keySet()) {
      final AggregatedQueryLeafProfilerResult aggregatedQueryLeafProfilerResult =
          queryThreadBreakdowns.get(sliceId).getSliceProfilerResult(sliceId);
      queryStartTime = Math.min(queryStartTime, aggregatedQueryLeafProfilerResult.getStartTime());
      queryEndTime =
          Math.max(
              queryEndTime,
              aggregatedQueryLeafProfilerResult.getStartTime()
                  + aggregatedQueryLeafProfilerResult.getTotalTime());
      sliceProfilerResults.add(aggregatedQueryLeafProfilerResult);
    }

    return sliceProfilerResults;
  }
}
