/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
