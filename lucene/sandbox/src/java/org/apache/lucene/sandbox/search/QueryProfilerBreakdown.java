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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Query;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
class QueryProfilerBreakdown {
  private static final Collection<QueryProfilerTimingType> QUERY_LEVEL_TIMING_TYPE =
      Arrays.stream(QueryProfilerTimingType.values()).filter(t -> !t.isLeafLevel()).toList();
  private final Map<QueryProfilerTimingType, QueryProfilerTimer> queryProfilerTimers;

  private final QueryLeafProfilerThreadAggregator queryLeafProfilerAggregator;

  /** Sole constructor. */
  public QueryProfilerBreakdown() {
    queryProfilerTimers = new HashMap<>();
    this.queryLeafProfilerAggregator = new QueryLeafProfilerThreadAggregator();

    for (QueryProfilerTimingType timingType : QUERY_LEVEL_TIMING_TYPE) {
      queryProfilerTimers.put(timingType, new QueryProfilerTimer());
    }
  }

  public final QueryProfilerTimer getTimer(QueryProfilerTimingType timingType) {
    if (timingType.isLeafLevel()) {
      return queryLeafProfilerAggregator.getTimer(timingType);
    }

    // Return the query level profiler timer if not
    // slice level
    return queryProfilerTimers.get(timingType);
  }

  /** Build a timing count breakdown. */
  public final QueryProfilerResult getQueryProfilerResult(
      Query query, List<QueryProfilerResult> childrenProfileResults) {
    long queryStartTime = Long.MAX_VALUE;
    long queryTotalTime = 0;
    final Map<String, Long> breakdownMap = HashMap.newHashMap(QUERY_LEVEL_TIMING_TYPE.size() * 2);
    for (QueryProfilerTimingType type : QUERY_LEVEL_TIMING_TYPE) {
      final QueryProfilerTimer timer = queryProfilerTimers.get(type);
      if (timer.getCount() > 0) {
        queryStartTime = Math.min(queryStartTime, timer.getEarliestTimerStartTime());
        queryTotalTime += timer.getApproximateTiming();
      }
      // TODO: Should we put only non-zero timer values in the final output?
      breakdownMap.put(type.toString(), queryProfilerTimers.get(type).getApproximateTiming());
      breakdownMap.put(type.toString() + "_count", queryProfilerTimers.get(type).getCount());
    }

    final List<AggregatedQueryLeafProfilerResult> threadProfilerResults =
        queryLeafProfilerAggregator.getAggregatedQueryLeafProfilerResults();
    queryStartTime = Math.min(queryStartTime, queryLeafProfilerAggregator.getQueryStartTime());
    queryTotalTime += queryLeafProfilerAggregator.getQueryTotalTime();

    return new QueryProfilerResult(
        getTypeFromQuery(query),
        getDescriptionFromQuery(query),
        Collections.unmodifiableMap(breakdownMap),
        threadProfilerResults,
        childrenProfileResults,
        queryStartTime,
        queryTotalTime);
  }

  private String getTypeFromQuery(Query query) {
    // Anonymous classes won't have a name,
    // we need to get the super class
    if (query.getClass().getSimpleName().isEmpty()) {
      return query.getClass().getSuperclass().getSimpleName();
    }
    return query.getClass().getSimpleName();
  }

  private String getDescriptionFromQuery(Query query) {
    return query.toString();
  }
}
