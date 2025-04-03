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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.CollectionUtil;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
class QueryProfilerBreakdown {
  private static final Collection<QueryProfilerTimingType> QUERY_LEVEL_TIMING_TYPE =
      Arrays.stream(QueryProfilerTimingType.values()).filter(t -> !t.isSliceLevel()).toList();
  private final Map<QueryProfilerTimingType, QueryProfilerTimer> queryProfilerTimers;
  private final ConcurrentMap<Long, QuerySliceProfilerBreakdown> threadToSliceBreakdown;

  /** Sole constructor. */
  public QueryProfilerBreakdown() {
    queryProfilerTimers = new HashMap<>();
    threadToSliceBreakdown = new ConcurrentHashMap<>();

    for (QueryProfilerTimingType timingType : QUERY_LEVEL_TIMING_TYPE) {
      queryProfilerTimers.put(timingType, new QueryProfilerTimer());
    }
  }

  public final QueryProfilerTimer getTimer(QueryProfilerTimingType timingType) {
    if (timingType.isSliceLevel()) {
      return getQuerySliceProfilerBreakdown().getTimer(timingType);
    }

    // Return the query level profiler timer if not
    // slice level
    return queryProfilerTimers.get(timingType);
  }

  private QuerySliceProfilerBreakdown getQuerySliceProfilerBreakdown() {
    final long currentThreadId = Thread.currentThread().threadId();
    // See please https://bugs.openjdk.java.net/browse/JDK-8161372
    final QuerySliceProfilerBreakdown profilerBreakdown =
        threadToSliceBreakdown.get(currentThreadId);

    if (profilerBreakdown != null) {
      return profilerBreakdown;
    }

    return threadToSliceBreakdown.computeIfAbsent(
        currentThreadId, _ -> new QuerySliceProfilerBreakdown());
  }

  /** Build a timing count breakdown. */
  public final QueryProfilerResult getQueryProfilerResult(
      Query query, List<QueryProfilerResult> childrenProfileResults) {
    long queryStartTime = Long.MAX_VALUE;
    long queryEndTime = Long.MIN_VALUE;
    final Map<String, Long> breakdownMap =
        CollectionUtil.newHashMap(QUERY_LEVEL_TIMING_TYPE.size() * 2);
    for (QueryProfilerTimingType type : QUERY_LEVEL_TIMING_TYPE) {
      final QueryProfilerTimer timer = queryProfilerTimers.get(type);
      queryStartTime = Math.min(queryStartTime, timer.getEarliestTimerStartTime());
      queryStartTime =
          Math.max(
              queryStartTime, timer.getEarliestTimerStartTime() + timer.getApproximateTiming());
      breakdownMap.put(type.toString(), queryProfilerTimers.get(type).getApproximateTiming());
      breakdownMap.put(type.toString() + "_count", queryProfilerTimers.get(type).getCount());
    }

    final List<QuerySliceProfilerResult> sliceProfilerResults = new ArrayList<>();
    for (Long sliceId : threadToSliceBreakdown.keySet()) {
      final QuerySliceProfilerResult querySliceProfilerResult =
          threadToSliceBreakdown.get(sliceId).getSliceProfilerResult(sliceId);
      queryStartTime = Math.min(queryStartTime, querySliceProfilerResult.getStartTime());
      queryStartTime =
          Math.max(
              queryStartTime,
              querySliceProfilerResult.getStartTime() + querySliceProfilerResult.getTotalTime());
      sliceProfilerResults.add(querySliceProfilerResult);
    }

    return new QueryProfilerResult(
        getTypeFromQuery(query),
        getDescriptionFromQuery(query),
        Collections.unmodifiableMap(breakdownMap),
        sliceProfilerResults,
        childrenProfileResults,
        queryStartTime,
        queryEndTime);
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
