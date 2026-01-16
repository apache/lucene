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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
public class QueryLeafProfilerBreakdown {
  private static final Collection<QueryProfilerTimingType> LEAF_LEVEL_TIMING_TYPE =
      Arrays.stream(QueryProfilerTimingType.values()).filter(t -> t.isLeafLevel()).toList();

  /** The accumulated timings for this query node */
  private final QueryProfilerTimer[] timers;

  EnumMap<QueryProfilerTimingType, QueryProfilerTimer> map;

  /** Sole constructor. */
  public QueryLeafProfilerBreakdown() {
    timers = new QueryProfilerTimer[LEAF_LEVEL_TIMING_TYPE.size()];
    for (int i = 0; i < timers.length; ++i) {
      timers[i] = new QueryProfilerTimer();
    }
  }

  public QueryProfilerTimer getTimer(QueryProfilerTimingType type) {
    return timers[type.ordinal()];
  }

  /** Build a timing count breakdown. */
  public final Map<String, Long> toBreakdownMap() {
    Map<String, Long> map = HashMap.newHashMap(timers.length * 2);
    for (QueryProfilerTimingType type : LEAF_LEVEL_TIMING_TYPE) {
      map.put(type.toString(), timers[type.ordinal()].getApproximateTiming());
      map.put(type.toString() + "_count", timers[type.ordinal()].getCount());
    }
    return Collections.unmodifiableMap(map);
  }

  public final AggregatedQueryLeafProfilerResult getLeafProfilerResult(Thread thread) {
    final Map<String, Long> map = HashMap.newHashMap(LEAF_LEVEL_TIMING_TYPE.size() * 2);
    long sliceStartTime = Long.MAX_VALUE;
    long sliceEndTime = Long.MIN_VALUE;
    for (QueryProfilerTimingType type : LEAF_LEVEL_TIMING_TYPE) {
      final QueryProfilerTimer timer = timers[type.ordinal()];
      // Consider timer for updating start/total time only
      // if it was used
      if (timer.getCount() > 0) {
        sliceStartTime = Math.min(sliceStartTime, timer.getEarliestTimerStartTime());
        sliceEndTime =
            Math.max(
                sliceEndTime, timer.getEarliestTimerStartTime() + timer.getApproximateTiming());
      }
      map.put(type.toString(), timer.getApproximateTiming());
      map.put(type.toString() + "_count", timer.getCount());
    }
    return new AggregatedQueryLeafProfilerResult(
        thread, map, sliceStartTime, sliceEndTime - sliceStartTime);
  }

  public final long toTotalTime() {
    long total = 0;
    for (QueryProfilerTimer timer : timers) {
      total += timer.getApproximateTiming();
    }
    return total;
  }
}
