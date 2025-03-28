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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
class QueryProfilerBreakdown {
  private final ConcurrentMap<Long, QuerySliceProfilerBreakdown> threadToSliceBreakdown;

  /** Sole constructor. */
  public QueryProfilerBreakdown() {
    threadToSliceBreakdown = new ConcurrentHashMap<>();
  }

  public final QuerySliceProfilerBreakdown getQuerySliceProfilerBreakdown(final long sliceId) {
    return threadToSliceBreakdown.get(sliceId);
  }

  public final Collection<Long> getSlices() {
    return threadToSliceBreakdown.keySet();
  }

  public final QueryProfilerTimer getTimer(QueryProfilerTimingType timingType) {
    // What about Weight timer, ideally that should be shared across slices?
    // Maybe we can pull the Weight timer up into query profiler breakdown instead
    // of slice level breakdown? But how do we show that information in the profiler
    // tree? Repeat the same weight information across slices across queries/child queries?
    return getQuerySliceProfilerBreakdown().getTimer(timingType);
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
}
