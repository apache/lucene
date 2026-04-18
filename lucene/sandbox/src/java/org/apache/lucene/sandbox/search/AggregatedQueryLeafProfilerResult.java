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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * This class is the internal representation of a profiled Query, corresponding to a single node in
 * the query tree. It is built after the query has finished executing and is merely a structured
 * representation, rather than the entity that collects the timing profile.
 */
public class AggregatedQueryLeafProfilerResult {
  private final Thread thread;
  private final Map<String, Long> breakdown;
  private final long startTime;
  private final long totalTime;

  public AggregatedQueryLeafProfilerResult(
      Thread thread, Map<String, Long> breakdown, long startTime, long totalTime) {
    this.thread = thread;
    this.breakdown =
        Collections.unmodifiableMap(
            Objects.requireNonNull(breakdown, "required breakdown argument missing"));
    this.startTime = startTime;
    this.totalTime = totalTime;
  }

  /**
   * Returns the thread that executed query for these leaves
   *
   * @return thread that executed query node
   */
  public Thread getThread() {
    return thread;
  }

  /**
   * Returns the timing breakdown for this node.
   *
   * @return map containing time breakdown across different operation types
   */
  public Map<String, Long> getTimeBreakdown() {
    return breakdown;
  }

  /**
   * Returns the start time for this query node execution.
   *
   * @return start time in nanoseconds
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the total time (inclusive of children) for this query node.
   *
   * @return elapsed time in nanoseconds
   */
  public long getTotalTime() {
    return totalTime;
  }
}
