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
 * Profiling result for a single leaf <b>partition</b> — a doc-id range of one segment that was
 * searched as part of a {@link SliceProfilerResult slice}. Holds the timing breakdown attributed to
 * that partition alone, plus its identity (segment ordinal and doc-id range).
 */
public class AggregatedQueryLeafProfilerResult {
  private final int segmentOrd;
  private final int minDocId;
  private final int maxDocId;
  private final Map<String, Long> breakdown;
  private final long startTime;
  private final long totalTime;

  public AggregatedQueryLeafProfilerResult(
      int segmentOrd,
      int minDocId,
      int maxDocId,
      Map<String, Long> breakdown,
      long startTime,
      long totalTime) {
    this.segmentOrd = segmentOrd;
    this.minDocId = minDocId;
    this.maxDocId = maxDocId;
    this.breakdown =
        Collections.unmodifiableMap(
            Objects.requireNonNull(breakdown, "required breakdown argument missing"));
    this.startTime = startTime;
    this.totalTime = totalTime;
  }

  /**
   * Returns the ordinal of the segment this partition belongs to, or {@code -1} if the timings were
   * recorded outside of a partitioned search (e.g. a count satisfied from index statistics).
   *
   * @return the segment ordinal, or {@code -1} if unknown
   */
  public int getSegmentOrd() {
    return segmentOrd;
  }

  /**
   * Returns the inclusive lower bound of the doc-id range this partition searched, or {@code -1} if
   * unknown.
   *
   * @return the minimum doc id, or {@code -1} if unknown
   */
  public int getMinDocId() {
    return minDocId;
  }

  /**
   * Returns the exclusive upper bound of the doc-id range this partition searched, or {@code -1} if
   * unknown.
   *
   * @return the maximum doc id, or {@code -1} if unknown
   */
  public int getMaxDocId() {
    return maxDocId;
  }

  /**
   * Returns the timing breakdown for this partition.
   *
   * @return map containing time breakdown across different operation types
   */
  public Map<String, Long> getTimeBreakdown() {
    return breakdown;
  }

  /**
   * Returns the start time for this partition's execution.
   *
   * @return start time in nanoseconds
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the total time (inclusive of children) for this partition.
   *
   * @return elapsed time in nanoseconds
   */
  public long getTotalTime() {
    return totalTime;
  }
}
