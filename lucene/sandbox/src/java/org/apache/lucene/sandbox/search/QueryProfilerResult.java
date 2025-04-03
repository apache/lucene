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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class is the internal representation of a profiled Query, corresponding to a single node in
 * the query tree. It is built after the query has finished executing and is merely a structured
 * representation, rather than the entity that collects the timing profile.
 *
 * <p>Each QueryProfilerResult has a List of QueryProfilerResult, which will contain "children"
 * queries if applicable
 */
public class QueryProfilerResult {
  private final String type;
  private final String description;
  private final long startTime;
  private final long totalTime;
  private final Map<String, Long> breakdown;
  private final List<QuerySliceProfilerResult> sliceBreakdowns;
  private final List<QueryProfilerResult> childrenProfileResults;

  public QueryProfilerResult(
      String type,
      String description,
      Map<String, Long> breakdown,
      List<QuerySliceProfilerResult> sliceBreakdowns,
      List<QueryProfilerResult> childrenProfileResults,
      long startTime,
      long totalTime) {
    this.type = type;
    this.description = description;
    this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
    this.sliceBreakdowns =
        Objects.requireNonNull(sliceBreakdowns, "required slice breakdowns argument missing");
    this.childrenProfileResults =
        childrenProfileResults == null ? Collections.emptyList() : childrenProfileResults;
    this.startTime = startTime;
    this.totalTime = totalTime;
  }

  /** Retrieve the lucene description of this query (e.g. the "explain" text) */
  public String getDescription() {
    return description;
  }

  /** Retrieve the name of the entry (e.g. "TermQuery" or "LongTermsAggregator") */
  public String getQueryName() {
    return type;
  }

  /** The timing breakdown for this node. */
  public Map<String, Long> getTimeBreakdown() {
    return Collections.unmodifiableMap(breakdown);
  }

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

  public List<QuerySliceProfilerResult> getSliceBreakdowns() {
    return Collections.unmodifiableList(sliceBreakdowns);
  }

  /** Returns a list of all profiled children queries */
  public List<QueryProfilerResult> getProfiledChildren() {
    return Collections.unmodifiableList(childrenProfileResults);
  }
}
