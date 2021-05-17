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

package org.apache.lucene.sandbox.queries.profile;

import java.util.List;

/**
 * Public interface and serialization container for profiled timings of the Collectors used in the
 * search. Children CollectorResult's may be embedded inside of a parent CollectorResult
 */
public class CollectorResult {

  /** A more friendly representation of the Collector's class name */
  private final String collectorName;

  /** A "hint" to help provide some context about this Collector */
  private final String reason;

  /** The total elapsed time for this Collector */
  private final Long time;

  /** A list of children collectors "embedded" inside this collector */
  private List<CollectorResult> children;

  public CollectorResult(
      String collectorName, String reason, Long time, List<CollectorResult> children) {
    this.collectorName = collectorName;
    this.reason = reason;
    this.time = time;
    this.children = children;
  }

  /** @return the profiled time for this collector (inclusive of children) */
  public long getTime() {
    return this.time;
  }

  /** @return a human readable "hint" about what this collector was used for */
  public String getReason() {
    return this.reason;
  }

  /** @return the lucene class name of the collector */
  public String getName() {
    return this.collectorName;
  }

  /** @return a list of children collectors */
  public List<CollectorResult> getProfiledChildren() {
    return children;
  }
}
