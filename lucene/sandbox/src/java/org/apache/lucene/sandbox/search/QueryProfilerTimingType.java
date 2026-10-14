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

import java.util.Locale;

/** This enum breaks down the query into different sections to describe what was timed. */
public enum QueryProfilerTimingType {
  COUNT(true),
  BUILD_SCORER(true),
  NEXT_DOC(true),
  ADVANCE(true),
  MATCH(true),
  SCORE(true),
  SHALLOW_ADVANCE(true),
  COMPUTE_MAX_SCORE(true),
  SET_MIN_COMPETITIVE_SCORE(true),

  // IMPORTANT: Global timer types must be defined after all the
  // slice level timers to preserve the contiguous enum ordinals
  CREATE_WEIGHT(false);

  private boolean leafLevel;

  private QueryProfilerTimingType(boolean leafLevel) {
    this.leafLevel = leafLevel;
  }

  /** Indicates if the operation runs on a LeafReader as opposed to the top-level IndexReader */
  public boolean isLeafLevel() {
    return leafLevel;
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
