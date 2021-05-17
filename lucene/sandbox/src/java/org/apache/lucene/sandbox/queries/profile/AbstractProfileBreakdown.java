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

import static java.util.Collections.emptyMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution. A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 */
public abstract class AbstractProfileBreakdown<T extends Enum<T>> {

  /** The accumulated timings for this query node */
  private final ProfileTimer[] timings;

  private final T[] timingTypes;

  /** Sole constructor. */
  public AbstractProfileBreakdown(Class<T> clazz) {
    this.timingTypes = clazz.getEnumConstants();
    timings = new ProfileTimer[timingTypes.length];
    for (int i = 0; i < timings.length; ++i) {
      timings[i] = new ProfileTimer();
    }
  }

  public ProfileTimer getTimer(T timing) {
    return timings[timing.ordinal()];
  }

  public void setTimer(T timing, ProfileTimer timer) {
    timings[timing.ordinal()] = timer;
  }

  /** Build a timing count breakdown. */
  public final Map<String, Long> toBreakdownMap() {
    Map<String, Long> map = new HashMap<>(timings.length * 2);
    for (T timingType : timingTypes) {
      map.put(timingType.toString(), timings[timingType.ordinal()].getApproximateTiming());
      map.put(timingType.toString() + "_count", timings[timingType.ordinal()].getCount());
    }
    return Collections.unmodifiableMap(map);
  }

  /** Fetch extra debugging information. */
  protected Map<String, Object> toDebugMap() {
    return emptyMap();
  }

  public final long toNodeTime() {
    long total = 0;
    for (T timingType : timingTypes) {
      total += timings[timingType.ordinal()].getApproximateTiming();
    }
    return total;
  }
}
