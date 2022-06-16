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

package org.apache.lucene.search;

import java.util.concurrent.atomic.AtomicLong;

/** Used for defining custom algorithms to allow searches to early terminate */
abstract class HitsThresholdChecker {
  /** Implementation of HitsThresholdChecker which allows global hit counting */
  private static class GlobalHitsThresholdChecker extends HitsThresholdChecker {
    private final AtomicLong globalHitCount = new AtomicLong();

    GlobalHitsThresholdChecker(int totalHitsThreshold) {
      super(totalHitsThreshold);
      assert totalHitsThreshold != Integer.MAX_VALUE;
    }

    @Override
    void incrementHitCount(int increment) {
      globalHitCount.addAndGet(increment);
    }

    @Override
    boolean isThresholdReached() {
      return globalHitCount.getAcquire() > getHitsThreshold();
    }
  }

  /** Default implementation of HitsThresholdChecker to be used for single threaded execution */
  private static class LocalHitsThresholdChecker extends HitsThresholdChecker {
    private int hitCount;

    LocalHitsThresholdChecker(int totalHitsThreshold) {
      super(totalHitsThreshold);
      assert totalHitsThreshold != Integer.MAX_VALUE;
    }

    @Override
    void incrementHitCount(int increment) {
      hitCount += increment;
    }

    @Override
    boolean isThresholdReached() {
      return hitCount > getHitsThreshold();
    }
  }

  /**
   * No-op implementation of {@link HitsThresholdChecker} that does no counting, as the threshold
   * can never be reached. This is useful for cases where early termination is never desired, so
   * that the overhead of counting hits can be avoided.
   */
  private static final HitsThresholdChecker EXACT_HITS_COUNT_THRESHOLD_CHECKER =
      new HitsThresholdChecker(Integer.MAX_VALUE) {
        @Override
        void incrementHitCount(int increment) {
          // noop
        }

        @Override
        boolean isThresholdReached() {
          return false;
        }
      };

  /*
   * Returns a threshold checker that is useful for single threaded searches
   */
  static HitsThresholdChecker create(final int totalHitsThreshold) {
    return totalHitsThreshold == Integer.MAX_VALUE
        ? HitsThresholdChecker.EXACT_HITS_COUNT_THRESHOLD_CHECKER
        : new LocalHitsThresholdChecker(totalHitsThreshold);
  }

  /*
   * Returns a threshold checker that is based on a shared counter
   */
  static HitsThresholdChecker createShared(final int totalHitsThreshold) {
    return totalHitsThreshold == Integer.MAX_VALUE
        ? HitsThresholdChecker.EXACT_HITS_COUNT_THRESHOLD_CHECKER
        : new GlobalHitsThresholdChecker(totalHitsThreshold);
  }

  private final int totalHitsThreshold;

  HitsThresholdChecker(int totalHitsThreshold) {
    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException(
          "totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }
    this.totalHitsThreshold = totalHitsThreshold;
  }

  final int getHitsThreshold() {
    return totalHitsThreshold;
  }

  abstract boolean isThresholdReached();

  abstract void incrementHitCount(int increment);
}
