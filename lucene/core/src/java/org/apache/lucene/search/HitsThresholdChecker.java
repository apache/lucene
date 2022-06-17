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

    GlobalHitsThresholdChecker(int totalHitsThreshold, int numHits) {
      super(totalHitsThreshold, numHits);
    }

    @Override
    void incrementHitCount(int increment) {
      globalHitCount.addAndGet(increment);
    }

    @Override
    boolean isThresholdReached() {
      return globalHitCount.getAcquire() > getHitsThreshold();
    }

    @Override
    boolean isNumHitsReached() {
      return globalHitCount.getAcquire() > getNumHits();
    }
  }

  /** Default implementation of HitsThresholdChecker to be used for single threaded execution */
  private static class LocalHitsThresholdChecker extends HitsThresholdChecker {
    private int hitCount;

    LocalHitsThresholdChecker(int totalHitsThreshold, int numHits) {
      super(totalHitsThreshold, numHits);
    }

    @Override
    void incrementHitCount(int increment) {
      hitCount += increment;
    }

    @Override
    boolean isThresholdReached() {
      return hitCount > getHitsThreshold();
    }

    @Override
    boolean isNumHitsReached() {
      return hitCount > getNumHits();
    }
  }

  /*
   * Returns a threshold checker that is useful for single threaded searches
   */
  static HitsThresholdChecker create(final int totalHitsThreshold, final int numHits) {
    return new LocalHitsThresholdChecker(totalHitsThreshold, numHits);
  }

  /*
   * Returns a threshold checker that is based on a shared counter
   */
  static HitsThresholdChecker createShared(final int totalHitsThreshold, final int numHits) {
    return new GlobalHitsThresholdChecker(totalHitsThreshold, numHits);
  }

  private final int totalHitsThreshold;
  private final int numHits;

  HitsThresholdChecker(int totalHitsThreshold, int numHits) {
    if (numHits < 0) {
      throw new IllegalArgumentException("numHits must be >= 0, got " + numHits);
    }
    if (totalHitsThreshold < numHits) {
      throw new IllegalArgumentException(
          "totalHitsThreshold must be >= numHits, got " + totalHitsThreshold + " < " + numHits);
    }
    this.totalHitsThreshold = totalHitsThreshold;
    this.numHits = numHits;
  }

  final int getHitsThreshold() {
    return totalHitsThreshold;
  }

  final int getNumHits() {
    return numHits;
  }

  abstract boolean isThresholdReached();

  abstract boolean isNumHitsReached();

  abstract void incrementHitCount(int increment);
}
