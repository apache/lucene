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
package org.apache.lucene.misc.search;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;

/**
 * Counts hits like {@link TotalHitCountCollector}, but stops counting once the number of hits
 * counted by all collectors of the same {@link PartialHitCountCollectorManager} exceeds the
 * threshold of the manager. Collectors may count more hits than the threshold before they stop
 * counting, see {@link PartialHitCountCollectorManager} for details.
 *
 * <p>Instances are created via {@link PartialHitCountCollectorManager#newCollector()}.
 */
public final class PartialHitCountCollector implements Collector {

  // Maximum number of hits a collector counts between two syncs with the shared hit count, the same
  // interval as MaxScoreAccumulator. Documented in the javadocs of PartialHitCountCollectorManager.
  // package-private for testing
  static final int SYNC_INTERVAL = 0x400;

  final TotalHitCountCollector hitCountCollector;
  private final int totalHitsThreshold;
  private final int numSlices;
  private final AtomicLong globalHitCount;
  private int publishedHitCount;
  private int nextSyncHitCount;
  private boolean earlyTerminated;

  PartialHitCountCollector(
      TotalHitCountCollector hitCountCollector,
      int totalHitsThreshold,
      int numSlices,
      AtomicLong globalHitCount) {
    this.hitCountCollector = hitCountCollector;
    this.totalHitsThreshold = totalHitsThreshold;
    this.numSlices = numSlices;
    this.globalHitCount = globalHitCount;
  }

  /**
   * Returns the number of hits counted by this collector, excluding the hits counted by other
   * collectors of the same {@link PartialHitCountCollectorManager}.
   */
  public int getTotalHits() {
    return hitCountCollector.getTotalHits();
  }

  /**
   * Returns {@code true} if this collector stopped counting because the threshold was exceeded, in
   * which case {@link #getTotalHits()} may be lower than the number of hits that this collector
   * would have counted otherwise.
   */
  public boolean isEarlyTerminated() {
    return earlyTerminated;
  }

  @Override
  public ScoreMode scoreMode() {
    return hitCountCollector.scoreMode();
  }

  @Override
  public void setWeight(Weight weight) {
    hitCountCollector.setWeight(weight);
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    if (totalHitsThreshold == Integer.MAX_VALUE) {
      return hitCountCollector.getLeafCollector(context);
    }

    if (sync()) {
      earlyTerminate();
    }

    final LeafCollector leafCollector;
    try {
      leafCollector = hitCountCollector.getLeafCollector(context);
    } catch (CollectionTerminatedException e) {
      // finish() is not called when getLeafCollector throws, so publish the count of the leaf here
      sync();
      throw e;
    }

    return new LeafCollector() {
      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafCollector.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        leafCollector.collect(doc);
        maybeEarlyTerminate();
      }

      @Override
      public void collect(DocIdStream stream) throws IOException {
        leafCollector.collect(stream);
        maybeEarlyTerminate();
      }

      @Override
      public void finish() throws IOException {
        leafCollector.finish();
        sync();
      }
    };
  }

  private void maybeEarlyTerminate() {
    if (hitCountCollector.getTotalHits() >= nextSyncHitCount && sync()) {
      earlyTerminate();
    }
  }

  /**
   * Adds the hits of this collector to the shared hit count, schedules the next sync, and returns
   * whether the shared hit count exceeds the threshold.
   */
  private boolean sync() {
    int hitCount = hitCountCollector.getTotalHits();
    int unpublishedHitCount = hitCount - publishedHitCount;
    publishedHitCount = hitCount;
    long globalHits =
        unpublishedHitCount == 0
            ? globalHitCount.get()
            : globalHitCount.addAndGet(unpublishedHitCount);
    // sync again after this collector's share of the remaining hits, or after SYNC_INTERVAL hits
    long remainingHitCount = totalHitsThreshold - globalHits + 1;
    nextSyncHitCount = hitCount + Math.clamp(remainingHitCount / numSlices, 1, SYNC_INTERVAL);
    return globalHits > totalHitsThreshold;
  }

  private void earlyTerminate() {
    earlyTerminated = true;
    throw new CollectionTerminatedException();
  }
}
