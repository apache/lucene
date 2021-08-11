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

import java.io.IOException;
import java.util.Collection;

/**
 * Create a TopScoreDocCollectorManager which uses a shared hit counter to maintain number of hits
 * and a shared {@link MaxScoreAccumulator} to propagate the minimum score across segments
 *
 * <p>Note that a new collectorManager should be created for each search due to its internal states.
 */
public class TopScoreDocCollectorManager
    implements CollectorManager<TopScoreDocCollector, TopDocs> {
  private final int numHits;
  private final ScoreDoc after;
  private final HitsThresholdChecker hitsThresholdChecker;
  private final MaxScoreAccumulator minScoreAcc;

  public TopScoreDocCollectorManager(int numHits, ScoreDoc after, int totalHitsThreshold) {
    if (numHits <= 0) {
      throw new IllegalArgumentException(
          "numHits must be > 0; please use TotalHitCountCollectorManager if you just need the total hit count");
    }

    this.numHits = numHits;
    this.after = after;
    /*
    nocommit
    Should the following two be passed in instead? Possible custom initialization based on executor status and slices?
    On the other hand, in a single-threaded environment, shared HitsThresholdChecker and MaxScoreAccumulator should be fast without lock contention anyway?

    final HitsThresholdChecker hitsThresholdChecker =
            (executor == null || leafSlices.length <= 1)
                    ? HitsThresholdChecker.create(Math.max(TOTAL_HITS_THRESHOLD, numHits))
                    : HitsThresholdChecker.createShared(Math.max(TOTAL_HITS_THRESHOLD, numHits));

    final MaxScoreAccumulator minScoreAcc = (executor == null || leafSlices.length <= 1) ? null : new MaxScoreAccumulator();
    */
    this.hitsThresholdChecker =
        HitsThresholdChecker.createShared(Math.max(totalHitsThreshold, numHits));
    this.minScoreAcc = new MaxScoreAccumulator();
  }

  /**
   * Creates a new {@link TopScoreDocCollectorManager} given the number of hits to collect and the
   * number of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
   * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
   * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
   * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
   * but will also likely make query processing slower.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel objects.
   */
  public static TopScoreDocCollectorManager create(int numHits, int totalHitsThreshold) {
    return new TopScoreDocCollectorManager(numHits, null, totalHitsThreshold);
  }

  @Override
  public TopScoreDocCollector newCollector() {
    if (after == null) {
      return new TopScoreDocCollector.SimpleTopScoreDocCollector(
          numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      return new TopScoreDocCollector.PagingTopScoreDocCollector(
          numHits, after, hitsThresholdChecker, minScoreAcc);
    }
  }

  @Override
  public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
    final TopDocs[] topDocs = new TopDocs[collectors.size()];
    int i = 0;
    for (TopScoreDocCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(0, numHits, topDocs);
  }
}
