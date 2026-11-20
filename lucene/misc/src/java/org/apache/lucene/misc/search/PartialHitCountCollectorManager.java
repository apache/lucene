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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.Weight;

/**
 * Collector manager based on {@link PartialHitCountCollector} that counts hits accurately up to a
 * threshold, and stops counting once the threshold is exceeded. This is useful when counting all
 * hits would be costly and a lower bound is enough beyond some number of hits, such as "more than
 * 1000 hits". For exact hit counts, use {@link IndexSearcher#count} or {@link
 * TotalHitCountCollectorManager} instead. To also collect top hits, combine it with other collector
 * managers via {@link MultiCollectorManager}.
 *
 * <p>If the query matches {@code totalHitsThreshold} hits or fewer, the returned {@link TotalHits}
 * is accurate and its relation is {@link Relation#EQUAL_TO}. Otherwise, its relation is {@link
 * Relation#GREATER_THAN_OR_EQUAL_TO} if counting stopped early, in which case its value is a lower
 * bound of the hit count that is greater than {@code totalHitsThreshold}, or {@link
 * Relation#EQUAL_TO} if all hits got counted anyway. Counting only stops once the threshold is
 * exceeded, so a threshold of {@code 0} still counts the first hit and a query that matches no
 * documents gets an accurate count of {@code 0}.
 *
 * <p>The threshold applies to the hits counted across all slices. Collectors may count more hits
 * than the threshold before they stop counting, because:
 *
 * <ul>
 *   <li>segments whose hit count can be computed without collecting hits, see {@link Weight#count},
 *       are counted in full;
 *   <li>a batch of documents may be collected at once, see {@link DocIdStream}, and is counted in
 *       full;
 *   <li>collectors only see the hits counted by other collectors when they sync, which they do
 *       after counting their share of the hits left until the threshold is exceeded, divided by the
 *       number of slices, or after 1024 hits, whichever comes first. So collectors that count
 *       concurrently may exceed the threshold by up to 1024 hits each.
 * </ul>
 *
 * <p>A manager instance may be reused for successive searches, but not for concurrent searches, and
 * not after a search that failed with an exception: its shared hit count is only reset by {@link
 * #reduce}.
 *
 * @lucene.experimental
 */
public class PartialHitCountCollectorManager
    implements CollectorManager<PartialHitCountCollector, TotalHits> {

  private final TotalHitCountCollectorManager hitCountManager;
  private final int totalHitsThreshold;
  private final int numSlices;
  private final AtomicLong globalHitCount = new AtomicLong();

  /**
   * Creates a new partial hit count collector manager, providing the array of leaf slices that
   * search targets, which can be retrieved via {@link IndexSearcher#getSlices()} for the searcher.
   *
   * @param leafSlices the slices of the searcher that the collector manager is used with
   * @param totalHitsThreshold the number of hits to count accurately. If the query matches more
   *     than {@code totalHitsThreshold} hits then its hit count may be a lower bound. On the other
   *     hand if the query matches {@code totalHitsThreshold} hits or fewer then the hit count is
   *     accurate. {@link Integer#MAX_VALUE} may be used to make the hit count accurate.
   */
  public PartialHitCountCollectorManager(
      IndexSearcher.LeafSlice[] leafSlices, int totalHitsThreshold) {
    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException(
          "totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }
    this.hitCountManager = new TotalHitCountCollectorManager(leafSlices);
    this.totalHitsThreshold = totalHitsThreshold;
    // searchers of empty indexes have no slices
    this.numSlices = Math.max(1, leafSlices.length);
  }

  @Override
  public PartialHitCountCollector newCollector() throws IOException {
    return new PartialHitCountCollector(
        hitCountManager.newCollector(), totalHitsThreshold, numSlices, globalHitCount);
  }

  @Override
  public TotalHits reduce(Collection<PartialHitCountCollector> collectors) throws IOException {
    List<TotalHitCountCollector> hitCountCollectors = new ArrayList<>(collectors.size());
    boolean earlyTerminated = false;
    for (PartialHitCountCollector collector : collectors) {
      hitCountCollectors.add(collector.hitCountCollector);
      earlyTerminated |= collector.isEarlyTerminated();
    }
    int totalHits = hitCountManager.reduce(hitCountCollectors);
    // Make the same collector manager instance reusable across multiple searches.
    globalHitCount.set(0);
    return new TotalHits(
        totalHits, earlyTerminated ? Relation.GREATER_THAN_OR_EQUAL_TO : Relation.EQUAL_TO);
  }
}
