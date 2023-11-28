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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Create a TopFieldCollectorManager which uses a shared hit counter to maintain number of hits and
 * a shared {@link MaxScoreAccumulator} to propagate the minimum score across segments if the
 * primary sort is by relevancy.
 *
 * <p>Note that a new collectorManager should be created for each search due to its internal states.
 */
public class TopFieldCollectorManager implements CollectorManager<TopFieldCollector, TopFieldDocs> {
  private final Sort sort;
  private final int numHits;
  private final FieldDoc after;
  private final HitsThresholdChecker hitsThresholdChecker;
  private final MaxScoreAccumulator minScoreAcc;
  private final List<TopFieldCollector> collectors;
  private final boolean supportsConcurrency;
  private boolean collectorCreated;

  /**
   * Creates a new {@link TopFieldCollectorManager} from the given arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort the sort criteria (SortFields).
   * @param numHits the number of results to collect.
   * @param after the previous doc after which matching docs will be collected.
   * @param totalHitsThreshold the number of docs to count accurately. If the query matches more
   *     than {@code totalHitsThreshold} hits then its hit count will be a lower bound. On the other
   *     hand if the query matches less than or exactly {@code totalHitsThreshold} hits then the hit
   *     count of the result will be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *     count accurate, but this will also make query processing slower.
   * @param supportsConcurrency to use thread-safe and slower internal states for count tracking.
   */
  public TopFieldCollectorManager(
      Sort sort, int numHits, FieldDoc after, int totalHitsThreshold, boolean supportsConcurrency) {
    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException(
          "totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException(
          "numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (sort.getSort().length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (after != null) {
      if (after.fields == null) {
        throw new IllegalArgumentException(
            "after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException(
            "after.fields has "
                + after.fields.length
                + " values but sort has "
                + sort.getSort().length);
      }
    }

    this.sort = sort;
    this.numHits = numHits;
    this.after = after;
    this.supportsConcurrency = supportsConcurrency;
    this.hitsThresholdChecker =
        supportsConcurrency
            ? HitsThresholdChecker.createShared(Math.max(totalHitsThreshold, numHits))
            : HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits));
    this.minScoreAcc = supportsConcurrency ? new MaxScoreAccumulator() : null;
    this.collectors = new ArrayList<>();
  }

  /**
   * Creates a new {@link TopFieldCollectorManager} from the given arguments, with thread-safe
   * internal states.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort the sort criteria (SortFields).
   * @param numHits the number of results to collect.
   * @param after the previous doc after which matching docs will be collected.
   * @param totalHitsThreshold the number of docs to count accurately. If the query matches more
   *     than {@code totalHitsThreshold} hits then its hit count will be a lower bound. On the other
   *     hand if the query matches less than or exactly {@code totalHitsThreshold} hits then the hit
   *     count of the result will be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *     count accurate, but this will also make query processing slower.
   */
  public TopFieldCollectorManager(Sort sort, int numHits, FieldDoc after, int totalHitsThreshold) {
    this(sort, numHits, after, totalHitsThreshold, true);
  }

  /**
   * Creates a new {@link TopFieldCollectorManager} from the given arguments, with thread-safe
   * internal states.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort the sort criteria (SortFields).
   * @param numHits the number of results to collect.
   * @param totalHitsThreshold the number of docs to count accurately. If the query matches more
   *     than {@code totalHitsThreshold} hits then its hit count will be a lower bound. On the other
   *     hand if the query matches less than or exactly {@code totalHitsThreshold} hits then the hit
   *     count of the result will be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *     count accurate, but this will also make query processing slower.
   */
  public TopFieldCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this(sort, numHits, null, totalHitsThreshold, true);
  }

  @Override
  public TopFieldCollector newCollector() {
    if (collectorCreated && supportsConcurrency == false) {
      throw new IllegalStateException(
          "This TopFieldCollectorManager was created without concurrency (supportsConcurrency=false), but multiple collectors are being created");
    } else {
      collectorCreated = true;
    }

    FieldValueHitQueue<FieldValueHitQueue.Entry> queue =
        FieldValueHitQueue.create(sort.getSort(), numHits);

    TopFieldCollector collector;
    if (after == null) {
      // inform a comparator that sort is based on this single field
      // to enable some optimizations for skipping over non-competitive documents
      // We can't set single sort when the `after` parameter is non-null as it's
      // an implicit sort over the document id.
      if (queue.comparators.length == 1) {
        queue.comparators[0].setSingleSort();
      }
      collector =
          new TopFieldCollector.SimpleFieldCollector(
              sort, queue, numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException(
            "after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException(
            "after.fields has "
                + after.fields.length
                + " values but sort has "
                + sort.getSort().length);
      }
      collector =
          new TopFieldCollector.PagingFieldCollector(
              sort, queue, after, numHits, hitsThresholdChecker, minScoreAcc);
    }

    collectors.add(collector);
    return collector;
  }

  @Override
  public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
    final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
    int i = 0;
    for (TopFieldCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(sort, 0, numHits, topDocs);
  }

  public List<TopFieldCollector> getCollectors() {
    return collectors;
  }
}
