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
package org.apache.lucene.sandbox.facet.plain.histograms;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.internal.hppc.LongIntHashMap.LongIntCursor;
import org.apache.lucene.search.CollectorManager;

/**
 * {@link CollectorManager} that computes a histogram of the distribution of the values of a field.
 *
 * <p>It takes an {@code bucketWidth} as a parameter and counts the number of documents that fall
 * into intervals [0, bucketWidth), [bucketWidth, 2*bucketWidth), etc. The keys of the returned
 * {@link LongIntHashMap} identify these intervals as the quotient of the integer division by {@code
 * bucketWidth}. Said otherwise, a key equal to {@code k} maps to values in the interval {@code [k *
 * bucketWidth, (k+1) * bucketWidth)}.
 *
 * <p>This implementation is optimized for the case when {@code field} is part of the index sort and
 * has a {@link FieldType#setDocValuesSkipIndexType skip index}.
 *
 * <p>Note: this collector is inspired from "YU, Muzhi, LIN, Zhaoxiang, SUN, Jinan, et al.
 * TencentCLS: the cloud log service with high query performances. Proceedings of the VLDB
 * Endowment, 2022, vol. 15, no 12, p. 3472-3482.", where the authors describe how they run
 * "histogram queries" by sorting the index by timestamp and pre-computing ranges of doc IDs for
 * every possible bucket.
 */
public final class HistogramCollectorManager
    implements CollectorManager<HistogramCollector, LongIntHashMap> {

  private static final int DEFAULT_MAX_BUCKETS = 1024;

  private final String field;
  private final long bucketWidth;
  private final int maxBuckets;
  private final ConcurrentMap<LeafReaderContext, Boolean> leafBulkCollected;

  /**
   * Compute a histogram of the distribution of the values of the given {@code field} according to
   * the given {@code bucketWidth}. This configures a maximum number of buckets equal to the default
   * of 1024.
   */
  public HistogramCollectorManager(String field, long bucketWidth) {
    this(field, bucketWidth, DEFAULT_MAX_BUCKETS);
  }

  /**
   * Expert constructor.
   *
   * @param maxBuckets Max allowed number of buckets. Note that this is checked at runtime and on a
   *     best-effort basis.
   */
  public HistogramCollectorManager(String field, long bucketWidth, int maxBuckets) {
    this.field = Objects.requireNonNull(field);
    if (bucketWidth < 2) {
      throw new IllegalArgumentException("bucketWidth must be at least 2, got: " + bucketWidth);
    }
    this.bucketWidth = bucketWidth;
    if (maxBuckets < 1) {
      throw new IllegalArgumentException("maxBuckets must be at least 1, got: " + maxBuckets);
    }
    this.maxBuckets = maxBuckets;
    this.leafBulkCollected = new ConcurrentHashMap<>();
  }

  @Override
  public HistogramCollector newCollector() throws IOException {
    return new HistogramCollector(field, bucketWidth, maxBuckets, leafBulkCollected);
  }

  @Override
  public LongIntHashMap reduce(Collection<HistogramCollector> collectors) throws IOException {
    LongIntHashMap reduced = new LongIntHashMap();
    for (HistogramCollector collector : collectors) {
      for (LongIntCursor cursor : collector.getCounts()) {
        reduced.addTo(cursor.key, cursor.value);
        HistogramCollector.checkMaxBuckets(reduced.size(), maxBuckets);
      }
    }
    return reduced;
  }
}
