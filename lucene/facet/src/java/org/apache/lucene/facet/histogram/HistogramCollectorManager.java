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
package org.apache.lucene.facet.histogram;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.internal.hppc.LongIntHashMap.LongIntCursor;
import org.apache.lucene.search.CollectorManager;

/**
 * {@link CollectorManager} that computes a histogram of the distribution of the values of a field.
 *
 * <p>The returned {@link LongIntHashMap} maps quotients to the number of documents whose value
 * returns this number when divided by the given {@code interval}.
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

  private final String field;
  private final long interval;

  /** Sole constructor. */
  public HistogramCollectorManager(String field, long interval) {
    this.field = Objects.requireNonNull(field);
    this.interval = interval;
    if (interval < 2) {
      throw new IllegalArgumentException("interval must be at least 2, got: " + interval);
    }
  }

  @Override
  public HistogramCollector newCollector() throws IOException {
    return new HistogramCollector(field, interval);
  }

  @Override
  public LongIntHashMap reduce(Collection<HistogramCollector> collectors) throws IOException {
    LongIntHashMap reduced = new LongIntHashMap();
    for (HistogramCollector collector : collectors) {
      for (LongIntCursor cursor : collector.getCounts()) {
        reduced.addTo(cursor.key, cursor.value);
      }
    }
    return reduced;
  }
}
