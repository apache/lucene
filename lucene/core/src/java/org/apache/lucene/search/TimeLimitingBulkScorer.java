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
import java.util.Objects;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.util.Bits;

/**
 * The {@link TimeLimitingBulkScorer} is used to timeout search requests that take longer than the
 * maximum allowed search time limit. After this time is exceeded, the search thread is stopped by
 * throwing a {@link TimeLimitingBulkScorer.TimeExceededException}.
 *
 * @see org.apache.lucene.index.ExitableDirectoryReader
 */
final class TimeLimitingBulkScorer extends BulkScorer {
  // We score chunks of documents at a time so as to avoid the cost of checking the timeout for
  // every document we score.
  static final int INTERVAL = 100;

  /** Thrown when elapsed search time exceeds allowed search time. */
  @SuppressWarnings("serial")
  static class TimeExceededException extends RuntimeException {

    private TimeExceededException() {
      super("TimeLimit Exceeded");
    }

    @Override
    public Throwable fillInStackTrace() {
      // never re-thrown so we can save the expensive stacktrace
      return this;
    }
  }

  private final BulkScorer in;
  private final QueryTimeout queryTimeout;

  /**
   * Create a TimeLimitingBulkScorer wrapper over another {@link BulkScorer} with a specified
   * timeout.
   *
   * @param bulkScorer the wrapped {@link BulkScorer}
   * @param queryTimeout max time allowed for collecting hits after which {@link
   *     TimeLimitingBulkScorer.TimeExceededException} is thrown
   */
  public TimeLimitingBulkScorer(BulkScorer bulkScorer, QueryTimeout queryTimeout) {
    this.in = bulkScorer;
    this.queryTimeout = Objects.requireNonNull(queryTimeout);
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int interval = INTERVAL;
    while (min < max) {
      final int newMax = (int) Math.min((long) min + interval, max);
      final int newInterval =
          interval + (interval >> 1); // increase the interval by 50% on each iteration
      // overflow check
      if (interval < newInterval) {
        interval = newInterval;
      }
      if (queryTimeout.shouldExit()) {
        throw new TimeLimitingBulkScorer.TimeExceededException();
      }
      min = in.score(collector, acceptDocs, min, newMax); // in is the wrapped bulk scorer
    }
    return min;
  }

  @Override
  public long cost() {
    return in.cost();
  }
}
