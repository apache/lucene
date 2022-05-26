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
import org.apache.lucene.index.QueryTimeoutImpl;
import org.apache.lucene.util.Bits;

/**
 * The {@link TimeLimitingCollector} is used to timeout search requests that take longer than the
 * maximum allowed search time limit. After this time is exceeded, the search thread is stopped by
 * throwing a {@link TimeLimitingCollector.TimeExceededException}.
 *
 * @see org.apache.lucene.index.ExitableDirectoryReader
 */
public class TimeLimitingBulkScorer extends BulkScorer {
  /** Thrown when elapsed search time exceeds allowed search time. */
  @SuppressWarnings("serial")
  public static class TimeExceededException extends RuntimeException {

    private TimeExceededException() {
      super("TimeLimit Exceeded");
    }
  }

  private BulkScorer in;
  private QueryTimeoutImpl queryTimeout;

  public TimeLimitingBulkScorer(BulkScorer bulkScorer, QueryTimeoutImpl queryTimeout) {
    this.in = bulkScorer;
    this.queryTimeout = queryTimeout;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int interval = 100;
    while (min < max) {
      final int newMax = (int) Math.min((long) min + interval, max);
      if (queryTimeout.shouldExit() == true) {
        throw new TimeLimitingBulkScorer.TimeExceededException();
      }
      min = in.score(collector, acceptDocs, min, newMax); // in is the wrapped bulk scorer
    }
    return min;
  }

  @Override
  public long cost() {
    return 0;
  }
}
