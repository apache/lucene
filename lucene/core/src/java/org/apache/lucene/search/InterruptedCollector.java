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
import org.apache.lucene.index.LeafReaderContext;

/**
 * The {@link InterruptedCollector} is used to interrupt the search thread that needs to be
 * interrupted, the search thread is stopped by throwing a {@link SearchInterruptedException}
 */
public class InterruptedCollector implements Collector {

  /** Thrown when the search thread is Interrupted */
  public static class SearchInterruptedException extends RuntimeException {
    /** The number of times that thread interruption status have been checked */
    private long checkInterruptedTimes;

    private SearchInterruptedException(long checkInterruptedTimes) {
      super("searchInterrupted check " + checkInterruptedTimes + " times");
    }

    public long getCheckInterruptedTimes() {
      return checkInterruptedTimes;
    }
  }

  private final Collector collector;
  private final boolean forceInterrupted;
  private long checkInterruptedTimes = 0;

  /**
   * Create a InterruptedCollector wrapper over another {@link Collector} which can be interrupted
   *
   * @param collector the wrapped {@link Collector}
   * @param forceInterrupted force Interrupted when thread receive an interruption signal. but this
   *     may cause performance penalty. It is not recommended when the number of Leaf Context
   *     documents exceeds 200W (It takes about 10ms to determine the interruption status)
   */
  public InterruptedCollector(Collector collector, boolean forceInterrupted) {
    this.collector = collector;
    this.forceInterrupted = forceInterrupted;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    checkInterruptedTimes++;
    // determine the interruption status, in LeafContext level
    if (Thread.currentThread().isInterrupted()) {
      throw new SearchInterruptedException(checkInterruptedTimes);
    }

    return new FilterLeafCollector(collector.getLeafCollector(context)) {
      @Override
      public void collect(int doc) throws IOException {
        if (forceInterrupted) {
          checkInterruptedTimes++;
          // determine the interruption status, in document level
          if (Thread.currentThread().isInterrupted()) {
            throw new SearchInterruptedException(checkInterruptedTimes);
          }
        }
        in.collect(doc);
      }
    };
  }

  @Override
  public ScoreMode scoreMode() {
    return collector.scoreMode();
  }
}
