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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains the maximum score and its corresponding document id concurrently across leaves/threads,
 * so that a leaf can raise its own minimum competitive score based on the progress of others.
 *
 * <p>The doc id is a global doc id, which may exceed {@link Integer#MAX_VALUE}, so the {@code
 * (score, doc)} pair is held in an {@link AtomicReference} updated with a compare-and-set max (by
 * score descending, then doc ascending). Updates happen only when a leaf raises its min competitive
 * score, so the extra allocation is off the per-hit hot path.
 */
final class MaxScoreAccumulator {
  // we use 2^10-1 to check the remainder with a bitwise operation
  private static final int DEFAULT_INTERVAL = 0x3ff;

  private final AtomicReference<DocAndScore> acc = new AtomicReference<>(null);

  // non-final and visible for tests
  long modInterval;

  MaxScoreAccumulator() {
    this.modInterval = DEFAULT_INTERVAL;
  }

  void accumulate(long docId, float score) {
    final DocAndScore update = new DocAndScore(docId, score);
    for (DocAndScore prev = acc.get(); update.isMoreCompetitiveThan(prev); prev = acc.get()) {
      if (acc.compareAndSet(prev, update)) {
        return;
      }
    }
  }

  DocAndScore get() {
    return acc.get();
  }

  /**
   * A (score, global doc) pair. {@link #isMoreCompetitiveThan} defines the ordering the accumulator
   * maximizes: higher score wins, and among equal scores the lower doc id wins (top-N search favors
   * lower doc ids on score ties).
   */
  record DocAndScore(long docId, float score) {
    boolean isMoreCompetitiveThan(DocAndScore other) {
      if (other == null) {
        return true;
      }
      if (score != other.score) {
        return score > other.score;
      }
      return docId < other.docId;
    }
  }
}
