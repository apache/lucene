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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.PriorityQueue;

/** Bulk scorer for {@link DisjunctionMaxQuery} when the tie-break multiplier is zero. */
final class DisjunctionMaxBulkScorer extends BulkScorer {

  // Same window size as BooleanScorer
  private static final int WINDOW_SIZE = 4096;

  private static class BulkScorerAndNext {
    public final BulkScorer scorer;
    public int next = 0;

    BulkScorerAndNext(BulkScorer scorer) {
      this.scorer = Objects.requireNonNull(scorer);
    }
  }

  // WINDOW_SIZE + 1 to ease iteration on the bit set
  private final FixedBitSet windowMatches = new FixedBitSet(WINDOW_SIZE + 1);
  private final float[] windowScores = new float[WINDOW_SIZE];
  private final PriorityQueue<BulkScorerAndNext> scorers;
  private final SimpleScorable topLevelScorable = new SimpleScorable();
  private int windowMin;

  /**
   * Reusable inner {@link LeafCollector} that collects matches into the window bitset and scores
   * array. This avoids allocating a new anonymous collector object per sub-scorer per window.
   */
  private final LeafCollector innerCollector =
      new LeafCollector() {

        private Scorable scorer;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          this.scorer = scorer;
          if (topLevelScorable.minCompetitiveScore != 0f) {
            scorer.setMinCompetitiveScore(topLevelScorable.minCompetitiveScore);
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          final int delta = doc - windowMin;
          windowMatches.set(delta);
          windowScores[delta] = Math.max(windowScores[delta], scorer.score());
        }
      };

  DisjunctionMaxBulkScorer(List<BulkScorer> scorers) {
    if (scorers.size() < 2) {
      throw new IllegalArgumentException();
    }
    this.scorers =
        PriorityQueue.usingComparator(scorers.size(), Comparator.comparingInt(b -> b.next));
    for (BulkScorer scorer : scorers) {
      this.scorers.add(new BulkScorerAndNext(scorer));
    }
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    BulkScorerAndNext top = scorers.top();

    while (top.next < max) {
      windowMin = Math.max(top.next, min);
      final int windowMax = MathUtil.unsignedMin(max, windowMin + WINDOW_SIZE);

      // First compute matches / scores in the window
      do {
        top.next = top.scorer.score(innerCollector, acceptDocs, windowMin, windowMax);
        top = scorers.updateTop();
      } while (top.next < windowMax);

      // Then replay, resetting windowScores entries inline to avoid a full Arrays.fill
      collector.setScorer(topLevelScorable);
      for (int windowDoc = windowMatches.nextSetBit(0);
          windowDoc != DocIdSetIterator.NO_MORE_DOCS;
          windowDoc = windowMatches.nextSetBit(windowDoc + 1)) {
        topLevelScorable.score = windowScores[windowDoc];
        windowScores[windowDoc] = 0f;
        collector.collect(windowMin + windowDoc);
      }

      // Only the bitset needs clearing; windowScores entries were reset during replay above.
      windowMatches.clear();
    }

    return top.next;
  }

  @Override
  public long cost() {
    long cost = 0;
    for (BulkScorerAndNext scorer : scorers) {
      cost += scorer.scorer.cost();
    }
    return cost;
  }
}
