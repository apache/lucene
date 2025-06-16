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
import java.util.Comparator;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;

/**
 * {@link BulkScorer} that is used for pure disjunctions and disjunctions that have low values of
 * {@link BooleanQuery.Builder#setMinimumNumberShouldMatch(int)} and dense clauses. This scorer
 * scores documents by batches of 4,096 docs.
 */
final class BooleanScorer extends BulkScorer {

  static final int SHIFT = 12;
  static final int SIZE = 1 << SHIFT;
  static final int MASK = SIZE - 1;

  static class Bucket {
    double score;
    int freq;
  }

  // One bucket per doc ID in the window, non-null if scores are needed or if frequencies need to be
  // counted
  final Bucket[] buckets;
  final FixedBitSet matching = new FixedBitSet(SIZE);

  final DisiWrapper[] leads;
  final PriorityQueue<DisiWrapper> head;
  final PriorityQueue<DisiWrapper> tail;
  final Score score = new Score();
  final int minShouldMatch;
  final long cost;
  final boolean needsScores;
  private final DocAndFloatFeatureBuffer docAndScoreBuffer = new DocAndFloatFeatureBuffer();

  BooleanScorer(Collection<Scorer> scorers, int minShouldMatch, boolean needsScores) {
    if (minShouldMatch < 1 || minShouldMatch > scorers.size()) {
      throw new IllegalArgumentException(
          "minShouldMatch should be within 1..num_scorers. Got " + minShouldMatch);
    }
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException(
          "This scorer can only be used with two scorers or more, got " + scorers.size());
    }
    if (needsScores || minShouldMatch > 1) {
      buckets = new Bucket[SIZE];
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] = new Bucket();
      }
    } else {
      buckets = null;
    }
    this.leads = new DisiWrapper[scorers.size()];
    this.head =
        PriorityQueue.usingComparator(
            scorers.size() - minShouldMatch + 1, Comparator.comparingInt(d -> d.doc));
    this.tail =
        PriorityQueue.usingComparator(minShouldMatch - 1, Comparator.comparingLong(d -> d.cost));
    this.minShouldMatch = minShouldMatch;
    this.needsScores = needsScores;
    LongArrayList costs = new LongArrayList(scorers.size());
    for (Scorer scorer : scorers) {
      DisiWrapper w = new DisiWrapper(scorer, false);
      costs.add(w.cost);
      final DisiWrapper evicted = tail.insertWithOverflow(w);
      if (evicted != null) {
        head.add(evicted);
      }
    }
    this.cost = ScorerUtil.costWithMinShouldMatch(costs.stream(), costs.size(), minShouldMatch);
  }

  @Override
  public long cost() {
    return cost;
  }

  private void scoreWindowIntoBitSetAndReplay(
      LeafCollector collector,
      Bits acceptDocs,
      int base,
      int min,
      int max,
      DisiWrapper[] scorers,
      int numScorers)
      throws IOException {
    for (int i = 0; i < numScorers; ++i) {
      final DisiWrapper w = scorers[i];
      assert w.doc < max;

      DocIdSetIterator it = w.iterator;
      if (w.doc < min) {
        it.advance(min);
      }
      if (buckets == null) { // means minShouldMatch=1 and scores are not needed
        // This doesn't apply live docs, so we'll need to apply them later
        it.intoBitSet(max, matching, base);
      } else if (needsScores) {
        for (w.scorer.nextDocsAndScores(max, acceptDocs, docAndScoreBuffer);
            docAndScoreBuffer.size > 0;
            w.scorer.nextDocsAndScores(max, acceptDocs, docAndScoreBuffer)) {
          for (int index = 0; index < docAndScoreBuffer.size; ++index) {
            final int doc = docAndScoreBuffer.docs[index];
            final float score = docAndScoreBuffer.features[index];
            final int d = doc & MASK;
            matching.set(d);
            final Bucket bucket = buckets[d];
            bucket.freq++;
            bucket.score += score;
          }
        }
      } else {
        // Scores are not needed but we need to keep track of freqs to know which hits match
        assert minShouldMatch > 1;
        for (int doc = it.docID(); doc < max; doc = it.nextDoc()) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            final int d = doc & MASK;
            matching.set(d);
            final Bucket bucket = buckets[d];
            bucket.freq++;
          }
        }
      }

      w.doc = it.docID();
    }

    if (buckets == null) {
      if (acceptDocs != null) {
        // In this case, live docs have not been applied yet.
        acceptDocs.applyMask(matching, base);
      }
      collector.collect(new BitSetDocIdStream(matching, base));
    } else {
      FixedBitSet matching = BooleanScorer.this.matching;
      Bucket[] buckets = BooleanScorer.this.buckets;
      long[] bitArray = matching.getBits();
      for (int idx = 0; idx < bitArray.length; idx++) {
        long bits = bitArray[idx];
        while (bits != 0L) {
          int ntz = Long.numberOfTrailingZeros(bits);
          final int indexInWindow = (idx << 6) | ntz;
          final Bucket bucket = buckets[indexInWindow];
          if (bucket.freq >= minShouldMatch) {
            score.score = (float) bucket.score;
            collector.collect(base | indexInWindow);
          }
          bucket.freq = 0;
          bucket.score = 0;
          bits ^= 1L << ntz;
        }
      }
    }

    matching.clear();
  }

  private DisiWrapper advance(int min) throws IOException {
    assert tail.size() == minShouldMatch - 1;
    final PriorityQueue<DisiWrapper> head = this.head;
    final PriorityQueue<DisiWrapper> tail = this.tail;
    DisiWrapper headTop = head.top();
    DisiWrapper tailTop = tail.top();
    while (headTop.doc < min) {
      if (tailTop == null || headTop.cost <= tailTop.cost) {
        headTop.doc = headTop.iterator.advance(min);
        headTop = head.updateTop();
      } else {
        // swap the top of head and tail
        final DisiWrapper previousHeadTop = headTop;
        tailTop.doc = tailTop.iterator.advance(min);
        headTop = head.updateTop(tailTop);
        tailTop = tail.updateTop(previousHeadTop);
      }
    }
    return headTop;
  }

  private void scoreWindowMultipleScorers(
      LeafCollector collector,
      Bits acceptDocs,
      int windowBase,
      int windowMin,
      int windowMax,
      int maxFreq)
      throws IOException {
    while (maxFreq < minShouldMatch && maxFreq + tail.size() >= minShouldMatch) {
      // a match is still possible
      final DisiWrapper candidate = tail.pop();
      if (candidate.doc < windowMin) {
        candidate.doc = candidate.iterator.advance(windowMin);
      }
      if (candidate.doc < windowMax) {
        leads[maxFreq++] = candidate;
      } else {
        head.add(candidate);
      }
    }

    if (maxFreq >= minShouldMatch) {
      // There might be matches in other scorers from the tail too
      for (DisiWrapper disiWrapper : tail) {
        leads[maxFreq++] = disiWrapper;
      }
      tail.clear();

      scoreWindowIntoBitSetAndReplay(
          collector, acceptDocs, windowBase, windowMin, windowMax, leads, maxFreq);
    }

    // Push back scorers into head and tail
    for (int i = 0; i < maxFreq; ++i) {
      final DisiWrapper evicted = head.insertWithOverflow(leads[i]);
      if (evicted != null) {
        tail.add(evicted);
      }
    }
  }

  private void scoreWindowSingleScorer(
      DisiWrapper w,
      LeafCollector collector,
      Bits acceptDocs,
      int windowMin,
      int windowMax,
      int max)
      throws IOException {
    assert tail.size() == 0;
    final int nextWindowBase = head.top().doc & ~MASK;
    final int end = Math.max(windowMax, Math.min(max, nextWindowBase));

    DocIdSetIterator it = w.iterator;
    int doc = w.doc;
    if (doc < windowMin) {
      doc = it.advance(windowMin);
    }
    collector.setScorer(w.scorer);
    for (; doc < end; doc = it.nextDoc()) {
      if (acceptDocs == null || acceptDocs.get(doc)) {
        collector.collect(doc);
      }
    }
    w.doc = doc;

    // reset the scorer that should be used for the general case
    collector.setScorer(score);
  }

  private DisiWrapper scoreWindow(
      DisiWrapper top, LeafCollector collector, Bits acceptDocs, int min, int max)
      throws IOException {
    final int windowBase = top.doc & ~MASK; // find the window that the next match belongs to
    final int windowMin = Math.max(min, windowBase);
    final int windowMax = Math.min(max, windowBase + SIZE);

    // Fill 'leads' with all scorers from 'head' that are in the right window
    leads[0] = head.pop();
    int maxFreq = 1;
    while (head.size() > 0 && head.top().doc < windowMax) {
      leads[maxFreq++] = head.pop();
    }

    if (minShouldMatch == 1 && maxFreq == 1) {
      // special case: only one scorer can match in the current window,
      // we can collect directly
      final DisiWrapper bulkScorer = leads[0];
      scoreWindowSingleScorer(bulkScorer, collector, acceptDocs, windowMin, windowMax, max);
      return head.add(bulkScorer);
    } else {
      // general case, collect through a bit set first and then replay
      scoreWindowMultipleScorers(collector, acceptDocs, windowBase, windowMin, windowMax, maxFreq);
      return head.top();
    }
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(score);

    DisiWrapper top = advance(min);
    while (top.doc < max) {
      top = scoreWindow(top, collector, acceptDocs, min, max);
    }

    return top.doc;
  }
}
