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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;

final class MaxScoreBulkScorer extends BulkScorer {

  static final int INNER_WINDOW_SIZE = 1 << 11;

  private final int maxDoc;
  // All scorers, sorted by increasing max score.
  final DisiWrapper[] allScorers;
  private final DisiWrapper[] scratch;
  // These are the last scorers from `allScorers` that are "essential", ie. required for a match to
  // have a competitive score.
  private final DisiPriorityQueue essentialQueue;
  // Index of the first essential scorer, ie. essentialQueue contains all scorers from
  // allScorers[firstEssentialScorer:]. All scorers below this index are non-essential.
  int firstEssentialScorer;
  // Index of the first scorer that is required, this scorer and all following scorers are required
  // for a document to match.
  int firstRequiredScorer;
  private final long cost;
  float minCompetitiveScore;
  private ScoreAndDoc scorable = new ScoreAndDoc();
  final double[] maxScoreSums;

  private final long[] windowMatches = new long[FixedBitSet.bits2words(INNER_WINDOW_SIZE)];
  private final double[] windowScores = new double[INNER_WINDOW_SIZE];

  MaxScoreBulkScorer(int maxDoc, List<Scorer> scorers) throws IOException {
    this.maxDoc = maxDoc;
    allScorers = new DisiWrapper[scorers.size()];
    scratch = new DisiWrapper[allScorers.length];
    int i = 0;
    long cost = 0;
    for (Scorer scorer : scorers) {
      DisiWrapper w = new DisiWrapper(scorer);
      cost += w.cost;
      allScorers[i++] = w;
    }
    this.cost = cost;
    essentialQueue = new DisiPriorityQueue(allScorers.length);
    maxScoreSums = new double[allScorers.length];
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    // This scorer computes outer windows based on impacts that are stored in the index. These outer
    // windows should be small enough to provide good upper bounds of scores, and big enough to make
    // sure we spend more time collecting docs than recomputing windows.
    // Then within these outer windows, it creates inner windows of size WINDOW_SIZE that help
    // collect matches into a bitset and save the overhead of rebalancing the priority queue on
    // every match.
    int outerWindowMin = min;
    outer:
    while (outerWindowMin < max) {
      int outerWindowMax = computeOuterWindowMax(outerWindowMin);
      outerWindowMax = Math.min(outerWindowMax, max);

      while (true) {
        updateMaxWindowScores(outerWindowMin, outerWindowMax);
        if (partitionScorers() == false) {
          // No matches in this window
          outerWindowMin = outerWindowMax;
          continue outer;
        }

        // There is a dependency between windows and maximum scores, as we compute windows based on
        // maximum scores and maximum scores based on windows.
        // So the approach consists of starting by computing a window based on the set of essential
        // scorers from the _previous_ window and then iteratively recompute maximum scores and
        // windows as long as the window size decreases.
        // In general the set of essential scorers is rather stable over time so this would exit
        // after a single iteration, but there is a change that some scorers got swapped between the
        // set of essential and non-essential scorers, in which case there may be multiple
        // iterations of this loop.

        int newOuterWindowMax = computeOuterWindowMax(outerWindowMin);
        if (newOuterWindowMax >= outerWindowMax) {
          break;
        }
        outerWindowMax = newOuterWindowMax;
      }

      DisiWrapper top = essentialQueue.top();
      while (top.doc < outerWindowMin) {
        top.doc = top.iterator.advance(outerWindowMin);
        top = essentialQueue.updateTop();
      }

      while (top.doc < outerWindowMax) {
        scoreInnerWindow(collector, acceptDocs, outerWindowMax);
        top = essentialQueue.top();
      }

      outerWindowMin = outerWindowMax;
    }

    return nextCandidate(max);
  }

  private void scoreInnerWindow(LeafCollector collector, Bits acceptDocs, int max)
      throws IOException {
    if (allScorers.length - firstRequiredScorer >= 2) {
      scoreInnerWindowAsConjunction(collector, acceptDocs, max);
    } else {
      DisiWrapper top = essentialQueue.top();
      DisiWrapper top2 = essentialQueue.top2();
      if (top2 == null) {
        scoreInnerWindowSingleEssentialClause(collector, acceptDocs, max);
      } else if (top2.doc - INNER_WINDOW_SIZE / 2 >= top.doc) {
        // The first half of the window would match a single clause. Let's collect this single
        // clause until the next doc ID of the next clause.
        scoreInnerWindowSingleEssentialClause(collector, acceptDocs, Math.min(max, top2.doc));
      } else {
        scoreInnerWindowMultipleEssentialClauses(collector, acceptDocs, max);
      }
    }
  }

  private void scoreInnerWindowSingleEssentialClause(
      LeafCollector collector, Bits acceptDocs, int upTo) throws IOException {
    DisiWrapper top = essentialQueue.top();

    // single essential clause in this window, we can iterate it directly and skip the bitset.
    // this is a common case for 2-clauses queries
    for (int doc = top.doc; doc < upTo; doc = top.iterator.nextDoc()) {
      if (acceptDocs != null && acceptDocs.get(doc) == false) {
        continue;
      }
      scoreNonEssentialClauses(collector, doc, top.scorer.score(), firstEssentialScorer);
    }
    top.doc = top.iterator.docID();
    essentialQueue.updateTop();
  }

  private void scoreInnerWindowAsConjunction(LeafCollector collector, Bits acceptDocs, int max)
      throws IOException {
    assert firstEssentialScorer == allScorers.length - 1;
    assert firstRequiredScorer <= allScorers.length - 2;
    DisiWrapper lead1 = allScorers[allScorers.length - 1];
    assert essentialQueue.size() == 1;
    assert lead1 == essentialQueue.top();
    DisiWrapper lead2 = allScorers[allScorers.length - 2];
    if (lead1.doc < lead2.doc) {
      lead1.doc = lead1.iterator.advance(Math.min(lead2.doc, max));
    }
    // maximum score contribution of all scorers but the lead
    double maxScoreSumAtLead2 = maxScoreSums[allScorers.length - 2];

    outer:
    while (lead1.doc < max) {

      if (acceptDocs != null && acceptDocs.get(lead1.doc) == false) {
        lead1.doc = lead1.iterator.nextDoc();
        continue;
      }

      double score = lead1.scorer.score();

      // We specialize handling the second best scorer, which seems to help a bit with performance.
      // But this is the exact same logic as in the below for loop.
      if ((float) MathUtil.sumUpperBound(score + maxScoreSumAtLead2, allScorers.length)
          < minCompetitiveScore) {
        // a competitive match is not possible according to max scores, skip to the next candidate
        lead1.doc = lead1.iterator.nextDoc();
        continue;
      }

      if (lead2.doc < lead1.doc) {
        lead2.doc = lead2.iterator.advance(lead1.doc);
      }
      if (lead2.doc != lead1.doc) {
        lead1.doc = lead1.iterator.advance(Math.min(lead2.doc, max));
        continue;
      }

      score += lead2.scorer.score();

      for (int i = allScorers.length - 3; i >= firstRequiredScorer; --i) {
        if ((float) MathUtil.sumUpperBound(score + maxScoreSums[i], allScorers.length)
            < minCompetitiveScore) {
          // a competitive match is not possible according to max scores, skip to the next candidate
          lead1.doc = lead1.iterator.nextDoc();
          continue outer;
        }

        DisiWrapper w = allScorers[i];
        if (w.doc < lead1.doc) {
          w.doc = w.iterator.advance(lead1.doc);
        }
        if (w.doc != lead1.doc) {
          lead1.doc = lead1.iterator.advance(Math.min(w.doc, max));
          continue outer;
        }
        score += w.scorer.score();
      }

      scoreNonEssentialClauses(collector, lead1.doc, score, firstRequiredScorer);
      lead1.doc = lead1.iterator.nextDoc();
    }
  }

  private void scoreInnerWindowMultipleEssentialClauses(
      LeafCollector collector, Bits acceptDocs, int max) throws IOException {
    DisiWrapper top = essentialQueue.top();

    int innerWindowMin = top.doc;
    int innerWindowMax = (int) Math.min(max, (long) innerWindowMin + INNER_WINDOW_SIZE);

    // Collect matches of essential clauses into a bitset
    do {
      for (int doc = top.doc; doc < innerWindowMax; doc = top.iterator.nextDoc()) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          final int i = doc - innerWindowMin;
          windowMatches[i >>> 6] |= 1L << i;
          windowScores[i] += top.scorer.score();
        }
      }
      top.doc = top.iterator.docID();
      top = essentialQueue.updateTop();
    } while (top.doc < innerWindowMax);

    for (int wordIndex = 0; wordIndex < windowMatches.length; ++wordIndex) {
      long bits = windowMatches[wordIndex];
      windowMatches[wordIndex] = 0L;
      while (bits != 0L) {
        int ntz = Long.numberOfTrailingZeros(bits);
        bits ^= 1L << ntz;
        int index = wordIndex << 6 | ntz;
        int doc = innerWindowMin + index;
        double score = windowScores[index];
        windowScores[index] = 0d;

        scoreNonEssentialClauses(collector, doc, score, firstEssentialScorer);
      }
    }
  }

  private int computeOuterWindowMax(int windowMin) throws IOException {
    // Only use essential scorers to compute the window's max doc ID, in order to avoid constantly
    // recomputing max scores over small windows
    final int firstWindowLead = Math.min(firstEssentialScorer, allScorers.length - 1);
    int windowMax = DocIdSetIterator.NO_MORE_DOCS;
    for (int i = firstWindowLead; i < allScorers.length; ++i) {
      final DisiWrapper scorer = allScorers[i];
      final int upTo = scorer.scorer.advanceShallow(Math.max(scorer.doc, windowMin));
      windowMax = (int) Math.min(windowMax, upTo + 1L); // upTo is inclusive
    }

    // Score at least an entire inner window of docs
    windowMax =
        Math.max(
            windowMax, (int) Math.min(Integer.MAX_VALUE, (long) windowMin + INNER_WINDOW_SIZE));

    return windowMax;
  }

  void updateMaxWindowScores(int windowMin, int windowMax) throws IOException {
    for (DisiWrapper scorer : allScorers) {
      if (scorer.doc < windowMax) {
        if (scorer.doc < windowMin) {
          // Make sure to advance shallow if necessary to get as good score upper bounds as
          // possible.
          scorer.scorer.advanceShallow(windowMin);
        }
        scorer.maxWindowScore = scorer.scorer.getMaxScore(windowMax - 1);
      } else {
        // This scorer has no documents in the considered window.
        scorer.maxWindowScore = 0;
      }
    }
  }

  private void scoreNonEssentialClauses(
      LeafCollector collector, int doc, double essentialScore, int numNonEssentialClauses)
      throws IOException {
    double score = essentialScore;
    for (int i = numNonEssentialClauses - 1; i >= 0; --i) {
      float maxPossibleScore =
          (float) MathUtil.sumUpperBound(score + maxScoreSums[i], allScorers.length);
      if (maxPossibleScore < minCompetitiveScore) {
        // Hit is not competitive.
        return;
      }

      DisiWrapper scorer = allScorers[i];
      if (scorer.doc < doc) {
        scorer.doc = scorer.iterator.advance(doc);
      }
      if (scorer.doc == doc) {
        score += scorer.scorer.score();
      }
    }

    scorable.doc = doc;
    scorable.score = (float) score;
    collector.collect(doc);
  }

  boolean partitionScorers() {
    // Partitioning scorers is an optimization problem: the optimal set of non-essential scorers is
    // the subset of scorers whose sum of max window scores is less than the minimum competitive
    // score that maximizes the sum of costs.
    // Computing the optimal solution to this problem would take O(2^num_clauses). As a first
    // approximation, we take the first scorers sorted by max_window_score / cost whose sum of max
    // scores is less than the minimum competitive scores. In the common case, maximum scores are
    // inversely correlated with document frequency so this is the same as only sorting by maximum
    // score, as described in the MAXSCORE paper and gives the optimal solution. However, this can
    // make a difference when using custom scores (like FuzzyQuery), high query-time boosts, or
    // scoring based on wacky weights.
    System.arraycopy(allScorers, 0, scratch, 0, allScorers.length);
    Arrays.sort(
        scratch,
        Comparator.comparingDouble(
            scorer -> (double) scorer.maxWindowScore / Math.max(1L, scorer.cost)));
    double maxScoreSum = 0;
    firstEssentialScorer = 0;
    for (int i = 0; i < allScorers.length; ++i) {
      final DisiWrapper w = scratch[i];
      double newMaxScoreSum = maxScoreSum + w.maxWindowScore;
      float maxScoreSumFloat =
          (float) MathUtil.sumUpperBound(newMaxScoreSum, firstEssentialScorer + 1);
      if (maxScoreSumFloat < minCompetitiveScore) {
        maxScoreSum = newMaxScoreSum;
        allScorers[firstEssentialScorer] = w;
        maxScoreSums[firstEssentialScorer] = maxScoreSum;
        firstEssentialScorer++;
      } else {
        allScorers[allScorers.length - 1 - (i - firstEssentialScorer)] = w;
      }
    }

    firstRequiredScorer = allScorers.length;

    if (firstEssentialScorer == allScorers.length) {
      return false;
    }

    essentialQueue.clear();
    for (int i = firstEssentialScorer; i < allScorers.length; ++i) {
      essentialQueue.add(allScorers[i]);
    }

    if (firstEssentialScorer == allScorers.length - 1) { // single essential clause
      // If there is a single essential clause and matching it plus all non-essential clauses but
      // the best one is not enough to yield a competitive match, the we know that hits must match
      // both the essential clause and the best non-essential clause. Here are some examples when
      // this optimization would kick in:
      //   `quick fox`  when maxscore(quick) = 1, maxscore(fox) = 1, minCompetitiveScore = 1.5
      //   `the quick fox` when maxscore (the) = 0.1, maxscore(quick) = 1, maxscore(fox) = 1,
      //       minCompetitiveScore = 1.5
      firstRequiredScorer = allScorers.length - 1;
      double maxRequiredScore = allScorers[firstEssentialScorer].maxWindowScore;

      while (firstRequiredScorer > 0) {
        double maxPossibleScoreWithoutPreviousClause = maxRequiredScore;
        if (firstRequiredScorer > 1) {
          maxPossibleScoreWithoutPreviousClause += maxScoreSums[firstRequiredScorer - 2];
        }
        if ((float) maxPossibleScoreWithoutPreviousClause >= minCompetitiveScore) {
          break;
        }
        // The sum of maximum scores ignoring the previous clause is less than the minimum
        // competitive
        --firstRequiredScorer;
        maxRequiredScore += allScorers[firstRequiredScorer].maxWindowScore;
      }
    }

    return true;
  }

  /** Return the next candidate on or after {@code rangeEnd}. */
  private int nextCandidate(int rangeEnd) {
    if (rangeEnd >= maxDoc) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }

    int next = DocIdSetIterator.NO_MORE_DOCS;
    for (DisiWrapper scorer : allScorers) {
      if (scorer.doc < rangeEnd) {
        return rangeEnd;
      } else {
        next = Math.min(next, scorer.doc);
      }
    }
    return next;
  }

  @Override
  public long cost() {
    return cost;
  }

  private class ScoreAndDoc extends Scorable {

    float score;
    int doc = -1;

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() {
      return score;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      MaxScoreBulkScorer.this.minCompetitiveScore = minScore;
    }
  }
}
