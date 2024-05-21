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
import org.apache.lucene.search.Weight.DefaultBulkScorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MathUtil;

/**
 * BulkScorer implementation of {@link BlockMaxConjunctionScorer} that focuses on top-level
 * conjunctions over clauses that do not have two-phase iterators. Use a {@link DefaultBulkScorer}
 * around a {@link BlockMaxConjunctionScorer} if you need two-phase support. Another difference with
 * {@link BlockMaxConjunctionScorer} is that this scorer computes scores on the fly in order to be
 * able to skip evaluating more clauses if the total score would be under the minimum competitive
 * score anyway. This generally works well because computing a score is cheaper than decoding a
 * block of postings.
 */
final class BlockMaxConjunctionBulkScorer extends BulkScorer {

  private final Scorer[] scorers;
  private final DocIdSetIterator[] iterators;
  private final DocIdSetIterator lead1, lead2;
  private final Scorer scorer1, scorer2;
  private final DocAndScore scorable = new DocAndScore();
  private final double[] sumOfOtherClauses;
  private final int maxDoc;

  BlockMaxConjunctionBulkScorer(int maxDoc, List<Scorer> scorers) throws IOException {
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException("Expected 2 or more scorers, got " + scorers.size());
    }
    this.scorers = scorers.toArray(Scorer[]::new);
    Arrays.sort(this.scorers, Comparator.comparingLong(scorer -> scorer.iterator().cost()));
    this.iterators =
        Arrays.stream(this.scorers).map(Scorer::iterator).toArray(DocIdSetIterator[]::new);
    lead1 = iterators[0];
    lead2 = iterators[1];
    scorer1 = this.scorers[0];
    scorer2 = this.scorers[1];
    this.sumOfOtherClauses = new double[this.scorers.length];
    for (int i = 0; i < sumOfOtherClauses.length; i++) {
      sumOfOtherClauses[i] = Double.POSITIVE_INFINITY;
    }
    this.maxDoc = maxDoc;
  }

  private float computeMaxScore(int windowMin, int windowMax) throws IOException {
    for (int i = 0; i < scorers.length; ++i) {
      scorers[i].advanceShallow(windowMin);
    }

    double maxWindowScore = 0;
    for (int i = 0; i < scorers.length; ++i) {
      float maxClauseScore = scorers[i].getMaxScore(windowMax);
      sumOfOtherClauses[i] = maxClauseScore;
      maxWindowScore += maxClauseScore;
    }
    for (int i = sumOfOtherClauses.length - 2; i >= 0; --i) {
      sumOfOtherClauses[i] += sumOfOtherClauses[i + 1];
    }
    return (float) maxWindowScore;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    int windowMin = Math.max(lead1.docID(), min);
    while (windowMin < max) {
      // Use impacts of the least costly scorer to compute windows
      // NOTE: windowMax is inclusive
      int windowMax = Math.min(scorers[0].advanceShallow(windowMin), max - 1);

      float maxWindowScore = Float.POSITIVE_INFINITY;
      if (0 < scorable.minCompetitiveScore) {
        maxWindowScore = computeMaxScore(windowMin, windowMax);
      }
      scoreWindow(collector, acceptDocs, windowMin, windowMax + 1, maxWindowScore);
      windowMin = Math.max(lead1.docID(), windowMax + 1);
    }

    return windowMin >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : windowMin;
  }

  private void scoreWindow(
      LeafCollector collector, Bits acceptDocs, int min, int max, float maxWindowScore)
      throws IOException {
    if (maxWindowScore < scorable.minCompetitiveScore) {
      // no hits are competitive
      return;
    }

    if (lead1.docID() < min) {
      lead1.advance(min);
    }

    final double sumOfOtherMaxScoresAt1 = sumOfOtherClauses[1];

    advanceHead:
    for (int doc = lead1.docID(); doc < max; ) {
      if (acceptDocs != null && acceptDocs.get(doc) == false) {
        doc = lead1.nextDoc();
        continue;
      }

      // Compute the score as we find more matching clauses, in order to skip advancing other
      // clauses if the total score has no chance of being competitive. This works well because
      // computing a score is usually cheaper than decoding a full block of postings and
      // frequencies.
      final boolean hasMinCompetitiveScore = scorable.minCompetitiveScore > 0;
      double currentScore;
      if (hasMinCompetitiveScore) {
        currentScore = scorer1.score();
      } else {
        currentScore = 0;
      }

      // This is the same logic as in the below for loop, specialized for the 2nd least costly
      // clause. This seems to help the JVM.

      // First check if we have a chance of having a match based on max scores
      if (hasMinCompetitiveScore
          && (float) MathUtil.sumUpperBound(currentScore + sumOfOtherMaxScoresAt1, scorers.length)
              < scorable.minCompetitiveScore) {
        doc = lead1.nextDoc();
        continue advanceHead;
      }

      // NOTE: lead2 may be on `doc` already if we `continue`d on the previous loop iteration.
      if (lead2.docID() < doc) {
        int next = lead2.advance(doc);
        if (next != doc) {
          doc = lead1.advance(next);
          continue advanceHead;
        }
      }
      assert lead2.docID() == doc;
      if (hasMinCompetitiveScore) {
        currentScore += scorer2.score();
      }

      for (int i = 2; i < iterators.length; ++i) {
        // First check if we have a chance of having a match based on max scores
        if (hasMinCompetitiveScore
            && (float) MathUtil.sumUpperBound(currentScore + sumOfOtherClauses[i], scorers.length)
                < scorable.minCompetitiveScore) {
          doc = lead1.nextDoc();
          continue advanceHead;
        }

        // NOTE: these iterators may be on `doc` already if we called `continue advanceHead` on the
        // previous loop iteration.
        if (iterators[i].docID() < doc) {
          int next = iterators[i].advance(doc);
          if (next != doc) {
            doc = lead1.advance(next);
            continue advanceHead;
          }
        }
        assert iterators[i].docID() == doc;
        if (hasMinCompetitiveScore) {
          currentScore += scorers[i].score();
        }
      }

      if (hasMinCompetitiveScore == false) {
        for (Scorer scorer : scorers) {
          currentScore += scorer.score();
        }
      }
      scorable.doc = doc;
      scorable.score = (float) currentScore;
      collector.collect(doc);
      // The collect() call may have updated the minimum competitive score.
      if (maxWindowScore < scorable.minCompetitiveScore) {
        // no more hits are competitive
        return;
      }

      doc = lead1.nextDoc();
    }
  }

  @Override
  public long cost() {
    return lead1.cost();
  }

  private static class DocAndScore extends Scorable {

    int doc = -1;
    float score;
    float minCompetitiveScore;

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      this.minCompetitiveScore = minScore;
    }
  }
}
