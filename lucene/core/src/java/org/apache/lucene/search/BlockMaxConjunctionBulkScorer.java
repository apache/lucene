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
import org.apache.lucene.util.MathUtil;

/**
 * BulkScorer implementation of {@link BlockMaxConjunctionScorer} that focuses on top-level
 * conjunctions. It is similar to BS1 on disjunctions in that it moves evaluation from doc-at-a-time
 * to window-at-a-time. This approach tends to be more friendly to CPU caches and the JVM C2
 * compiler, and usually runs significantly faster than {@link BlockMaxConjunctionScorer}. This
 * scorer has two kinds of windows: outer windows, which are defined by impact boundaries, and inner
 * windows, which are the number of documents that get evaluated at once.
 */
final class BlockMaxConjunctionBulkScorer extends BulkScorer {

  /**
   * Size of an inner window, expressed as a number of matches of the lead clause, ie. the one which
   * has the least number of matches. Higher values of this window size tend to make evaluation more
   * CPU-friendly, but reduce chances of skipping large ranges of doc IDs thanks for other clauses.
   */
  private static final int INNER_WINDOW_SIZE = 64;

  private final Scorer[] scorers;
  private final DocIdSetIterator[] approximations;
  private final TwoPhaseIterator[] twoPhases;
  private final DocIdSetIterator leadApproximation;
  private final DocAndScore scorable = new DocAndScore();
  private final double[] sumOfOtherClauseScores;
  private final int maxDoc;

  private final int[] candidateMatches = new int[INNER_WINDOW_SIZE];
  private final double[] candidateScores = new double[INNER_WINDOW_SIZE];

  BlockMaxConjunctionBulkScorer(List<Scorer> scorers, int maxDoc) throws IOException {
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException("Expected 2 or more scorers, got " + scorers.size());
    }
    this.scorers = scorers.toArray(Scorer[]::new);
    this.approximations = new DocIdSetIterator[this.scorers.length];
    this.twoPhases = new TwoPhaseIterator[this.scorers.length];
    Arrays.sort(this.scorers, Comparator.comparingLong(scorer -> scorer.iterator().cost()));
    for (int i = 0; i < this.scorers.length; ++i) {
      twoPhases[i] = this.scorers[i].twoPhaseIterator();
      if (twoPhases[i] == null) {
        approximations[i] = this.scorers[i].iterator();
      } else {
        approximations[i] = twoPhases[i].approximation();
      }
    }
    this.leadApproximation = approximations[0];
    this.sumOfOtherClauseScores = new double[this.scorers.length];
    this.maxDoc = maxDoc;
  }

  private boolean invariant() {
    for (int i = 1; i < approximations.length; ++i) {
      assert approximations[i].docID() <= approximations[0].docID();
    }
    return true;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    int windowMin = Math.max(leadApproximation.docID(), min);
    while (windowMin < max) {
      // Use impacts of the least costly scorer to compute windows
      int windowMax = Math.min(scorers[0].advanceShallow(windowMin), max - 1);
      if (windowMax != Integer.MAX_VALUE) {
        windowMax++; // advanceShallow is inclusive
      }
      for (int i = 1; i < scorers.length; ++i) {
        scorers[i].advanceShallow(windowMin);
      }

      for (int i = 0; i < scorers.length; ++i) {
        sumOfOtherClauseScores[i] = scorers[i].getMaxScore(windowMax);
      }
      double maxWindowScore = 0;
      for (double maxScore : sumOfOtherClauseScores) {
        maxWindowScore += maxScore;
      }
      for (int i = sumOfOtherClauseScores.length - 2; i >= 0; --i) {
        sumOfOtherClauseScores[i] += sumOfOtherClauseScores[i + 1];
      }

      scoreOuterWindow(collector, acceptDocs, windowMin, windowMax, (float) maxWindowScore);

      windowMin = Math.max(leadApproximation.docID(), windowMax);
    }

    if (windowMin >= maxDoc) {
      windowMin = DocIdSetIterator.NO_MORE_DOCS;
    }

    return windowMin;
  }

  private void scoreOuterWindow(
      LeafCollector collector, Bits acceptDocs, int min, int max, float maxWindowScore)
      throws IOException {
    if (maxWindowScore < scorable.minCompetitiveScore) {
      // no hits are competitive
      return;
    }

    if (leadApproximation.docID() < min) {
      leadApproximation.advance(min);
    }

    while (leadApproximation.docID() < max) {
      scoreInnerWindow(collector, acceptDocs, max);
    }
  }

  private void scoreInnerWindow(LeafCollector collector, Bits acceptDocs, int max)
      throws IOException {
    assert invariant();

    int numMatches = 0;
    final TwoPhaseIterator leadTwoPhase = twoPhases[0];
    final Scorer leadScorer = scorers[0];
    for (int doc = leadApproximation.docID();
        doc < max && numMatches < INNER_WINDOW_SIZE;
        doc = leadApproximation.nextDoc()) {
      if ((acceptDocs == null || acceptDocs.get(doc))
          && (leadTwoPhase == null || leadTwoPhase.matches())) {
        candidateMatches[numMatches] = doc;
        candidateScores[numMatches] = leadScorer.score();
        ++numMatches;
      }
    }

    int nextCandidate = leadApproximation.docID();

    for (int i = 1; i < scorers.length; ++i) {

      final DocIdSetIterator approximation = approximations[i];
      final TwoPhaseIterator twoPhase = twoPhases[i];
      final Scorer scorer = scorers[i];
      final double sumOfOtherClauses = this.sumOfOtherClauseScores[i];

      int previousNumMatches = numMatches;
      numMatches = 0;
      for (int j = 0; j < previousNumMatches; ++j) {

        final double candidateScore = candidateScores[j];

        if (MathUtil.sumUpperBound(candidateScore + sumOfOtherClauses, scorers.length)
            < scorable.minCompetitiveScore) {
          // Match can't be competitive anyway, skip advancing
          continue;
        }

        final int candidateDoc = candidateMatches[j];

        int iteratorDoc = approximation.docID();
        if (iteratorDoc < candidateDoc) {
          iteratorDoc = approximation.advance(candidateDoc);
        }

        if (iteratorDoc == candidateDoc && (twoPhase == null || twoPhase.matches())) {
          candidateMatches[numMatches] = candidateDoc;
          candidateScores[numMatches] = candidateScore + scorer.score();
          numMatches++;
        }
      }

      nextCandidate = Math.max(nextCandidate, approximation.docID());
    }

    for (int i = 0; i < numMatches; ++i) {
      scorable.score = (float) candidateScores[i];
      collector.collect(candidateMatches[i]);
    }

    if (leadApproximation.docID() < nextCandidate) {
      leadApproximation.advance(nextCandidate);
    }
    assert invariant();
  }

  @Override
  public long cost() {
    return leadApproximation.cost();
  }

  private static class DocAndScore extends Scorable {

    float score;
    float minCompetitiveScore;

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
