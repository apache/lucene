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
  private final Scorable[] scorables;
  private final DocIdSetIterator[] iterators;
  private final DocIdSetIterator lead;
  private final DocAndScore scorable = new DocAndScore();
  private final double[] sumOfOtherClauses;
  private final int maxDoc;
  private final DocAndScoreBuffer docAndScoreBuffer = new DocAndScoreBuffer();
  private final DocAndScoreAccBuffer docAndScoreAccBuffer = new DocAndScoreAccBuffer();

  BlockMaxConjunctionBulkScorer(int maxDoc, List<Scorer> scorers) throws IOException {
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException("Expected 2 or more scorers, got " + scorers.size());
    }
    this.scorers = scorers.toArray(Scorer[]::new);
    Arrays.sort(this.scorers, Comparator.comparingLong(scorer -> scorer.iterator().cost()));
    this.scorables =
        Arrays.stream(this.scorers).map(ScorerUtil::likelyTermScorer).toArray(Scorable[]::new);
    this.iterators =
        Arrays.stream(this.scorers).map(Scorer::iterator).toArray(DocIdSetIterator[]::new);
    lead = ScorerUtil.likelyImpactsEnum(iterators[0]);
    this.sumOfOtherClauses = new double[this.scorers.length];
    Arrays.fill(sumOfOtherClauses, Double.POSITIVE_INFINITY);
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

    int windowMin = Math.max(lead.docID(), min);
    while (windowMin < max) {
      // Use impacts of the least costly scorer to compute windows
      // NOTE: windowMax is inclusive
      int windowMax = Math.min(scorers[0].advanceShallow(windowMin), max - 1);

      float maxWindowScore = Float.POSITIVE_INFINITY;
      if (0 < scorable.minCompetitiveScore) {
        maxWindowScore = computeMaxScore(windowMin, windowMax);
      }
      scoreWindow(collector, acceptDocs, windowMin, windowMax + 1, maxWindowScore);
      windowMin = Math.max(lead.docID(), windowMax + 1);
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

    if (lead.docID() < min) {
      lead.advance(min);
    }
    if (lead.docID() >= max) {
      return;
    }

    for (scorers[0].nextDocsAndScores(max, acceptDocs, docAndScoreBuffer);
        docAndScoreBuffer.size > 0;
        scorers[0].nextDocsAndScores(max, acceptDocs, docAndScoreBuffer)) {

      docAndScoreAccBuffer.copyFrom(docAndScoreBuffer);

      for (int i = 1; i < scorers.length; ++i) {
        if (scorable.minCompetitiveScore > 0) {
          ScorerUtil.filterCompetitiveHits(
              docAndScoreAccBuffer,
              sumOfOtherClauses[i],
              scorable.minCompetitiveScore,
              scorers.length);
        }

        ScorerUtil.applyRequiredClause(docAndScoreAccBuffer, iterators[i], scorables[i]);
      }

      for (int i = 0; i < docAndScoreAccBuffer.size; ++i) {
        scorable.score = (float) docAndScoreAccBuffer.scores[i];
        collector.collect(docAndScoreAccBuffer.docs[i]);
      }
    }

    int maxOtherDoc = -1;
    for (int i = 1; i < iterators.length; ++i) {
      maxOtherDoc = Math.max(iterators[i].docID(), maxOtherDoc);
    }
    if (lead.docID() < maxOtherDoc) {
      lead.advance(maxOtherDoc);
    }
  }

  @Override
  public long cost() {
    return lead.cost();
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
