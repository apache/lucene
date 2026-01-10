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

  private static final int MAX_WINDOW_SIZE = 65536;

  private final Scorer[] scorers;
  private final Scorable[] scorables;
  private final DocIdSetIterator[] iterators;
  private final DocIdSetIterator lead;
  private final SimpleScorable scorable = new SimpleScorable();
  private final double[] sumOfOtherClauses;
  private final int maxDoc;
  private final DocAndFloatFeatureBuffer docAndScoreBuffer = new DocAndFloatFeatureBuffer();
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
        Arrays.stream(this.scorers)
            .map(Scorer::iterator)
            .map(ScorerUtil::likelyImpactsEnum)
            .toArray(DocIdSetIterator[]::new);
    lead = iterators[0];
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
    if (scorable.minCompetitiveScore == 0) {
      windowMin = scoreDocFirstUntilDynamicPruning(collector, acceptDocs, min, max);
    }

    while (windowMin < max) {
      // Use impacts of the least costly scorer to compute windows
      // NOTE: windowMax is inclusive
      int windowMax = Math.min(scorers[0].advanceShallow(windowMin), max - 1);
      // Ensure the scoring window not too big, this especially works for the default implementation
      // of `Scorer#advanceShallow` which may return `DocIdSetIterator#NO_MORE_DOCS`.
      windowMax = MathUtil.unsignedMin(windowMax, windowMin + MAX_WINDOW_SIZE);

      float maxWindowScore = computeMaxScore(windowMin, windowMax);
      scoreWindowScoreFirst(collector, acceptDocs, windowMin, windowMax + 1, maxWindowScore);
      windowMin = Math.max(lead.docID(), windowMax + 1);
    }

    return windowMin >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : windowMin;
  }

  /**
   * Score a window of doc IDs by first finding agreement between all iterators and only then
   * compute scores and call the collector until dynamic pruning kicks in.
   */
  private int scoreDocFirstUntilDynamicPruning(
      LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int doc = lead.docID();
    if (doc < min) {
      doc = lead.advance(min);
    }

    outer:
    while (doc < max) {
      if (acceptDocs == null || acceptDocs.get(doc)) {
        for (int i = 1; i < iterators.length; ++i) {
          DocIdSetIterator iterator = iterators[i];
          int otherDoc = iterator.docID();
          if (otherDoc < doc) {
            otherDoc = iterator.advance(doc);
          }
          if (doc != otherDoc) {
            doc = lead.advance(otherDoc);
            continue outer;
          }
        }

        double score = 0;
        for (Scorable scorable : scorables) {
          score += scorable.score();
        }
        scorable.score = (float) score;
        collector.collect(doc);
        if (scorable.minCompetitiveScore > 0) {
          return lead.nextDoc();
        }
      }
      doc = lead.nextDoc();
    }
    return doc;
  }

  /**
   * Score a window of doc IDs by computing matches and scores on the lead costly clause, then
   * iterate other clauses one by one to remove documents that do not match and increase the global
   * score by the score of the current clause. This is often faster when a minimum competitive score
   * is set, as score computations can be more efficient (e.g. thanks to vectorization) and because
   * we can skip advancing other clauses if the global score so far is not high enough for a doc to
   * have a chance of being competitive.
   */
  private void scoreWindowScoreFirst(
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
        double sumOfOtherClause = sumOfOtherClauses[i];
        if (sumOfOtherClause != sumOfOtherClauses[i - 1]) {
          // two equal consecutive values mean that the first clause always returns a score of zero,
          // so we don't need to filter hits by score again.
          ScorerUtil.filterCompetitiveHits(
              docAndScoreAccBuffer, sumOfOtherClause, scorable.minCompetitiveScore, scorers.length);
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
}
