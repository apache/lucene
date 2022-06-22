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

import static org.apache.lucene.search.ScorerUtil.costWithMinShouldMatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/** Scorer implementing Block-Max Maxscore algorithm */
public class BlockMaxMaxscoreScorer extends Scorer {
  private final ScoreMode scoreMode;
  private final int scalingFactor;

  // current doc ID of the leads
  private int doc;

  // doc id boundary that all scorers maxScore are valid
  private int upTo = -1;

  // heap of scorers ordered by doc ID
  private final DisiPriorityQueue essentialsScorers;
  // list of scorers ordered by maxScore
  private final List<DisiWrapper> maxScoreSortedEssentialScorers;

  // list of scorers whose sum of maxScore is less than minCompetitiveScore, ordered by maxScore
  private final List<DisiWrapper> nonEssentialScorers;

  private final DisiWrapper[] allScorers;

  // sum of max scores of scorers in nonEssentialScorers list
  private long nonEssentialMaxScoreSum;

  private long cost;

  private final MaxScoreSumPropagator maxScoreSumPropagator;

  // scaled min competitive score
  private long minCompetitiveScore = 0;

  /**
   * Constructs a Scorer
   *
   * @param weight The weight to be used.
   * @param scorers The sub scorers this Scorer should iterate on for optional clauses
   * @param scoreMode The scoreMode
   */
  public BlockMaxMaxscoreScorer(Weight weight, List<Scorer> scorers, ScoreMode scoreMode)
      throws IOException {
    super(weight);
    assert scoreMode == ScoreMode.TOP_SCORES;

    this.scoreMode = scoreMode;
    this.doc = -1;
    this.cost =
        costWithMinShouldMatch(
            scorers.stream().map(Scorer::iterator).mapToLong(DocIdSetIterator::cost),
            scorers.size(),
            1);

    essentialsScorers = new DisiPriorityQueue(scorers.size());
    maxScoreSortedEssentialScorers = new LinkedList<>();
    nonEssentialScorers = new ArrayList<>(scorers.size());

    // this logic copied the one used in WANDScorer - may need to be extracted out from WANDScorer
    double maxScoreSumDouble = 0;
    for (Scorer scorer : scorers) {
      scorer.advanceShallow(0);
      float maxScore = scorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS);
      maxScoreSumDouble += maxScore;
    }
    maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);
    final float maxScoreSum = maxScoreSumPropagator.scoreSumUpperBound(maxScoreSumDouble);
    scalingFactor = WANDScorer.scalingFactor(maxScoreSum);

    allScorers = scorers.stream().map(DisiWrapper::new).toArray(DisiWrapper[]::new);
    Collections.addAll(nonEssentialScorers, allScorers);
  }

  @Override
  public DocIdSetIterator iterator() {
    // twoPhaseIterator needed to honor scorer.setMinCompetitiveScore guarantee
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    DocIdSetIterator approximation =
        new DocIdSetIterator() {

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }

          @Override
          public int advance(int target) throws IOException {
            while (true) {

              if (target > upTo) {
                updateMaxScoresAndLists(target);
              } else {
                // minCompetitiveScore might have increased,
                // move potentially no-longer-competitive scorers from essential to non-essential
                // list
                movePotentiallyNonCompetitiveScorers();
              }

              assert target <= upTo;

              DisiWrapper top = essentialsScorers.top();

              if (top == null) {
                // all scorers in non-essential list, skip to next boundary or return no_more_docs
                if (upTo == NO_MORE_DOCS) {
                  return doc = NO_MORE_DOCS;
                } else {
                  target = upTo + 1;
                }
              } else {
                // position all scorers in essential list to on or after target
                while (top.doc < target) {
                  top.doc = top.iterator.advance(target);
                  top = essentialsScorers.updateTop();
                }

                if (top.doc == NO_MORE_DOCS) {
                  return doc = NO_MORE_DOCS;
                } else if (top.doc > upTo) {
                  target = upTo + 1;
                } else {
                  long matchedMaxScoreSum = nonEssentialMaxScoreSum;

                  for (DisiWrapper w : nonEssentialScorers) {
                    if (w.doc > top.doc) {
                      // this scorer won't be able to contribute score on top.doc, hence taking off
                      // maxScore
                      matchedMaxScoreSum -= w.maxScore;
                    }
                  }

                  for (DisiWrapper w = essentialsScorers.topList(); w != null; w = w.next) {
                    matchedMaxScoreSum += WANDScorer.scaleMaxScore(w.scorer.score(), scalingFactor);
                  }

                  if (matchedMaxScoreSum < minCompetitiveScore) {
                    target = top.doc + 1;
                  } else {
                    return doc = top.doc;
                  }
                }
              }
            }
          }

          private void movePotentiallyNonCompetitiveScorers() {
            boolean isListAdjusted = false;
            while (maxScoreSortedEssentialScorers.size() > 0
                && nonEssentialMaxScoreSum + maxScoreSortedEssentialScorers.get(0).maxScore
                    < minCompetitiveScore) {
              DisiWrapper nextLeastContributingScorer = maxScoreSortedEssentialScorers.remove(0);
              nonEssentialMaxScoreSum += nextLeastContributingScorer.maxScore;
              nonEssentialScorers.add(nextLeastContributingScorer);

              isListAdjusted = true;
            }

            if (isListAdjusted) {
              essentialsScorers.clear();
              for (DisiWrapper w : maxScoreSortedEssentialScorers) {
                essentialsScorers.add(w);
              }
            }
          }

          private void updateMaxScoresAndLists(int target) throws IOException {
            assert target > upTo;
            // Next candidate doc id is above interval boundary, or minCompetitive has increased.
            // Find next interval boundary.
            // Block boundary alignment strategy is adapted from "Optimizing Top-k Document
            // Retrieval Strategies for Block-Max Indexes" by Dimopoulos, Nepomnyachiy and Suel.
            // Find the block interval boundary that is the minimum of all participating scorer's
            // block boundary. Then run BMM within each interval.
            updateUpToAndMaxScore(target);
            repartitionLists();
          }

          private void updateUpToAndMaxScore(int target) throws IOException {
            // reset upTo
            upTo = -1;
            for (DisiWrapper w : allScorers) {
              upTo = Math.max(w.scorer.advanceShallow(Math.max(w.doc, target)), upTo);
            }
            assert target <= upTo;

            for (DisiWrapper w : allScorers) {
              if (w.doc <= upTo) {
                w.maxScore = WANDScorer.scaleMaxScore(w.scorer.getMaxScore(upTo), scalingFactor);
              } else {
                // This scorer won't be able to contribute to match for target, setting its maxScore
                // to 0 so it goes into nonEssentialList
                w.maxScore = 0;
              }
            }
          }

          private void repartitionLists() {
            essentialsScorers.clear();
            maxScoreSortedEssentialScorers.clear();
            nonEssentialScorers.clear();
            Arrays.sort(allScorers, (w1, w2) -> Long.compare(w1.maxScore, w2.maxScore));

            // Re-partition the scorers into non-essential list and essential list, as defined in
            // the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
            nonEssentialMaxScoreSum = 0;
            for (DisiWrapper w : allScorers) {
              if (nonEssentialMaxScoreSum + w.maxScore < minCompetitiveScore) {
                nonEssentialScorers.add(w);
                nonEssentialMaxScoreSum += w.maxScore;
              } else {
                maxScoreSortedEssentialScorers.add(w);
                essentialsScorers.add(w);
              }
            }
          }

          @Override
          public long cost() {
            // fixed at initialization
            return cost;
          }
        };

    return new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        return WANDScorer.scaleMaxScore(score(), scalingFactor) >= minCompetitiveScore;
      }

      @Override
      public float matchCost() {
        // maximum number of scorer that matches() might advance
        return nonEssentialScorers.size();
      }
    };
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    // Propagate to improve score bounds
    maxScoreSumPropagator.advanceShallow(target);

    int result = DocIdSetIterator.NO_MORE_DOCS;
    for (DisiWrapper s : allScorers) {
      if (s.doc < target) {
        result = Math.min(result, s.scorer.advanceShallow(target));
      }
    }

    return result;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return maxScoreSumPropagator.getMaxScore(upTo);
  }

  @Override
  public float score() throws IOException {
    double sum = 0;

    for (DisiWrapper s = essentialsScorers.topList(); s != null; s = s.next) {
      assert s.doc == doc : s.doc + " " + doc;
      sum += s.scorer.score();
    }
    for (DisiWrapper s : nonEssentialScorers) {
      if (s.doc < doc) {
        s.doc = s.iterator.advance(doc);
      }
      if (s.doc == doc) {
        sum += s.scorer.score();
      }
    }
    return (float) sum;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public final Collection<ChildScorable> getChildren() {
    List<ChildScorable> matchingChildren = new ArrayList<>();
    for (DisiWrapper s : allScorers) {
      if (s.doc == doc) {
        matchingChildren.add(new ChildScorable(s.scorer, "SHOULD"));
      }
    }
    return matchingChildren;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    assert scoreMode == ScoreMode.TOP_SCORES
        : "minCompetitiveScore can only be set for ScoreMode.TOP_SCORES, but got: " + scoreMode;
    assert minScore >= 0;
    long scaledMinScore = WANDScorer.scaleMinScore(minScore, scalingFactor);
    // minScore increases monotonically
    assert scaledMinScore >= minCompetitiveScore;
    minCompetitiveScore = scaledMinScore;
    maxScoreSumPropagator.setMinCompetitiveScore(minScore);
  }
}
