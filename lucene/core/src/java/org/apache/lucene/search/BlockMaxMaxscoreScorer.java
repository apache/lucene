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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/** Scorer implementing Block-Max Maxscore algorithm */
class BlockMaxMaxscoreScorer extends Scorer {
  // current doc ID of the leads
  private int doc;

  // doc id boundary that all scorers maxScore are valid
  private int upTo;

  // heap of scorers ordered by doc ID
  private final DisiPriorityQueue essentialsScorers;

  // array of scorers ordered by maxScore
  private final DisiWrapper[] allScorers;

  // index of the first essential scorer in the `allScorers` array. All scorers before this index
  // are non-essential. All scorers on and after this index are essential.
  private int firstEssentialScorerIndex;

  // sum of max scores of scorers in nonEssentialScorers list
  private double nonEssentialMaxScoreSum;

  private final long cost;

  private final MaxScoreSumPropagator maxScoreSumPropagator;

  private float minCompetitiveScore;

  private double score;

  /**
   * Constructs a Scorer that scores doc based on Block-Max-Maxscore (BMM) algorithm
   * http://engineering.nyu.edu/~suel/papers/bmm.pdf . This algorithm has lower overhead compared to
   * WANDScorer, and could be used for simple disjunction queries.
   *
   * @param weight The weight to be used.
   * @param scorers The sub scorers this Scorer should iterate on for optional clauses.
   */
  public BlockMaxMaxscoreScorer(Weight weight, List<Scorer> scorers) throws IOException {
    super(weight);

    this.upTo = -1;
    this.doc = -1;
    this.minCompetitiveScore = 0;
    this.allScorers = new DisiWrapper[scorers.size()];
    this.essentialsScorers = new DisiPriorityQueue(scorers.size());
    this.firstEssentialScorerIndex = 0;

    long cost = 0;
    for (int i = 0; i < scorers.size(); i++) {
      DisiWrapper w = new DisiWrapper(scorers.get(i));
      cost += w.cost;
      allScorers[i] = w;
    }

    this.cost = cost;
    maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);
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
                  return doc = top.doc;
                }
              }
            }
          }

          private void movePotentiallyNonCompetitiveScorers() {
            boolean removedEssentialScorer = false;
            while (firstEssentialScorerIndex < allScorers.length
                && maxScoreSumPropagator.scoreSumUpperBound(
                        nonEssentialMaxScoreSum + allScorers[firstEssentialScorerIndex].maxScore)
                    < minCompetitiveScore) {
              DisiWrapper nextLeastContributingScorer = allScorers[firstEssentialScorerIndex++];
              nonEssentialMaxScoreSum += nextLeastContributingScorer.maxScore;
              removedEssentialScorer = true;
            }

            // list adjusted
            if (removedEssentialScorer) {
              essentialsScorers.clear();
              essentialsScorers.addAll(
                  allScorers,
                  firstEssentialScorerIndex,
                  allScorers.length - firstEssentialScorerIndex);
            }
          }

          private void updateMaxScoresAndLists(int target) throws IOException {
            assert target > upTo;
            // Next candidate doc id is above interval boundary, or minCompetitive has increased.
            // Find next interval boundary.
            // Block boundary alignment strategy is adapted from "Optimizing Top-k Document
            // Retrieval Strategies for Block-Max Indexes" by Dimopoulos, Nepomnyachiy and Suel.
            // Find the block interval boundary by computing statistics (max, avg etc.) from all
            // participating scorer's block boundary. Then run BMM within the boundary.
            updateUpToAndMaxScore(target);
            repartitionLists();
          }

          private void updateUpToAndMaxScore(int target) throws IOException {
            // reset upTo
            upTo = -1;
            for (DisiWrapper w : allScorers) {
              // using Math.max here is a good approach when there are only two clauses,
              // but when this scorer is used for more than two clauses, we may need to
              // consider other approaches such as avg, as the further out the boundary,
              // the higher maxScore would be for a scorer, which makes skipping based on
              // comparison with minCompetitiveScore harder / less effective.
              upTo = Math.max(w.scorer.advanceShallow(Math.max(w.doc, target)), upTo);
            }
            assert target <= upTo;

            for (DisiWrapper w : allScorers) {
              // The assertion below will hold as long as upTo was computed using Math.max
              // However, when the upTo computation method changes (to Math.avg etc),
              // we may need to also handle the scenario where w.doc > upTo
              assert w.doc <= upTo;
              w.maxScore = w.scorer.getMaxScore(upTo);
            }
          }

          private void repartitionLists() {
            firstEssentialScorerIndex = 0;
            Arrays.sort(allScorers, Comparator.comparingDouble(scorer -> scorer.maxScore));

            // Re-partition the scorers into non-essential list and essential list, as defined in
            // the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
            nonEssentialMaxScoreSum = 0;
            for (DisiWrapper w : allScorers) {
              if (maxScoreSumPropagator.scoreSumUpperBound(nonEssentialMaxScoreSum + w.maxScore)
                  >= minCompetitiveScore) {
                break;
              }
              firstEssentialScorerIndex++;
              nonEssentialMaxScoreSum += w.maxScore;
            }
            essentialsScorers.clear();
            essentialsScorers.addAll(
                allScorers,
                firstEssentialScorerIndex,
                allScorers.length - firstEssentialScorerIndex);
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
        // Start evaluating the score of the new document. It initially only includes essential
        // clauses and abort / return early if a match is not possible.
        // Scores of non-essential clauses get added later on to determine actual matches.
        score = 0;
        for (DisiWrapper w = essentialsScorers.topList(); w != null; w = w.next) {
          score += w.scorer.score();
        }

        final double docScoreUpperBound = score + nonEssentialMaxScoreSum;

        if (maxScoreSumPropagator.scoreSumUpperBound(docScoreUpperBound) < minCompetitiveScore) {
          return false;
        }

        // Continue to add scores of non-essential scorers
        for (int i = 0; i < firstEssentialScorerIndex; ++i) {
          DisiWrapper w = allScorers[i];
          if (w.doc < doc) {
            w.doc = w.iterator.advance(doc);
          }
          if (w.doc == doc) {
            score += allScorers[i].scorer.score();
          }
        }

        return score() >= minCompetitiveScore;
      }

      @Override
      public float matchCost() {
        // over-estimate
        return allScorers.length;
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
    return (float) score;
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
    assert minScore >= 0;
    minCompetitiveScore = minScore;
    maxScoreSumPropagator.setMinCompetitiveScore(minScore);
  }
}
