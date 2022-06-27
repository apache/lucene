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
import java.util.LinkedList;
import java.util.List;

/** Scorer implementing Block-Max Maxscore algorithm */
public class BlockMaxMaxscoreScorer extends Scorer {
  private final ScoreMode scoreMode;

  // current doc ID of the leads
  private int doc;

  // doc id boundary that all scorers maxScore are valid
  private int upTo = -1;

  // heap of scorers ordered by doc ID
  private final DisiPriorityQueue essentialsScorers;
  // list of scorers ordered by maxScore
  private final LinkedList<DisiWrapper> maxScoreSortedEssentialScorers;

  private final DisiWrapper[] allScorers;

  // sum of max scores of scorers in nonEssentialScorers list
  private float nonEssentialMaxScoreSum;

  private long cost;

  private final MaxScoreSumPropagator maxScoreSumPropagator;

  // scaled min competitive score
  private float minCompetitiveScore = 0;

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

    this.allScorers = new DisiWrapper[scorers.size()];
    int i = 0;
    this.essentialsScorers = new DisiPriorityQueue(scorers.size());
    this.maxScoreSortedEssentialScorers = new LinkedList<>();

    long cost = 0;
    for (Scorer scorer : scorers) {
      DisiWrapper w = new DisiWrapper(scorer);
      cost += w.cost;
      allScorers[i++] = w;
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
                  float matchedMaxScoreSum = nonEssentialMaxScoreSum;

                  for (DisiWrapper w = essentialsScorers.topList(); w != null; w = w.next) {
                    matchedMaxScoreSum += w.scorer.score();
                  }

                  if (matchedMaxScoreSum < minCompetitiveScore) {
                    // skip straight to next candidate doc from essential scorer
                    int docId = top.doc;
                    do {
                      top.doc = top.iterator.nextDoc();
                      top = essentialsScorers.updateTop();
                    } while (top.doc == docId);

                    target = top.doc;
                  } else {
                    return doc = top.doc;
                  }
                }
              }
            }
          }

          private void movePotentiallyNonCompetitiveScorers() {
            while (maxScoreSortedEssentialScorers.size() > 0
                && nonEssentialMaxScoreSum + maxScoreSortedEssentialScorers.get(0).maxScoreFloat
                    < minCompetitiveScore) {
              DisiWrapper nextLeastContributingScorer =
                  maxScoreSortedEssentialScorers.removeFirst();
              nonEssentialMaxScoreSum += nextLeastContributingScorer.maxScoreFloat;
            }

            // list adjusted
            if (essentialsScorers.size() != maxScoreSortedEssentialScorers.size()) {
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
                w.maxScoreFloat = w.scorer.getMaxScore(upTo);
              } else {
                // This scorer won't be able to contribute to match for target, setting its maxScore
                // to 0 so it goes into nonEssentialList
                w.maxScoreFloat = 0;
              }
            }
          }

          private void repartitionLists() {
            essentialsScorers.clear();
            maxScoreSortedEssentialScorers.clear();
            Arrays.sort(allScorers, Comparator.comparingDouble(scorer -> scorer.maxScoreFloat));

            // Re-partition the scorers into non-essential list and essential list, as defined in
            // the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
            nonEssentialMaxScoreSum = 0;
            for (DisiWrapper w : allScorers) {
              if (nonEssentialMaxScoreSum + w.maxScoreFloat < minCompetitiveScore) {
                nonEssentialMaxScoreSum += w.maxScoreFloat;
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
        return score() >= minCompetitiveScore;
      }

      @Override
      public float matchCost() {
        // maximum number of scorer that matches() might advance
        return allScorers.length - essentialsScorers.size();
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

    for (DisiWrapper w : allScorers) {
      if (w.doc < doc) {
        w.doc = w.iterator.advance(doc);
      }

      if (w.doc == doc) {
        sum += w.scorer.score();
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
    minCompetitiveScore = minScore;
    maxScoreSumPropagator.setMinCompetitiveScore(minScore);
  }
}
