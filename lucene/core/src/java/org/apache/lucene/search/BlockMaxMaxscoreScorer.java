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
import java.util.*;

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

  // list of scorers whose sum of maxScore is less than minCompetitiveScore, ordered by maxScore
  private final List<DisiWrapper> nonEssentialScorers;

  // sum of max scores of scorers in nonEssentialScorers list
  private long nonEssentialMaxScoreSum;

  // sum of score of scorers in essentialScorers list that are positioned on matching doc
  private long matchedDocScoreSum;

  private long cost;

  private final MaxScoreSumPropagator maxScoreSumPropagator;

  private final List<Scorer> scorers;

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
    this.scorers = scorers;
    this.cost =
        costWithMinShouldMatch(
            scorers.stream().map(Scorer::iterator).mapToLong(DocIdSetIterator::cost),
            scorers.size(),
            1);

    essentialsScorers = new DisiPriorityQueue(scorers.size());
    nonEssentialScorers = new LinkedList<>();

    scalingFactor = WANDScorer.getScalingFactor(scorers);
    maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);

    for (Scorer scorer : scorers) {
      nonEssentialScorers.add(new DisiWrapper(scorer));
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    DocIdSetIterator approximation =
        new DocIdSetIterator() {
          private long lastMinCompetitiveScore;

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
            doAdvance(target);

            while (doc != DocIdSetIterator.NO_MORE_DOCS
                && nonEssentialMaxScoreSum + matchedDocScoreSum < minCompetitiveScore) {
              doAdvance(doc + 1);
            }

            return doc;
          }

          private void doAdvance(int target) throws IOException {
            matchedDocScoreSum = 0;
            // Find next smallest doc id that is larger than or equal to target from the essential
            // scorers

            // If the next candidate doc id is still within interval boundary,
            if (lastMinCompetitiveScore == minCompetitiveScore && target <= upTo) {
              while (essentialsScorers.top().doc < target) {
                DisiWrapper w = essentialsScorers.pop();
                w.doc = w.iterator.advance(target);
                essentialsScorers.add(w);
              }

              if (essentialsScorers.top().doc <= upTo) {
                doc = essentialsScorers.top().doc;

                if (doc == NO_MORE_DOCS) {
                  return;
                }
              } else {
                doc = upTo + 1;
              }
            } else {
              lastMinCompetitiveScore = minCompetitiveScore;
              // Next candidate doc id is above interval boundary, or minCompetitive has increased.
              // Find next interval boundary.
              // Block boundary alignment strategy is adapted from "Optimizing Top-k Document
              // Retrieval Strategies for Block-Max Indexes" by Dimopoulos, Nepomnyachiy and Suel.
              // Find the block interval boundary that is the minimum of all participating scorer's
              // block boundary. Then run BMM within each interval.
              updateUpToAndMaxScore(target);

              repartitionLists();

              // maxScore of all scorers sum to less than minCompetitiveScore, no more result is
              // available up to upTo
              if (essentialsScorers.size() == 0) {
                // current bound no long valid
                doc = upTo + 1;
              } else {
                doc = essentialsScorers.top().doc;

                if (doc == NO_MORE_DOCS) {
                  return;
                }
              }
            }

            for (DisiWrapper w : essentialsScorers) {
              if (w.doc == doc && doc != NO_MORE_DOCS) {
                matchedDocScoreSum += WANDScorer.scaleMaxScore(w.scorer.score(), scalingFactor);
              }
            }
          }

          private void updateUpToAndMaxScore(int target) throws IOException {
            while (essentialsScorers.size() > 0) {
              nonEssentialScorers.add(essentialsScorers.pop());
            }

            // reset upTo
            upTo = DocIdSetIterator.NO_MORE_DOCS;
            for (DisiWrapper w : nonEssentialScorers) {
              if (w.doc < target) {
                upTo = Math.min(w.scorer.advanceShallow(target), upTo);
              } else {
                upTo = Math.min(w.scorer.advanceShallow(w.doc), upTo);
              }
            }
            assert target <= upTo;

            for (DisiWrapper w : nonEssentialScorers) {
              if (w.doc <= upTo) {
                w.maxScore = WANDScorer.scaleMaxScore(w.scorer.getMaxScore(upTo), scalingFactor);
              } else {
                // This scorer won't be able to contribute to match for target, setting its maxScore
                // to 0 so it goes into nonEssentialList
                w.maxScore = 0;
              }
              if (w.doc < target) {
                w.doc = w.iterator.advance(target);
              }
            }
          }

          private void repartitionLists() {
            Collections.sort(nonEssentialScorers, (w1, w2) -> (int) (w1.maxScore - w2.maxScore));

            // Re-partition the scorers into non-essential list and essential list, as defined in
            // the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
            nonEssentialMaxScoreSum = 0;
            for (int i = 0; i < nonEssentialScorers.size(); i++) {
              DisiWrapper w = nonEssentialScorers.get(i);
              if (nonEssentialMaxScoreSum + w.maxScore < minCompetitiveScore) {
                nonEssentialMaxScoreSum += w.maxScore;
              } else {
                // the logic is a bit ugly here...but as soon as we find maxScore of scorers in
                // non-essential list sum to above minCompetitiveScore, we move the rest of
                // scorers
                // into essential list
                for (int j = nonEssentialScorers.size() - 1; j >= i; j--) {
                  essentialsScorers.add(nonEssentialScorers.remove(j));
                }
                break;
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
        // The doc is a match when all scores sum above minCompetitiveScore
        for (DisiWrapper w : nonEssentialScorers) {
          if (w.doc < doc) {
            w.doc = w.iterator.advance(doc);
          }
        }

        if (matchedDocScoreSum >= minCompetitiveScore) {
          return true;
        }

        for (DisiWrapper w : nonEssentialScorers) {
          if (w.doc == doc) {
            matchedDocScoreSum += WANDScorer.scaleMaxScore(w.scorer.score(), scalingFactor);

            if (matchedDocScoreSum >= minCompetitiveScore) {
              return true;
            }
          }
        }

        return false;
      }

      @Override
      public float matchCost() {
        // maximum number of scorer that matches() might advance
        // use length of nonEssentials as it needs to check all scores
        return nonEssentialScorers.size();
      }
    };
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    int result = DocIdSetIterator.NO_MORE_DOCS;
    for (Scorer s : scorers) {
      if (s.docID() < target) {
        result = Math.min(result, s.advanceShallow(target));
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
    assert ensureAllMatchedAccountedFor();
    for (Scorer scorer : scorers) {
      if (scorer.docID() == doc) {
        sum += scorer.score();
      }
    }
    return (float) sum;
  }

  private boolean ensureAllMatchedAccountedFor() {
    for (Scorer scorer : scorers) {
      if (scorer.docID() < doc) {
        return false;
      }
    }

    // all scorers positioned on or after current doc
    return true;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public final Collection<ChildScorable> getChildren() {
    List<ChildScorable> matchingChildren = new ArrayList<>();
    for (Scorer scorer : scorers) {
      if (scorer.docID() == doc) {
        matchingChildren.add(new ChildScorable(scorer, "SHOULD"));
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
