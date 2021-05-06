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
import org.apache.lucene.util.Bits;

/** BulkScorer that leverages BMM algorithm within interval (min, max) */
public class BMMBulkScorer extends BulkScorer {
  private List<Scorer> scorers;
  private Weight weight;
  private ScoreMode scoreMode;
  private int scalingFactor;
  private long cost;
  private static final int FIXED_WINDOW_SIZE = 2048;

  public BMMBulkScorer(BooleanWeight weight, List<Scorer> scorers, ScoreMode scoreMode)
      throws IOException {
    assert scoreMode == ScoreMode.TOP_SCORES;
    this.weight = weight;
    this.scorers = scorers;
    this.scoreMode = scoreMode;
    this.scalingFactor = WANDScorer.getScalingFactor(scorers).orElse(0);
    this.cost =
        costWithMinShouldMatch(
            scorers.stream().map(Scorer::iterator).mapToLong(DocIdSetIterator::cost),
            scorers.size(),
            1);
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    // The BMM algorithm is only valid within the boundary [min, max)
    BMMBoundaryAwareScorer scorer = new BMMBoundaryAwareScorer(weight);
    TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
    DocIdSetIterator approximation = twoPhase.approximation;
    collector.setScorer(scorer);

    int lowerBound = min;
    int upperBound = getUpperBound(lowerBound, FIXED_WINDOW_SIZE, max);

    while (scorer.doc < max) {
      int doc;
      for (doc = scorer.updateIntervalBoundary(lowerBound, upperBound);
          doc < upperBound;
          doc = approximation.nextDoc()) {

        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
          collector.collect(doc);
        }
      }

      lowerBound = doc;
      upperBound = getUpperBound(lowerBound, FIXED_WINDOW_SIZE, max);
    }

    return scorer.doc;
  }

  private int getUpperBound(int lowerBound, int fixedWindowSize, int max) {
    // use minus instead of plus to avoid integer overflow when close to
    // DocIdSetIterator.NO_MORE_DOCS
    if (DocIdSetIterator.NO_MORE_DOCS - lowerBound < fixedWindowSize) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return Math.min(lowerBound + fixedWindowSize, max);
    }
  }

  @Override
  public long cost() {
    return cost;
  }

  private class BMMBoundaryAwareScorer extends Scorer {
    // current doc ID of the leads
    private int doc;

    // heap of scorers ordered by doc ID
    private final DisiPriorityQueue essentialsScorers;

    // list of scorers whose sum of maxScore is less than minCompetitiveScore, ordered by maxScore
    private List<DisiWrapper> nonEssentialScorers;

    // sum of max scores of scorers in nonEssentialScorers list
    private long nonEssentialMaxScoreSum;

    // sum of score of scorers in essentialScorers list that are positioned on matching doc
    private long matchedDocScoreSum;

    private final MaxScoreSumPropagator maxScoreSumPropagator;

    // upperBound on doc id where BMM algorithm is valid
    private int upperBound;

    // scaled min competitive score
    private long minCompetitiveScore = 0;

    /**
     * Constructs a Scorer
     *
     * @param weight The scorers <code>Weight</code>.
     */
    protected BMMBoundaryAwareScorer(Weight weight) throws IOException {
      super(weight);
      doc = -1;
      essentialsScorers = new DisiPriorityQueue(scorers.size());
      nonEssentialScorers = new LinkedList<>();
      maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);
      for (Scorer scorer : scorers) {
        nonEssentialScorers.add(new DisiWrapper(scorer));
      }
    }

    // returns the doc id from which iteration begins
    public int updateIntervalBoundary(int lowerBound, int upperBound) throws IOException {
      assert lowerBound <= upperBound : "loweBound: " + lowerBound + " upperBound: " + upperBound;
      this.upperBound = upperBound;

      // update all scorers' position and maxScore by the new boundary information
      long sumOfAllMaxScore = 0;
      int minNextDoc = DocIdSetIterator.NO_MORE_DOCS;
      List<DisiWrapper> newNonEssentialScorers = new LinkedList<>();

      while (essentialsScorers.size() > 0) {
        nonEssentialScorers.add(essentialsScorers.pop());
      }

      for (DisiWrapper w : nonEssentialScorers) {
        if (w.doc < lowerBound) {
          w.scorer.advanceShallow(lowerBound);
          w.doc = w.scorer.iterator().advance(lowerBound);
          minNextDoc = Math.min(minNextDoc, w.doc);
        } else if (w.doc != DocIdSetIterator.NO_MORE_DOCS) {
          w.scorer.advanceShallow(w.doc);
          minNextDoc = Math.min(minNextDoc, w.doc);
        }
        // maxScore valid between lowerBound and upperBound
        w.maxScore = WANDScorer.scaleMaxScore(w.scorer.getMaxScore(upperBound), scalingFactor);

        if (w.doc != DocIdSetIterator.NO_MORE_DOCS) {
          newNonEssentialScorers.add(w);
          sumOfAllMaxScore += w.maxScore;
        }
      }

      nonEssentialScorers = newNonEssentialScorers;

      // optimization: returns early if certain condition is met
      if (nonEssentialScorers.size() == 0) {
        // all scorers reach NO_MORE_DOCS
        doc = DocIdSetIterator.NO_MORE_DOCS;
        return doc;
      } else if (minNextDoc > upperBound) {
        // next candidate doc already beyond upperBound
        doc = minNextDoc;
        return doc;
      } else if (sumOfAllMaxScore < minCompetitiveScore) {
        // sum of all scorers' max score is lower than minCompetitiveScore
        doc = upperBound;
        return doc;
      }

      Collections.sort(nonEssentialScorers, (w1, w2) -> (int) (w1.maxScore - w2.maxScore));

      // Re-partition the scorers into non-essential list and essential list, as defined
      // in the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
      nonEssentialMaxScoreSum = 0;
      for (int i = 0; i < nonEssentialScorers.size(); i++) {
        DisiWrapper w = nonEssentialScorers.get(i);
        if (nonEssentialMaxScoreSum + w.maxScore < minCompetitiveScore) {
          nonEssentialMaxScoreSum += w.maxScore;
        } else {
          // the logic is a bit ugly here...but as soon as we find maxScore of scorers in
          // non-essential list sum to above minCompetitiveScore, we move the rest of
          // scorers into essential list
          for (int j = nonEssentialScorers.size() - 1; j >= i; j--) {
            essentialsScorers.add(nonEssentialScorers.remove(j));
          }
          break;
        }
      }

      // if sum of all maxScore is less than minCompetitiveScore, then doc would have been set to
      // upperBound and return already
      assert essentialsScorers.size() > 0;
      doc = essentialsScorers.top().doc;

      matchedDocScoreSum = 0;
      for (DisiWrapper w : essentialsScorers) {
        if (w.doc == doc) {
          matchedDocScoreSum += WANDScorer.scaleMaxScore(w.scorer.score(), scalingFactor);
        }
      }

      return doc;
    }

    @Override
    public DocIdSetIterator iterator() {
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
              // avoiding overflow causing target to be < 0
              assert 0 <= target && target <= upperBound;
              assert essentialsScorers.size() > 0;

              doAdvance(target);

              while (doc < upperBound
                  && nonEssentialMaxScoreSum + matchedDocScoreSum < minCompetitiveScore) {
                doAdvance(doc + 1);
              }

              return doc;
            }

            private void doAdvance(int target) throws IOException {
              // Find next smallest doc id that is larger than or equal to target from the essential
              // scorers
              if (essentialsScorers.top().doc == DocIdSetIterator.NO_MORE_DOCS) {
                doc = essentialsScorers.top().doc;
                return;
              }

              while (essentialsScorers.top().doc < target) {
                DisiWrapper w = essentialsScorers.pop();
                w.doc = w.iterator.advance(target);
                essentialsScorers.add(w);
              }

              // If the next candidate doc id is still within interval boundary,
              if (essentialsScorers.top().doc <= upperBound) {
                doc = essentialsScorers.top().doc;

                matchedDocScoreSum = 0;
                for (DisiWrapper w : essentialsScorers) {
                  if (w.doc == doc) {
                    matchedDocScoreSum += WANDScorer.scaleMaxScore(w.scorer.score(), scalingFactor);
                  }
                }
              } else {
                // next doc > upperBound already
                doc = upperBound;
              }
            }

            @Override
            public long cost() {
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
}
