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
  // list of scorers ordered by maxScore
  private final List<DisiWrapper> maxScoreSortedEssentialsScorers;

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
    maxScoreSortedEssentialsScorers = new LinkedList<>();
    nonEssentialScorers = new ArrayList<>(scorers.size());

    scalingFactor = WANDScorer.getScalingFactor(scorers);
    maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);

    allScorers = scorers.stream().map(DisiWrapper::new).toArray(DisiWrapper[]::new);
    Collections.addAll(nonEssentialScorers, allScorers);
  }

  @Override
  public DocIdSetIterator iterator() {

    return new DocIdSetIterator() {

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
            updateMaxScores(target);
          }

          if (maxScoreSortedEssentialsScorers.size() > 0
              && minCompetitiveScore - nonEssentialMaxScoreSum
                  > maxScoreSortedEssentialsScorers.get(0).maxScore) {
            adjustLists();
          }

          assert target <= upTo;

          DisiWrapper top = essentialsScorers.top();

          if (top == null) {
            if (upTo == NO_MORE_DOCS) {
              return doc = NO_MORE_DOCS;
            }
            target = upTo + 1;
            continue;
          }

          while (top.doc < target) {
            top.doc = top.iterator.advance(target);
            top = essentialsScorers.updateTop();
          }

          if (top.doc == NO_MORE_DOCS) {
            return doc = NO_MORE_DOCS;
          }

          if (top.doc > upTo) {
            target = upTo + 1;
            continue;
          }

          long matchedMaxScoreSum = nonEssentialMaxScoreSum;

          for (DisiWrapper w : nonEssentialScorers) {
            if (w.doc > top.doc) {
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

      private void updateMaxScores(int target) throws IOException {
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
          if (w.doc < target) {
            upTo = Math.max(w.scorer.advanceShallow(target), upTo);
          } else {
            upTo = Math.max(w.scorer.advanceShallow(w.doc), upTo);
          }
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

      private void adjustLists() {
        assert essentialsScorers.size() > 0;
        assert essentialsScorers.size() == maxScoreSortedEssentialsScorers.size()
            : "essentialScores size: "
                + essentialsScorers.size()
                + " , but maxScoreSortedEssentialScorersSize: "
                + maxScoreSortedEssentialsScorers.size();

        while (maxScoreSortedEssentialsScorers.size() > 0
            && nonEssentialMaxScoreSum + maxScoreSortedEssentialsScorers.get(0).maxScore
                < minCompetitiveScore) {
          DisiWrapper nextLeastContributingScorer = maxScoreSortedEssentialsScorers.remove(0);
          nonEssentialMaxScoreSum += nextLeastContributingScorer.maxScore;
          nonEssentialScorers.add(nextLeastContributingScorer);
        }

        essentialsScorers.clear();
        for (DisiWrapper w : maxScoreSortedEssentialsScorers) {
          essentialsScorers.add(w);
        }
      }

      private void repartitionLists() {
        essentialsScorers.clear();
        maxScoreSortedEssentialsScorers.clear();
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
            maxScoreSortedEssentialsScorers.add(w);
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
  }

  @Override
  public int advanceShallow(int target) throws IOException {
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
