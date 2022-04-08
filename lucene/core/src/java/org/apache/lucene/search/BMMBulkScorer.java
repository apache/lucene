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
  private DisiWrapper[] allScorers;
  private Weight weight;
  private ScoreMode scoreMode;
  private int scalingFactor;
  private long cost;
  private static final int FIXED_WINDOW_SIZE = 1024;

  public BMMBulkScorer(BooleanWeight weight, List<Scorer> scorers, ScoreMode scoreMode)
      throws IOException {
    assert scoreMode == ScoreMode.TOP_SCORES;
    this.weight = weight;
    this.scorers = scorers;
    this.allScorers = scorers.stream().map(DisiWrapper::new).toArray(DisiWrapper[]::new);
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
    DocIdSetIterator disi = scorer.iterator();
    collector.setScorer(scorer);

    int lowerBound = min;
    int upTo = getUpperBound(lowerBound, FIXED_WINDOW_SIZE, max);

    while (scorer.doc < max) {
      int doc;
      for (doc = scorer.updateBoundary(lowerBound, upTo); doc < upTo; doc = disi.nextDoc()) {

        if ((acceptDocs == null || acceptDocs.get(doc))) {
          collector.collect(doc);
        }
      }

      lowerBound = doc;
      upTo = getUpperBound(lowerBound, FIXED_WINDOW_SIZE, max);
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

    private final MaxScoreSumPropagator maxScoreSumPropagator;

    // upperBound on doc id where BMM algorithm is valid
    private int upTo;

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
      essentialsScorers = new DisiPriorityQueue(allScorers.length);
      nonEssentialScorers = new ArrayList<>(allScorers.length);
      Collections.addAll(nonEssentialScorers, allScorers);
      maxScoreSumPropagator = new MaxScoreSumPropagator(scorers);
    }

    // returns the doc id from which iteration begins
    public int updateBoundary(int min, int upTo) throws IOException {
      assert min <= upTo : "loweBound: " + min + " upTo: " + upTo;
      this.upTo = upTo;

      int minNextDoc = updateMaxScores(min, upTo);
      if (minNextDoc > upTo) {
        return doc = minNextDoc;
      }

      repartitionLists();

      // sum of all maxScore in this boundary lower than minCompetitiveScore, skip to next interval
      if (essentialsScorers.size() == 0) {
        return doc = upTo + 1;
      }

      // if sum of all maxScore is less than minCompetitiveScore, then doc would have been set to
      // upTo and return already
      assert essentialsScorers.size() > 0;
      return doc = essentialsScorers.top().doc;
    }

    public int updateMaxScores(int min, int upTo) throws IOException {
      int minNextDoc = DocIdSetIterator.NO_MORE_DOCS;

      for (DisiWrapper w : allScorers) {
        if (w.doc < min) {
          w.scorer.advanceShallow(min);
          w.doc = w.scorer.iterator().advance(min);
        } else if (w.doc != DocIdSetIterator.NO_MORE_DOCS) {
          w.scorer.advanceShallow(w.doc);
        }

        // maxScore valid between min and upTo
        w.maxScore = WANDScorer.scaleMaxScore(w.scorer.getMaxScore(upTo), scalingFactor);
        minNextDoc = Math.min(minNextDoc, w.doc);
      }

      return minNextDoc;
    }

    public void repartitionLists() {
      nonEssentialScorers.clear();
      essentialsScorers.clear();

      Collections.sort(nonEssentialScorers, (w1, w2) -> Long.compare(w1.maxScore, w2.maxScore));

      // Re-partition the scorers into non-essential list and essential list, as defined
      // in the "Optimizing Top-k Document Retrieval Strategies for Block-Max Indexes" paper.
      nonEssentialMaxScoreSum = 0;
      for (DisiWrapper w : allScorers) {
        if (nonEssentialMaxScoreSum + w.maxScore < minCompetitiveScore) {
          nonEssentialMaxScoreSum += w.maxScore;
          nonEssentialScorers.add(w);
        } else {
          essentialsScorers.add(w);
        }
      }
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
              return doc = upTo + 1;
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
              return doc = upTo + 1;
            }

            long matchedMaxScoreSum = nonEssentialMaxScoreSum;
            for (DisiWrapper w = essentialsScorers.topList(); w != null; w = w.next) {
              matchedMaxScoreSum += w.maxScore;
            }

            if (matchedMaxScoreSum < minCompetitiveScore) {
              target = top.doc + 1;
            } else {
              return doc = top.doc;
            }
          }
        }

        @Override
        public long cost() {
          return cost;
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
      for (DisiWrapper w : allScorers) {
        if (w.doc == doc) {
          matchingChildren.add(new ChildScorable(w.scorer, "SHOULD"));
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
