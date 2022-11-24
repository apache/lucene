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

final class MaxScoreBulkScorer extends BulkScorer {

  private final DisiWrapper[] allScorers;
  private final DisiPriorityQueue essentialQueue;
  private final MaxScoreSumPropagator maxScorePropagator;
  private final long cost;
  private float minCompetitiveScore;
  private boolean minCompetitiveScoreUpdated;
  private ScoreAndDoc scorable = new ScoreAndDoc();
  private int firstEssentialScorer;
  private final double[] maxScoreSums;

  MaxScoreBulkScorer(List<Scorer> scorers) throws IOException {
    allScorers = new DisiWrapper[scorers.size()];
    int i = 0;
    long cost = 0;
    for (Scorer scorer : scorers) {
      DisiWrapper w = new DisiWrapper(scorer);
      cost += w.cost;
      allScorers[i++] = w;
    }
    this.cost = cost;
    maxScorePropagator = new MaxScoreSumPropagator(scorers);
    essentialQueue = new DisiPriorityQueue(allScorers.length);
    maxScoreSums = new double[allScorers.length];
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    int windowMin = min;
    main:
    while (windowMin < max) {
      int windowMax = updateMaxWindowScores(windowMin);
      windowMax = Math.min(windowMax, max);
      if (partitionScorers() == false) {
        // No matches in this window
        windowMin = windowMax;
        continue;
      }

      DisiWrapper top = essentialQueue.top();
      while (top.doc < windowMin) {
        top.doc = top.iterator.advance(windowMin);
        top = essentialQueue.updateTop();
      }

      while (top.doc < windowMax) {
        if (acceptDocs == null || acceptDocs.get(top.doc)) {
          DisiWrapper topList = essentialQueue.topList();
          double score = topList.scorer.score();
          for (DisiWrapper w = topList.next; w != null; w = w.next) {
            score += w.scorer.score();
          }

          boolean possibleMatch = true;
          for (int i = firstEssentialScorer - 1; i >= 0; --i) {
            float maxPossibleScore = maxScorePropagator.scoreSumUpperBound(score + maxScoreSums[i]);
            if (maxPossibleScore < minCompetitiveScore) {
              possibleMatch = false;
              break;
            }

            DisiWrapper scorer = allScorers[i];
            if (scorer.doc < top.doc) {
              scorer.doc = scorer.iterator.advance(top.doc);
            }
            if (scorer.doc == top.doc) {
              score += scorer.scorer.score();
            }
          }

          if (possibleMatch) {
            scorable.doc = top.doc;
            scorable.score = (float) score;
            collector.collect(top.doc);
          }
        }
        int doc = top.doc;
        do {
          top.doc = top.iterator.nextDoc();
          top = essentialQueue.updateTop();
        } while (top.doc == doc);

        if (minCompetitiveScoreUpdated) {
          minCompetitiveScoreUpdated = false;
          if (partitionScorers()) {
            top = essentialQueue.top();
          } else {
            windowMin = windowMax;
            continue main;
          }
        }
      }
      windowMin = windowMax;
    }

    return nextCandidate(max);
  }

  private int updateMaxWindowScores(int windowMin) throws IOException {
    final int firstWindowLead = Math.min(firstEssentialScorer, allScorers.length - 1);
    for (int i = 0; i < firstWindowLead; ++i) {
      final DisiWrapper scorer = allScorers[i];
      if (scorer.doc < windowMin) {
        scorer.scorer.advanceShallow(windowMin);
      }
    }
    int windowMax = DocIdSetIterator.NO_MORE_DOCS;
    for (int i = firstWindowLead; i < allScorers.length; ++i) {
      final DisiWrapper scorer = allScorers[i];
      final int upTo = scorer.scorer.advanceShallow(Math.max(scorer.doc, windowMin));
      windowMax = (int) Math.min(windowMax, upTo + 1L); // upTo is inclusive
    }
    for (DisiWrapper scorer : allScorers) {
      if (scorer.doc < windowMax) {
        scorer.maxWindowScore = scorer.scorer.getMaxScore(windowMax - 1);
      } else {
        scorer.maxWindowScore = 0;
      }
    }
    return windowMax;
  }

  private boolean partitionScorers() {
    Arrays.sort(allScorers, Comparator.comparingDouble(scorer -> scorer.maxWindowScore));
    firstEssentialScorer = 0;
    double maxScoreSum = 0;
    for (; firstEssentialScorer < allScorers.length; ++firstEssentialScorer) {
      maxScoreSum += allScorers[firstEssentialScorer].maxWindowScore;
      maxScoreSums[firstEssentialScorer] = maxScoreSum;
      float maxScoreSumFloat =
          MaxScoreSumPropagator.scoreSumUpperBound(
              maxScoreSum,
              firstEssentialScorer + 1);
      if (maxScoreSumFloat >= minCompetitiveScore) {
        break;
      }
    }
    if (firstEssentialScorer == allScorers.length) {
      return false;
    }

    essentialQueue.clear();
    for (int i = firstEssentialScorer; i < allScorers.length; ++i) {
      essentialQueue.add(allScorers[i]);
    }
    return true;
  }

  /** Return the next candidate on or after {@code rangeEnd}. */
  private int nextCandidate(int rangeEnd) {
    int next = DocIdSetIterator.NO_MORE_DOCS;
    for (DisiWrapper scorer : allScorers) {
      if (scorer.doc < rangeEnd) {
        return rangeEnd;
      } else {
        next = Math.min(next, scorer.doc);
      }
    }
    return next;
  }

  @Override
  public long cost() {
    return cost;
  }

  private class ScoreAndDoc extends Scorable {

    float score;
    int doc = -1;

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() {
      return score;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      MaxScoreBulkScorer.this.minCompetitiveScore = minScore;
      maxScorePropagator.setMinCompetitiveScore(minScore);
      minCompetitiveScoreUpdated = true;
    }
  }
}
