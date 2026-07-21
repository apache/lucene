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
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Scorer for {@link LogOddsFusionQuery}. Combines sub-scorer outputs (assumed to be probabilities
 * in (0, 1)) via log-odds fusion with multiplicative confidence scaling.
 *
 * <p>The default scoring formula is:
 *
 * <pre>
 *   evidence    = logit(clamp(subScore))             for each matching sub-scorer
 *   logitSum    = sum of evidence values
 *   meanLogit   = logitSum / n   (n = total clause count, not just matching)
 *   scaledLogit = meanLogit * pow(n, alpha)
 *   score       = sigmoid(scaledLogit)
 * </pre>
 *
 * <p>Evidence logits may be negative internally, while the final score remains a non-negative
 * probability because it is passed through a sigmoid. Non-matching sub-scorers contribute zero
 * (neutral evidence). Optional softplus gating is available through {@link
 * LogOddsFusionQuery.Gating#SOFTPLUS} when callers explicitly want to discard negative evidence.
 *
 * @lucene.experimental
 */
final class LogOddsFusionScorer extends DisjunctionScorer {
  private final List<Scorer> subScorers;
  private final LogOddsFusionScoreFunction scoreFunction;
  private final IdentityHashMap<Scorer, Integer> scorerIndexMap;

  /**
   * Creates a new LogOddsFusionScorer.
   *
   * @param subScorers the sub scorers to combine
   * @param scoreFunction shared function that transforms and pools sub-scores
   * @param scoreMode the score mode
   * @param leadCost the lead cost for iteration
   */
  LogOddsFusionScorer(
      List<Scorer> subScorers,
      LogOddsFusionScoreFunction scoreFunction,
      ScoreMode scoreMode,
      long leadCost)
      throws IOException {
    super(subScorers, scoreMode, leadCost);
    this.subScorers = subScorers;
    this.scoreFunction = scoreFunction;
    this.scorerIndexMap = new IdentityHashMap<>(subScorers.size());
    for (int i = 0; i < subScorers.size(); i++) {
      this.scorerIndexMap.put(subScorers.get(i), i);
    }
  }

  @Override
  protected float score(DisiWrapper topList) throws IOException {
    double logitSum = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      int signalIndex = scorerIndexMap.get(w.scorer);
      logitSum += scoreFunction.contribution(w.scorable.score(), signalIndex);
    }
    return scoreFunction.score(logitSum);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    int upTo = DocIdSetIterator.NO_MORE_DOCS;
    for (Scorer scorer : subScorers) {
      if (scorer.docID() <= target) {
        upTo = Math.min(upTo, scorer.advanceShallow(target));
      } else if (scorer.docID() != DocIdSetIterator.NO_MORE_DOCS) {
        upTo = Math.min(upTo, scorer.docID() - 1);
      }
    }
    return upTo;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double maxLogitSum = 0;
    for (int i = 0; i < subScorers.size(); i++) {
      Scorer scorer = subScorers.get(i);
      if (scorer.docID() <= upTo) {
        maxLogitSum += scoreFunction.maxContribution(scorer.getMaxScore(upTo), i);
      }
    }
    return Math.nextUp(scoreFunction.score(maxLogitSum));
  }
}
