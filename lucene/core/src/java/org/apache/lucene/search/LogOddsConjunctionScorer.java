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
import java.util.List;

/**
 * Scorer for {@link LogOddsConjunctionQuery}. Combines sub-scorer outputs (assumed to be
 * probabilities in (0, 1)) via log-odds conjunction with multiplicative confidence scaling.
 *
 * <p>The scoring formula is:
 *
 * <pre>
 *   gatedLogit  = softplus(logit(clamp(subScore)))   for each matching sub-scorer
 *   logitSum    = sum of gatedLogit values
 *   meanLogit   = logitSum / n   (n = total clause count, not just matching)
 *   scaledLogit = meanLogit * pow(n, alpha)
 *   score       = sigmoid(scaledLogit)
 * </pre>
 *
 * <p>Softplus gating ({@code log(1 + exp(x))}) is applied to logit values before aggregation. This
 * distinguishes "absence of evidence" (non-matching sub-scorer, contributes 0) from "evidence of
 * absence" (matching sub-scorer with weak probability, contributes a small positive value). A
 * matching sub-scorer always contributes more than a non-matching one, preserving the ordering
 * among weak matches while ensuring that no match is ever penalized.
 *
 * <p>Non-matching sub-scorers contribute logit(0.5) = 0 (neutral evidence).
 */
final class LogOddsConjunctionScorer extends DisjunctionScorer {
  private static final float CLAMP_MIN = 1e-7f;
  private static final float CLAMP_MAX = 1f - 1e-7f;

  private final List<Scorer> subScorers;
  private final int totalClauses;
  private final float alpha;
  private final float scalingFactor;

  private final DisjunctionScoreBlockBoundaryPropagator disjunctionBlockPropagator;

  /**
   * Creates a new LogOddsConjunctionScorer.
   *
   * @param subScorers the sub scorers to combine
   * @param totalClauses the total number of clauses (including non-matching)
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param scoreMode the score mode
   * @param leadCost the lead cost for iteration
   */
  LogOddsConjunctionScorer(
      List<Scorer> subScorers, int totalClauses, float alpha, ScoreMode scoreMode, long leadCost)
      throws IOException {
    super(subScorers, scoreMode, leadCost);
    this.subScorers = subScorers;
    this.totalClauses = totalClauses;
    this.alpha = alpha;
    this.scalingFactor = (float) Math.pow(totalClauses, alpha);
    if (scoreMode == ScoreMode.TOP_SCORES) {
      this.disjunctionBlockPropagator = new DisjunctionScoreBlockBoundaryPropagator(subScorers);
    } else {
      this.disjunctionBlockPropagator = null;
    }
  }

  static float clampProbability(float p) {
    return Math.max(CLAMP_MIN, Math.min(CLAMP_MAX, p));
  }

  static float logit(float p) {
    float clamped = clampProbability(p);
    return (float) Math.log(clamped / (1f - clamped));
  }

  static float sigmoid(float x) {
    if (x >= 0) {
      return (float) (1.0 / (1.0 + Math.exp(-x)));
    } else {
      double expX = Math.exp(x);
      return (float) (expX / (1.0 + expX));
    }
  }

  /**
   * Softplus function: log(1 + exp(x)). Always positive, smooth approximation of ReLU. For large
   * positive x, approaches x. For large negative x, approaches 0 from above. At x=0, returns log(2)
   * ~ 0.693.
   *
   * <p>Uses a numerically stable formulation: for x &gt; 20, softplus(x) ~ x.
   */
  static float softplus(float x) {
    if (x > 20f) {
      return x;
    }
    return (float) Math.log1p(Math.exp(x));
  }

  @Override
  protected float score(DisiWrapper topList) throws IOException {
    double logitSum = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      float subScore = w.scorable.score();
      logitSum += softplus(logit(subScore));
    }
    // Non-matching sub-scorers contribute logit(0.5) = 0, softplus(0) = log(2) ~ 0.693.
    // But we do NOT add this for non-matching scorers: they contribute 0, not softplus(0).
    // This is the key distinction: a match always contributes softplus(logit) > 0,
    // while a non-match contributes exactly 0.
    float meanLogit = (float) (logitSum / totalClauses);
    float scaledLogit = meanLogit * scalingFactor;
    return sigmoid(scaledLogit);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    if (disjunctionBlockPropagator != null) {
      return disjunctionBlockPropagator.advanceShallow(target);
    }
    return super.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double maxLogitSum = 0;
    for (Scorer scorer : subScorers) {
      if (scorer.docID() <= upTo) {
        float maxSubScore = scorer.getMaxScore(upTo);
        maxLogitSum += softplus(logit(maxSubScore));
      }
    }
    // Safe upper bound: softplus(logit) is monotone in p, sum of upper bounds >= sum of actuals,
    // sigmoid is monotone
    float meanLogit = (float) (maxLogitSum / totalClauses);
    float scaledLogit = meanLogit * scalingFactor;
    return sigmoid(scaledLogit);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (disjunctionBlockPropagator != null) {
      disjunctionBlockPropagator.setMinCompetitiveScore(minScore);
    }
  }
}
