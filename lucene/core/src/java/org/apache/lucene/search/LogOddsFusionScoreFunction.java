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

/** Shared scoring function for {@link LogOddsFusionQuery} scorers and explanations. */
final class LogOddsFusionScoreFunction {
  private static final float CLAMP_MIN = 1e-7f;
  private static final float CLAMP_MAX = 1f - 1e-7f;

  private final int totalClauses;
  private final double scalingFactor;
  private final float[] signalWeights;
  private final float[] logitMin;
  private final float[] logitMax;
  private final LogOddsFusionQuery.Gating gating;
  private final double[] signalPriorLogits;
  private final double targetPriorLogit;
  private final float boost;

  LogOddsFusionScoreFunction(
      int totalClauses,
      float alpha,
      float[] signalWeights,
      float[] logitMin,
      float[] logitMax,
      LogOddsFusionQuery.Gating gating,
      float[] signalBaseRates,
      float baseRate,
      float boost) {
    this.totalClauses = totalClauses;
    this.scalingFactor = Math.pow(totalClauses, alpha);
    this.signalWeights = signalWeights;
    this.logitMin = logitMin;
    this.logitMax = logitMax;
    this.gating = gating;
    if (signalBaseRates == null) {
      this.signalPriorLogits = null;
    } else {
      this.signalPriorLogits = new double[signalBaseRates.length];
      for (int i = 0; i < signalBaseRates.length; i++) {
        this.signalPriorLogits[i] = priorLogit(signalBaseRates[i]);
      }
    }
    this.targetPriorLogit = priorLogit(baseRate);
    this.boost = boost;
  }

  /** Returns this matching signal's weighted contribution to the pooled evidence logit. */
  double contribution(float probability, int signalIndex) {
    double transformed = transformLogit(probability, signalIndex);
    return signalWeights == null ? transformed : signalWeights[signalIndex] * transformed;
  }

  /**
   * Returns a safe upper bound for an optional signal's contribution. A non-match contributes zero,
   * which is greater than a matching signal's contribution when all of its evidence is negative.
   */
  double maxContribution(float maxProbability, int signalIndex) {
    return Math.max(0d, contribution(maxProbability, signalIndex));
  }

  /** Converts a sum of signal contributions into an unboosted probability. */
  float probability(double contributionSum) {
    double pooled = signalWeights == null ? contributionSum / totalClauses : contributionSum;
    return sigmoid(targetPriorLogit + scalingFactor * pooled);
  }

  /** Converts a sum of signal contributions into the final, boosted score. */
  float score(double contributionSum) {
    return boost * probability(contributionSum);
  }

  private double transformLogit(float probability, int signalIndex) {
    double evidenceLogit = logit(probability);
    if (signalPriorLogits != null) {
      evidenceLogit -= signalPriorLogits[signalIndex];
    }
    if (logitMin != null) {
      double normalized =
          (evidenceLogit - logitMin[signalIndex]) / (logitMax[signalIndex] - logitMin[signalIndex]);
      return Math.clamp(normalized, 0d, 1d);
    }
    return switch (gating) {
      case NONE -> evidenceLogit;
      case SOFTPLUS -> softplus(evidenceLogit);
    };
  }

  static float clampProbability(float probability) {
    return Math.clamp(probability, CLAMP_MIN, CLAMP_MAX);
  }

  static double logit(float probability) {
    float clamped = clampProbability(probability);
    return Math.log(clamped / (1d - clamped));
  }

  static float sigmoid(double value) {
    if (value >= 0) {
      return (float) (1d / (1d + Math.exp(-value)));
    }
    double expValue = Math.exp(value);
    return (float) (expValue / (1d + expValue));
  }

  private static double softplus(double value) {
    if (value > 20d) {
      return value;
    }
    return Math.log1p(Math.exp(value));
  }

  private static double priorLogit(float baseRate) {
    return baseRate > 0f ? Math.log(baseRate / (1d - baseRate)) : 0d;
  }
}
