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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A query that combines calibrated sub-query probability scores via log-odds fusion. Sub-queries
 * are expected to produce scores in (0, 1), for example from {@link BayesianScoreQuery} wrapping a
 * BM25 query.
 *
 * <p>The combination formula resolves the shrinkage problem of naive probabilistic AND by:
 *
 * <ol>
 *   <li>Converting each sub-score to log-odds: logit(p) = log(p / (1 - p))
 *   <li>Computing the mean log-odds across all clauses (non-matching contribute 0 = neutral)
 *   <li>Applying multiplicative confidence scaling: meanLogit * n^alpha
 *   <li>Converting back to probability via sigmoid
 * </ol>
 *
 * <p>The alpha parameter controls the confidence scaling exponent. The default alpha=0.5 implements
 * the sqrt(n) scaling law from "From Bayesian Inference to Neural Computation".
 *
 * <p>Optional per-signal weights enable weighted Log-OP (Logarithmic Opinion Pooling) where each
 * signal's log-odds contribution is scaled by its reliability weight. Weights must be non-negative
 * and sum to 1. When weights are provided, the scoring formula becomes {@code sigmoid(n^alpha *
 * sum(w_i * logit(p_i)))} instead of the uniform mean. Negative evidence logits remain internal;
 * the final score is always non-negative because it is a sigmoid probability.
 *
 * <p>Per-signal base rates can optionally be removed from posterior logits before pooling, and a
 * target base rate can be added exactly once after pooling. Softplus gating remains available as an
 * explicit option, but is not used by default because it discards negative evidence.
 *
 * @see LogOddsFusionScorer
 * @lucene.experimental
 */
public final class LogOddsFusionQuery extends Query implements Iterable<Query> {

  /** Optional transformation of evidence logits before they are pooled. */
  public enum Gating {
    /** Preserve positive and negative evidence exactly. */
    NONE,
    /** Map all evidence to non-negative values using {@code log(1 + exp(x))}. */
    SOFTPLUS
  }

  private final Multiset<Query> clauses = new Multiset<>();
  private final List<Query> orderedClauses;
  private final float alpha;
  private final float[] signalWeights;
  private final float[] logitMin;
  private final float[] logitMax;
  private final Gating gating;
  private final float[] signalBaseRates;
  private final float baseRate;

  /**
   * Creates a new LogOddsFusionQuery with per-signal weights and optional logit normalization.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param weights per-signal weights (must be non-negative, finite, and sum to 1.0), or null for
   *     uniform weighting
   * @param logitMin per-signal evidence-logit lower bounds for min-max normalization, or null for
   *     no normalization
   * @param logitMax per-signal evidence-logit upper bounds for min-max normalization, or null for
   *     no normalization
   * @throws IllegalArgumentException if alpha is not in [0, 1], or weights/bounds are invalid
   */
  public LogOddsFusionQuery(
      Collection<? extends Query> clauses,
      float alpha,
      float[] weights,
      float[] logitMin,
      float[] logitMax) {
    this(clauses, alpha, weights, logitMin, logitMax, Gating.NONE, null, 0f);
  }

  /**
   * Creates a new LogOddsFusionQuery with explicit gating.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent
   * @param weights per-signal weights, or null for uniform weighting
   * @param gating evidence-logit gating; {@link Gating#NONE} preserves exact log-odds
   */
  public LogOddsFusionQuery(
      Collection<? extends Query> clauses, float alpha, float[] weights, Gating gating) {
    this(clauses, alpha, weights, null, null, gating, null, 0f);
  }

  /**
   * Creates a new LogOddsFusionQuery that removes per-signal base rates and adds a target base rate
   * exactly once.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent
   * @param weights per-signal weights, or null for uniform weighting
   * @param signalBaseRates per-signal posterior base rates in (0, 1), with 0 disabling subtraction,
   *     or null when inputs contain prior-free evidence
   * @param baseRate target base rate in (0, 1), or 0 for a neutral prior
   */
  public LogOddsFusionQuery(
      Collection<? extends Query> clauses,
      float alpha,
      float[] weights,
      float[] signalBaseRates,
      float baseRate) {
    this(clauses, alpha, weights, null, null, Gating.NONE, signalBaseRates, baseRate);
  }

  /**
   * Creates a fully configured LogOddsFusionQuery.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent in [0, 1]
   * @param weights per-signal weights, or null for uniform weighting
   * @param logitMin per-signal evidence-logit lower bounds, or null
   * @param logitMax per-signal evidence-logit upper bounds, or null
   * @param gating optional evidence-logit gating; must be {@link Gating#NONE} when normalization
   *     bounds are supplied
   * @param signalBaseRates per-signal posterior base rates, or null
   * @param baseRate target base rate, or 0 for a neutral prior
   */
  public LogOddsFusionQuery(
      Collection<? extends Query> clauses,
      float alpha,
      float[] weights,
      float[] logitMin,
      float[] logitMax,
      Gating gating,
      float[] signalBaseRates,
      float baseRate) {
    Objects.requireNonNull(clauses, "Collection of Queries must not be null");
    this.orderedClauses = new ArrayList<>(clauses.size());
    for (Query clause : clauses) {
      this.orderedClauses.add(Objects.requireNonNull(clause, "clauses must not contain null"));
    }
    this.clauses.addAll(this.orderedClauses);
    if (Float.isFinite(alpha) == false || alpha < 0 || alpha > 1) {
      throw new IllegalArgumentException("alpha must be in [0, 1], got " + alpha);
    }
    this.alpha = alpha;
    if (weights != null) {
      if (weights.length != orderedClauses.size()) {
        throw new IllegalArgumentException(
            "weights length "
                + weights.length
                + " must equal clauses size "
                + orderedClauses.size());
      }
      double sum = 0;
      for (float w : weights) {
        if (Float.isFinite(w) == false || w < 0) {
          throw new IllegalArgumentException("weights must be non-negative and finite, got " + w);
        }
        sum += w;
      }
      if (Math.abs(sum - 1.0f) > 1e-3f) {
        throw new IllegalArgumentException("weights must sum to 1.0, got " + sum);
      }
      this.signalWeights = weights.clone();
    } else {
      this.signalWeights = null;
    }
    if ((logitMin == null) != (logitMax == null)) {
      throw new IllegalArgumentException("logitMin and logitMax must either both be null or set");
    }
    if (logitMin != null) {
      if (logitMin.length != orderedClauses.size()) {
        throw new IllegalArgumentException(
            "logitMin length "
                + logitMin.length
                + " must equal clauses size "
                + orderedClauses.size());
      }
      if (logitMax.length != orderedClauses.size()) {
        throw new IllegalArgumentException(
            "logitMax length "
                + logitMax.length
                + " must equal clauses size "
                + orderedClauses.size());
      }
      for (int i = 0; i < logitMin.length; i++) {
        if (Float.isFinite(logitMin[i]) == false
            || Float.isFinite(logitMax[i]) == false
            || logitMin[i] >= logitMax[i]) {
          throw new IllegalArgumentException(
              "logit bounds must be finite and min < max at index "
                  + i
                  + ", got ["
                  + logitMin[i]
                  + ", "
                  + logitMax[i]
                  + "]");
        }
      }
      this.logitMin = logitMin.clone();
      this.logitMax = logitMax.clone();
    } else {
      this.logitMin = null;
      this.logitMax = null;
    }
    this.gating = Objects.requireNonNull(gating, "gating must not be null");
    if (logitMin != null && gating != Gating.NONE) {
      throw new IllegalArgumentException(
          "gating must be NONE when logit normalization bounds are supplied");
    }
    if (signalBaseRates != null) {
      if (signalBaseRates.length != orderedClauses.size()) {
        throw new IllegalArgumentException(
            "signalBaseRates length "
                + signalBaseRates.length
                + " must equal clauses size "
                + orderedClauses.size());
      }
      for (int i = 0; i < signalBaseRates.length; i++) {
        validateBaseRate("signalBaseRates[" + i + "]", signalBaseRates[i]);
      }
      this.signalBaseRates = signalBaseRates.clone();
    } else {
      this.signalBaseRates = null;
    }
    validateBaseRate("baseRate", baseRate);
    this.baseRate = baseRate;
  }

  /**
   * Creates a new LogOddsFusionQuery with per-signal weights and exact log-odds aggregation.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param weights per-signal weights, or null for uniform weighting
   * @throws IllegalArgumentException if alpha is not in [0, 1], or weights are invalid
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses, float alpha, float[] weights) {
    this(clauses, alpha, weights, null, null);
  }

  /**
   * Creates a new LogOddsFusionQuery with uniform weighting and exact log-odds aggregation.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @throws IllegalArgumentException if alpha is not in [0, 1]
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses, float alpha) {
    this(clauses, alpha, null, null, null);
  }

  /**
   * Creates a new LogOddsFusionQuery with default alpha=0.5, uniform weighting, and exact log-odds
   * aggregation.
   *
   * @param clauses the sub-queries to combine
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses) {
    this(clauses, 0.5f, null, null, null);
  }

  @Override
  public Iterator<Query> iterator() {
    return getClauses().iterator();
  }

  /** Returns the clauses. */
  public Collection<Query> getClauses() {
    return Collections.unmodifiableCollection(orderedClauses);
  }

  /** Returns the alpha (confidence scaling exponent). */
  public float getAlpha() {
    return alpha;
  }

  /**
   * Returns a copy of the per-signal weights, or null if uniform weighting is used.
   *
   * <p>When non-null, the i-th element is the weight for the i-th clause in the order returned by
   * {@link #getClauses()}.
   */
  public float[] getWeights() {
    return signalWeights != null ? signalWeights.clone() : null;
  }

  /** Returns the configured evidence-logit gating. */
  public Gating getGating() {
    return gating;
  }

  /** Returns a copy of the per-signal base rates, or null if no priors are subtracted. */
  public float[] getSignalBaseRates() {
    return signalBaseRates != null ? signalBaseRates.clone() : null;
  }

  /** Returns the target base rate, or 0 when the target prior is neutral. */
  public float getBaseRate() {
    return baseRate;
  }

  private static void validateBaseRate(String name, float value) {
    if (Float.isFinite(value) == false || value < 0f || value >= 1f) {
      throw new IllegalArgumentException(name + " must be finite and in [0, 1), got " + value);
    }
  }

  /** Weight for LogOddsFusionQuery. */
  protected class LogOddsFusionWeight extends Weight {

    protected final ArrayList<Weight> weights = new ArrayList<>();
    private final ScoreMode scoreMode;
    private final float boost;

    public LogOddsFusionWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(LogOddsFusionQuery.this);
      for (Query clauseQuery : orderedClauses) {
        weights.add(searcher.createWeight(clauseQuery, scoreMode, 1f));
      }
      this.scoreMode = scoreMode;
      this.boost = boost;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      List<Matches> mis = new ArrayList<>();
      for (Weight weight : weights) {
        Matches mi = weight.matches(context, doc);
        if (mi != null) {
          mis.add(mi);
        }
      }
      return MatchesUtils.fromSubMatches(mis);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
      List<Float> activeWeightsList = signalWeights != null ? new ArrayList<>() : null;
      List<Float> activeLogitMinList = logitMin != null ? new ArrayList<>() : null;
      List<Float> activeLogitMaxList = logitMax != null ? new ArrayList<>() : null;
      List<Float> activeBaseRatesList = signalBaseRates != null ? new ArrayList<>() : null;

      for (int i = 0; i < weights.size(); i++) {
        ScorerSupplier ss = weights.get(i).scorerSupplier(context);
        if (ss != null) {
          scorerSuppliers.add(ss);
          if (activeWeightsList != null) {
            activeWeightsList.add(signalWeights[i]);
          }
          if (activeLogitMinList != null) {
            activeLogitMinList.add(logitMin[i]);
            activeLogitMaxList.add(logitMax[i]);
          }
          if (activeBaseRatesList != null) {
            activeBaseRatesList.add(signalBaseRates[i]);
          }
        }
      }

      if (scorerSuppliers.isEmpty()) {
        return null;
      }

      final int totalClauses = orderedClauses.size();
      final float[] activeWeights = toFloatArray(activeWeightsList);
      final float[] activeMin = toFloatArray(activeLogitMinList);
      final float[] activeMax = toFloatArray(activeLogitMaxList);
      final float[] activeBaseRates = toFloatArray(activeBaseRatesList);

      return new ScorerSupplier() {

        private long cost = -1;

        @Override
        public Scorer get(long leadCost) throws IOException {
          List<Scorer> scorers = new ArrayList<>();
          for (ScorerSupplier ss : scorerSuppliers) {
            scorers.add(ss.get(leadCost));
          }
          LogOddsFusionScoreFunction scoreFunction =
              new LogOddsFusionScoreFunction(
                  totalClauses,
                  alpha,
                  activeWeights,
                  activeMin,
                  activeMax,
                  gating,
                  activeBaseRates,
                  baseRate,
                  boost);
          return LogOddsFusionScorer.create(scorers, scoreFunction, scoreMode, leadCost);
        }

        @Override
        public long cost() {
          if (cost == -1) {
            long cost = 0;
            for (ScorerSupplier ss : scorerSuppliers) {
              cost += ss.cost();
            }
            this.cost = cost;
          }
          return cost;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      if (weights.size()
          > AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
        return false;
      }
      for (Weight w : weights) {
        if (w.isCacheable(ctx) == false) return false;
      }
      return true;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      boolean match = false;
      List<Explanation> subsOnMatch = new ArrayList<>();
      List<Explanation> subsOnNoMatch = new ArrayList<>();
      double logitSum = 0;
      int totalClauses = weights.size();
      LogOddsFusionScoreFunction scoreFunction =
          new LogOddsFusionScoreFunction(
              totalClauses,
              alpha,
              signalWeights,
              logitMin,
              logitMax,
              gating,
              signalBaseRates,
              baseRate,
              boost);

      for (int i = 0; i < weights.size(); i++) {
        Explanation e = weights.get(i).explain(context, doc);
        if (e.isMatch()) {
          match = true;
          subsOnMatch.add(e);
          logitSum += scoreFunction.contribution(e.getValue().floatValue(), i);
        } else {
          subsOnNoMatch.add(e);
        }
      }

      if (match) {
        String description;
        if (signalWeights != null) {
          description =
              "weighted log-odds fusion, computed as"
                  + " sigmoid(targetPrior + weightedEvidenceLogit * n^alpha) from:";
        } else {
          description =
              "log-odds fusion, computed as"
                  + " sigmoid(targetPrior + meanEvidenceLogit * n^alpha) from:";
        }
        Explanation probabilityExplanation =
            Explanation.match(scoreFunction.probability(logitSum), description, subsOnMatch);
        if (boost == 1f) {
          return probabilityExplanation;
        }
        return Explanation.match(
            scoreFunction.score(logitSum),
            "product of:",
            probabilityExplanation,
            Explanation.match(boost, "boost"));
      } else {
        return Explanation.noMatch("No matching clause", subsOnNoMatch);
      }
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new LogOddsFusionWeight(searcher, scoreMode, boost);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (orderedClauses.isEmpty()) {
      return new MatchNoDocsQuery("empty LogOddsFusionQuery");
    }

    if (orderedClauses.size() == 1 && isSingleClauseIdentity()) {
      return orderedClauses.get(0);
    }

    boolean actuallyRewritten = false;
    boolean allMatchNoDocs = true;
    List<Query> rewrittenClauses = new ArrayList<>();

    for (Query sub : orderedClauses) {
      Query rewrittenSub = sub.rewrite(indexSearcher);
      if (rewrittenSub != sub) {
        actuallyRewritten = true;
      }
      if (rewrittenSub.getClass() != MatchNoDocsQuery.class) {
        allMatchNoDocs = false;
      }
      // A non-matching signal contributes neutral evidence, but it still belongs to the declared
      // signal set and therefore still affects n^alpha and the uniform denominator. Keep it, along
      // with its parallel metadata, instead of changing the fusion formula during rewrite.
      rewrittenClauses.add(rewrittenSub);
    }

    if (allMatchNoDocs) {
      return new MatchNoDocsQuery("empty LogOddsFusionQuery");
    }
    if (actuallyRewritten == false) {
      return super.rewrite(indexSearcher);
    }

    LogOddsFusionQuery rewritten =
        new LogOddsFusionQuery(
            rewrittenClauses,
            alpha,
            signalWeights,
            logitMin,
            logitMax,
            gating,
            signalBaseRates,
            baseRate);
    if (rewrittenClauses.size() == 1 && rewritten.isSingleClauseIdentity()) {
      return rewrittenClauses.get(0);
    }
    return rewritten;
  }

  private boolean isSingleClauseIdentity() {
    return orderedClauses.size() == 1
        && gating == Gating.NONE
        && logitMin == null
        && signalBaseRates == null
        && baseRate == 0f;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    for (Query q : orderedClauses) {
      q.visit(v);
    }
  }

  @Override
  public String toString(String field) {
    String base =
        this.orderedClauses.stream()
            .map(
                subquery -> {
                  if (subquery instanceof BooleanQuery) {
                    return "(" + subquery.toString(field) + ")";
                  }
                  return subquery.toString(field);
                })
            .collect(Collectors.joining(" & ", "LogOdds(", ")^" + alpha));
    StringBuilder result = new StringBuilder(base);
    if (signalWeights != null) {
      result.append(" w=").append(Arrays.toString(signalWeights));
    }
    if (logitMin != null) {
      result
          .append(" logitMin=")
          .append(Arrays.toString(logitMin))
          .append(" logitMax=")
          .append(Arrays.toString(logitMax));
    }
    if (gating != Gating.NONE) {
      result.append(" gating=").append(gating);
    }
    if (signalBaseRates != null) {
      result.append(" signalBaseRates=").append(Arrays.toString(signalBaseRates));
    }
    if (baseRate > 0f) {
      result.append(" baseRate=").append(baseRate);
    }
    return result.toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LogOddsFusionQuery other) {
    return Float.floatToIntBits(alpha) == Float.floatToIntBits(other.alpha)
        && Arrays.equals(signalWeights, other.signalWeights)
        && Arrays.equals(logitMin, other.logitMin)
        && Arrays.equals(logitMax, other.logitMax)
        && gating == other.gating
        && Arrays.equals(signalBaseRates, other.signalBaseRates)
        && Float.floatToIntBits(baseRate) == Float.floatToIntBits(other.baseRate)
        && (hasPerSignalMetadata()
            ? Objects.equals(orderedClauses, other.orderedClauses)
            : Objects.equals(clauses, other.clauses));
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + Float.floatToIntBits(alpha);
    h = 31 * h + (hasPerSignalMetadata() ? orderedClauses.hashCode() : clauses.hashCode());
    h = 31 * h + Arrays.hashCode(signalWeights);
    h = 31 * h + Arrays.hashCode(logitMin);
    h = 31 * h + Arrays.hashCode(logitMax);
    h = 31 * h + gating.hashCode();
    h = 31 * h + Arrays.hashCode(signalBaseRates);
    h = 31 * h + Float.floatToIntBits(baseRate);
    return h;
  }

  private boolean hasPerSignalMetadata() {
    return signalWeights != null || logitMin != null || signalBaseRates != null;
  }

  private static float[] toFloatArray(List<Float> list) {
    if (list == null) {
      return null;
    }
    float[] result = new float[list.size()];
    for (int i = 0; i < list.size(); i++) {
      result[i] = list.get(i);
    }
    return result;
  }
}
