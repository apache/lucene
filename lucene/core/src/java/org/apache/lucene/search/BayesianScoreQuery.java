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
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A query wrapper that transforms the inner query's score into a calibrated probability via sigmoid
 * calibration: {@code P = sigmoid(alpha * (score - beta))}.
 *
 * <p>This implements the query-level Bayesian transform from "Bayesian BM25": the inner query
 * (typically a multi-term BooleanQuery with BM25Similarity) produces a raw score, and this wrapper
 * maps it to a probability in (0, 1) suitable for combination with other probability signals via
 * {@link LogOddsFusionQuery}.
 *
 * <p>The alpha parameter controls the sigmoid steepness (score sensitivity), and beta controls the
 * midpoint (decision boundary). These can be set manually or estimated from the score distribution.
 *
 * @lucene.experimental
 */
public final class BayesianScoreQuery extends Query {

  private final Query query;
  private final float alpha;
  private final float beta;

  /**
   * Creates a BayesianScoreQuery.
   *
   * @param query the inner query whose scores will be transformed
   * @param alpha sigmoid steepness (must be positive and finite)
   * @param beta sigmoid midpoint (must be finite)
   */
  public BayesianScoreQuery(Query query, float alpha, float beta) {
    this.query = Objects.requireNonNull(query);
    if (Float.isFinite(alpha) == false || alpha <= 0) {
      throw new IllegalArgumentException("alpha must be a positive finite value, got " + alpha);
    }
    if (Float.isFinite(beta) == false) {
      throw new IllegalArgumentException("beta must be a finite value, got " + beta);
    }
    this.alpha = alpha;
    this.beta = beta;
  }

  /** Returns the wrapped query. */
  public Query getQuery() {
    return query;
  }

  /** Returns the sigmoid steepness parameter. */
  public float getAlpha() {
    return alpha;
  }

  /** Returns the sigmoid midpoint parameter. */
  public float getBeta() {
    return beta;
  }

  static float sigmoid(float x) {
    if (x >= 0) {
      return (float) (1.0 / (1.0 + Math.exp(-x)));
    } else {
      double expX = Math.exp(x);
      return (float) (expX / (1.0 + expX));
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight innerWeight = query.createWeight(searcher, scoreMode, boost);
    if (scoreMode.needsScores() == false) {
      return innerWeight;
    }
    return new BayesianScoreWeight(this, innerWeight);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = query.rewrite(indexSearcher);
    if (rewritten instanceof MatchNoDocsQuery) {
      return rewritten;
    }
    if (rewritten != query) {
      return new BayesianScoreQuery(rewritten, alpha, beta);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public String toString(String field) {
    return "BayesianScore(" + query.toString(field) + ", alpha=" + alpha + ", beta=" + beta + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(BayesianScoreQuery other) {
    return query.equals(other.query)
        && Float.floatToIntBits(alpha) == Float.floatToIntBits(other.alpha)
        && Float.floatToIntBits(beta) == Float.floatToIntBits(other.beta);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + query.hashCode();
    h = 31 * h + Float.floatToIntBits(alpha);
    h = 31 * h + Float.floatToIntBits(beta);
    return h;
  }

  private class BayesianScoreWeight extends Weight {
    private final Weight innerWeight;

    BayesianScoreWeight(Query query, Weight innerWeight) {
      super(query);
      this.innerWeight = innerWeight;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return innerWeight.matches(context, doc);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Explanation innerExpl = innerWeight.explain(context, doc);
      if (innerExpl.isMatch() == false) {
        return innerExpl;
      }
      float innerScore = innerExpl.getValue().floatValue();
      float transformed = sigmoid(alpha * (innerScore - beta));
      return Explanation.match(
          transformed,
          "sigmoid calibration, computed as sigmoid(alpha * (score - beta)) from:",
          innerExpl,
          Explanation.match(alpha, "alpha, sigmoid steepness"),
          Explanation.match(beta, "beta, sigmoid midpoint"));
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      ScorerSupplier innerSupplier = innerWeight.scorerSupplier(context);
      if (innerSupplier == null) {
        return null;
      }
      return new ScorerSupplier() {
        @Override
        public Scorer get(long leadCost) throws IOException {
          Scorer innerScorer = innerSupplier.get(leadCost);
          return new BayesianScoreScorer(innerScorer);
        }

        @Override
        public long cost() {
          return innerSupplier.cost();
        }

        @Override
        public void setTopLevelScoringClause() {
          innerSupplier.setTopLevelScoringClause();
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return innerWeight.isCacheable(ctx);
    }
  }

  private class BayesianScoreScorer extends FilterScorer {

    BayesianScoreScorer(Scorer in) {
      super(in);
    }

    @Override
    public float score() throws IOException {
      float innerScore = in.score();
      return sigmoid(alpha * (innerScore - beta));
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      float innerMax = in.getMaxScore(upTo);
      // sigmoid is monotone, so max(sigmoid(f(x))) = sigmoid(max(f(x)))
      return sigmoid(alpha * (innerMax - beta));
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      // Invert the sigmoid to get the minimum inner score needed:
      // minScore = sigmoid(alpha * (innerScore - beta))
      // => alpha * (innerScore - beta) = logit(minScore)
      // => innerScore = logit(minScore) / alpha + beta
      if (minScore > 0f && minScore < 1f) {
        float clamped = Math.max(1e-7f, Math.min(1f - 1e-7f, minScore));
        float logitMin = (float) Math.log(clamped / (1f - clamped));
        float innerMin = logitMin / alpha + beta;
        in.setMinCompetitiveScore(Math.max(0f, innerMin));
      }
    }
  }
}
