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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A query that combines sub-query probability scores via log-odds fusion. Sub-queries are
 * expected to produce scores in (0, 1) representing probabilities (e.g., from {@link
 * BayesianScoreQuery} wrapping a BM25 query, or KNN cosine similarity).
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
 * @see LogOddsFusionScorer
 * @lucene.experimental
 */
public final class LogOddsFusionQuery extends Query implements Iterable<Query> {

  private final Multiset<Query> clauses = new Multiset<>();
  private final List<Query> orderedClauses;
  private final float alpha;

  /**
   * Creates a new LogOddsFusionQuery.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @throws IllegalArgumentException if alpha is not in [0, 1]
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses, float alpha) {
    Objects.requireNonNull(clauses, "Collection of Queries must not be null");
    if (Float.isNaN(alpha) || alpha < 0 || alpha > 1) {
      throw new IllegalArgumentException("alpha must be in [0, 1], got " + alpha);
    }
    this.alpha = alpha;
    this.clauses.addAll(clauses);
    this.orderedClauses = new ArrayList<>(clauses);
  }

  /**
   * Creates a new LogOddsFusionQuery with default alpha=0.5 (sqrt(n) scaling law).
   *
   * @param clauses the sub-queries to combine
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses) {
    this(clauses, 0.5f);
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

  /** Weight for LogOddsFusionQuery. */
  protected class LogOddsFusionWeight extends Weight {

    protected final ArrayList<Weight> weights = new ArrayList<>();
    private final ScoreMode scoreMode;

    public LogOddsFusionWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(LogOddsFusionQuery.this);
      for (Query clauseQuery : clauses) {
        weights.add(searcher.createWeight(clauseQuery, scoreMode, boost));
      }
      this.scoreMode = scoreMode;
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
      for (Weight w : weights) {
        ScorerSupplier ss = w.scorerSupplier(context);
        if (ss != null) {
          scorerSuppliers.add(ss);
        }
      }

      if (scorerSuppliers.isEmpty()) {
        return null;
      } else if (scorerSuppliers.size() == 1) {
        return scorerSuppliers.get(0);
      } else {
        final int totalClauses = clauses.size();
        return new ScorerSupplier() {

          private long cost = -1;

          @Override
          public Scorer get(long leadCost) throws IOException {
            List<Scorer> scorers = new ArrayList<>();
            for (ScorerSupplier ss : scorerSuppliers) {
              scorers.add(ss.get(leadCost));
            }
            return new LogOddsFusionScorer(scorers, totalClauses, alpha, scoreMode, leadCost);
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

          @Override
          public void setTopLevelScoringClause() {
            for (ScorerSupplier ss : scorerSuppliers) {
              ss.setTopLevelScoringClause();
            }
          }
        };
      }
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

      for (Weight wt : weights) {
        Explanation e = wt.explain(context, doc);
        if (e.isMatch()) {
          match = true;
          subsOnMatch.add(e);
          float subScore = e.getValue().floatValue();
          logitSum += LogOddsFusionScorer.softplus(LogOddsFusionScorer.logit(subScore));
        } else if (match == false) {
          subsOnNoMatch.add(e);
        }
      }

      if (match) {
        // Non-matching contribute logit(0.5) = 0
        float meanLogit = (float) (logitSum / totalClauses);
        float scalingFactor = (float) Math.pow(totalClauses, alpha);
        float scaledLogit = meanLogit * scalingFactor;
        float score = LogOddsFusionScorer.sigmoid(scaledLogit);

        return Explanation.match(
            score,
            "log-odds fusion, computed as sigmoid(meanLogit * n^alpha) from:",
            subsOnMatch);
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
    if (clauses.isEmpty()) {
      return new MatchNoDocsQuery("empty LogOddsFusionQuery");
    }

    if (clauses.size() == 1) {
      return clauses.iterator().next();
    }

    boolean actuallyRewritten = false;
    List<Query> rewrittenClauses = new ArrayList<>();
    for (Query sub : clauses) {
      Query rewrittenSub = sub.rewrite(indexSearcher);
      actuallyRewritten |= rewrittenSub != sub;
      rewrittenClauses.add(rewrittenSub);
    }

    if (actuallyRewritten) {
      return new LogOddsFusionQuery(rewrittenClauses, alpha);
    }

    return super.rewrite(indexSearcher);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    for (Query q : clauses) {
      q.visit(v);
    }
  }

  @Override
  public String toString(String field) {
    return this.orderedClauses.stream()
        .map(
            subquery -> {
              if (subquery instanceof BooleanQuery) {
                return "(" + subquery.toString(field) + ")";
              }
              return subquery.toString(field);
            })
        .collect(Collectors.joining(" & ", "LogOdds(", ")^" + alpha));
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LogOddsFusionQuery other) {
    return alpha == other.alpha && Objects.equals(clauses, other.clauses);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + Float.floatToIntBits(alpha);
    h = 31 * h + Objects.hashCode(clauses);
    return h;
  }
}
