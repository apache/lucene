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
package org.apache.lucene.queries.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScoringRewrite;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.util.PriorityQueue;

/**
 * Wraps any {@link MultiTermQuery} as a {@link SpanQuery}, so it can be nested within other
 * SpanQuery classes.
 *
 * <p>The query is rewritten by default to a {@link SpanOrQuery} containing the expanded terms, but
 * this can be customized.
 *
 * <p>Example:
 *
 * <blockquote>
 *
 * <pre><code class="language-java">
 * WildcardQuery wildcard = new WildcardQuery(new Term("field", "bro?n"));
 * SpanQuery spanWildcard = new SpanMultiTermQueryWrapper&lt;WildcardQuery&gt;(wildcard);
 * // do something with spanWildcard, such as use it in a SpanFirstQuery
 * </code></pre>
 *
 * </blockquote>
 */
public class SpanMultiTermQueryWrapper<Q extends MultiTermQuery> extends SpanQuery {

  protected final Q query;
  private SpanRewriteMethod rewriteMethod;

  /**
   * Create a new SpanMultiTermQueryWrapper.
   *
   * @param query Query to wrap.
   */
  public SpanMultiTermQueryWrapper(Q query) {
    this.query = Objects.requireNonNull(query);
    this.rewriteMethod = selectRewriteMethod(query);
  }

  private static SpanRewriteMethod selectRewriteMethod(MultiTermQuery query) {
    MultiTermQuery.RewriteMethod method = query.getRewriteMethod();
    if (method instanceof TopTermsRewrite) {
      final int pqsize = ((TopTermsRewrite<?>) method).getSize();
      return new TopTermsSpanBooleanQueryRewrite(pqsize);
    } else {
      return SCORING_SPAN_QUERY_REWRITE;
    }
  }

  /** Expert: returns the rewriteMethod */
  public final SpanRewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /** Expert: sets the rewrite method. This only makes sense to be a span rewrite method. */
  public final void setRewriteMethod(SpanRewriteMethod rewriteMethod) {
    this.rewriteMethod = rewriteMethod;
  }

  @Override
  public String getField() {
    return query.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    throw new IllegalArgumentException("Rewrite first!");
  }

  /** Returns the wrapped query */
  public Query getWrappedQuery() {
    return query;
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append("SpanMultiTermQueryWrapper(");
    // NOTE: query.toString must be placed in a temp local to avoid compile errors on Java 8u20
    // see
    // https://bugs.openjdk.java.net/browse/JDK-8056984?page=com.atlassian.streams.streams-jira-plugin:activity-stream-issue-tab
    String queryStr = query.toString(field);
    builder.append(queryStr);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    return rewriteMethod.rewrite(indexSearcher, query);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(query.getField())) {
      query.visit(visitor.getSubVisitor(Occur.MUST, this));
    }
  }

  @Override
  public int hashCode() {
    return classHash() * 31 + query.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && query.equals(((SpanMultiTermQueryWrapper<?>) other).query);
  }

  /** Abstract class that defines how the query is rewritten. */
  public abstract static class SpanRewriteMethod extends MultiTermQuery.RewriteMethod {
    @Override
    public abstract SpanQuery rewrite(IndexSearcher indexSearcher, MultiTermQuery query)
        throws IOException;
  }

  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a {@link Occur#SHOULD}
   * clause in a BooleanQuery, and keeps the scores as computed by the query.
   *
   * @see #setRewriteMethod
   */
  public static final SpanRewriteMethod SCORING_SPAN_QUERY_REWRITE =
      new SpanRewriteMethod() {
        private final ScoringRewrite<List<SpanQuery>> delegate =
            new ScoringRewrite<>() {
              @Override
              protected List<SpanQuery> getTopLevelBuilder() {
                return new ArrayList<>();
              }

              @Override
              protected Query build(List<SpanQuery> builder) {
                return new SpanOrQuery(builder.toArray(SpanQuery[]::new));
              }

              @Override
              protected void checkMaxClauseCount(int count) {
                // we accept all terms as SpanOrQuery has no limits
              }

              @Override
              protected void addClause(
                  List<SpanQuery> topLevel,
                  Term term,
                  int docCount,
                  float boost,
                  TermStates states) {
                final SpanTermQuery q = new SpanTermQuery(term, states);
                topLevel.add(q);
              }
            };

        @Override
        public SpanQuery rewrite(IndexSearcher indexSearcher, MultiTermQuery query)
            throws IOException {
          return (SpanQuery) delegate.rewrite(indexSearcher, query);
        }
      };

  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a {@link Occur#SHOULD}
   * clause in a BooleanQuery, and keeps the scores as computed by the query.
   *
   * <p>This rewrite method only uses the top scoring terms so it will not overflow the boolean max
   * clause count.
   *
   * @see #setRewriteMethod
   */
  public static final class TopTermsSpanBooleanQueryRewrite extends SpanRewriteMethod {
    private final TopTermsRewrite<List<SpanQuery>> delegate;

    /** Create a TopTermsSpanBooleanQueryRewrite for at most <code>size</code> terms. */
    public TopTermsSpanBooleanQueryRewrite(int size) {
      delegate =
          new TopTermsRewrite<>(size) {
            @Override
            protected int getMaxSize() {
              return Integer.MAX_VALUE;
            }

            @Override
            protected List<SpanQuery> getTopLevelBuilder() {
              return new ArrayList<>();
            }

            @Override
            protected Query build(List<SpanQuery> builder) {
              return new SpanOrQuery(builder.toArray(SpanQuery[]::new));
            }

            @Override
            protected void addClause(
                List<SpanQuery> topLevel, Term term, int docFreq, float boost, TermStates states) {
              final SpanTermQuery q = new SpanTermQuery(term, states);
              topLevel.add(q);
            }
          };
    }

    /** return the maximum priority queue size */
    public int getSize() {
      return delegate.getSize();
    }

    @Override
    public SpanQuery rewrite(IndexSearcher indexSearcher, MultiTermQuery query) throws IOException {
      return (SpanQuery) delegate.rewrite(indexSearcher, query);
    }

    @Override
    public int hashCode() {
      return 31 * delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final TopTermsSpanBooleanQueryRewrite other = (TopTermsSpanBooleanQueryRewrite) obj;
      return delegate.equals(other.delegate);
    }
  }

  /**
   * A rewrite method that translates each term into a SpanTermQuery within a {@link SpanOrQuery},
   * but retains only the most frequent terms so it will not overflow the boolean max clause count.
   *
   * <p>Unlike {@link TopTermsSpanBooleanQueryRewrite}, which keeps the top terms by boost, this
   * method ranks terms by their index statistics &mdash; document frequency, optionally breaking
   * ties by total term frequency &mdash; and keeps the {@code maxSize} most frequent ones.
   *
   * @see #setRewriteMethod
   */
  public static final class FrequentTermsSpanBooleanQueryRewrite extends SpanRewriteMethod {
    /*
    This is built on ScoringRewrite rather than TopTermsRewrite because TopTermsRewrite caps terms while collecting
    them per segment, which requires a ranking key that is stable across segments (such as boost). Document and total
    term frequencies are only per-segment during collection, so instead ScoringRewrite first collects every matching
    term — aggregating TermStates across all segments — and this rewrite then retains the maxSize most frequent using
    those aggregated statistics.
    */

    /** A collected term paired with its {@link TermStates}, used for ranking. */
    public static final class ScoreTerm {
      public final Term term;
      public final TermStates termState;

      public ScoreTerm(Term term, TermStates termState) {
        this.term = term;
        this.termState = termState;
      }
    }

    /** Ranks terms by document frequency; the least frequent term is dropped first. */
    public static final Comparator<ScoreTerm> DF_ORDER =
        Comparator.comparingInt(st -> st.termState.docFreq());

    /** Ranks terms by document frequency, breaking ties by total term frequency. */
    public static final Comparator<ScoreTerm> DF_THEN_TTF_ORDER =
        Comparator.comparingInt((ScoreTerm st) -> st.termState.docFreq())
            .thenComparingLong(st -> st.termState.totalTermFreq());

    private final int maxSize;
    private final Comparator<ScoreTerm> order;
    private final ScoringRewrite<PriorityQueue<ScoreTerm>> delegate;

    /** Create a rewrite that keeps at most {@code maxSize} terms, ranked by {@link #DF_ORDER}. */
    public FrequentTermsSpanBooleanQueryRewrite(int maxSize) {
      this(maxSize, DF_ORDER);
    }

    /** Create a rewrite that keeps at most {@code maxSize} terms, ranked by {@code order}. */
    public FrequentTermsSpanBooleanQueryRewrite(int maxSize, Comparator<ScoreTerm> order) {
      this.maxSize = maxSize;
      this.order = order;
      this.delegate =
          new ScoringRewrite<>() {
            @Override
            protected PriorityQueue<ScoreTerm> getTopLevelBuilder() {
              return PriorityQueue.usingComparator(maxSize, order);
            }

            @Override
            protected Query build(PriorityQueue<ScoreTerm> builder) {
              SpanQuery[] result = new SpanQuery[builder.size()];
              for (int pos = 0; pos < result.length; pos++) {
                ScoreTerm st = builder.pop();
                result[pos] = new SpanTermQuery(st.term, st.termState);
              }
              return new SpanOrQuery(result);
            }

            @Override
            protected void checkMaxClauseCount(int count) {
              // no-op: the priority queue already bounds the number of retained terms
            }

            @Override
            protected void addClause(
                PriorityQueue<ScoreTerm> topLevel,
                Term term,
                int docCount,
                float boost,
                TermStates states) {
              topLevel.insertWithOverflow(new ScoreTerm(term, states));
            }
          };
    }

    /** Returns the maximum number of terms retained by this rewrite. */
    public int getMaxSize() {
      return maxSize;
    }

    @Override
    public SpanQuery rewrite(IndexSearcher indexSearcher, MultiTermQuery query) throws IOException {
      return (SpanQuery) delegate.rewrite(indexSearcher, query);
    }

    @Override
    public int hashCode() {
      return 31 * (31 + maxSize) + order.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      final FrequentTermsSpanBooleanQueryRewrite other = (FrequentTermsSpanBooleanQueryRewrite) obj;
      return maxSize == other.maxSize && order.equals(other.order);
    }
  }
}
