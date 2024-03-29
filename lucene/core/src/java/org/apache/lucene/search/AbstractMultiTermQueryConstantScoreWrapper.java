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
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Contains functionality common to both {@link MultiTermQueryConstantScoreBlendedWrapper} and
 * {@link MultiTermQueryConstantScoreWrapper}. Internal implementation detail only. Not meant as an
 * extension point for users.
 *
 * @lucene.internal
 */
abstract class AbstractMultiTermQueryConstantScoreWrapper<Q extends MultiTermQuery> extends Query
    implements Accountable {
  // mtq that matches 16 terms or less will be executed as a regular disjunction
  static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

  protected final Q query;

  protected AbstractMultiTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public long ramBytesUsed() {
    if (query instanceof Accountable) {
      return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + ((Accountable) query).ramBytesUsed();
    }
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  public boolean equals(final Object other) {
    return sameClassAs(other)
        && query.equals(((AbstractMultiTermQueryConstantScoreWrapper<?>) other).query);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + query.hashCode();
  }

  /** Returns the encapsulated query */
  public Q getQuery() {
    return query;
  }

  /** Returns the field name for this query */
  public String getField() {
    return query.getField();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField())) {
      query.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
    }
  }

  protected static final class TermAndState {
    final BytesRef term;
    final TermState state;
    final int docFreq;
    final long totalTermFreq;

    TermAndState(BytesRef term, TermState state, int docFreq, long totalTermFreq) {
      this.term = term;
      this.state = state;
      this.docFreq = docFreq;
      this.totalTermFreq = totalTermFreq;
    }
  }

  protected static final class WeightOrDocIdSetIterator {
    final Weight weight;
    final DocIdSetIterator iterator;

    WeightOrDocIdSetIterator(Weight weight) {
      this.weight = Objects.requireNonNull(weight);
      this.iterator = null;
    }

    WeightOrDocIdSetIterator(DocIdSetIterator iterator) {
      this.iterator = iterator;
      this.weight = null;
    }
  }

  protected abstract static class RewritingWeight extends ConstantScoreWeight {
    private final MultiTermQuery q;
    private final ScoreMode scoreMode;
    private final IndexSearcher searcher;

    protected RewritingWeight(
        MultiTermQuery q, float boost, ScoreMode scoreMode, IndexSearcher searcher) {
      super(q, boost);
      this.q = q;
      this.scoreMode = scoreMode;
      this.searcher = searcher;
    }

    /**
     * Rewrite the query as either a {@link Weight} or a {@link DocIdSetIterator} wrapped in a
     * {@link WeightOrDocIdSetIterator}. Before this is called, the weight will attempt to "collect"
     * found terms up to a threshold. If fewer terms than the threshold are found, the query will
     * simply be rewritten into a {@link BooleanQuery} and this method will not be called. This will
     * only be called if it is determined there are more found terms. At the point this method is
     * invoked, {@code termsEnum} will be positioned on the next "uncollected" term. The terms that
     * were already collected will be in {@code collectedTerms}.
     */
    protected abstract WeightOrDocIdSetIterator rewriteInner(
        LeafReaderContext context,
        int fieldDocCount,
        Terms terms,
        TermsEnum termsEnum,
        List<TermAndState> collectedTerms)
        throws IOException;

    private WeightOrDocIdSetIterator rewrite(LeafReaderContext context, Terms terms)
        throws IOException {
      assert terms != null;

      final int fieldDocCount = terms.getDocCount();
      final TermsEnum termsEnum = q.getTermsEnum(terms);
      assert termsEnum != null;

      final List<TermAndState> collectedTerms = new ArrayList<>();
      if (collectTerms(fieldDocCount, termsEnum, collectedTerms)) {
        // build a boolean query
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (TermAndState t : collectedTerms) {
          final TermStates termStates = new TermStates(searcher.getTopReaderContext());
          termStates.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
          bq.add(new TermQuery(new Term(q.field, t.term), termStates), BooleanClause.Occur.SHOULD);
        }
        Query q = new ConstantScoreQuery(bq.build());
        final Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
        return new WeightOrDocIdSetIterator(weight);
      }

      // Too many terms to rewrite as a simple bq. Invoke rewriteInner logic to handle rewriting:
      return rewriteInner(context, fieldDocCount, terms, termsEnum, collectedTerms);
    }

    private boolean collectTerms(int fieldDocCount, TermsEnum termsEnum, List<TermAndState> terms)
        throws IOException {
      final int threshold =
          Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
      for (int i = 0; i < threshold; i++) {
        final BytesRef term = termsEnum.next();
        if (term == null) {
          return true;
        }
        TermState state = termsEnum.termState();
        int docFreq = termsEnum.docFreq();
        TermAndState termAndState =
            new TermAndState(BytesRef.deepCopyOf(term), state, docFreq, termsEnum.totalTermFreq());
        if (fieldDocCount == docFreq) {
          // If the term contains every document with a value for the field, we can ignore all
          // other terms:
          terms.clear();
          terms.add(termAndState);
          return true;
        }
        terms.add(termAndState);
      }
      return termsEnum.next() == null;
    }

    private Scorer scorerForIterator(DocIdSetIterator iterator) {
      if (iterator == null) {
        return null;
      }
      return new ConstantScoreScorer(this, score(), scoreMode, iterator);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      final Terms terms = context.reader().terms(q.getField());
      if (terms == null) {
        return null;
      }
      final WeightOrDocIdSetIterator weightOrIterator = rewrite(context, terms);
      if (weightOrIterator == null) {
        return null;
      } else if (weightOrIterator.weight != null) {
        return weightOrIterator.weight.bulkScorer(context);
      } else {
        final Scorer scorer = scorerForIterator(weightOrIterator.iterator);
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      final ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      return scorerSupplier.get(Long.MAX_VALUE);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      final Terms terms = context.reader().terms(q.field);
      if (terms == null) {
        return null;
      }
      return MatchesUtils.forField(
          q.field,
          () ->
              DisjunctionMatchesIterator.fromTermsEnum(
                  context, doc, q, q.field, q.getTermsEnum(terms)));
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      final Terms terms = context.reader().terms(q.getField());
      if (terms == null) {
        return null;
      }

      final long cost = estimateCost(terms, q.getTermsCount());

      final Weight weight = this;
      return new ScorerSupplier() {
        @Override
        public Scorer get(long leadCost) throws IOException {
          WeightOrDocIdSetIterator weightOrIterator = rewrite(context, terms);
          final Scorer scorer;
          if (weightOrIterator == null) {
            scorer = null;
          } else if (weightOrIterator.weight != null) {
            scorer = weightOrIterator.weight.scorer(context);
          } else {
            scorer = scorerForIterator(weightOrIterator.iterator);
          }

          // It's against the API contract to return a null scorer from a non-null ScoreSupplier.
          // So if our ScoreSupplier was non-null (i.e., thought there might be hits) but we now
          // find that there are actually no hits, we need to return an empty Scorer as opposed
          // to null:
          return Objects.requireNonNullElseGet(
              scorer,
              () -> new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.empty()));
        }

        @Override
        public long cost() {
          return cost;
        }
      };
    }

    private static long estimateCost(Terms terms, long queryTermsCount) throws IOException {
      // Estimate the cost. If the MTQ can provide its term count, we can do a better job
      // estimating.
      // Cost estimation reasoning is:
      // 1. If we don't know how many query terms there are, we assume that every term could be
      //    in the MTQ and estimate the work as the total docs across all terms.
      // 2. If we know how many query terms there are...
      //    2a. Assume every query term matches at least one document (queryTermsCount).
      //    2b. Determine the total number of docs beyond the first one for each term.
      //        That count provides a ceiling on the number of extra docs that could match beyond
      //        that first one. (We omit the first since it's already been counted in 2a).
      // See: LUCENE-10207
      long cost;
      if (queryTermsCount == -1) {
        cost = terms.getSumDocFreq();
      } else {
        long potentialExtraCost = terms.getSumDocFreq();
        final long indexedTermCount = terms.size();
        if (indexedTermCount != -1) {
          potentialExtraCost -= indexedTermCount;
        }
        cost = queryTermsCount + potentialExtraCost;
      }

      return cost;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }
}
