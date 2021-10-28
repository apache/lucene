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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * This class also provides the functionality behind {@link MultiTermQuery#CONSTANT_SCORE_REWRITE}.
 * It tries to rewrite per-segment as a boolean query that returns a constant score and otherwise
 * fills a bit set with matches and builds a Scorer on top of this bit set.
 */
final class MultiTermQueryConstantScoreWrapper<Q extends MultiTermQuery> extends Query
    implements Accountable {

  // mtq that matches 16 terms or less will be executed as a regular disjunction
  private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

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

  private static class TermAndState {
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

  private static class WeightOrDocIdSet {
    final Weight weight;
    final DocIdSet set;

    WeightOrDocIdSet(Weight weight) {
      this.weight = Objects.requireNonNull(weight);
      this.set = null;
    }

    WeightOrDocIdSet(DocIdSet bitset) {
      this.set = bitset;
      this.weight = null;
    }
  }

  protected final Q query;

  /** Wrap a {@link MultiTermQuery} as a Filter. */
  protected MultiTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  public final boolean equals(final Object other) {
    return sameClassAs(other)
        && query.equals(((MultiTermQueryConstantScoreWrapper<?>) other).query);
  }

  @Override
  public final int hashCode() {
    return 31 * classHash() + query.hashCode();
  }

  /** Returns the encapsulated query */
  public Q getQuery() {
    return query;
  }

  /** Returns the field name for this query */
  public final String getField() {
    return query.getField();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Terms indexTerms = context.reader().terms(query.getField());
        if (indexTerms == null) {
          return null;
        }

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
        final long cost;
        final int queryTermsCount = query.getTermsCount();
        if (queryTermsCount == -1) {
          cost = indexTerms.getSumDocFreq();
        } else {
          cost = queryTermsCount + (indexTerms.getSumDocFreq() - indexTerms.size());
        }

        final Weight weight = this;
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            WeightOrDocIdSet weightOrBitSet = rewrite(context, indexTerms);
            final Scorer scorer;
            if (weightOrBitSet.weight != null) {
              scorer = weightOrBitSet.weight.scorer(context);
            } else {
              scorer = scorerForDocIdSet(weight, score(), scoreMode, weightOrBitSet.set);
            }

            // It's against the API contract to return a null scorer from a non-null ScoreSupplier.
            // So if our ScoreSupplier was non-null (i.e., thought there might be hits) but we now
            // find that there are actually no hits, we need to return an empty Scorer as opposed
            // to null:
            return Objects.requireNonNullElseGet(
                scorer,
                () ->
                    new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.empty()));
          }

          @Override
          public long cost() {
            return cost;
          }
        };
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
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final Terms indexTerms = context.reader().terms(query.getField());
        if (indexTerms == null) {
          return null;
        }

        final WeightOrDocIdSet weightOrBitSet = rewrite(context, indexTerms);
        if (weightOrBitSet.weight != null) {
          // If we're using a Weight, let it provide its bulk scorer:
          return weightOrBitSet.weight.bulkScorer(context);
        } else {
          // If we're using a pre-populated DocIdSet, produce a DefaultBulkScorer over it:
          final Scorer scorer = scorerForDocIdSet(this, score(), scoreMode, weightOrBitSet.set);
          if (scorer == null) {
            return null;
          }
          return new DefaultBulkScorer(scorer);
        }
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        final Terms indexTerms = context.reader().terms(query.field);
        if (indexTerms == null) {
          return null;
        }
        return MatchesUtils.forField(
            query.field,
            () ->
                DisjunctionMatchesIterator.fromTermsEnum(
                    context, doc, query, query.field, query.getTermsEnum(indexTerms)));
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      /**
       * On the given leaf context, try to either rewrite to a disjunction if there are few terms,
       * or build a bitset containing matching docs.
       */
      private WeightOrDocIdSet rewrite(LeafReaderContext context, Terms indexTerms)
          throws IOException {
        assert indexTerms != null;

        // Create a TermsEnum that will produce the intersection of the query terms with the indexed
        // terms:
        final TermsEnum termsEnum = query.getTermsEnum(indexTerms);
        assert termsEnum != null;

        // Collect terms up to BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD:
        final List<TermAndState> collectedTerms = new ArrayList<>();
        if (collectTerms(termsEnum, collectedTerms)) {
          // If there were fewer than BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD terms, build a BQ:
          final BooleanQuery.Builder bq = new BooleanQuery.Builder();
          for (TermAndState t : collectedTerms) {
            final TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
            bq.add(new TermQuery(new Term(query.field, t.term), termStates), Occur.SHOULD);
          }
          final Query q = new ConstantScoreQuery(bq.build());
          final Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
          return new WeightOrDocIdSet(weight);
        }

        // More than BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD terms, so we'll build a bit set of the
        // matching docs. First, go back through the terms already collected and add their docs:
        PostingsEnum docs = null;
        final DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc(), indexTerms);
        if (collectedTerms.isEmpty() == false) {
          final TermsEnum termsEnum2 = indexTerms.iterator();
          for (TermAndState t : collectedTerms) {
            termsEnum2.seekExact(t.term, t.state);
            docs = termsEnum2.postings(docs, PostingsEnum.NONE);
            builder.add(docs);
          }
        }

        // Iterate the remaining terms and finish filling the bit set:
        do {
          // (remember that termsEnum was left positioned on the next term beyond the threshold):
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          builder.add(docs);
        } while (termsEnum.next() != null);

        return new WeightOrDocIdSet(builder.build());
      }

      /**
       * Try to collect terms from the given terms enum and return true iff all terms could be
       * collected. If {@code false} is returned, the enum is left positioned on the next term.
       */
      private boolean collectTerms(TermsEnum termsEnum, List<TermAndState> terms)
          throws IOException {
        final int threshold =
            Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
        for (int i = 0; i < threshold; i++) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            return true;
          }
          final TermState state = termsEnum.termState();
          terms.add(
              new TermAndState(
                  BytesRef.deepCopyOf(term),
                  state,
                  termsEnum.docFreq(),
                  termsEnum.totalTermFreq()));
        }
        return termsEnum.next() == null;
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField())) {
      query.visit(visitor.getSubVisitor(Occur.FILTER, this));
    }
  }

  /** Provide a ConstantScoreScorer over the pre-populated DocIdSet */
  private static Scorer scorerForDocIdSet(
      Weight weight, float score, ScoreMode scoreMode, DocIdSet set) throws IOException {
    if (set == null) {
      return null;
    }
    final DocIdSetIterator disi = set.iterator();
    if (disi == null) {
      return null;
    }
    return new ConstantScoreScorer(weight, score, scoreMode, disi);
  }
}
