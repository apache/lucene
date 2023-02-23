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
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * This class provides the functionality behind {@link
 * MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE}. It maintains a boolean query-like approach over
 * the most costly terms with a limit of {@link #BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD} while
 * rewriting the remaining terms into a filter bitset.
 */
final class MultiTermQueryConstantScoreBlendedWrapper<Q extends MultiTermQuery> extends Query
    implements Accountable {

  // mtq that matches 16 terms or less will be executed as a regular disjunction
  private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;
  // postings lists under this threshold will always be "pre-processed" into a bitset
  private static final int POSTINGS_PRE_PROCESS_THRESHOLD = 512;

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

  private static class WeightOrDocIdSetIterator {
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

  private final Q query;

  /** Wrap a {@link MultiTermQuery} as a Filter. */
  MultiTermQueryConstantScoreBlendedWrapper(Q query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  public boolean equals(final Object other) {
    return sameClassAs(other)
        && query.equals(((MultiTermQueryConstantScoreBlendedWrapper<?>) other).query);
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
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      private boolean collectTerms(int fieldDocCount, TermsEnum termsEnum, List<TermAndState> terms)
          throws IOException {
        final int threshold =
            Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
        for (int i = 0; i < threshold; ++i) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            return true;
          }
          TermState state = termsEnum.termState();
          int docFreq = termsEnum.docFreq();
          TermAndState termAndState =
              new TermAndState(
                  BytesRef.deepCopyOf(term), state, docFreq, termsEnum.totalTermFreq());
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

      private WeightOrDocIdSetIterator rewrite(LeafReaderContext context) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          // field does not exist
          return new WeightOrDocIdSetIterator((DocIdSetIterator) null);
        }

        final int fieldDocCount = terms.getDocCount();
        final TermsEnum termsEnum = query.getTermsEnum(terms);
        assert termsEnum != null;

        final List<TermAndState> collectedTerms = new ArrayList<>();
        if (collectTerms(fieldDocCount, termsEnum, collectedTerms)) {
          // build a boolean query
          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          for (TermAndState t : collectedTerms) {
            final TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
            bq.add(new TermQuery(new Term(query.field, t.term), termStates), Occur.SHOULD);
          }
          Query q = new ConstantScoreQuery(bq.build());
          final Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
          return new WeightOrDocIdSetIterator(weight);
        }

        // Too many terms: go back to the terms we already collected and start building the bit set
        DocIdSetBuilder otherTerms = new DocIdSetBuilder(context.reader().maxDoc(), terms);
        PriorityQueue<PostingsEnum> highFrequencyTerms =
            new PriorityQueue<>(collectedTerms.size()) {
              @Override
              protected boolean lessThan(PostingsEnum a, PostingsEnum b) {
                return a.cost() < b.cost();
              }
            };
        if (collectedTerms.isEmpty() == false) {
          TermsEnum termsEnum2 = terms.iterator();
          for (TermAndState t : collectedTerms) {
            termsEnum2.seekExact(t.term, t.state);
            PostingsEnum postings = termsEnum2.postings(null, PostingsEnum.NONE);
            if (t.docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
              otherTerms.add(postings);
            } else {
              highFrequencyTerms.add(postings);
            }
          }
        }

        // Then collect remaining terms
        PostingsEnum reuse = null;
        do {
          reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
          // If a term contains all docs with a value for the specified field, we can discard the
          // other terms and just use the dense term's postings:
          int docFreq = termsEnum.docFreq();
          if (fieldDocCount == docFreq) {
            TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(
                termsEnum.termState(), context.ord, docFreq, termsEnum.totalTermFreq());
            Query q =
                new ConstantScoreQuery(
                    new TermQuery(new Term(query.field, termsEnum.term()), termStates));
            Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
            return new WeightOrDocIdSetIterator(weight);
          }
          if (docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
            otherTerms.add(reuse);
          } else {
            PostingsEnum dropped = highFrequencyTerms.insertWithOverflow(reuse);
            if (dropped != null) {
              otherTerms.add(dropped);
            }
            // Reuse the postings that drop out of the PQ. Note that `dropped` will be null here
            // if nothing is evicted, meaning we will _not_ reuse any postings (which is intentional
            // since we can't reuse postings that are in the PQ).
            reuse = dropped;
          }
        } while (termsEnum.next() != null);

        DisiPriorityQueue subs = new DisiPriorityQueue(highFrequencyTerms.size() + 1);
        for (DocIdSetIterator disi : highFrequencyTerms) {
          subs.add(new DisiWrapper(disi));
        }
        subs.add(new DisiWrapper(otherTerms.build().iterator()));

        return new WeightOrDocIdSetIterator(new DisjunctionDISIApproximation(subs));
      }

      private Scorer scorer(DocIdSetIterator iterator) throws IOException {
        if (iterator == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, iterator);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final WeightOrDocIdSetIterator weightOrBitSet = rewrite(context);
        if (weightOrBitSet.weight != null) {
          return weightOrBitSet.weight.bulkScorer(context);
        } else {
          final Scorer scorer = scorer(weightOrBitSet.iterator);
          if (scorer == null) {
            return null;
          }
          return new DefaultBulkScorer(scorer);
        }
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          return null;
        }
        return MatchesUtils.forField(
            query.field,
            () ->
                DisjunctionMatchesIterator.fromTermsEnum(
                    context, doc, query, query.field, query.getTermsEnum(terms)));
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final WeightOrDocIdSetIterator weightOrBitSet = rewrite(context);
        if (weightOrBitSet.weight != null) {
          return weightOrBitSet.weight.scorer(context);
        } else {
          return scorer(weightOrBitSet.iterator);
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField())) {
      query.visit(visitor.getSubVisitor(Occur.FILTER, this));
    }
  }
}
