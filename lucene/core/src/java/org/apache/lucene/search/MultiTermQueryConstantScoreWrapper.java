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
import org.apache.lucene.index.LeafReader;
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

      /**
       * On the given leaf context, try to either rewrite to a disjunction if there are few terms,
       * or build a bitset containing matching docs.
       */
      private WeightOrDocIdSet rewrite(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();

        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          // field does not exist
          return new WeightOrDocIdSet((DocIdSet) null);
        }

        final TermsEnum termsEnum = query.getTermsEnum(terms);
        assert termsEnum != null;

        PostingsEnum docs = null;

        // We will first try to collect up to 'threshold' terms into 'matchingTerms'
        // if there are too many terms, we will fall back to building the 'builder'
        final int threshold =
            Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
        List<TermAndState> collectedTerms = new ArrayList<>();
        DocIdSetBuilder builder = null;

        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
          // If any term contains the complete segment's docs, we can short-circuit since this
          // query is a disjunction over all terms. Additionally, if a term contains all docs in
          // its field, we can use that term's postings:
          int docFreq = termsEnum.docFreq();
          if (reader.maxDoc() == docFreq) {
            return new WeightOrDocIdSet(DocIdSet.all(docFreq));
          } else if (terms.getDocCount() == docFreq) {
            TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(termsEnum.termState(), context.ord, docFreq, termsEnum.totalTermFreq());
            Query q = new ConstantScoreQuery(new TermQuery(new Term(query.field, term), termStates));
            Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
            return new WeightOrDocIdSet(weight);
          }

          if (collectedTerms == null) {
            assert builder != null;
            docs = termsEnum.postings(docs, PostingsEnum.NONE);
            builder.add(docs);
          } else if (collectedTerms.size() < threshold) {
            collectedTerms.add(
                new TermAndState(
                    BytesRef.deepCopyOf(term),
                    termsEnum.termState(),
                    termsEnum.docFreq(),
                    termsEnum.totalTermFreq()));
          } else {
            assert collectedTerms.size() == threshold;
            builder = new DocIdSetBuilder(reader.maxDoc(), terms);
            docs = termsEnum.postings(docs, PostingsEnum.NONE);
            builder.add(docs);
            TermsEnum termsEnum2 = terms.iterator();
            for (TermAndState t : collectedTerms) {
              termsEnum2.seekExact(t.term, t.state);
              docs = termsEnum2.postings(docs, PostingsEnum.NONE);
              builder.add(docs);
            }
            collectedTerms = null;
          }
        }
        if (collectedTerms != null) {
          assert builder == null;
          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          for (TermAndState t : collectedTerms) {
            final TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(t.state, context.ord, t.docFreq, t.totalTermFreq);
            bq.add(new TermQuery(new Term(query.field, t.term), termStates), Occur.SHOULD);
          }
          Query q = new ConstantScoreQuery(bq.build());
          final Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
          return new WeightOrDocIdSet(weight);
        } else {
          assert builder != null;
          return new WeightOrDocIdSet(builder.build());
        }
      }

      private Scorer scorer(DocIdSet set) throws IOException {
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final WeightOrDocIdSet weightOrBitSet = rewrite(context);
        if (weightOrBitSet.weight != null) {
          return weightOrBitSet.weight.bulkScorer(context);
        } else {
          final Scorer scorer = scorer(weightOrBitSet.set);
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
        final WeightOrDocIdSet weightOrBitSet = rewrite(context);
        if (weightOrBitSet.weight != null) {
          return weightOrBitSet.weight.scorer(context);
        } else {
          return scorer(weightOrBitSet.set);
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
