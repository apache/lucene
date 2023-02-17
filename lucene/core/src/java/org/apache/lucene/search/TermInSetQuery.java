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
import java.util.SortedSet;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * Specialization for a disjunction over many terms that behaves like a {@link ConstantScoreQuery}
 * over a {@link BooleanQuery} containing only {@link
 * org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 *
 * <p>For instance in the following example, both {@code q1} and {@code q2} would yield the same
 * scores:
 *
 * <pre class="prettyprint">
 * Query q1 = new TermInSetQuery("field", new BytesRef("foo"), new BytesRef("bar"));
 *
 * BooleanQuery bq = new BooleanQuery();
 * bq.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
 * bq.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);
 * Query q2 = new ConstantScoreQuery(bq);
 * </pre>
 *
 * <p>When there are few terms, this query executes like a regular disjunction. However, when there
 * are many terms, instead of merging iterators on the fly, it will populate a bit set with matching
 * docs and return a {@link Scorer} over this bit set.
 *
 * <p>NOTE: This query produces scores that are equal to its boost
 */
public class TermInSetQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(TermInSetQuery.class);
  // Same threshold as MultiTermQueryConstantScoreWrapper
  static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;
  // postings lists under this threshold will always be "pre-processed" into a bitset
  private static final int POSTINGS_PRE_PROCESS_THRESHOLD = 512;

  private final String field;
  private final PrefixCodedTerms termData;
  private final int termDataHashCode; // cached hashcode of termData

  /** Creates a new {@link TermInSetQuery} from the given collection of terms. */
  public TermInSetQuery(String field, Collection<BytesRef> terms) {
    BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
    // already sorted if we are a SortedSet with natural order
    boolean sorted =
        terms instanceof SortedSet && ((SortedSet<BytesRef>) terms).comparator() == null;
    if (!sorted) {
      ArrayUtil.timSort(sortedTerms);
    }
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    for (BytesRef term : sortedTerms) {
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else if (previous.get().equals(term)) {
        continue; // deduplicate
      }
      builder.add(field, term);
      previous.copyBytes(term);
    }
    this.field = field;
    termData = builder.finish();
    termDataHashCode = termData.hashCode();
  }

  /** Creates a new {@link TermInSetQuery} from the given array of terms. */
  public TermInSetQuery(String field, BytesRef... terms) {
    this(field, Arrays.asList(terms));
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    final int threshold =
        Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
    if (termData.size() <= threshold) {
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      TermIterator iterator = termData.iterator();
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        bq.add(new TermQuery(new Term(iterator.field(), BytesRef.deepCopyOf(term))), Occur.SHOULD);
      }
      return new ConstantScoreQuery(bq.build());
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    if (termData.size() == 1) {
      visitor.consumeTerms(this, new Term(field, termData.iterator().next()));
    }
    if (termData.size() > 1) {
      visitor.consumeTermsMatching(this, field, this::asByteRunAutomaton);
    }
  }

  // TODO: this is extremely slow. we should not be doing this.
  private ByteRunAutomaton asByteRunAutomaton() {
    TermIterator iterator = termData.iterator();
    List<Automaton> automata = new ArrayList<>();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      automata.add(Automata.makeBinary(term));
    }
    Automaton automaton =
        Operations.determinize(
            Operations.union(automata), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    return new CompiledAutomaton(automaton).runAutomaton;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(TermInSetQuery other) {
    // no need to check 'field' explicitly since it is encoded in 'termData'
    // termData might be heavy to compare so check the hash code first
    return termDataHashCode == other.termDataHashCode && termData.equals(other.termData);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + termDataHashCode;
  }

  /** Returns the terms wrapped in a PrefixCodedTerms. */
  public PrefixCodedTerms getTermData() {
    return termData;
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":(");

    TermIterator iterator = termData.iterator();
    boolean first = true;
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (!first) {
        builder.append(' ');
      }
      first = false;
      builder.append(Term.toString(term));
    }
    builder.append(')');

    return builder.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + termData.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
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

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        Terms terms = Terms.getTerms(context.reader(), field);
        if (terms.hasPositions() == false) {
          return super.matches(context, doc);
        }
        return MatchesUtils.forField(
            field,
            () ->
                DisjunctionMatchesIterator.fromTermsEnum(
                    context, doc, getQuery(), field, termData.iterator()));
      }

      /**
       * On the given leaf context, try to either rewrite to a disjunction if there are few matching
       * terms, or build a bitset containing matching docs.
       */
      private WeightOrDocIdSetIterator rewrite(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();

        Terms terms = reader.terms(field);
        if (terms == null) {
          return null;
        }

        final int fieldDocCount = terms.getDocCount();
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum reuse = null;
        TermIterator iterator = termData.iterator();

        // We will first try to collect up to 'threshold' terms into 'unprocessed'
        // if there are too many terms, we will fall back to building the 'processedPostings'
        final int threshold =
            Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
        assert termData.size() > threshold : "Query should have been rewritten";
        List<PostingsEnum> unprocessed = new ArrayList<>(threshold);
        PriorityQueue<PostingsEnum> unprocessedPq = null;
        DocIdSetBuilder processedPostings = null;

        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
          assert field.equals(iterator.field());
          if (termsEnum.seekExact(term) == false) {
            continue;
          }

          // If a term contains all docs with a value for the specified field (likely rare),
          // we can discard the other terms and just use the dense term's postings:
          int docFreq = termsEnum.docFreq();
          if (fieldDocCount == docFreq) {
            TermStates termStates = new TermStates(searcher.getTopReaderContext());
            termStates.register(
                termsEnum.termState(), context.ord, docFreq, termsEnum.totalTermFreq());
            Query q =
                new ConstantScoreQuery(
                    new TermQuery(new Term(field, termsEnum.term()), termStates));
            Weight weight = searcher.rewrite(q).createWeight(searcher, scoreMode, score());
            return new WeightOrDocIdSetIterator(weight);
          }

          // All small postings get immediately processed into our bitset:
          if (docFreq <= POSTINGS_PRE_PROCESS_THRESHOLD) {
            if (processedPostings == null) {
              processedPostings = new DocIdSetBuilder(reader.maxDoc(), terms);
            }
            reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
            processedPostings.add(reuse);
            continue;
          }

          PostingsEnum p = termsEnum.postings(null, PostingsEnum.NONE);
          if (unprocessedPq != null) {
            // We've already switched to using a PQ, so find the next smallest postings of
            // all the remaining unprocessed ones and process it into our bitset:
            PostingsEnum evicted = unprocessedPq.insertWithOverflow(p);
            assert evicted != null;
            processedPostings.add(evicted);
          } else if (unprocessed.size() < threshold) {
            // We haven't hit our "unprocessed" limit yet, so just add to our list:
            unprocessed.add(p);
          } else {
            // We're at our "unprocessed" limit, so we'll switch to a PQ in order to always
            // lazily process the next shortest postings into our bitset:
            assert unprocessed.size() == threshold;
            unprocessedPq =
                new PriorityQueue<>(threshold) {
                  @Override
                  protected boolean lessThan(PostingsEnum a, PostingsEnum b) {
                    // Note: No tie-breaker for equal cost means that "larger" terms tied for lowest
                    // cost will get immediately processed, avoiding churn. The PQ logic requires an
                    // insertWithOverflow candidate to be strictly less than the smallest entry to
                    // be
                    // added to the queue:
                    return a.cost() < b.cost();
                  }
                };
            unprocessedPq.addAll(unprocessed);
            unprocessed = null;
            PostingsEnum evicted = unprocessedPq.insertWithOverflow(p);
            assert evicted != null;
            if (processedPostings == null) {
              processedPostings = new DocIdSetBuilder(reader.maxDoc(), terms);
            }
            processedPostings.add(evicted);
          }
        }

        int iteratorCount = processedPostings != null ? 1 : 0;
        Iterator<PostingsEnum> unprocessedIt;
        if (unprocessed != null) {
          iteratorCount += unprocessed.size();
          unprocessedIt = unprocessed.iterator();
        } else {
          iteratorCount += unprocessedPq.size();
          unprocessedIt = unprocessedPq.iterator();
        }

        if (iteratorCount == 0) {
          return null;
        }

        DisiPriorityQueue disiPq = new DisiPriorityQueue(iteratorCount);
        while (unprocessedIt.hasNext()) {
          disiPq.add(new DisiWrapper(unprocessedIt.next()));
        }

        if (processedPostings != null) {
          disiPq.add(new DisiWrapper(processedPostings.build().iterator()));
        }

        return new WeightOrDocIdSetIterator(new DisjunctionDISIApproximation(disiPq));
      }

      private Scorer scorer(DocIdSetIterator disi) throws IOException {
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final WeightOrDocIdSetIterator weightOrBitSet = rewrite(context);
        if (weightOrBitSet == null) {
          return null;
        } else if (weightOrBitSet.weight != null) {
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
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        Terms indexTerms = context.reader().terms(field);
        if (indexTerms == null) {
          return null;
        }

        // Cost estimation reasoning is:
        //  1. Assume every query term matches at least one document (queryTermsCount).
        //  2. Determine the total number of docs beyond the first one for each term.
        //     That count provides a ceiling on the number of extra docs that could match beyond
        //     that first one. (We omit the first since it's already been counted in #1).
        // This approach still provides correct worst-case cost in general, but provides tighter
        // estimates for primary-key-like fields. See: LUCENE-10207

        // TODO: This cost estimation may grossly overestimate since we have no index statistics
        // for the specific query terms. While it's nice to avoid the cost of intersecting the
        // query terms with the index, it could be beneficial to do that work and get better
        // cost estimates.
        final long cost;
        final long queryTermsCount = termData.size();
        long potentialExtraCost = indexTerms.getSumDocFreq();
        final long indexedTermCount = indexTerms.size();
        if (indexedTermCount != -1) {
          potentialExtraCost -= indexedTermCount;
        }
        cost = queryTermsCount + potentialExtraCost;

        final Weight weight = this;
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            WeightOrDocIdSetIterator weightOrDocIdSetIterator = rewrite(context);
            final Scorer scorer;
            if (weightOrDocIdSetIterator == null) {
              scorer = null;
            } else if (weightOrDocIdSetIterator.weight != null) {
              scorer = weightOrDocIdSetIterator.weight.scorer(context);
            } else {
              scorer = scorer(weightOrDocIdSetIterator.iterator);
            }

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
        final ScorerSupplier supplier = scorerSupplier(context);
        if (supplier == null) {
          return null;
        }
        return supplier.get(Long.MAX_VALUE);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Only cache instances that have a reasonable size. Otherwise it might cause memory issues
        // with the query cache if most memory ends up being spent on queries rather than doc id
        // sets.
        return ramBytesUsed() <= RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;
      }
    };
  }
}
