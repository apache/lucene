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
import java.util.SortedSet;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.LongBitSet;
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
  // number of terms we'll "pre-seek" to validate; limits heap if there are many terms
  private static final int PRE_SEEK_TERM_LIMIT = 16;
  // postings lists under this threshold will always be "pre-processed" into a bitset
  private static final int POSTINGS_PRE_PROCESS_THRESHOLD = 512;
  // max number of clauses we'll manage/check during scoring (these remain "unprocessed")
  private static final int MAX_UNPROCESSED_POSTINGS =
      Math.min(IndexSearcher.getMaxClauseCount(), 16);

  private final String field;
  private final PrefixCodedTerms termData;
  private final int termDataHashCode; // cached hashcode of termData

  /** Creates a new {@link TermInSetQuery} from the given collection of terms. */
  public TermInSetQuery(String field, Collection<BytesRef> terms) {
    BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
    // already sorted if we are a SortedSet with natural order
    boolean sorted =
        terms instanceof SortedSet && ((SortedSet<BytesRef>) terms).comparator() == null;
    if (sorted == false) {
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
    if (termData.size() == 0) {
      return new MatchNoDocsQuery();
    }
    if (termData.size() == 1) {
      Term term = new Term(field, termData.iterator().next());
      return new ConstantScoreQuery(new TermQuery(term));
    }
    // TODO: Rewriting means we'll always use a postings-based approach, which can be worse than
    // a doc values approach if the terms are expensive in aggregate compared to the lead cost.
    // May be worth not rewriting or using a lower threshold.
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
      if (first == false) {
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

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        // Implement term-at-a-time scoring for top-level scoring as it is likely more efficient
        // than doc-at-a-time:
        final LeafReader reader = context.reader();
        final Terms t = Terms.getTerms(reader, field);
        final TermsEnum termsEnum = t.iterator();
        int fieldDocCount = t.getDocCount();
        PostingsEnum singleton = null;
        PostingsEnum reuse = null;
        DocIdSetBuilder builder = null;
        TermIterator termIterator = termData.iterator();
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
          if (termsEnum.seekExact(term) == false) {
            continue;
          }

          // If we find a "completely dense" postings list, we can use it directly:
          int termDocFreq = termsEnum.docFreq();
          if (fieldDocCount == termDocFreq) {
            singleton = termsEnum.postings(reuse, PostingsEnum.NONE);
            break;
          }

          reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
          if (builder == null) {
            builder = new DocIdSetBuilder(reader.maxDoc());
          }
          builder.add(reuse);
        }

        final DocIdSetIterator it;
        if (singleton != null) {
          it = singleton;
        } else if (builder == null) {
          // No terms found:
          return null;
        } else {
          it = builder.build().iterator();
        }
        final long cost = it.cost();

        return new BulkScorer() {
          @Override
          public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
              throws IOException {
            final DummyScorable dummy = new DummyScorable(boost);
            collector.setScorer(dummy);

            int doc = it.docID();
            if (doc < min) {
              doc = it.advance(min);
            }

            while (doc < max) {
              if (acceptDocs == null || acceptDocs.get(doc)) {
                dummy.docID = doc;
                collector.collect(doc);
              }
              doc = it.nextDoc();
            }

            return doc;
          }

          @Override
          public long cost() {
            return cost;
          }
        };
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier supplier = scorerSupplier(context);
        if (supplier == null) {
          return null;
        } else {
          return supplier.get(Long.MAX_VALUE);
        }
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        assert termData.size() > BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD;

        final LeafReader reader = context.reader();
        final FieldInfo fi = reader.getFieldInfos().fieldInfo(field);
        if (fi == null) {
          // Field not in segment:
          return null;
        }
        final DocValuesType dvType = fi.getDocValuesType();

        final Terms t = Terms.getTerms(reader, field);

        // Cost estimation reasoning is:
        //  1. Assume every query term matches at least one document (queryTermsCount).
        //  2. Determine the total number of docs beyond the first one for each term.
        //     That count provides a ceiling on the number of extra docs that could match beyond
        //     that first one. (We omit the first since it's already been counted in #1).
        // This approach still provides correct worst-case cost in general, but provides tighter
        // estimates for primary-key-like fields. See: LUCENE-10207
        final long queryTermsCount = termData.size();
        final long sumDocFreq = t.getSumDocFreq();
        final long indexedTermCount = t.size();
        long potentialExtraCost = sumDocFreq;
        if (indexedTermCount != -1) {
          potentialExtraCost -= indexedTermCount;
        }
        final long cost = queryTermsCount + potentialExtraCost;

        final boolean isPrimaryKeyField = indexedTermCount != -1 && sumDocFreq == indexedTermCount;

        return new ScorerSupplier() {

          @Override
          public Scorer get(long leadCost) throws IOException {
            assert termData.size() > 1;

            // If there are no doc values indexed, we have to use a postings-based approach:
            if (dvType != DocValuesType.SORTED && dvType != DocValuesType.SORTED_SET) {
              return postingsScorer(reader, t.getDocCount(), t.iterator(), null);
            }

            // Establish a threshold for switching to doc values. Give postings a significant
            // advantage for the primary-key case, since many of the primary-key terms may not
            // actually be in this segment. The 8x factor is arbitrary, based on IndexOrDVQuery,
            // but has performed well in benchmarks:
            long candidateSizeThreshold = isPrimaryKeyField ? leadCost << 3 : leadCost;

            if (termData.size() > candidateSizeThreshold) {
              // If the number of terms is > the number of candidates, DV should perform better.
              // TODO: This assumes all terms are present in the segment. If the actual number of
              // found terms in the segment is significantly smaller, this can be the wrong
              // decision. Maybe we can do better? Possible bloom filter application? This is
              // why we special-case primary-key cases above.
              return docValuesScorer(reader);
            }

            // For a primary-key field, at this point, it's highly likely that a postings-based
            // approach is the right solution as docIDs will be pulsed. Go straight to a postings-
            // solution:
            if (isPrimaryKeyField) {
              return postingsScorer(reader, t.getDocCount(), t.iterator(), null);
            }

            // Begin estimating the postings-approach cost term-by-term, with a limit on the
            // total number of terms we check. If we reach the limit and a postings-approach
            // still seems cheaper than a DV approach, we'll use it. The term limit both
            // ensures we don't do too much wasted term seeking work if we end up using DVs,
            // and also limits our heap usage since we keep track of term states for all the
            // terms:
            long expectedAdvances = 0;
            int visitedTermCount = 0;
            int foundTermCount = 0;
            int fieldDocCount = t.getDocCount();
            TermsEnum termsEnum = t.iterator();
            // Note: We can safely cast termData.size() to an int here since all the terms
            // originally came into the ctor in a Collection:
            List<TermState> termStates =
                new ArrayList<>(Math.min(PRE_SEEK_TERM_LIMIT, Math.toIntExact(termData.size())));
            TermState lastTermState = null;
            BytesRefBuilder lastTerm = null;
            TermIterator termIterator = termData.iterator();
            for (BytesRef term = termIterator.next();
                term != null && visitedTermCount < PRE_SEEK_TERM_LIMIT;
                term = termIterator.next(), visitedTermCount++) {
              if (termsEnum.seekExact(term) == false) {
                // Keep track that the term wasn't found:
                termStates.add(null);
                continue;
              }

              // If we find a "completely dense" postings list, we can use it directly:
              int termDocFreq = termsEnum.docFreq();
              if (fieldDocCount == termDocFreq) {
                PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
                return scorerFor(postings);
              }

              // As expectedAdvances grows, it becomes more-and-more likely a doc-values approach
              // will be more cost-effective. Since the cost of a doc-values approach is bound
              // by candidateSize, we chose to use it if expectedAdvances grows beyond a certain
              // point:
              expectedAdvances += Math.min(termDocFreq, leadCost);
              if (expectedAdvances > candidateSizeThreshold) {
                return docValuesScorer(reader);
              }

              foundTermCount++;
              lastTermState = termsEnum.termState();
              if (lastTerm == null) {
                lastTerm = new BytesRefBuilder();
              }
              lastTerm.copyBytes(term);
              termStates.add(lastTermState);
            }

            if (visitedTermCount == termData.size()) {
              // If we visited all the terms, we do one more check for some special-cases:
              if (foundTermCount == 0) {
                return emptyScorer();
              } else if (foundTermCount == 1) {
                termsEnum.seekExact(lastTerm.get(), lastTermState);
                PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
                return scorerFor(postings);
              }
            }

            // If we reach this point, it's likely a postings-based approach will prove more
            // cost-effective:
            return postingsScorer(
                context.reader(), fieldDocCount, termsEnum, termStates.iterator());
          }

          @Override
          public long cost() {
            return cost;
          }
        };
      }

      private Scorer emptyScorer() {
        return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.empty());
      }

      private Scorer scorerFor(DocIdSetIterator it) {
        return new ConstantScoreScorer(this, score(), scoreMode, it);
      }

      private Scorer scorerFor(TwoPhaseIterator it) {
        return new ConstantScoreScorer(this, score(), scoreMode, it);
      }

      private Scorer postingsScorer(
          LeafReader reader, int fieldDocCount, TermsEnum termsEnum, Iterator<TermState> termStates)
          throws IOException {
        List<DisiWrapper> unprocessed = null;
        PriorityQueue<DisiWrapper> unprocessedPq = null;
        DocIdSetBuilder processedPostings = null;
        PostingsEnum reuse = null;
        TermIterator termIterator = termData.iterator();
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
          // Use any term states we have:
          if (termStates != null && termStates.hasNext()) {
            TermState termState = termStates.next();
            if (termState == null) {
              // Term wasn't found in the segment; move on:
              continue;
            }
            termsEnum.seekExact(term, termState);
          } else {
            if (termsEnum.seekExact(term) == false) {
              continue;
            }

            // If we find a "completely dense" postings list, we can use it directly:
            if (fieldDocCount == termsEnum.docFreq()) {
              reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
              return scorerFor(reuse);
            }
          }

          if (termsEnum.docFreq() < POSTINGS_PRE_PROCESS_THRESHOLD) {
            // For small postings lists, we pre-process them directly into a bitset since they
            // are unlikely to benefit from skipping:
            if (processedPostings == null) {
              processedPostings = new DocIdSetBuilder(reader.maxDoc());
            }
            reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
            processedPostings.add(reuse);
          } else {
            DisiWrapper w = new DisiWrapper(termsEnum.postings(null, PostingsEnum.NONE));
            if (unprocessedPq != null) {
              // We've hit our unprocessed postings limit, so use our PQ:
              DisiWrapper evicted = unprocessedPq.insertWithOverflow(w);
              processedPostings.add(evicted.iterator);
            } else {
              if (unprocessed == null) {
                unprocessed = new ArrayList<>(MAX_UNPROCESSED_POSTINGS);
              }
              if (unprocessed.size() < MAX_UNPROCESSED_POSTINGS) {
                unprocessed.add(w);
              } else {
                // Hit our unprocessed postings limit; switch to PQ:
                unprocessedPq =
                    new PriorityQueue<>(MAX_UNPROCESSED_POSTINGS) {
                      @Override
                      protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
                        return a.cost < b.cost;
                      }
                    };
                unprocessedPq.addAll(unprocessed);
                unprocessed = null;

                DisiWrapper evicted = unprocessedPq.insertWithOverflow(w);
                if (processedPostings == null) {
                  processedPostings = new DocIdSetBuilder(reader.maxDoc());
                }
                processedPostings.add(evicted.iterator);
              }
            }
          }
        }

        // No terms found:
        if (processedPostings == null && unprocessed == null) {
          assert unprocessedPq == null;
          return emptyScorer();
        }

        // Single term found and _not_ pre-built into the bitset:
        if (processedPostings == null && unprocessed.size() == 1) {
          assert unprocessedPq == null;
          return scorerFor(unprocessed.get(0).iterator);
        }

        // All postings built into the bitset:
        if (processedPostings != null && unprocessed == null && unprocessedPq == null) {
          return scorerFor(processedPostings.build().iterator());
        }

        DisiPriorityQueue pq;
        final long cost;
        if (unprocessed != null) {
          assert unprocessedPq == null;
          if (processedPostings != null) {
            unprocessed.add(new DisiWrapper(processedPostings.build().iterator()));
          }
          pq = new DisiPriorityQueue(unprocessed.size());
          pq.addAll(unprocessed.toArray(new DisiWrapper[0]), 0, unprocessed.size());
          cost = unprocessed.stream().mapToLong(w -> w.cost).sum();
        } else {
          assert unprocessedPq != null && processedPostings != null;
          assert unprocessedPq.size() == MAX_UNPROCESSED_POSTINGS;
          DisiWrapper[] postings = new DisiWrapper[MAX_UNPROCESSED_POSTINGS + 1];
          int i = 0;
          long c = 0;
          for (DisiWrapper w : unprocessedPq) {
            c += w.cost;
            postings[i] = w;
            i++;
          }
          DisiWrapper w = new DisiWrapper(processedPostings.build().iterator());
          cost = c + w.cost;
          postings[i] = w;
          pq = new DisiPriorityQueue(postings.length);
          pq.addAll(postings, 0, postings.length);
        }

        // This iterator is slightly different from a standard disjunction iterator in that it
        // short-circuits whenever it finds a target doc (instead of ensuring all sub-iterators
        // are positioned on the same doc). This can be done since we're not using it to score,
        // only filter, and there are no second-phase checks:
        DocIdSetIterator it =
            new DocIdSetIterator() {
              private int docID = -1;

              @Override
              public int docID() {
                return docID;
              }

              @Override
              public int nextDoc() throws IOException {
                return doNext(docID + 1);
              }

              @Override
              public int advance(int target) throws IOException {
                return doNext(target);
              }

              private int doNext(int target) throws IOException {
                DisiWrapper top = pq.top();
                do {
                  top.doc = top.approximation.advance(target);
                  if (top.doc == target) {
                    pq.updateTop();
                    docID = target;
                    return docID;
                  }
                  top = pq.updateTop();
                } while (top.doc < target);

                docID = top.doc;
                return docID;
              }

              @Override
              public long cost() {
                return cost;
              }
            };

        return scorerFor(it);
      }

      private Scorer docValuesScorer(LeafReader reader) throws IOException {
        SortedSetDocValues dv = DocValues.getSortedSet(reader, field);

        boolean hasAtLeastOneTerm = false;
        LongBitSet ords = new LongBitSet(dv.getValueCount());
        PrefixCodedTerms.TermIterator termIterator = termData.iterator();
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
          long ord = dv.lookupTerm(term);
          if (ord >= 0) {
            ords.set(ord);
            hasAtLeastOneTerm = true;
          }
        }

        if (hasAtLeastOneTerm == false) {
          return emptyScorer();
        }

        TwoPhaseIterator it;
        SortedDocValues singleton = DocValues.unwrapSingleton(dv);
        if (singleton != null) {
          it =
              new TwoPhaseIterator(singleton) {
                @Override
                public boolean matches() throws IOException {
                  return ords.get(singleton.ordValue());
                }

                @Override
                public float matchCost() {
                  return 1f;
                }
              };
        } else {
          it =
              new TwoPhaseIterator(dv) {
                @Override
                public boolean matches() throws IOException {
                  for (int i = 0; i < dv.docValueCount(); i++) {
                    if (ords.get(dv.nextOrd())) {
                      return true;
                    }
                  }
                  return false;
                }

                @Override
                public float matchCost() {
                  return 1f;
                }
              };
        }

        return scorerFor(it);
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

  private static final class DummyScorable extends Scorable {
    private final float score;
    int docID = -1;

    DummyScorable(float score) {
      this.score = score;
    }

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public int docID() {
      return docID;
    }
  }
}
