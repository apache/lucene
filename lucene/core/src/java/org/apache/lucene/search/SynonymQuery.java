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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.PriorityQueue;

/**
 * A query that treats multiple terms as synonyms.
 *
 * <p>For scoring purposes, this query tries to score the terms as if you had indexed them as one
 * term: it will match any of the terms but only invoke the similarity a single time, scoring the
 * sum of all term frequencies for the document.
 */
public final class SynonymQuery extends Query {

  private final TermAndBoost[] terms;
  private final String field;

  /** A builder for {@link SynonymQuery}. */
  public static class Builder {
    private final String field;
    private final List<TermAndBoost> terms = new ArrayList<>();

    /**
     * Sole constructor
     *
     * @param field The target field name
     */
    public Builder(String field) {
      this.field = field;
    }

    /** Adds the provided {@code term} as a synonym. */
    public Builder addTerm(Term term) {
      return addTerm(term, 1f);
    }

    /**
     * Adds the provided {@code term} as a synonym, document frequencies of this term will be
     * boosted by {@code boost}.
     */
    public Builder addTerm(Term term, float boost) {
      if (field.equals(term.field()) == false) {
        throw new IllegalArgumentException("Synonyms must be across the same field");
      }
      return addTerm(term.bytes(), boost);
    }

    /**
     * Adds the provided {@code term} as a synonym, document frequencies of this term will be
     * boosted by {@code boost}.
     */
    public Builder addTerm(BytesRef term, float boost) {
      if (Float.isNaN(boost) || Float.compare(boost, 0f) <= 0 || Float.compare(boost, 1f) > 0) {
        throw new IllegalArgumentException(
            "boost must be a positive float between 0 (exclusive) and 1 (inclusive)");
      }
      terms.add(new TermAndBoost(term, boost));
      if (terms.size() > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      return this;
    }

    /** Builds the {@link SynonymQuery}. */
    public SynonymQuery build() {
      terms.sort(Comparator.comparing(a -> a.term));
      return new SynonymQuery(terms.toArray(new TermAndBoost[0]), field);
    }
  }

  /**
   * Creates a new SynonymQuery, matching any of the supplied terms.
   *
   * <p>The terms must all have the same field.
   */
  private SynonymQuery(TermAndBoost[] terms, String field) {
    this.terms = Objects.requireNonNull(terms);
    this.field = Objects.requireNonNull(field);
  }

  /** Returns the terms of this {@link SynonymQuery} */
  public List<Term> getTerms() {
    return Arrays.stream(terms).map(t -> new Term(field, t.term)).toList();
  }

  /** Returns the field name of this {@link SynonymQuery} */
  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("Synonym(");
    for (int i = 0; i < terms.length; i++) {
      if (i != 0) {
        builder.append(" ");
      }
      Query termQuery = new TermQuery(new Term(this.field, terms[i].term));
      builder.append(termQuery.toString(field));
      if (terms[i].boost != 1f) {
        builder.append("^");
        builder.append(terms[i].boost);
      }
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Arrays.hashCode(terms);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other)
        && field.equals(((SynonymQuery) other).field)
        && Arrays.equals(terms, ((SynonymQuery) other).terms);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    // optimize zero and non-boosted single term cases
    if (terms.length == 0) {
      return new BooleanQuery.Builder().build();
    }
    if (terms.length == 1 && terms[0].boost == 1f) {
      return new TermQuery(new Term(field, terms[0].term));
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    Term[] ts = Arrays.stream(terms).map(t -> new Term(field, t.term)).toArray(Term[]::new);
    v.consumeTerms(this, ts);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    if (scoreMode.needsScores()) {
      return new SynonymWeight(this, searcher, scoreMode, boost);
    } else {
      // if scores are not needed, let BooleanWeight deal with optimizing that case.
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      for (TermAndBoost term : terms) {
        bq.add(new TermQuery(new Term(field, term.term)), BooleanClause.Occur.SHOULD);
      }
      return searcher
          .rewrite(bq.build())
          .createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, boost);
    }
  }

  class SynonymWeight extends Weight {
    private final TermStates[] termStates;
    private final Similarity similarity;
    private final Similarity.SimScorer simWeight;
    private final ScoreMode scoreMode;

    SynonymWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.scoreMode = scoreMode;
      CollectionStatistics collectionStats = searcher.collectionStatistics(field);
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[terms.length];
      for (int i = 0; i < termStates.length; i++) {
        Term term = new Term(field, terms[i].term);
        TermStates ts = TermStates.build(searcher, term, true);
        termStates[i] = ts;
        if (ts.docFreq() > 0) {
          TermStatistics termStats =
              searcher.termStatistics(term, ts.docFreq(), ts.totalTermFreq());
          docFreq = Math.max(termStats.docFreq(), docFreq);
          totalTermFreq += termStats.totalTermFreq();
        }
      }
      this.similarity = searcher.getSimilarity();
      if (docFreq > 0) {
        TermStatistics pseudoStats =
            new TermStatistics(new BytesRef("synonym pseudo-term"), docFreq, totalTermFreq);
        this.simWeight = similarity.scorer(boost, collectionStats, pseudoStats);
      } else {
        this.simWeight = null; // no terms exist at all, we won't use similarity
      }
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      Terms indexTerms = context.reader().terms(field);
      if (indexTerms == null) {
        return super.matches(context, doc);
      }
      List<Term> termList = Arrays.stream(terms).map(t -> new Term(field, t.term)).toList();
      return MatchesUtils.forField(
          field,
          () -> DisjunctionMatchesIterator.fromTerms(context, doc, getQuery(), field, termList));
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          final float freq;
          if (scorer instanceof SynonymScorer) {
            freq = ((SynonymScorer) scorer).freq();
          } else if (scorer instanceof FreqBoostTermScorer) {
            freq = ((FreqBoostTermScorer) scorer).freq();
          } else {
            assert scorer instanceof TermScorer;
            freq = ((TermScorer) scorer).freq();
          }
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          NumericDocValues norms = context.reader().getNormValues(field);
          long norm = 1L;
          if (norms != null && norms.advanceExact(doc)) {
            norm = norms.longValue();
          }
          Explanation scoreExplanation = simWeight.explain(freqExplanation, norm);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight("
                  + getQuery()
                  + " in "
                  + doc
                  + ") ["
                  + similarity.getClass().getSimpleName()
                  + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      @SuppressWarnings({"rawtypes", "unchecked"})
      IOSupplier<TermState>[] termStateSuppliers = new IOSupplier[terms.length];
      for (int i = 0; i < terms.length; i++) {
        // schedule the I/O for terms dictionary lookups in the background
        termStateSuppliers[i] = termStates[i].get(context);
      }

      return new ScorerSupplier() {

        List<PostingsEnum> iterators;
        List<ImpactsEnum> impacts;
        List<Float> termBoosts;
        long cost;

        private void init() throws IOException {
          if (iterators != null) {
            return;
          }
          iterators = new ArrayList<>();
          impacts = new ArrayList<>();
          termBoosts = new ArrayList<>();
          cost = 0L;

          for (int i = 0; i < terms.length; i++) {
            IOSupplier<TermState> supplier = termStateSuppliers[i];
            TermState state = supplier == null ? null : supplier.get();
            if (state != null) {
              TermsEnum termsEnum = context.reader().terms(field).iterator();
              termsEnum.seekExact(terms[i].term, state);
              if (scoreMode == ScoreMode.TOP_SCORES) {
                ImpactsEnum impactsEnum = termsEnum.impacts(PostingsEnum.FREQS);
                iterators.add(impactsEnum);
                impacts.add(impactsEnum);
              } else {
                PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
                iterators.add(postingsEnum);
                impacts.add(new SlowImpactsEnum(postingsEnum));
              }
              termBoosts.add(terms[i].boost);
            }
          }

          for (DocIdSetIterator iterator : iterators) {
            cost += iterator.cost();
          }
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
          init();

          if (iterators.isEmpty()) {
            return new ConstantScoreScorer(0f, scoreMode, DocIdSetIterator.empty());
          }

          NumericDocValues norms = context.reader().getNormValues(field);

          // we must optimize this case (term not in segment), disjunctions require >= 2 subs
          if (iterators.size() == 1) {
            final TermScorer scorer;
            if (scoreMode == ScoreMode.TOP_SCORES) {
              scorer = new TermScorer(impacts.get(0), simWeight, norms);
            } else {
              scorer = new TermScorer(iterators.get(0), simWeight, norms);
            }
            float boost = termBoosts.get(0);
            return scoreMode == ScoreMode.COMPLETE_NO_SCORES || boost == 1f
                ? scorer
                : new FreqBoostTermScorer(boost, scorer, simWeight, norms);
          } else {

            // we use termscorers + disjunction as an impl detail
            DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
            for (int i = 0; i < iterators.size(); i++) {
              PostingsEnum postings = iterators.get(i);
              final TermScorer termScorer = new TermScorer(postings, simWeight, norms);
              float boost = termBoosts.get(i);
              final DisiWrapperFreq wrapper = new DisiWrapperFreq(termScorer, boost);
              queue.add(wrapper);
            }
            // Even though it is called approximation, it is accurate since none of
            // the sub iterators are two-phase iterators.
            DocIdSetIterator iterator = new DisjunctionDISIApproximation(queue);

            float[] boosts = new float[impacts.size()];
            for (int i = 0; i < boosts.length; i++) {
              boosts[i] = termBoosts.get(i);
            }
            ImpactsSource impactsSource = mergeImpacts(impacts.toArray(new ImpactsEnum[0]), boosts);
            MaxScoreCache maxScoreCache = new MaxScoreCache(impactsSource, simWeight);
            ImpactsDISI impactsDisi = new ImpactsDISI(iterator, maxScoreCache);

            if (scoreMode == ScoreMode.TOP_SCORES) {
              // TODO: only do this when this is the top-level scoring clause
              // (ScorerSupplier#setTopLevelScoringClause) to save the overhead of wrapping with
              // ImpactsDISI when it would not help
              iterator = impactsDisi;
            }

            return new SynonymScorer(queue, iterator, impactsDisi, simWeight, norms);
          }
        }

        @Override
        public long cost() {
          try {
            init();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          return cost;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

  /** Merge impacts for multiple synonyms. */
  static ImpactsSource mergeImpacts(ImpactsEnum[] impactsEnums, float[] boosts) {
    assert impactsEnums.length == boosts.length;
    return new ImpactsSource() {

      static class SubIterator {
        final Iterator<Impact> iterator;
        int previousFreq;
        Impact current;

        SubIterator(Iterator<Impact> iterator) {
          this.iterator = iterator;
          this.current = iterator.next();
        }

        void next() {
          previousFreq = current.freq;
          if (iterator.hasNext() == false) {
            current = null;
          } else {
            current = iterator.next();
          }
        }
      }

      @Override
      public Impacts getImpacts() throws IOException {
        final Impacts[] impacts = new Impacts[impactsEnums.length];
        // Use the impacts that have the lower next boundary as a lead.
        // It will decide on the number of levels and the block boundaries.
        Impacts tmpLead = null;
        for (int i = 0; i < impactsEnums.length; ++i) {
          impacts[i] = impactsEnums[i].getImpacts();
          if (tmpLead == null || impacts[i].getDocIdUpTo(0) < tmpLead.getDocIdUpTo(0)) {
            tmpLead = impacts[i];
          }
        }
        final Impacts lead = tmpLead;
        return new Impacts() {

          @Override
          public int numLevels() {
            // Delegate to the lead
            return lead.numLevels();
          }

          @Override
          public int getDocIdUpTo(int level) {
            // Delegate to the lead
            return lead.getDocIdUpTo(level);
          }

          /**
           * Return the minimum level whose impacts are valid up to {@code docIdUpTo}, or {@code -1}
           * if there is no such level.
           */
          private int getLevel(Impacts impacts, int docIdUpTo) {
            for (int level = 0, numLevels = impacts.numLevels(); level < numLevels; ++level) {
              if (impacts.getDocIdUpTo(level) >= docIdUpTo) {
                return level;
              }
            }
            return -1;
          }

          @Override
          public List<Impact> getImpacts(int level) {
            final int docIdUpTo = getDocIdUpTo(level);

            List<List<Impact>> toMerge = new ArrayList<>();

            for (int i = 0; i < impactsEnums.length; ++i) {
              if (impactsEnums[i].docID() <= docIdUpTo) {
                int impactsLevel = getLevel(impacts[i], docIdUpTo);
                if (impactsLevel == -1) {
                  // One instance doesn't have impacts that cover up to docIdUpTo
                  // Return impacts that trigger the maximum score
                  return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
                }
                final List<Impact> impactList;
                if (boosts[i] != 1f) {
                  float boost = boosts[i];
                  impactList =
                      impacts[i].getImpacts(impactsLevel).stream()
                          .map(
                              impact ->
                                  new Impact((int) Math.ceil(impact.freq * boost), impact.norm))
                          .toList();
                } else {
                  impactList = impacts[i].getImpacts(impactsLevel);
                }
                toMerge.add(impactList);
              }
            }
            assert toMerge.size()
                > 0; // otherwise it would mean the docID is > docIdUpTo, which is wrong

            if (toMerge.size() == 1) {
              // common if one synonym is common and the other one is rare
              return toMerge.get(0);
            }

            PriorityQueue<SubIterator> pq =
                new PriorityQueue<>(impacts.length) {
                  @Override
                  protected boolean lessThan(SubIterator a, SubIterator b) {
                    if (a.current == null) { // means iteration is finished
                      return false;
                    }
                    if (b.current == null) {
                      return true;
                    }
                    return Long.compareUnsigned(a.current.norm, b.current.norm) < 0;
                  }
                };
            for (List<Impact> impacts : toMerge) {
              pq.add(new SubIterator(impacts.iterator()));
            }

            List<Impact> mergedImpacts = new ArrayList<>();

            // Idea: merge impacts by norm. The tricky thing is that we need to
            // consider norm values that are not in the impacts too. For
            // instance if the list of impacts is [{freq=2,norm=10}, {freq=4,norm=12}],
            // there might well be a document that has a freq of 2 and a length of 11,
            // which was just not added to the list of impacts because {freq=2,norm=10}
            // is more competitive. So the way it works is that we track the sum of
            // the term freqs that we have seen so far in order to account for these
            // implicit impacts.

            long sumTf = 0;
            SubIterator top = pq.top();
            do {
              final long norm = top.current.norm;
              do {
                sumTf += top.current.freq - top.previousFreq;
                top.next();
                top = pq.updateTop();
              } while (top.current != null && top.current.norm == norm);

              final int freqUpperBound = (int) Math.min(Integer.MAX_VALUE, sumTf);
              if (mergedImpacts.isEmpty()) {
                mergedImpacts.add(new Impact(freqUpperBound, norm));
              } else {
                Impact prevImpact = mergedImpacts.get(mergedImpacts.size() - 1);
                assert Long.compareUnsigned(prevImpact.norm, norm) < 0;
                if (freqUpperBound > prevImpact.freq) {
                  mergedImpacts.add(new Impact(freqUpperBound, norm));
                } // otherwise the previous impact is already more competitive
              }
            } while (top.current != null);

            return mergedImpacts;
          }
        };
      }

      @Override
      public void advanceShallow(int target) throws IOException {
        for (ImpactsEnum impactsEnum : impactsEnums) {
          if (impactsEnum.docID() < target) {
            impactsEnum.advanceShallow(target);
          }
        }
      }
    };
  }

  private static class SynonymScorer extends Scorer {

    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final MaxScoreCache maxScoreCache;
    private final ImpactsDISI impactsDisi;
    private final SimScorer scorer;
    private final NumericDocValues norms;

    SynonymScorer(
        DisiPriorityQueue queue,
        DocIdSetIterator iterator,
        ImpactsDISI impactsDisi,
        SimScorer scorer,
        NumericDocValues norms) {
      this.queue = queue;
      this.iterator = iterator;
      this.maxScoreCache = impactsDisi.getMaxScoreCache();
      this.impactsDisi = impactsDisi;
      this.scorer = scorer;
      this.norms = norms;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    float freq() throws IOException {
      DisiWrapperFreq w = (DisiWrapperFreq) queue.topList();
      float freq = w.freq();
      for (w = (DisiWrapperFreq) w.next; w != null; w = (DisiWrapperFreq) w.next) {
        freq += w.freq();
      }
      return freq;
    }

    @Override
    public float score() throws IOException {
      long norm = 1L;
      if (norms != null && norms.advanceExact(iterator.docID())) {
        norm = norms.longValue();
      }
      return scorer.score(freq(), norm);
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return maxScoreCache.getMaxScore(upTo);
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return maxScoreCache.advanceShallow(target);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) {
      impactsDisi.setMinCompetitiveScore(minScore);
    }
  }

  private static class DisiWrapperFreq extends DisiWrapper {
    final PostingsEnum pe;
    final float boost;

    DisiWrapperFreq(Scorer scorer, float boost) {
      super(scorer);
      this.pe = (PostingsEnum) scorer.iterator();
      this.boost = boost;
    }

    float freq() throws IOException {
      return boost * pe.freq();
    }
  }

  private static class FreqBoostTermScorer extends FilterScorer {
    final float boost;
    final TermScorer in;
    final SimScorer scorer;
    final NumericDocValues norms;

    public FreqBoostTermScorer(
        float boost, TermScorer in, SimScorer scorer, NumericDocValues norms) {
      super(in);
      if (Float.isNaN(boost) || Float.compare(boost, 0f) < 0 || Float.compare(boost, 1f) > 0) {
        throw new IllegalArgumentException(
            "boost must be a positive float between 0 (exclusive) and 1 (inclusive)");
      }
      this.boost = boost;
      this.in = in;
      this.scorer = scorer;
      this.norms = norms;
    }

    float freq() throws IOException {
      return boost * in.freq();
    }

    @Override
    public float score() throws IOException {
      long norm = 1L;
      if (norms != null && norms.advanceExact(in.docID())) {
        norm = norms.longValue();
      }
      return scorer.score(freq(), norm);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return in.advanceShallow(target);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      in.setMinCompetitiveScore(minScore);
    }
  }

  private record TermAndBoost(BytesRef term, float boost) {}
}
