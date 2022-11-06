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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

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
      Collections.sort(terms, Comparator.comparing(a -> a.term));
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
    this.field = field;
  }

  public List<Term> getTerms() {
    return Collections.unmodifiableList(
        Arrays.stream(terms).map(TermAndBoost::getTerm).collect(Collectors.toList()));
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("Synonym(");
    for (int i = 0; i < terms.length; i++) {
      if (i != 0) {
        builder.append(" ");
      }
      Query termQuery = new TermQuery(terms[i].term);
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
    return sameClassAs(other) && Arrays.equals(terms, ((SynonymQuery) other).terms);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    // optimize zero and non-boosted single term cases
    if (terms.length == 0) {
      return new BooleanQuery.Builder().build();
    }
    if (terms.length == 1 && terms[0].boost == 1f) {
      return new TermQuery(terms[0].term);
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    Term[] ts = Arrays.stream(terms).map(t -> t.term).toArray(Term[]::new);
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
        bq.add(new TermQuery(term.term), BooleanClause.Occur.SHOULD);
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
      CollectionStatistics collectionStats = searcher.collectionStatistics(terms[0].term.field());
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[terms.length];
      for (int i = 0; i < termStates.length; i++) {
        TermStates ts = TermStates.build(searcher.getTopReaderContext(), terms[i].term, true);
        termStates[i] = ts;
        if (ts.docFreq() > 0) {
          TermStatistics termStats =
              searcher.termStatistics(terms[i].term, ts.docFreq(), ts.totalTermFreq());
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
      String field = terms[0].term.field();
      Terms indexTerms = context.reader().terms(field);
      if (indexTerms == null) {
        return super.matches(context, doc);
      }
      List<Term> termList =
          Arrays.stream(terms).map(TermAndBoost::getTerm).collect(Collectors.toList());
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
          LeafSimScorer docScorer =
              new LeafSimScorer(simWeight, context.reader(), terms[0].term.field(), true);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
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
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<ImpactsEnum> impacts = new ArrayList<>();
      List<Float> termBoosts = new ArrayList<>();
      for (int i = 0; i < terms.length; i++) {
        TermState state = termStates[i].get(context);
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(terms[i].term.field()).iterator();
          termsEnum.seekExact(terms[i].term.bytes(), state);
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

      if (iterators.isEmpty()) {
        return null;
      }

      LeafSimScorer simScorer =
          new LeafSimScorer(simWeight, context.reader(), terms[0].term.field(), true);

      // we must optimize this case (term not in segment), disjunctions require >= 2 subs
      if (iterators.size() == 1) {
        final TermScorer scorer;
        if (scoreMode == ScoreMode.TOP_SCORES) {
          scorer = new TermScorer(this, impacts.get(0), simScorer);
        } else {
          scorer = new TermScorer(this, iterators.get(0), simScorer);
        }
        float boost = termBoosts.get(0);
        return scoreMode == ScoreMode.COMPLETE_NO_SCORES || boost == 1f
            ? scorer
            : new FreqBoostTermScorer(boost, scorer, simScorer);
      }

      // we use termscorers + disjunction as an impl detail
      DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
      for (int i = 0; i < iterators.size(); i++) {
        PostingsEnum postings = iterators.get(i);
        final TermScorer termScorer = new TermScorer(this, postings, simScorer);
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
      ImpactsDISI impactsDisi = new ImpactsDISI(iterator, impactsSource, simScorer.getSimScorer());

      if (scoreMode == ScoreMode.TOP_SCORES) {
        iterator = impactsDisi;
      }

      return new SynonymScorer(this, queue, iterator, impactsDisi, simScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

  /** Merge impacts for multiple synonyms. */
  static ImpactsSource mergeImpacts(ImpactsEnum[] impactsEnums, float[] boosts) {
    assert impactsEnums.length == boosts.length;
    return new SynonymImpactsSource(impactsEnums, boosts);
  }

  private static class SynonymScorer extends Scorer {

    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final ImpactsDISI impactsDisi;
    private final LeafSimScorer simScorer;

    SynonymScorer(
        Weight weight,
        DisiPriorityQueue queue,
        DocIdSetIterator iterator,
        ImpactsDISI impactsDisi,
        LeafSimScorer simScorer) {
      super(weight);
      this.queue = queue;
      this.iterator = iterator;
      this.impactsDisi = impactsDisi;
      this.simScorer = simScorer;
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
      return simScorer.score(iterator.docID(), freq());
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return impactsDisi.getMaxScore(upTo);
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return impactsDisi.advanceShallow(target);
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
    final LeafSimScorer docScorer;

    public FreqBoostTermScorer(float boost, TermScorer in, LeafSimScorer docScorer) {
      super(in);
      if (Float.isNaN(boost) || Float.compare(boost, 0f) < 0 || Float.compare(boost, 1f) > 0) {
        throw new IllegalArgumentException(
            "boost must be a positive float between 0 (exclusive) and 1 (inclusive)");
      }
      this.boost = boost;
      this.in = in;
      this.docScorer = docScorer;
    }

    float freq() throws IOException {
      return boost * in.freq();
    }

    @Override
    public float score() throws IOException {
      assert docID() != DocIdSetIterator.NO_MORE_DOCS;
      return docScorer.score(in.docID(), freq());
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

  private static class TermAndBoost {
    final Term term;
    final float boost;

    TermAndBoost(Term term, float boost) {
      this.term = term;
      this.boost = boost;
    }

    Term getTerm() {
      return term;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TermAndBoost that = (TermAndBoost) o;
      return Float.compare(that.boost, boost) == 0 && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
      return Objects.hash(term, boost);
    }
  }
}
