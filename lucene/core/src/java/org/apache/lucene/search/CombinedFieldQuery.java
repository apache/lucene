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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SmallFloat;

/**
 * A {@link Query} that treats multiple fields as a single stream and scores terms as if they had
 * been indexed in a single field whose values would be the union of the values of the provided
 * fields.
 *
 * <p>The query works as follows:
 *
 * <ol>
 *   <li>Given a list of fields and weights, it pretends there is a synthetic combined field where
 *       all terms have been indexed. It computes new term and collection statistics for this
 *       combined field.
 *   <li>It uses a disjunction iterator and {@link IndexSearcher#getSimilarity} to score documents.
 * </ol>
 *
 * <p>In order for a similarity to be compatible, {@link Similarity#computeNorm} must be additive:
 * the norm of the combined field is the sum of norms for each individual field. The norms must also
 * be encoded using {@link SmallFloat#intToByte4}. These requirements hold for all similarities that
 * don't customize {@link Similarity#computeNorm}, which includes {@link BM25Similarity} and {@link
 * DFRSimilarity}. Per-field similarities are not supported.
 *
 * <p>The query also requires that either all fields or no fields have norms enabled. Having only
 * some fields with norms enabled can result in errors.
 *
 * <p>This query assumes that all fields share the same analyzer. Scores may not make much sense if
 * all fields don't have the same analyzer.
 *
 * <p>The scoring is based on BM25F's simple formula described in:
 * http://www.staff.city.ac.uk/~sb317/papers/foundations_bm25_review.pdf. This query implements the
 * same approach but allows other similarities besides {@link
 * org.apache.lucene.search.similarities.BM25Similarity}.
 *
 * @lucene.experimental
 */
public final class CombinedFieldQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(CombinedFieldQuery.class);

  /** A builder for {@link CombinedFieldQuery}. */
  public static class Builder {
    private final Map<String, FieldAndWeight> fieldAndWeights = new HashMap<>();
    private final BytesRef term;

    /** Create a builder for the given term {@link String}. */
    public Builder(String term) {
      this.term = new BytesRef(term);
    }

    /** Create a builder for the given term bytes. */
    public Builder(BytesRef term) {
      this.term = BytesRef.deepCopyOf(term);
    }

    /**
     * Adds a field to this builder.
     *
     * @param field The field name.
     */
    public Builder addField(String field) {
      return addField(field, 1f);
    }

    /**
     * Adds a field to this builder.
     *
     * @param field The field name.
     * @param weight The weight associated to this field.
     */
    public Builder addField(String field, float weight) {
      if (weight < 1) {
        throw new IllegalArgumentException("weight must be greater or equal to 1");
      }
      fieldAndWeights.put(field, new FieldAndWeight(field, weight));
      return this;
    }

    /** Builds the {@link CombinedFieldQuery}. */
    public CombinedFieldQuery build() {
      if (fieldAndWeights.size() > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      return new CombinedFieldQuery(new TreeMap<>(fieldAndWeights), term);
    }
  }

  record FieldAndWeight(String field, float weight) {}

  // sorted map for fields.
  private final TreeMap<String, FieldAndWeight> fieldAndWeights;
  // term bytes
  private final BytesRef term;
  // array of terms per field, sorted by field
  private final Term[] fieldTerms;

  private final long ramBytesUsed;

  private CombinedFieldQuery(TreeMap<String, FieldAndWeight> fieldAndWeights, BytesRef term) {
    this.fieldAndWeights = fieldAndWeights;
    this.term = Objects.requireNonNull(term);
    if (fieldAndWeights.size() > IndexSearcher.getMaxClauseCount()) {
      throw new IndexSearcher.TooManyClauses();
    }
    this.fieldTerms = new Term[fieldAndWeights.size()];
    int pos = 0;
    for (String field : fieldAndWeights.keySet()) {
      fieldTerms[pos++] = new Term(field, term);
    }

    this.ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(fieldAndWeights)
            + RamUsageEstimator.sizeOfObject(fieldTerms)
            + RamUsageEstimator.sizeOfObject(term);
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("CombinedFieldQuery((");
    int pos = 0;
    for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(fieldWeight.field);
      if (fieldWeight.weight != 1f) {
        builder.append("^");
        builder.append(fieldWeight.weight);
      }
    }
    builder.append(")(");
    builder.append(Term.toString(term));
    builder.append("))");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (sameClassAs(o) == false) return false;
    CombinedFieldQuery that = (CombinedFieldQuery) o;
    return Objects.equals(fieldAndWeights, that.fieldAndWeights) && term.equals(that.term);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hash(fieldAndWeights);
    result = 31 * result + term.hashCode();
    return result;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (fieldAndWeights.isEmpty()) {
      return new BooleanQuery.Builder().build();
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    Term[] selectedTerms =
        Arrays.stream(fieldTerms).filter(t -> visitor.acceptField(t.field())).toArray(Term[]::new);
    if (selectedTerms.length > 0) {
      QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
      v.consumeTerms(this, selectedTerms);
    }
  }

  private BooleanQuery rewriteToBoolean() {
    // rewrite to a simple disjunction if the score is not needed.
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (Term term : fieldTerms) {
      bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
    }
    return bq.build();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    validateConsistentNorms(searcher.getIndexReader());
    if (scoreMode.needsScores()) {
      return new CombinedFieldWeight(this, searcher, scoreMode, boost);
    } else {
      // rewrite to a simple disjunction if the score is not needed.
      Query bq = rewriteToBoolean();
      return searcher.rewrite(bq).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, boost);
    }
  }

  private void validateConsistentNorms(IndexReader reader) {
    boolean allFieldsHaveNorms = true;
    boolean noFieldsHaveNorms = true;

    for (LeafReaderContext context : reader.leaves()) {
      FieldInfos fieldInfos = context.reader().getFieldInfos();
      for (String field : fieldAndWeights.keySet()) {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        if (fieldInfo != null) {
          allFieldsHaveNorms &= fieldInfo.hasNorms();
          noFieldsHaveNorms &= fieldInfo.omitsNorms();
        }
      }
    }

    if (allFieldsHaveNorms == false && noFieldsHaveNorms == false) {
      throw new IllegalArgumentException(
          getClass().getSimpleName()
              + " requires norms to be consistent across fields: some fields cannot "
              + " have norms enabled, while others have norms disabled");
    }
  }

  class CombinedFieldWeight extends Weight {
    private final IndexSearcher searcher;
    private final TermStates[] termStates;
    private final Similarity.SimScorer simWeight;

    CombinedFieldWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.searcher = searcher;
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[fieldTerms.length];
      for (int i = 0; i < termStates.length; i++) {
        FieldAndWeight field = fieldAndWeights.get(fieldTerms[i].field());
        TermStates ts = TermStates.build(searcher, fieldTerms[i], true);
        termStates[i] = ts;
        if (ts.docFreq() > 0) {
          TermStatistics termStats =
              searcher.termStatistics(fieldTerms[i], ts.docFreq(), ts.totalTermFreq());
          docFreq = Math.max(termStats.docFreq(), docFreq);
          totalTermFreq += (double) field.weight * termStats.totalTermFreq();
        }
      }
      if (docFreq > 0) {
        CollectionStatistics pseudoCollectionStats = mergeCollectionStatistics(searcher);
        TermStatistics pseudoTermStatistics =
            new TermStatistics(new BytesRef("pseudo_term"), docFreq, Math.max(1, totalTermFreq));
        this.simWeight =
            searcher.getSimilarity().scorer(boost, pseudoCollectionStats, pseudoTermStatistics);
      } else {
        this.simWeight = null;
      }
    }

    private CollectionStatistics mergeCollectionStatistics(IndexSearcher searcher)
        throws IOException {
      long maxDoc = 0;
      long docCount = 0;
      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      for (FieldAndWeight fieldWeight : fieldAndWeights.values()) {
        CollectionStatistics collectionStats = searcher.collectionStatistics(fieldWeight.field);
        if (collectionStats != null) {
          maxDoc = Math.max(collectionStats.maxDoc(), maxDoc);
          docCount = Math.max(collectionStats.docCount(), docCount);
          sumDocFreq = Math.max(collectionStats.sumDocFreq(), sumDocFreq);
          sumTotalTermFreq += (double) fieldWeight.weight * collectionStats.sumTotalTermFreq();
        }
      }

      return new CollectionStatistics(
          "pseudo_field", maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      Weight weight =
          searcher.rewrite(rewriteToBoolean()).createWeight(searcher, ScoreMode.COMPLETE, 1f);
      return weight.matches(context, doc);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          assert scorer instanceof CombinedFieldScorer;
          float freq = ((CombinedFieldScorer) scorer).freq();
          MultiNormsLeafSimScorer docScorer =
              new MultiNormsLeafSimScorer(
                  simWeight, context.reader(), fieldAndWeights.values(), true);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + "), result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<FieldAndWeight> fields = new ArrayList<>();
      long cost = 0;
      for (int i = 0; i < fieldTerms.length; i++) {
        IOSupplier<TermState> supplier = termStates[i].get(context);
        TermState state = supplier == null ? null : supplier.get();
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(fieldTerms[i].field()).iterator();
          termsEnum.seekExact(fieldTerms[i].bytes(), state);
          PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
          iterators.add(postingsEnum);
          fields.add(fieldAndWeights.get(fieldTerms[i].field()));
          cost += postingsEnum.cost();
        }
      }

      if (iterators.isEmpty()) {
        return null;
      }

      MultiNormsLeafSimScorer scoringSimScorer =
          new MultiNormsLeafSimScorer(simWeight, context.reader(), fieldAndWeights.values(), true);

      final long finalCost = cost;
      return new ScorerSupplier() {

        @Override
        public Scorer get(long leadCost) throws IOException {
          // we use termscorers + disjunction as an impl detail
          List<DisiWrapper> wrappers = new ArrayList<>(iterators.size());
          for (int i = 0; i < iterators.size(); i++) {
            float weight = fields.get(i).weight;
            wrappers.add(
                new WeightedDisiWrapper(new TermScorer(iterators.get(i), simWeight, null), weight));
          }
          // Even though it is called approximation, it is accurate since none of
          // the sub iterators are two-phase iterators.
          DisjunctionDISIApproximation iterator =
              new DisjunctionDISIApproximation(wrappers, leadCost);
          return new CombinedFieldScorer(iterator, scoringSimScorer);
        }

        @Override
        public long cost() {
          return finalCost;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class WeightedDisiWrapper extends DisiWrapper {
    final PostingsEnum postingsEnum;
    final float weight;

    WeightedDisiWrapper(Scorer scorer, float weight) {
      super(scorer, false);
      this.weight = weight;
      this.postingsEnum = (PostingsEnum) scorer.iterator();
    }

    float freq() throws IOException {
      return weight * postingsEnum.freq();
    }
  }

  private static class CombinedFieldScorer extends Scorer {
    private final DisjunctionDISIApproximation iterator;
    private final MultiNormsLeafSimScorer simScorer;
    private final float maxScore;

    CombinedFieldScorer(DisjunctionDISIApproximation iterator, MultiNormsLeafSimScorer simScorer) {
      this.iterator = iterator;
      this.simScorer = simScorer;
      this.maxScore = simScorer.getSimScorer().score(Float.POSITIVE_INFINITY, 1L);
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    float freq() throws IOException {
      DisiWrapper w = iterator.topList();
      float freq = ((WeightedDisiWrapper) w).freq();
      for (w = w.next; w != null; w = w.next) {
        freq += ((WeightedDisiWrapper) w).freq();
        if (freq < 0) { // overflow
          return Integer.MAX_VALUE;
        }
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
      return maxScore;
    }
  }
}
