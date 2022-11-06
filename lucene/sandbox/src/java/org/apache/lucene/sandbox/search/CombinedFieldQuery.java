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
package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ImpactsDISI;
import org.apache.lucene.search.ImpactsMergingUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SynonymImpactsSource;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermScorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SmallFloat;

/**
 * A {@link Query} that treats multiple fields as a single stream and scores terms as if you had
 * indexed them as a single term in a single field.
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
 * compute norms the same way as {@link SimilarityBase#computeNorm}, which includes {@link
 * BM25Similarity} and {@link DFRSimilarity}. Per-field similarities are not supported.
 *
 * <p>The query also requires that either all fields or no fields have norms enabled. Having only
 * some fields with norms enabled can result in errors.
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
    private final Set<BytesRef> termsSet = new HashSet<>();

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

    /** Adds a term to this builder. */
    public Builder addTerm(BytesRef term) {
      if (termsSet.size() > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      termsSet.add(term);
      return this;
    }

    /** Builds the {@link CombinedFieldQuery}. */
    public CombinedFieldQuery build() {
      int size = fieldAndWeights.size() * termsSet.size();
      if (size > IndexSearcher.getMaxClauseCount()) {
        throw new IndexSearcher.TooManyClauses();
      }
      BytesRef[] terms = termsSet.toArray(new BytesRef[0]);
      return new CombinedFieldQuery(new TreeMap<>(fieldAndWeights), terms);
    }
  }

  static class FieldAndWeight {
    final String field;
    final float weight;

    FieldAndWeight(String field, float weight) {
      this.field = field;
      this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldAndWeight that = (FieldAndWeight) o;
      return Float.compare(that.weight, weight) == 0 && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, weight);
    }
  }

  // sorted map for fields.
  private final TreeMap<String, FieldAndWeight> fieldAndWeights;
  // array of terms, sorted.
  private final BytesRef[] terms;
  // array of terms per field, sorted
  private final Term[] fieldTerms;

  private final long ramBytesUsed;

  private CombinedFieldQuery(TreeMap<String, FieldAndWeight> fieldAndWeights, BytesRef[] terms) {
    this.fieldAndWeights = fieldAndWeights;
    this.terms = terms;
    int numFieldTerms = fieldAndWeights.size() * terms.length;
    if (numFieldTerms > IndexSearcher.getMaxClauseCount()) {
      throw new IndexSearcher.TooManyClauses();
    }
    this.fieldTerms = new Term[numFieldTerms];
    Arrays.sort(terms);
    int pos = 0;
    for (String field : fieldAndWeights.keySet()) {
      for (BytesRef term : terms) {
        fieldTerms[pos++] = new Term(field, term);
      }
    }

    this.ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(fieldAndWeights)
            + RamUsageEstimator.sizeOfObject(fieldTerms)
            + RamUsageEstimator.sizeOfObject(terms);
  }

  public List<Term> getTerms() {
    return Collections.unmodifiableList(Arrays.asList(fieldTerms));
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
    pos = 0;
    for (BytesRef term : terms) {
      if (pos++ != 0) {
        builder.append(" ");
      }
      builder.append(term.utf8ToString());
    }
    builder.append("))");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (sameClassAs(o) == false) return false;
    CombinedFieldQuery that = (CombinedFieldQuery) o;
    return Objects.equals(fieldAndWeights, that.fieldAndWeights)
        && Arrays.equals(terms, that.terms);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hash(fieldAndWeights);
    result = 31 * result + Arrays.hashCode(terms);
    return result;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (terms.length == 0 || fieldAndWeights.isEmpty()) {
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
    private final ScoreMode scoreMode;

    CombinedFieldWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.scoreMode = scoreMode;
      this.searcher = searcher;
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[fieldTerms.length];

      for (int i = 0; i < fieldTerms.length; i++) {
        FieldAndWeight field = fieldAndWeights.get(fieldTerms[i].field());
        TermStates ts = TermStates.build(searcher.getTopReaderContext(), fieldTerms[i], true);
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
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<FieldAndWeight> fields = new ArrayList<>();
      Map<String, List<ImpactsEnum>> tempFieldImpactsEnums = new HashMap<>(fieldAndWeights.size());

      float maxWeight = Float.MIN_VALUE;
      for (int i = 0; i < fieldTerms.length; i++) {
        TermState state = termStates[i].get(context);
        if (state != null) {
          String fieldName = fieldTerms[i].field();
          fields.add(fieldAndWeights.get(fieldName));
          tempFieldImpactsEnums.putIfAbsent(fieldName, new ArrayList<>());

          TermsEnum termsEnum = context.reader().terms(fieldName).iterator();
          termsEnum.seekExact(fieldTerms[i].bytes(), state);

          if (scoreMode == ScoreMode.TOP_SCORES) {
            ImpactsEnum impactsEnum = termsEnum.impacts(PostingsEnum.FREQS);
            iterators.add(impactsEnum);
            tempFieldImpactsEnums.get(fieldName).add(impactsEnum);
          } else {
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            iterators.add(postingsEnum);
          }
        }
      }

      if (iterators.isEmpty()) {
        return null;
      }

      MultiNormsLeafSimScorer scoringSimScorer =
          new MultiNormsLeafSimScorer(simWeight, context.reader(), fieldAndWeights.values(), true);
      LeafSimScorer nonScoringSimScorer =
          new LeafSimScorer(simWeight, context.reader(), "pseudo_field", false);

      // we use termscorers + disjunction as an impl detail
      DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
      for (int i = 0; i < iterators.size(); i++) {
        FieldAndWeight fieldAndWeight = fields.get(i);
        queue.add(
            new WeightedDisiWrapper(
                new TermScorer(this, iterators.get(i), nonScoringSimScorer),
                fieldAndWeight.weight));
      }

      // Even though it is called approximation, it is accurate since none of
      // the sub iterators are two-phase iterators.
      DocIdSetIterator iterator = new DisjunctionDISIApproximation(queue);
      ImpactsDISI impactsDisi = null;

      if (scoreMode == ScoreMode.TOP_SCORES) {

        Map<String, ImpactsEnum[]> fieldImpactsEnums = new HashMap<>();
        Map<String, float[]> fieldWeights = new HashMap<>();

        for (Map.Entry<String, List<ImpactsEnum>> e : tempFieldImpactsEnums.entrySet()) {
          fieldImpactsEnums.put(e.getKey(), e.getValue().toArray(new ImpactsEnum[0]));

          float[] weights = new float[e.getValue().size()];
          Arrays.fill(weights, fieldAndWeights.get(e.getKey()).weight);
          fieldWeights.put(e.getKey(), weights);
        }

        ImpactsSource impactsSource = mergeImpacts(fieldImpactsEnums, fieldWeights);
        iterator = impactsDisi = new ImpactsDISI(iterator, impactsSource, simWeight);
      }

      return new CombinedFieldScorer(this, queue, iterator, impactsDisi, scoringSimScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  /** Merge impacts for combined field. */
  static ImpactsSource mergeImpacts(
      Map<String, ImpactsEnum[]> fieldImpactsEnum, Map<String, float[]> fieldWeights) {

    return new ImpactsSource() {
      Map<String, SynonymImpactsSource> fieldImpactsSource = null;

      @Override
      public Impacts getImpacts() throws IOException {
        if (fieldImpactsSource == null) {
          fieldImpactsSource = new HashMap<>();
          for (Map.Entry<String, ImpactsEnum[]> e : fieldImpactsEnum.entrySet()) {
            SynonymImpactsSource source =
                new SynonymImpactsSource(e.getValue(), fieldWeights.get(e.getKey()));
            fieldImpactsSource.put(e.getKey(), source);
          }
        }

        return new Impacts() {
          @Override
          public int numLevels() {
            // max of levels across fields' impactEnums
            int result = 0;

            for (SynonymImpactsSource s : fieldImpactsSource.values()) {
              try {
                result = Math.max(result, s.getImpacts().numLevels());
              } catch (IOException e) {
                // nocommit to be handled
                e.printStackTrace();
              }
            }

            return result;
          }

          @Override
          public int getDocIdUpTo(int level) {
            // min of docIdUpTo across fields' impactEnums
            int result = Integer.MAX_VALUE;

            for (SynonymImpactsSource s : fieldImpactsSource.values()) {
              Impacts impacts;
              try {
                impacts = s.getImpacts();
                if (impacts.numLevels() > level) {
                  result = Math.min(result, impacts.getDocIdUpTo(level));
                }
              } catch (IOException e) {
                // nocommit to be handled
                e.printStackTrace();
              }
            }

            return result;
          }

          // this can't loop over each field's SynonymImpactsSource.getImpacts().getImpacts(level)
          // and then combine impacts,
          // as docIdUpTo of each SynonymImpactsSource.getImpacts().getImpacts(level) might be
          // different for the same level
          @Override
          public List<Impact> getImpacts(int level) {
            final int docIdUpTo = getDocIdUpTo(level);
            final Map<String, List<Impact>> mergedImpactsPerField =
                getMergedImpactsPerField(docIdUpTo);

            return mergeImpactsAcrossFields(mergedImpactsPerField);
          }

          private Map<String, List<Impact>> getMergedImpactsPerField(int docIdUpTo) {
            final Map<String, List<Impact>> result = new HashMap<>(fieldImpactsEnum.size());

            for (Map.Entry<String, ImpactsEnum[]> e : fieldImpactsEnum.entrySet()) {
              String field = e.getKey();
              ImpactsEnum[] impactsEnums = e.getValue();
              List<Impact> mergedImpacts =
                  ImpactsMergingUtils.mergeImpactsPerField(
                      impactsEnums,
                      fieldImpactsSource.get(field).impacts(),
                      fieldWeights.get(field),
                      docIdUpTo,
                      true);

              if (mergedImpacts.size() == 0) {
                // all impactEnums for this field were positioned beyond docIdUpTo, continue to next
                // field
                continue;
              } else if (mergedImpacts.size() == 1
                  && mergedImpacts.get(0).freq == Integer.MAX_VALUE
                  && mergedImpacts.get(0).norm == 1L) {
                // one field gets impacts that trigger maximum score, pass it up
                return Collections.singletonMap(field, mergedImpacts);
              } else {
                result.put(field, mergedImpacts);
              }
            }

            return result;
          }

          private List<Impact> mergeImpactsAcrossFields(
              Map<String, List<Impact>> mergedImpactsPerField) {
            if (mergedImpactsPerField.size() == 1) {
              return mergedImpactsPerField.values().iterator().next();
            }

            // upper-bound by creating an impact that should be most competitive: <maxFreqSum,
            // minNorm>
            // this is done to avoid the potential costly combinatorial explosion from accurate
            // computation
            // on merged impacts across fields
            long maxFreqSum = 0;
            long minNorm = Long.MAX_VALUE;
            for (List<Impact> impacts : mergedImpactsPerField.values()) {
              // highest freq at the end of each impact list
              maxFreqSum += impacts.get(impacts.size() - 1).freq;
              // lowest norm at the start of each impact list
              minNorm = Math.min(minNorm, impacts.get(0).norm);
            }

            return Collections.singletonList(
                new Impact((int) Math.min(maxFreqSum, Integer.MAX_VALUE), minNorm));
          }
        };
      }

      @Override
      public void advanceShallow(int target) throws IOException {
        for (ImpactsEnum[] impactsEnums : fieldImpactsEnum.values()) {
          for (ImpactsEnum impactsEnum : impactsEnums) {
            if (impactsEnum.docID() < target) {
              impactsEnum.advanceShallow(target);
            }
          }
        }

        fieldImpactsSource = null;
      }
    };
  }

  private static class WeightedDisiWrapper extends DisiWrapper {
    final float weight;

    WeightedDisiWrapper(Scorer scorer, float weight) {
      super(scorer);
      this.weight = weight;
    }

    float freq() throws IOException {
      return weight * ((PostingsEnum) iterator).freq();
    }
  }

  private static class CombinedFieldScorer extends Scorer {
    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final MultiNormsLeafSimScorer simScorer;
    private final ImpactsDISI impactsDISI;

    CombinedFieldScorer(
        Weight weight,
        DisiPriorityQueue queue,
        DocIdSetIterator iterator,
        ImpactsDISI impactsDISI,
        MultiNormsLeafSimScorer simScorer) {
      super(weight);
      this.queue = queue;
      this.iterator = iterator;
      this.impactsDISI = impactsDISI;
      this.simScorer = simScorer;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    float freq() throws IOException {
      DisiWrapper w = queue.topList();
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
      if (impactsDISI != null) {
        return impactsDISI.getMaxScore(upTo);
      } else {
        return Float.POSITIVE_INFINITY;
      }
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      if (impactsDISI != null) {
        return impactsDISI.advanceShallow(target);
      } else {
        return super.advanceShallow(target);
      }
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (impactsDISI != null) {
        impactsDISI.setMinCompetitiveScore(minScore);
      } else {
        super.setMinCompetitiveScore(minScore);
      }
    }
  }
}
