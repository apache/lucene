package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PIMPhraseWeight extends Weight {

  private final PIMPhraseScoreStats scoreStats;

  protected PIMPhraseWeight(PIMPhraseQuery query, PIMPhraseScoreStats scoreStats) {
    super(query);
    this.scoreStats = scoreStats;
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    return new PIMBulkScorer(scorer(context));
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    return new PIMScorer(this, matchPhrase(context));
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return explainWithPhraseQuery(context, doc);
  }

  private Explanation explainWithPhraseQuery(LeafReaderContext context, int doc) throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    return weight.explain(context, doc);
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  private List<PIMMatch> matchPhrase(LeafReaderContext context) throws IOException {
    //PIMMatcher pimMatcher = context.reader().pimMatcher(field);
    //The score can be computed in the DPUs, but it needs to store the table doc->norm (see LeafSimScorer.getNormValue())
    // this table can be an array: n bits per norm value, with n determined based on [min,max] value.
    return matchWithPhraseQuery(context);
  }

  private List<PIMMatch> matchWithPhraseQuery(LeafReaderContext context) throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    BulkScorer bulkScorer = weight.bulkScorer(context);
    if (bulkScorer == null) {
      return Collections.emptyList();
    }
    List<PIMMatch> matches = new ArrayList<>();
    LeafCollector collector = new LeafCollector() {

      Scorable scorer;

      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        matches.add(new PIMMatch(doc, scorer.score()));
      }
    };
    bulkScorer.score(collector, null);
    return matches;
  }

  private PhraseQuery buildPhraseQuery() {
    PIMPhraseQuery query = (PIMPhraseQuery) getQuery();
    PhraseQuery.Builder builder = new PhraseQuery.Builder()
      .setSlop(query.getSlop());
    for (int i = 0; i < query.getTerms().length; i++) {
      builder.add(query.getTerms()[i], query.getPositions()[i]);
    }
    return builder.build();
  }
}
