package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Weight class for phrase search on PIM system */
public class PimPhraseWeight extends Weight {

  private final PimPhraseScoreStats scoreStats;

  protected PimPhraseWeight(PimPhraseQuery query, PimPhraseScoreStats scoreStats) {
    super(query);
    this.scoreStats = scoreStats;
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    return new PimBulkScorer(scorer(context));
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    return matchPhrase(context);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return explainWithPhraseQuery(context, doc);
  }

  private Explanation explainWithPhraseQuery(LeafReaderContext context, int doc)
      throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    return weight.explain(context, doc);
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  private Scorer matchPhrase(LeafReaderContext context) throws IOException {

    // The score can be computed in the DPUs, but it needs to store the table doc->norm (see
    // LeafSimScorer.getNormValue())
    // this table can be an array: n bits per norm value, with n determined based on [min,max]
    // value.

    // if the PIM system is ready to answer queries for this context use it
    if (PimSystemManager.get().isReady(context)
        && PimSystemManager.get().isQuerySupported(getQuery())) {

      System.out.println(
          ">> Query is offloaded to PIM system "
              + (PimSystemManager.USE_SOFTWARE_MODEL ? "simulator" : "hardware")
              + " for segment "
              + context.ord);
      PimPhraseQuery pimQuery = (PimPhraseQuery) getQuery();
      LeafSimScorer simScorer =
          new LeafSimScorer(
              scoreStats.similarity.scorer(
                  scoreStats.boost, scoreStats.collectionStats, scoreStats.termStats),
              context.reader(),
              pimQuery.getField(),
              scoreStats.scoreMode.needsScores());
      try {
        return new PimScorer(
            this,
            new DpuResults(pimQuery, PimSystemManager.get().search(context, pimQuery), simScorer));
      } catch (PimSystemManager.PimQueryQueueFullException e) {
        // PimSystemManager queue is full, handle the query on CPU
        return matchWithPhraseQuery(context);
        // for testing fail if this happens
        // System.out.println(e.getMessage());
        // throw new RuntimeException();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    } else {
      return matchWithPhraseQuery(context);
    }
  }

  private Scorer matchWithPhraseQuery(LeafReaderContext context) throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    return weight.scorer(context);
  }

  private PhraseQuery buildPhraseQuery() {
    PimPhraseQuery query = (PimPhraseQuery) getQuery();
    PhraseQuery.Builder builder = new PhraseQuery.Builder().setSlop(query.getSlop());
    for (int i = 0; i < query.getTerms().length; i++) {
      builder.add(query.getTerms()[i], query.getPositions()[i]);
    }
    return builder.build();
  }
}
