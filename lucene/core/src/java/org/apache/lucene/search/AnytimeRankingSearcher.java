package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.HeuristicSLAEstimator;
import org.apache.lucene.util.SLAEstimator;

/**
 * SLA-aware searcher that applies bin-aware boosting and early termination.
 */
public final class AnytimeRankingSearcher {

  private final IndexSearcher searcher;
  private final int topK;
  private final long baseSlaMs;
  private final SLAEstimator estimator;

  /** Constructs a searcher with SLA cutoff and bin boosting. */
  public AnytimeRankingSearcher(IndexSearcher searcher, int topK, long baseSlaMs, String field) {
    this.searcher = searcher;
    this.topK = topK;
    this.baseSlaMs = baseSlaMs;
    this.estimator = new HeuristicSLAEstimator(field);
  }

  /** Executes a search with bin-aware scoring and SLA cutoff. */
  public TopDocs search(Query query) throws IOException {
    long estimated = (long) estimator.estimate(query, searcher.getIndexReader(), baseSlaMs);
    AnytimeRankingCollectorManager manager =
        new AnytimeRankingCollectorManager(topK, estimated, searcher.getIndexReader());
    return searcher.search(query, manager);
  }

  /** Returns the wrapped IndexSearcher. */
  public IndexSearcher getSearcher() {
    return searcher;
  }
}