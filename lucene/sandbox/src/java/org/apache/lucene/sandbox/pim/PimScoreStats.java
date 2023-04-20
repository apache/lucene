package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;

public class PimScoreStats {

  public final IndexSearcher searcher;
  public final Similarity similarity;
  public final ScoreMode scoreMode;
  public final float boost;
  public final CollectionStatistics collectionStats;

  public PimScoreStats(IndexSearcher searcher,
                       Similarity similarity,
                       ScoreMode scoreMode,
                       float boost,
                       CollectionStatistics collectionStats) {
    this.searcher = searcher;
    this.similarity = similarity;
    this.scoreMode = scoreMode;
    this.boost = boost;
    this.collectionStats = collectionStats;
  }
}
