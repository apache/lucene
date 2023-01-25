package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

public class PIMPhraseScoreStats extends PIMScoreStats {

  public final TermStatistics[] termStats;

  public PIMPhraseScoreStats(IndexSearcher searcher,
                             Similarity similarity,
                             ScoreMode scoreMode,
                             float boost,
                             CollectionStatistics collectionStats,
                             TermStatistics[] termStats) {
    super(searcher, similarity, scoreMode, boost, collectionStats);
    this.termStats = termStats;
  }
}
