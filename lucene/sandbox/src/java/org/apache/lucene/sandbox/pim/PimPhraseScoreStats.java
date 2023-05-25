package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

/**
 * statistics for PIM phrase search
 */
public class PimPhraseScoreStats extends PimScoreStats {

  public final TermStatistics[] termStats;

  public PimPhraseScoreStats(IndexSearcher searcher,
                             Similarity similarity,
                             ScoreMode scoreMode,
                             float boost,
                             CollectionStatistics collectionStats,
                             TermStatistics[] termStats) {
    super(searcher, similarity, scoreMode, boost, collectionStats);
    this.termStats = termStats;
  }
}
