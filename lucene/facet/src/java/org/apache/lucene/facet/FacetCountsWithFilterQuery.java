package org.apache.lucene.facet;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;

/**
 * Base class for facet counts.
 * It allows for a query to be passed in to filter the match set.
 */
public abstract class FacetCountsWithFilterQuery extends Facets {
  /**
   * Optional: if specified, we first test this Query to see whether the document should be checked
   * for matching ranges. If this is null, all documents are checked.
   */
  protected final Query fastMatchQuery;

  /** Create {@code FacetCounts} */
  protected FacetCountsWithFilterQuery(Query fastMatchQuery) {
    this.fastMatchQuery = fastMatchQuery;
  }

  /**
   * Create a {@link org.apache.lucene.search.DocIdSetIterator} from the provided {@code hits} that
   * relies on {@code fastMatchQuery} if available for first-pass filtering. A null response
   * indicates no documents will match.
   */
  protected DocIdSetIterator createIterator(FacetsCollector.MatchingDocs hits) throws IOException {
    if (fastMatchQuery == null) {
      return hits.bits.iterator();
    }

    final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(hits.context);
    final IndexSearcher searcher = new IndexSearcher(topLevelContext);
    searcher.setQueryCache(null);
    final Weight fastMatchWeight =
        searcher.createWeight(searcher.rewrite(fastMatchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
    final Scorer s = fastMatchWeight.scorer(hits.context);
    if (s == null) {
      return null; // no hits from the fastMatchQuery; return null
    }

    DocIdSetIterator fastMatchDocs = s.iterator();
    return ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), fastMatchDocs));
  }
}
