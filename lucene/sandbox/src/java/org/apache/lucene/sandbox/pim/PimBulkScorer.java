package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Bulk scorer for PIM
 */
public class PimBulkScorer extends BulkScorer {

  private final Scorer scorer;

  public PimBulkScorer(Scorer scorer) {
    this.scorer = scorer;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorer);
    DocIdSetIterator disi = scorer.iterator();
    int doc = scorer.docID();
    if (doc < min) {
      doc = disi.advance(min);
    }
    while (doc < max) {
      if (acceptDocs == null || acceptDocs.get(doc)) {
        collector.collect(doc);
      }
      doc = disi.nextDoc();
    }
    return doc;
  }

  @Override
  public long cost() {
    return 0;
  }
}
