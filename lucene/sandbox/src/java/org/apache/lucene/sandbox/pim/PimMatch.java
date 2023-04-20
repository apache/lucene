package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.DocIdSetIterator;

public class PimMatch {

  public static final PimMatch UNSET = new PimMatch(-1, 0f);
  public static final PimMatch NO_MORE_RESULTS = new PimMatch(DocIdSetIterator.NO_MORE_DOCS, 0f);

  public final int docId;
  public final float score;

  public PimMatch(int docId, float score) {
    this.docId = docId;
    this.score = score;
  }
}
