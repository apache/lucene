package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.DocIdSetIterator;

public class PIMMatch {

  public static final PIMMatch UNSET = new PIMMatch(-1, 0f);
  public static final PIMMatch NO_MORE_RESULTS = new PIMMatch(DocIdSetIterator.NO_MORE_DOCS, 0f);

  public final int docId;
  public final float score;

  public PIMMatch(int docId, float score) {
    this.docId = docId;
    this.score = score;
  }
}
