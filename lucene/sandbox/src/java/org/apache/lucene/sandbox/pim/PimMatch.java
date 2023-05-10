package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.DocIdSetIterator;
import java.util.Objects;

public class PimMatch {

  public static final PimMatch UNSET = new PimMatch(-1, 0f);
  public static final PimMatch NO_MORE_RESULTS = new PimMatch(DocIdSetIterator.NO_MORE_DOCS, 0f);

  public final int docId;
  public final float score;

  public PimMatch(int docId, float score) {
    this.docId = docId;
    this.score = score;
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) return true;
    if (!(o instanceof PimMatch)) {
      return false;
    }
    PimMatch other = (PimMatch) o;
    return (docId == other.docId) && (score == other.score);
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, score);
  }


}
