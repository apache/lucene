package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Scorer for PIM
 */
public class PimScorer extends Scorer {

  private PimMatch current;
  private float minCompetitiveScore;
  private DpuResults dpuResults;

  public PimScorer(Weight weight, DpuResults dpuResults) {
    super(weight);
    this.dpuResults = dpuResults;
    current = PimMatch.UNSET;
  }

  @Override
  public float score() throws IOException {
    return current.score;
  }

  @Override
  public int docID() {
    return current.docId;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return current.docId;
      }

      @Override
      public int nextDoc() {
        do {
          if (!dpuResults.next()) {
            current = PimMatch.NO_MORE_RESULTS;
            break;
          }
          current = dpuResults.match();
        } while (current.score < minCompetitiveScore);
        return current.docId;
      }

      @Override
      public int advance(int target) {
        int docId;
        while ((docId = nextDoc()) < target) {
        }
        return docId;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    minCompetitiveScore = minScore;
  }

  @Override
  public float getMaxScore(int upTo) {
    throw new UnsupportedOperationException();//TODO
  }
}
