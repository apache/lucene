package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

/**
 * Scorer for PIM
 */
public class PimScorer extends Scorer {

  private final List<PimMatch> matches;
  private int index;
  private PimMatch current;
  private float minCompetitiveScore;

  public PimScorer(Weight weight, List<PimMatch> matches) {
    super(weight);
    this.matches = matches;
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
          if (index >= matches.size()) {
            current = PimMatch.NO_MORE_RESULTS;
            break;
          }
          current = matches.get(index++);
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
