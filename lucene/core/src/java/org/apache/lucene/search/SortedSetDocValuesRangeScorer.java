package org.apache.lucene.search;

import org.apache.lucene.document.DocValuesRangeIterator;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;
import java.util.Collection;

public class SortedSetDocValuesRangeScorer extends Scorer {
  final Scorer delegate;

  public SortedSetDocValuesRangeScorer(SortedSetDocValues values, long minOrd, long maxOrd, ScoreMode scoreMode, float score, DocValuesSkipper skipper) throws IOException {
    delegate = setupScorer(values, minOrd, maxOrd, scoreMode, score, skipper);
  }

  static Scorer setupScorer(SortedSetDocValues values, long minOrd, long maxOrd, ScoreMode scoreMode, float score, DocValuesSkipper skipper) throws IOException {
    // no terms matched in this segment
    if (minOrd > maxOrd
        || (skipper != null
        && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
      return new ConstantScoreScorer(score, scoreMode, DocIdSetIterator.empty());
    }

    final SortedDocValues singleton = DocValues.unwrapSingleton(values);
    TwoPhaseIterator iterator;
    if (singleton != null) {
      iterator =
          new TwoPhaseIterator(singleton) {
            @Override
            public boolean matches() throws IOException {
              final long ord = singleton.ordValue();
              return ord >= minOrd && ord <= maxOrd;
            }

            @Override
            public float matchCost() {
              return 2; // 2 comparisons
            }
          };
    } else {
      iterator =
          new TwoPhaseIterator(values) {
            @Override
            public boolean matches() throws IOException {
              for (int i = 0; i < values.docValueCount(); i++) {
                long ord = values.nextOrd();
                if (ord < minOrd) {
                  continue;
                }
                // Values are sorted, so the first ord that is >= minOrd is our best
                // candidate
                return ord <= maxOrd;
              }
              return false; // all ords were < minOrd
            }

            @Override
            public float matchCost() {
              return 2; // 2 comparisons
            }
          };
    }
    if (skipper != null) {
      iterator = new DocValuesRangeIterator(iterator, skipper, minOrd, maxOrd);
    }
    return new ConstantScoreScorer(score, scoreMode, iterator);
  }

  @Override
  public int docID() {
    return delegate.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return delegate.iterator();
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return delegate.getMaxScore(upTo);
  }

  @Override
  public float score() throws IOException {
    return delegate.score();
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return delegate.twoPhaseIterator();
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return delegate.advanceShallow(target);
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    return delegate.smoothingScore(docId);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    delegate.setMinCompetitiveScore(minScore);
  }

  @Override
  public Collection<ChildScorable> getChildren() throws IOException {
    return delegate.getChildren();
  }
}
