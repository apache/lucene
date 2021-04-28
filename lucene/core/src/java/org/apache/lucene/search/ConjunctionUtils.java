package org.apache.lucene.search;

import static org.apache.lucene.search.ConjunctionDISI.addIterator;
import static org.apache.lucene.search.ConjunctionDISI.addScorer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Helper methods for building conjunction iterators */
public final class ConjunctionUtils {

  /**
   * Create a conjunction over the provided {@link Scorer}s. Note that the returned {@link
   * DocIdSetIterator} might leverage two-phase iteration in which case it is possible to retrieve
   * the {@link TwoPhaseIterator} using {@link TwoPhaseIterator#unwrap}.
   */
  public static DocIdSetIterator intersectScorers(Collection<Scorer> scorers) {
    if (scorers.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (Scorer scorer : scorers) {
      addScorer(scorer, allIterators, twoPhaseIterators);
    }

    return ConjunctionDISI.createConjunction(allIterators, twoPhaseIterators);
  }

  /**
   * Create a conjunction over the provided DocIdSetIterators. Note that the returned {@link
   * DocIdSetIterator} might leverage two-phase iteration in which case it is possible to retrieve
   * the {@link TwoPhaseIterator} using {@link TwoPhaseIterator#unwrap}.
   */
  public static DocIdSetIterator intersectIterators(List<DocIdSetIterator> iterators) {
    if (iterators.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (DocIdSetIterator iterator : iterators) {
      addIterator(iterator, allIterators, twoPhaseIterators);
    }

    return ConjunctionDISI.createConjunction(allIterators, twoPhaseIterators);
  }
}
