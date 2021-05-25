/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

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
  public static DocIdSetIterator intersectIterators(List<? extends DocIdSetIterator> iterators) {
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

  /**
   * Create a conjunction over the provided set of DocIdSetIterators and TwoPhaseIterators, using
   * two-phase iterator where possible. Note that the returned {@link DocIdSetIterator} might
   * leverage two-phase iteration in which case it is possible to retrieve the {@link
   * TwoPhaseIterator} using {@link TwoPhaseIterator#unwrap}.
   *
   * @param allIterators a list of DocIdSetIterators to combine
   * @param twoPhaseIterators a list of TwoPhaseIterators to combine
   */
  public static DocIdSetIterator createConjunction(
      List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    return ConjunctionDISI.createConjunction(allIterators, twoPhaseIterators);
  }

  /**
   * Given a two-phase iterator, find any sub-iterators and add them to the provided
   * DocIdSetIterator and TwoPhaseIterator lists
   */
  public static void addTwoPhaseIterator(
      TwoPhaseIterator twoPhaseIter,
      List<DocIdSetIterator> allIterators,
      List<TwoPhaseIterator> twoPhaseIterators) {
    ConjunctionDISI.addTwoPhaseIterator(twoPhaseIter, allIterators, twoPhaseIterators);
  }

  /**
   * Given a DocIdSetIterator, find any sub-iterators or two-phase iterators and add them to the
   * provided DocIdSetIterator and TwoPhaseIterator lists
   */
  public static void addIterator(
      DocIdSetIterator disi,
      List<DocIdSetIterator> allIterators,
      List<TwoPhaseIterator> twoPhaseIterators) {
    ConjunctionDISI.addIterator(disi, allIterators, twoPhaseIterators);
  }
}
