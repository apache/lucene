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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {

  final DocIdSetIterator disi;
  final Collection<Scorer> scoringScorers;
  final Collection<Scorer> filterScorers;

  /**
   * Create a new {@link ConjunctionScorer} with provided filterScorers and scoringScorers. Note
   * that these two set of scorers should not have overlap now.
   */
  ConjunctionScorer(
      Weight weight, Collection<Scorer> filteringScorers, Collection<Scorer> scoringScorers) {
    super(weight);

    if (Collections.disjoint(filteringScorers, scoringScorers) == false) {
      throw new IllegalArgumentException(
          "filteringScorers and scoringScorers should not contain common elements.");
    }
    final boolean twoPhase =
        filteringScorers.stream().anyMatch(scorer -> scorer.twoPhaseIterator() != null)
            || scoringScorers.stream().anyMatch(scorer -> scorer.twoPhaseIterator() != null);

    if (twoPhase || filteringScorers.isEmpty()) {
      // do not use peek next non matching doc id
      List<Scorer> allScorers = new ArrayList<>();
      allScorers.addAll(scoringScorers);
      allScorers.addAll(filteringScorers);
      this.disi = ConjunctionUtils.intersectScorers(allScorers);
    } else {
      // use wrapper disi for filtering scorers
      final DocIdSetIterator filteringDisi = ConjunctionUtils.intersectScorers(filteringScorers);
      final DocIdSetIterator scoringDisi;
      if (scoringScorers.isEmpty()) {
        this.disi = filteringDisi;
      } else {
        scoringDisi = ConjunctionUtils.intersectScorers(scoringScorers);
        this.disi =
            new DocIdSetIterator() {
              @Override
              public int docID() {
                return scoringDisi.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                return doNext(scoringDisi.nextDoc());
              }

              @Override
              public int advance(int target) throws IOException {
                return doNext(scoringDisi.advance(target));
              }

              private int doNext(int doc) throws IOException {
                while (true) {
                  assert doc == scoringDisi.docID();
                  if (doc == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                  }

                  if (filteringDisi.docID() == NO_MORE_DOCS) {
                    scoringDisi.advance(NO_MORE_DOCS);
                    return NO_MORE_DOCS;
                  }

                  if (doc < filteringDisi.docID()) {
                    doc = scoringDisi.advance(filteringDisi.docID());
                  }

                  if (doc == filteringDisi.docID()) {
                    assert doc == scoringDisi.docID();
                    return doc;
                  } else if (doc < filteringDisi.peekNextNonMatchingDocID()) {
                    return doc;
                  } else {
                    // filteringDisi.doc <= doc
                    filteringDisi.advance(doc);
                  }
                }
              }

              @Override
              public long cost() {
                return Math.min(scoringDisi.cost(), filteringDisi.cost());
              }
            };
      }
    }

    this.scoringScorers = scoringScorers;
    this.filterScorers = filteringScorers;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return TwoPhaseIterator.unwrap(disi);
  }

  @Override
  public DocIdSetIterator iterator() {
    return disi;
  }

  @Override
  public int docID() {
    return disi.docID();
  }

  @Override
  public float score() throws IOException {
    double sum = 0.0d;
    for (Scorer scorer : scoringScorers) {
      sum += scorer.score();
    }
    return (float) sum;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    // This scorer is only used for TOP_SCORES when there is at most one scoring clause
    switch (scoringScorers.size()) {
      case 0:
        return 0;
      case 1:
        return scoringScorers.iterator().next().getMaxScore(upTo);
      default:
        return Float.POSITIVE_INFINITY;
    }
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    if (scoringScorers.size() == 1) {
      return scoringScorers.iterator().next().advanceShallow(target);
    }
    return super.advanceShallow(target);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    // This scorer is only used for TOP_SCORES when there is a single scoring clause
    if (scoringScorers.size() == 1) {
      scoringScorers.iterator().next().setMinCompetitiveScore(minScore);
    }
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    ArrayList<ChildScorable> children = new ArrayList<>();
    for (Scorer scorer : scoringScorers) {
      children.add(new ChildScorable(scorer, "MUST"));
    }
    // Did the previous implementation contain a bug, as required scorers may contain non-scoring
    // scorers?
    for (Scorer scorer : filterScorers) {
      children.add(new ChildScorable(scorer, "MUST"));
    }
    return children;
  }

  static final class DocsAndFreqs {
    final long cost;
    final DocIdSetIterator iterator;
    int doc = -1;

    DocsAndFreqs(DocIdSetIterator iterator) {
      this.iterator = iterator;
      this.cost = iterator.cost();
    }
  }
}
