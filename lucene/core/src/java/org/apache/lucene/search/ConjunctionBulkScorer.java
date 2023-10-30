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
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.util.Bits;

/**
 * BulkScorer implementation of {@link ConjunctionScorer}. For simplicity, it focuses on scorers
 * that produce regular {@link DocIdSetIterator}s and not {@link TwoPhaseIterator}s.
 */
final class ConjunctionBulkScorer extends BulkScorer {

  private final Scorer[] scoringScorers;
  private final DocIdSetIterator lead1, lead2;
  private final List<DocIdSetIterator> others;
  private final Scorable scorable;

  ConjunctionBulkScorer(List<Scorer> requiredScoring, List<Scorer> requiredNoScoring)
      throws IOException {
    final int numClauses = requiredScoring.size() + requiredNoScoring.size();
    if (numClauses <= 1) {
      throw new IllegalArgumentException("Expected 2 or more clauses, got " + numClauses);
    }
    List<Scorer> allScorers = new ArrayList<>();
    allScorers.addAll(requiredScoring);
    allScorers.addAll(requiredNoScoring);

    this.scoringScorers = requiredScoring.toArray(Scorer[]::new);
    List<DocIdSetIterator> iterators = new ArrayList<>();
    for (Scorer scorer : allScorers) {
      iterators.add(scorer.iterator());
    }
    Collections.sort(iterators, Comparator.comparingLong(DocIdSetIterator::cost));
    lead1 = iterators.get(0);
    lead2 = iterators.get(1);
    others = List.copyOf(iterators.subList(2, iterators.size()));
    scorable =
        new Scorable() {
          @Override
          public float score() throws IOException {
            double score = 0;
            for (Scorer scorer : scoringScorers) {
              score += scorer.score();
            }
            return (float) score;
          }

          @Override
          public Collection<ChildScorable> getChildren() throws IOException {
            ArrayList<ChildScorable> children = new ArrayList<>();
            for (Scorer scorer : allScorers) {
              children.add(new ChildScorable(scorer, "MUST"));
            }
            return children;
          }

          @Override
          public int docID() {
            return lead1.docID();
          }
        };
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    assert lead1.docID() >= lead2.docID();

    if (lead1.docID() < min) {
      lead1.advance(min);
    }

    if (lead1.docID() >= max) {
      return lead1.docID();
    }

    collector.setScorer(scorable);

    List<DocIdSetIterator> otherIterators = this.others;
    DocIdSetIterator collectorIterator = collector.competitiveIterator();
    if (collectorIterator != null) {
      otherIterators = new ArrayList<>(otherIterators);
      otherIterators.add(collectorIterator);
    }

    final DocIdSetIterator[] others = otherIterators.toArray(DocIdSetIterator[]::new);

    // In the main for loop, we want to be able to rely on the invariant that lead1.docID() >
    // lead2.doc(). However it's possible that these two are equal on the first document in a
    // scoring window. So we treat this case separately here.
    if (lead1.docID() == lead2.docID()) {
      final int doc = lead1.docID();
      if (acceptDocs == null || acceptDocs.get(doc)) {
        boolean match = true;
        for (DocIdSetIterator it : others) {
          if (it.docID() < doc) {
            int next = it.advance(doc);
            if (next != doc) {
              lead1.advance(next);
              match = false;
              break;
            }
          }
          assert it.docID() == doc;
        }

        if (match) {
          collector.collect(doc);
          lead1.nextDoc();
        }
      } else {
        lead1.nextDoc();
      }
    }

    advanceHead:
    for (int doc = lead1.docID(); doc < max; ) {
      assert lead2.docID() < doc;

      if (acceptDocs != null && acceptDocs.get(doc) == false) {
        doc = lead1.nextDoc();
        continue;
      }

      // We maintain the invariant that lead2.docID() < lead1.docID() so that we don't need to check
      // if lead2 is already on the same doc as lead1 here.
      int next2 = lead2.advance(doc);
      if (next2 != doc) {
        doc = lead1.advance(next2);
        if (doc != next2) {
          continue;
        } else if (doc >= max) {
          break;
        } else if (acceptDocs != null && acceptDocs.get(doc) == false) {
          doc = lead1.nextDoc();
          continue;
        }
      }
      assert lead2.docID() == doc;

      for (DocIdSetIterator it : others) {
        if (it.docID() < doc) {
          int next = it.advance(doc);
          if (next != doc) {
            doc = lead1.advance(next);
            continue advanceHead;
          }
        }
        assert it.docID() == doc;
      }

      collector.collect(doc);
      doc = lead1.nextDoc();
    }

    return lead1.docID();
  }

  @Override
  public long cost() {
    return lead1.cost();
  }
}
