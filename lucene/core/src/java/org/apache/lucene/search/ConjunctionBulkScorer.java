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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.util.Bits;

/**
 * BulkScorer implementation of {@link ConjunctionScorer}. For simplicity, it focuses on scorers
 * that produce regular {@link DocIdSetIterator}s and not {@link TwoPhaseIterator}s.
 */
final class ConjunctionBulkScorer extends BulkScorer {

  private static final int WINDOW_SIZE = 8_196;

  private final int maxDoc;
  private final Scorer[] scoringScorers;
  private final List<DocIdSetIterator> iterators;
  private final Scorable scorable;

  ConjunctionBulkScorer(int maxDoc, List<Scorer> requiredScoring, List<Scorer> requiredNoScoring)
      throws IOException {
    final int numClauses = requiredScoring.size() + requiredNoScoring.size();
    if (numClauses <= 1) {
      throw new IllegalArgumentException("Expected 2 or more clauses, got " + numClauses);
    }
    List<Scorer> allScorers = new ArrayList<>();
    allScorers.addAll(requiredScoring);
    allScorers.addAll(requiredNoScoring);

    this.maxDoc = maxDoc;
    this.scoringScorers = requiredScoring.toArray(Scorer[]::new);
    iterators = new ArrayList<>();
    for (Scorer scorer : allScorers) {
      iterators.add(scorer.iterator());
    }
    Collections.sort(iterators, Comparator.comparingLong(DocIdSetIterator::cost));
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
        };
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    for (long windowMin = min; windowMin < max; windowMin += WINDOW_SIZE) {
      final int windowMax = (int) Math.min(max, windowMin + WINDOW_SIZE);
      scoreWindow(collector, acceptDocs, (int) windowMin, windowMax);
    }

    if (max >= maxDoc) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      int next = max;
      for (DocIdSetIterator it : iterators) {
        if (it.docID() > next) {
          next = it.docID();
        }
      }
      return next;
    }
  }

  private void scoreWindow(LeafCollector collector, Bits acceptDocs, int min, int max)
      throws IOException {
    List<DocIdSetIterator> iterators = new LinkedList<>(this.iterators);
    DocIdSetIterator collectorIterator = collector.competitiveIterator();
    if (collectorIterator != null) {
      iterators.add(collectorIterator);
    }

    if (scoringScorers.length == 0) {
      // For simplicity, we only skip using peekNextNonMatchingDocID when not scoring.
      for (Iterator<DocIdSetIterator> it = iterators.iterator(); it.hasNext(); ) {
        DocIdSetIterator next = it.next();

        if (next.docID() < min && next.peekNextNonMatchingDocID() <= min) {
          // peekNextNonMatchingDocID may return a doc ID before the window only because it's not
          // advanced enough
          next.advance(min);
        }

        if (next.docID() <= min && next.peekNextNonMatchingDocID() >= max) {
          // This conjunction matches all docs in the window, we don't need to evaluate it.
          it.remove();
        }
      }
    }

    if (iterators.isEmpty()) {
      // All clauses match the whole window.
      for (int doc = min; doc < max; ++doc) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          collector.collect(doc);
        }
      }
      return;
    } else if (iterators.size() == 1) {
      // A single clause doesn't match the whole window.
      DocIdSetIterator iterator = iterators.getFirst();
      if (iterator.docID() < min) {
        iterator.advance(min);
      }
      for (int doc = iterator.docID(); doc < max; doc = iterator.nextDoc()) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          collector.collect(doc);
        }
      }
      return;
    }

    final DocIdSetIterator lead1 = iterators.get(0);
    final DocIdSetIterator lead2 = iterators.get(1);
    final DocIdSetIterator[] others =
        iterators.subList(2, iterators.size()).toArray(DocIdSetIterator[]::new);

    if (lead1.docID() < min) {
      lead1.advance(min);
    }
    if (lead1.docID() >= max || lead2.docID() >= max) {
      return;
    }

    // In the main for loop, we want to be able to rely on the invariant that lead1.docID() is
    // greater than any other doc ID. So fix it now if necessary.
    if (lead1.docID() < lead2.docID()) {
      lead1.advance(lead2.docID());
    }
    if (lead1.docID() == lead2.docID()) {
      int doc = lead1.docID();
      if (acceptDocs == null || acceptDocs.get(doc)) {
        boolean match = true;
        for (DocIdSetIterator it : others) {
          int next = it.docID();
          if (next < doc) {
            next = it.advance(doc);
          }
          if (next != doc) {
            lead1.advance(next);
            match = false;
            break;
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
        int next = it.docID();
        if (next < doc) {
          next = it.advance(doc);
        }
        if (next != doc) {
          doc = lead1.advance(next);
          continue advanceHead;
        }
      }

      collector.collect(doc);
      doc = lead1.nextDoc();
    }
  }

  @Override
  public long cost() {
    return iterators.get(0).cost();
  }
}
