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
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;

/**
 * BulkScorer implementation of {@link ConjunctionScorer} that is specialized for dense clauses.
 * Whenever sensible, it intersects clauses by loading their matches into a bit set and computing
 * the intersection of clauses by and-ing these bit sets.
 */
final class DenseConjunctionBulkScorer extends BulkScorer {

  private record DisiWrapper(DocIdSetIterator approximation, TwoPhaseIterator twoPhase) {
    DisiWrapper(DocIdSetIterator iterator) {
      this(iterator, null);
    }

    DisiWrapper(TwoPhaseIterator twoPhase) {
      this(twoPhase.approximation(), twoPhase);
    }

    int docID() {
      return approximation().docID();
    }

    int docIDRunEnd() throws IOException {
      if (twoPhase() == null) {
        return approximation().docIDRunEnd();
      } else {
        return twoPhase().docIDRunEnd();
      }
    }

    void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      if (twoPhase() == null) {
        approximation().intoBitSet(upTo, bitSet, offset);
      } else {
        twoPhase().intoBitSet(upTo, bitSet, offset);
      }
    }
  }

  // Use a small-ish window size to make sure that we can take advantage of gaps in the postings of
  // clauses that are not leading iteration.
  static final int WINDOW_SIZE = 4096;
  // Only use bit sets to compute the intersection if more than 1/32th of the docs are expected to
  // match. Experiments suggested that values that are a bit higher than this would work better, but
  // we're erring on the conservative side.
  static final int DENSITY_THRESHOLD_INVERSE = Long.SIZE / 2;

  private final int maxDoc;
  private final List<DisiWrapper> iterators;
  private final SimpleScorable scorable;

  private final FixedBitSet windowMatches = new FixedBitSet(WINDOW_SIZE);
  private final FixedBitSet clauseWindowMatches = new FixedBitSet(WINDOW_SIZE);
  private final List<DisiWrapper> windowClauses = new ArrayList<>();
  // Reused by the leap-frog path.
  private final List<DocIdSetIterator> windowApproximations = new ArrayList<>();
  private final List<TwoPhaseIterator> windowTwoPhases = new ArrayList<>();

  static DenseConjunctionBulkScorer of(List<Scorer> filters, int maxDoc, float constantScore) {
    List<DocIdSetIterator> iterators = new ArrayList<>();
    List<TwoPhaseIterator> twoPhases = new ArrayList<>();
    for (Scorer filter : filters) {
      TwoPhaseIterator twoPhase = filter.twoPhaseIterator();
      if (twoPhase != null) {
        twoPhases.add(twoPhase);
      } else {
        iterators.add(filter.iterator());
      }
    }
    return new DenseConjunctionBulkScorer(iterators, twoPhases, maxDoc, constantScore);
  }

  DenseConjunctionBulkScorer(
      List<DocIdSetIterator> iterators,
      List<TwoPhaseIterator> twoPhases,
      int maxDoc,
      float constantScore) {
    if (iterators.isEmpty() && twoPhases.isEmpty()) {
      throw new IllegalArgumentException("Expected one or more iterators, got 0");
    }
    this.maxDoc = maxDoc;
    this.iterators = new ArrayList<>();
    for (DocIdSetIterator iterator : iterators) {
      this.iterators.add(new DisiWrapper(iterator));
    }
    for (TwoPhaseIterator twoPhase : twoPhases) {
      this.iterators.add(new DisiWrapper(twoPhase));
    }
    // Plain approximations before two-phase ones, so matches() only runs on docs that already
    // satisfy every approximation; within each group, cheapest approximation first (lead skipping).
    // Two-phase clauses that tie on approximation cost are further ordered by matchCost, so a cheap
    // confirmation (e.g. a doc values range) runs before an expensive one (e.g. a script) even when
    // both report the same approximation cost.
    this.iterators.sort(
        Comparator.<DisiWrapper>comparingInt(w -> w.twoPhase() == null ? 0 : 1)
            .thenComparingLong(w -> w.approximation().cost())
            .thenComparingDouble(w -> w.twoPhase() == null ? 0 : w.twoPhase().matchCost()));
    this.scorable = new SimpleScorable();
    scorable.score = constantScore;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    List<DisiWrapper> iterators = this.iterators;
    if (collector.competitiveIterator() != null) {
      iterators = new ArrayList<>(iterators);
      iterators.add(new DisiWrapper(collector.competitiveIterator()));
    }

    for (DisiWrapper w : iterators) {
      min = Math.max(min, w.approximation().docID());
    }

    max = Math.min(max, maxDoc);

    DisiWrapper lead = iterators.get(0);
    if (lead.docID() < min) {
      min = lead.approximation.advance(min);
    }

    while (min < max) {
      if (scorable.minCompetitiveScore > scorable.score) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }
      min = scoreWindow(collector, acceptDocs, iterators, min, max);
    }

    if (lead.docID() > max) {
      return lead.docID();
    } else if (max >= maxDoc) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return max;
    }
  }

  private static int advance(FixedBitSet set, int i) {
    if (i >= WINDOW_SIZE) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return set.nextSetBit(i);
    }
  }

  private int scoreWindow(
      LeafCollector collector, Bits acceptDocs, List<DisiWrapper> iterators, int min, int max)
      throws IOException {

    // Advance all iterators to the first doc that is greater than or equal to min. This is
    // important as this is the only place where we can take advantage of a large gap between
    // consecutive matches in any clause.
    for (DisiWrapper w : iterators) {
      if (w.docID() >= min) {
        min = w.docID();
      } else {
        min = w.approximation().advance(min);
      }
      if (min >= max) {
        return min;
      }
    }

    // Partition clauses of the conjunction into:
    //  - clauses that don't fully match the first half of the window and get evaluated via
    // #loadIntoBitSet or leaf-frog,
    //  - other clauses that are used to compute the greatest possible window size that they fully
    // match.
    // This logic helps align scoring windows with the natural #docIDRunEnd() boundaries of the
    // data, which helps evaluate fewer clauses per window - without allowing windows to become too
    // small thanks to the WINDOW_SIZE/2 threshold.
    int minDocIDRunEnd = max;
    final int minRunEndThreshold = MathUtil.unsignedMin(min + WINDOW_SIZE / 2, max);
    for (DisiWrapper w : iterators) {
      int docIdRunEnd = w.docIDRunEnd();
      if (w.docID() > min || docIdRunEnd < minRunEndThreshold) {
        windowClauses.add(w);
      } else {
        minDocIDRunEnd = Math.min(minDocIDRunEnd, docIdRunEnd);
      }
    }

    if (acceptDocs == null && windowClauses.isEmpty()) {
      // We have a large range of doc IDs that all match.
      collector.collectRange(min, minDocIDRunEnd);
      return minDocIDRunEnd;
    }

    int bitsetWindowMax = MathUtil.unsignedMin(minDocIDRunEnd, WINDOW_SIZE + min);

    // The bit-set path pays a fixed per-window cost (materialize the lead, applyMask, cardinality,
    // BitSetDocIdStream); it wins when most candidates must be tested anyway. But for a sparse
    // window confirmed by a two-phase clause that can be skipped, plain leap-frog -- which only
    // touches surviving docs and never materializes a bit set -- is cheaper.
    //
    // A two-phase clause whose approximation matches every doc (cost >= maxDoc, e.g. a skip-indexed
    // doc-values range, whose block iterator reports NO_MORE_DOCS) cannot be skipped and stays on
    // the bit-set path for its vectorized intoBitSet. A selective approximation (cost < maxDoc,
    // e.g. a phrase) can be skipped, so a sparse window is cheaper via leap-frog.
    boolean skippableTwoPhase = false;
    for (DisiWrapper w : windowClauses) {
      if (w.twoPhase() != null && w.approximation().cost() < maxDoc) {
        skippableTwoPhase = true;
        break;
      }
    }
    // "sparse" means the lead clause's cost estimates it matches at most 1/4 of the document
    // space -- cheap enough that leap-frog's per-survivor confirmation beats materializing a bit
    // set for the whole window.
    boolean sparse =
        windowClauses.isEmpty() == false
            && windowClauses.get(0).approximation().cost() <= (long) maxDoc / 4;

    if (skippableTwoPhase && sparse) {
      for (DisiWrapper w : windowClauses) {
        windowApproximations.add(w.approximation());
        if (w.twoPhase() != null) {
          windowTwoPhases.add(w.twoPhase());
        }
      }
      windowTwoPhases.sort(Comparator.comparingDouble(TwoPhaseIterator::matchCost));
      scoreWindowUsingLeapFrog(
          collector, acceptDocs, windowApproximations, windowTwoPhases, min, bitsetWindowMax);
      windowApproximations.clear();
      windowTwoPhases.clear();
    } else {
      scoreWindowUsingBitSet(collector, acceptDocs, windowClauses, min, bitsetWindowMax);
    }
    windowClauses.clear();

    return bitsetWindowMax;
  }

  private void scoreWindowUsingBitSet(
      LeafCollector collector,
      Bits acceptDocs,
      List<DisiWrapper> iterators,
      int windowBase,
      int windowMax)
      throws IOException {
    assert windowMax > windowBase;
    assert windowMatches.scanIsEmpty();
    assert clauseWindowMatches.scanIsEmpty();

    if (iterators.isEmpty()) {
      // This happens if all clauses fully matched the window and there are deleted docs.
      windowMatches.set(0, windowMax - windowBase);
    } else {
      DisiWrapper lead = iterators.get(0);
      if (lead.docID() < windowBase) {
        lead.approximation().advance(windowBase);
      }
      lead.intoBitSet(windowMax, windowMatches, windowBase);
    }

    if (acceptDocs != null) {
      // Apply live docs.
      acceptDocs.applyMask(windowMatches, windowBase);
    }

    int windowSize = windowMax - windowBase;
    int threshold = windowSize / DENSITY_THRESHOLD_INVERSE;
    int upTo = 1; // the leading clause at index 0 is already applied
    for (int cardinality = windowMatches.cardinality();
        upTo < iterators.size() && cardinality >= threshold;
        upTo++, cardinality = windowMatches.cardinality()) {
      DisiWrapper other = iterators.get(upTo);
      if (other.docID() < windowBase) {
        other.approximation().advance(windowBase);
      }
      TwoPhaseIterator twoPhase = other.twoPhase();
      if (twoPhase != null) {
        // Confirm only against docs that are still candidates: the default implementation walks
        // survivors one at a time, never decoding a doc another clause already excluded, while
        // implementations with real bulk support (e.g. a block-based skip index) can classify
        // whole spans of the candidate set without calling matches() at all.
        twoPhase.applyMask(windowMax, windowMatches, windowBase);
      } else {
        // Plain iterator: load its own matches in bulk and intersect.
        other.intoBitSet(windowMax, clauseWindowMatches, windowBase);
        windowMatches.and(clauseWindowMatches);
        clauseWindowMatches.clear();
      }
    }

    if (upTo < iterators.size()) {
      // If the leading clause is sparse on this doc ID range or if the intersection became sparse
      // after applying a few clauses, we finish evaluating the intersection using the traditional
      // leap-frog approach. This proved important with a query such as "+secretary +of +state" on
      // wikibigall, where the intersection becomes sparse after intersecting "secretary" and
      // "state". As the leap-frog only visits surviving docs, two-phase clauses confirm matches()
      // here only on docs that no cheaper clause already excluded.
      advanceHead:
      for (int windowMatch = windowMatches.nextSetBit(0);
          windowMatch != DocIdSetIterator.NO_MORE_DOCS; ) {
        int doc = windowBase + windowMatch;
        // First confirm every remaining approximation is on doc...
        for (int i = upTo; i < iterators.size(); ++i) {
          DocIdSetIterator other = iterators.get(i).approximation();
          int otherDoc = other.docID();
          if (otherDoc < doc) {
            otherDoc = other.advance(doc);
          }
          if (doc != otherDoc) {
            windowMatch = advance(windowMatches, otherDoc - windowBase);
            continue advanceHead;
          }
        }
        // ...then run the (more expensive) two-phase confirmations, only on surviving docs.
        for (int i = upTo; i < iterators.size(); ++i) {
          TwoPhaseIterator twoPhase = iterators.get(i).twoPhase();
          if (twoPhase != null && twoPhase.matches() == false) {
            windowMatch = advance(windowMatches, windowMatch + 1);
            continue advanceHead;
          }
        }
        collector.collect(doc);
        windowMatch = advance(windowMatches, windowMatch + 1);
      }
    } else {
      collector.collect(new BitSetDocIdStream(windowMatches, windowBase));
    }

    windowMatches.clear();
  }

  // Confirm two-phase matches() only on docs that survived every approximation, without
  // materializing a window bit set. Cheaper than the bit-set path for sparse windows with a
  // skippable two-phase clause (see scoreWindow).
  private static void scoreWindowUsingLeapFrog(
      LeafCollector collector,
      Bits acceptDocs,
      List<DocIdSetIterator> approximations,
      List<TwoPhaseIterator> twoPhases,
      int min,
      int max)
      throws IOException {
    assert twoPhases.size() > 0;
    assert approximations.size() >= twoPhases.size();

    if (approximations.size() == 1) {
      // scoreWindowUsingLeapFrog is only used if there is at least one two-phase iterator, so our
      // single clause is a two-phase iterator
      assert twoPhases.size() == 1;
      DocIdSetIterator approximation = approximations.get(0);
      TwoPhaseIterator twoPhase = twoPhases.get(0);
      if (approximation.docID() < min) {
        approximation.advance(min);
      }
      for (int doc = approximation.docID(); doc < max; doc = approximation.nextDoc()) {
        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
          collector.collect(doc);
        }
      }
    } else {
      DocIdSetIterator lead1 = approximations.get(0);
      DocIdSetIterator lead2 = approximations.get(1);

      if (lead1.docID() < min) {
        lead1.advance(min);
      }

      advanceHead:
      for (int doc = lead1.docID(); doc < max; ) {
        if (acceptDocs != null && acceptDocs.get(doc) == false) {
          doc = lead1.nextDoc();
          continue;
        }
        int doc2 = lead2.docID();
        if (doc2 < doc) {
          doc2 = lead2.advance(doc);
        }
        if (doc != doc2) {
          doc = lead1.advance(Math.min(doc2, max));
          continue;
        }
        for (int i = 2; i < approximations.size(); ++i) {
          DocIdSetIterator other = approximations.get(i);
          int docN = other.docID();
          if (docN < doc) {
            docN = other.advance(doc);
          }
          if (doc != docN) {
            doc = lead1.advance(Math.min(docN, max));
            continue advanceHead;
          }
        }
        for (TwoPhaseIterator twoPhase : twoPhases) {
          if (twoPhase.matches() == false) {
            doc = lead1.nextDoc();
            continue advanceHead;
          }
        }
        collector.collect(doc);
        doc = lead1.nextDoc();
      }
    }
  }

  @Override
  public long cost() {
    return iterators.get(0).approximation().cost();
  }
}
