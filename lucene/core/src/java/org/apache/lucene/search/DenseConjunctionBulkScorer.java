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

/**
 * BulkScorer implementation of {@link ConjunctionScorer} that is specialized for dense clauses.
 * Whenever sensible, it intersects clauses by loading their matches into a bit set and computing
 * the intersection of clauses by and-ing these bit sets.
 *
 * <p>An empty set of iterators is interpreted as meaning that all docs in [0, maxDoc) match.
 */
final class DenseConjunctionBulkScorer extends BulkScorer {

  // Use a small-ish window size to make sure that we can take advantage of gaps in the postings of
  // clauses that are not leading iteration.
  static final int WINDOW_SIZE = 4096;
  // Only use bit sets to compute the intersection if more than 1/32th of the docs are expected to
  // match. Experiments suggested that values that are a bit higher than this would work better, but
  // we're erring on the conservative side.
  static final int DENSITY_THRESHOLD_INVERSE = Long.SIZE / 2;

  private final int maxDoc;
  private final List<DocIdSetIterator> iterators;
  private final SimpleScorable scorable;

  private final FixedBitSet windowMatches = new FixedBitSet(WINDOW_SIZE);
  private final FixedBitSet clauseWindowMatches = new FixedBitSet(WINDOW_SIZE);
  private final DocIdStreamView docIdStreamView = new DocIdStreamView();
  private final RangeDocIdStream rangeDocIdStream = new RangeDocIdStream();
  private final SingleIteratorDocIdStream singleIteratorDocIdStream =
      new SingleIteratorDocIdStream();

  DenseConjunctionBulkScorer(List<DocIdSetIterator> iterators, int maxDoc, float constantScore) {
    this.maxDoc = maxDoc;
    iterators = new ArrayList<>(iterators);
    iterators.sort(Comparator.comparingLong(DocIdSetIterator::cost));
    this.iterators = iterators;
    this.scorable = new SimpleScorable();
    scorable.score = constantScore;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);
    List<DocIdSetIterator> iterators = this.iterators;
    if (collector.competitiveIterator() != null) {
      iterators = new ArrayList<>(iterators);
      iterators.add(collector.competitiveIterator());
    }

    for (DocIdSetIterator it : iterators) {
      min = Math.max(min, it.docID());
    }

    max = Math.min(max, maxDoc);

    DocIdSetIterator lead = null;
    if (iterators.isEmpty() == false) {
      lead = iterators.get(0);
      if (lead.docID() < min) {
        min = lead.advance(min);
      }
    }

    if (min >= max) {
      return min >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : min;
    }

    int windowMax = min;
    do {
      if (scorable.minCompetitiveScore > scorable.score) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }

      int windowBase = lead == null ? windowMax : lead.docID();
      windowMax = (int) Math.min(max, (long) windowBase + WINDOW_SIZE);
      if (windowMax > windowBase) {
        scoreWindowUsingBitSet(collector, acceptDocs, iterators, windowBase, windowMax);
      }
    } while (windowMax < max);

    if (lead != null) {
      return lead.docID();
    } else if (windowMax >= maxDoc) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return windowMax;
    }
  }

  private static int advance(FixedBitSet set, int i) {
    if (i >= WINDOW_SIZE) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return set.nextSetBit(i);
    }
  }

  private void scoreWindowUsingBitSet(
      LeafCollector collector,
      Bits acceptDocs,
      List<DocIdSetIterator> iterators,
      int windowBase,
      int windowMax)
      throws IOException {
    assert windowMax > windowBase;
    assert windowMatches.scanIsEmpty();
    assert clauseWindowMatches.scanIsEmpty();

    if (acceptDocs == null) {
      if (iterators.isEmpty()) {
        // All docs in the range match.
        rangeDocIdStream.from = windowBase;
        rangeDocIdStream.to = windowMax;
        collector.collect(rangeDocIdStream);
        return;
      } else if (iterators.size() == 1) {
        singleIteratorDocIdStream.iterator = iterators.get(0);
        singleIteratorDocIdStream.from = windowBase;
        singleIteratorDocIdStream.to = windowMax;
        collector.collect(singleIteratorDocIdStream);
        return;
      }
    }

    if (iterators.isEmpty()) {
      windowMatches.set(0, windowMax - windowBase);
    } else {
      DocIdSetIterator lead = iterators.get(0);
      lead.intoBitSet(windowMax, windowMatches, windowBase);
    }

    if (acceptDocs != null) {
      // Apply live docs.
      acceptDocs.applyMask(windowMatches, windowBase);
    }

    int windowSize = windowMax - windowBase;
    int threshold = windowSize / DENSITY_THRESHOLD_INVERSE;
    int upTo = 1; // the leading clause at index 0 is already applied
    for (; upTo < iterators.size() && windowMatches.cardinality() >= threshold; upTo++) {
      DocIdSetIterator other = iterators.get(upTo);
      if (other.docID() < windowBase) {
        other.advance(windowBase);
      }
      other.intoBitSet(windowMax, clauseWindowMatches, windowBase);
      windowMatches.and(clauseWindowMatches);
      clauseWindowMatches.clear();
    }

    if (upTo < iterators.size()) {
      // If the leading clause is sparse on this doc ID range or if the intersection became sparse
      // after applying a few clauses, we finish evaluating the intersection using the traditional
      // leap-frog approach. This proved important with a query such as "+secretary +of +state" on
      // wikibigall, where the intersection becomes sparse after intersecting "secretary" and
      // "state".
      advanceHead:
      for (int windowMatch = windowMatches.nextSetBit(0);
          windowMatch != DocIdSetIterator.NO_MORE_DOCS; ) {
        int doc = windowBase + windowMatch;
        for (int i = upTo; i < iterators.size(); ++i) {
          DocIdSetIterator other = iterators.get(i);
          int otherDoc = other.docID();
          if (otherDoc < doc) {
            otherDoc = other.advance(doc);
          }
          if (doc != otherDoc) {
            int clearUpTo = Math.min(WINDOW_SIZE, otherDoc - windowBase);
            windowMatches.clear(windowMatch, clearUpTo);
            windowMatch = advance(windowMatches, clearUpTo);
            continue advanceHead;
          }
        }
        windowMatch = advance(windowMatches, windowMatch + 1);
      }
    }

    docIdStreamView.windowBase = windowBase;
    collector.collect(docIdStreamView);
    windowMatches.clear();

    // If another clause is more advanced than the leading clause then advance the leading clause,
    // it's important to take advantage of large gaps in the postings lists of other clauses.
    if (iterators.size() >= 2) {
      DocIdSetIterator lead = iterators.get(0);
      int maxOtherDocID = -1;
      for (int i = 1; i < iterators.size(); ++i) {
        maxOtherDocID = Math.max(maxOtherDocID, iterators.get(i).docID());
      }
      if (lead.docID() < maxOtherDocID) {
        lead.advance(maxOtherDocID);
      }
    }
  }

  @Override
  public long cost() {
    if (iterators.isEmpty()) {
      return maxDoc;
    } else {
      return iterators.get(0).cost();
    }
  }

  final class DocIdStreamView extends DocIdStream {

    int windowBase;

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
      int windowBase = this.windowBase;
      long[] bitArray = windowMatches.getBits();
      for (int idx = 0; idx < bitArray.length; idx++) {
        long bits = bitArray[idx];
        while (bits != 0L) {
          int ntz = Long.numberOfTrailingZeros(bits);
          consumer.accept(windowBase + ((idx << 6) | ntz));
          bits ^= 1L << ntz;
        }
      }
    }

    @Override
    public int count() throws IOException {
      return windowMatches.cardinality();
    }
  }

  final class RangeDocIdStream extends DocIdStream {

    int from, to;

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
      for (int i = from; i < to; ++i) {
        consumer.accept(i);
      }
    }

    @Override
    public int count() throws IOException {
      return to - from;
    }
  }

  /** {@link DocIdStream} for a {@link DocIdSetIterator} with no live docs to apply. */
  final class SingleIteratorDocIdStream extends DocIdStream {

    int from, to;
    DocIdSetIterator iterator;

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
      // If there are no live docs to apply, loading matching docs into a bit set and then iterating
      // bits is unlikely to beat iterating the iterator directly.
      if (iterator.docID() < from) {
        iterator.advance(from);
      }
      for (int doc = iterator.docID(); doc < to; doc = iterator.nextDoc()) {
        consumer.accept(doc);
      }
    }

    @Override
    public int count() throws IOException {
      // If the collector is just interested in the count, loading in a bit set and counting bits is
      // often faster than incrementing a counter on every call to nextDoc().
      assert windowMatches.scanIsEmpty();
      iterator.intoBitSet(to, clauseWindowMatches, from);
      int count = clauseWindowMatches.cardinality();
      clauseWindowMatches.clear();
      return count;
    }
  }
}
