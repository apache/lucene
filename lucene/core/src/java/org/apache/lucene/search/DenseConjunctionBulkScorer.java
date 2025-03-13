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
  private final List<DocIdSetIterator> windowIterators = new ArrayList<>();
  private final DocIdStreamView docIdStreamView = new DocIdStreamView();
  private final RangeDocIdStream rangeDocIdStream = new RangeDocIdStream();
  private final SingleIteratorDocIdStream singleIteratorDocIdStream =
      new SingleIteratorDocIdStream();

  DenseConjunctionBulkScorer(List<DocIdSetIterator> iterators, int maxDoc, float constantScore) {
    if (iterators.isEmpty()) {
      throw new IllegalArgumentException("Expected one or more iterators, got 0");
    }
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

    DocIdSetIterator lead = iterators.get(0);
    if (lead.docID() < min) {
      min = lead.advance(min);
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
      LeafCollector collector, Bits acceptDocs, List<DocIdSetIterator> iterators, int min, int max)
      throws IOException {

    // Advance all iterators to the first doc that is greater than or equal to min. This is
    // important as this is the only place where we can take advantage of a large gap between
    // consecutive matches in any clause.
    for (DocIdSetIterator iterator : iterators) {
      if (iterator.docID() >= min) {
        min = iterator.docID();
      } else {
        min = iterator.advance(min);
      }
      if (min >= max) {
        return min;
      }
    }

    if (acceptDocs == null) {
      int minDocIDRunEnd = max;
      for (DocIdSetIterator iterator : iterators) {
        if (iterator.docID() > min) {
          minDocIDRunEnd = min;
          break;
        } else {
          minDocIDRunEnd = Math.min(minDocIDRunEnd, iterator.docIDRunEnd());
        }
      }

      if (minDocIDRunEnd - min >= WINDOW_SIZE / 2) {
        // We have a large range of doc IDs that all match.
        rangeDocIdStream.from = min;
        rangeDocIdStream.to = minDocIDRunEnd;
        collector.collect(rangeDocIdStream);
        return minDocIDRunEnd;
      }
    }

    int bitsetWindowMax = (int) Math.min(max, (long) min + WINDOW_SIZE);

    for (DocIdSetIterator it : iterators) {
      if (it.docID() > min || it.docIDRunEnd() < bitsetWindowMax) {
        windowIterators.add(it);
      }
    }

    if (acceptDocs == null && windowIterators.size() == 1) {
      // We have a range of doc IDs where all matches of an iterator are matches of the conjunction.
      singleIteratorDocIdStream.iterator = windowIterators.get(0);
      singleIteratorDocIdStream.from = min;
      singleIteratorDocIdStream.to = bitsetWindowMax;
      collector.collect(singleIteratorDocIdStream);
    } else {
      scoreWindowUsingBitSet(collector, acceptDocs, windowIterators, min, bitsetWindowMax);
    }
    windowIterators.clear();
    return bitsetWindowMax;
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

    if (iterators.isEmpty()) {
      // This happens if all clauses fully matched the window and there are deleted docs.
      windowMatches.set(0, windowMax - windowBase);
    } else {
      DocIdSetIterator lead = iterators.get(0);
      if (lead.docID() < windowBase) {
        lead.advance(windowBase);
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
            windowMatch = advance(windowMatches, otherDoc - windowBase);
            continue advanceHead;
          }
        }
        collector.collect(doc);
        windowMatch = advance(windowMatches, windowMatch + 1);
      }
    } else {
      docIdStreamView.windowBase = windowBase;
      collector.collect(docIdStreamView);
    }

    windowMatches.clear();
  }

  @Override
  public long cost() {
    return iterators.get(0).cost();
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
      if (iterator.docID() < from) {
        iterator.advance(from);
      }
      iterator.intoBitSet(to, clauseWindowMatches, from);
      int count = clauseWindowMatches.cardinality();
      clauseWindowMatches.clear();
      return count;
    }
  }
}
