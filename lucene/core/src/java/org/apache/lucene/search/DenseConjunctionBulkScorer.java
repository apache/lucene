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

  private final DocIdSetIterator lead;
  private final List<DocIdSetIterator> others;

  private final FixedBitSet windowMatches = new FixedBitSet(WINDOW_SIZE);
  private final FixedBitSet clauseWindowMatches = new FixedBitSet(WINDOW_SIZE);
  private final DocIdStreamView docIdStreamView = new DocIdStreamView();

  DenseConjunctionBulkScorer(List<DocIdSetIterator> iterators) {
    if (iterators.size() <= 1) {
      throw new IllegalArgumentException("Expected 2 or more clauses, got " + iterators.size());
    }
    iterators = new ArrayList<>(iterators);
    iterators.sort(Comparator.comparingLong(DocIdSetIterator::cost));
    lead = iterators.get(0);
    others = List.copyOf(iterators.subList(1, iterators.size()));
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    for (DocIdSetIterator it : others) {
      min = Math.max(min, it.docID());
    }

    if (lead.docID() < min) {
      lead.advance(min);
    }

    if (lead.docID() >= max) {
      return lead.docID();
    }

    List<DocIdSetIterator> otherIterators = this.others;
    DocIdSetIterator collectorIterator = collector.competitiveIterator();
    if (collectorIterator != null) {
      otherIterators = new ArrayList<>(otherIterators);
      otherIterators.add(collectorIterator);
    }

    final DocIdSetIterator[] others = otherIterators.toArray(DocIdSetIterator[]::new);

    int windowMax;
    do {
      windowMax = (int) Math.min(max, (long) lead.docID() + WINDOW_SIZE);
      scoreWindowUsingBitSet(collector, acceptDocs, others, windowMax);
    } while (windowMax < max);

    return lead.docID();
  }

  private static int advance(FixedBitSet set, int i) {
    if (i >= WINDOW_SIZE) {
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return set.nextSetBit(i);
    }
  }

  private void scoreWindowUsingBitSet(
      LeafCollector collector, Bits acceptDocs, DocIdSetIterator[] others, int max)
      throws IOException {
    assert windowMatches.scanIsEmpty();
    assert clauseWindowMatches.scanIsEmpty();

    int offset = lead.docID();
    lead.intoBitSet(acceptDocs, max, windowMatches, offset);

    int upTo = 0;
    for (;
        upTo < others.length
            && windowMatches.cardinality() >= WINDOW_SIZE / DENSITY_THRESHOLD_INVERSE;
        upTo++) {
      DocIdSetIterator other = others[upTo];
      if (other.docID() < offset) {
        other.advance(offset);
      }
      // No need to apply acceptDocs on other clauses since we already applied live docs on the
      // leading clause.
      other.intoBitSet(null, max, clauseWindowMatches, offset);
      windowMatches.and(clauseWindowMatches);
      clauseWindowMatches.clear();
    }

    if (upTo < others.length) {
      // If the leading clause is sparse on this doc ID range or if the intersection became sparse
      // after applying a few clauses, we finish evaluating the intersection using the traditional
      // leap-frog approach. This proved important with a query such as "+secretary +of +state" on
      // wikibigall, where the intersection becomes sparse after intersecting "secretary" and
      // "state".
      advanceHead:
      for (int windowMatch = windowMatches.nextSetBit(0);
          windowMatch != DocIdSetIterator.NO_MORE_DOCS; ) {
        int doc = offset + windowMatch;
        for (int i = upTo; i < others.length; ++i) {
          DocIdSetIterator other = others[i];
          int otherDoc = other.docID();
          if (otherDoc < doc) {
            otherDoc = other.advance(doc);
          }
          if (doc != otherDoc) {
            int clearUpTo = Math.min(WINDOW_SIZE, otherDoc - offset);
            windowMatches.clear(windowMatch, clearUpTo);
            windowMatch = advance(windowMatches, clearUpTo);
            continue advanceHead;
          }
        }
        windowMatch = advance(windowMatches, windowMatch + 1);
      }
    }

    docIdStreamView.offset = offset;
    collector.collect(docIdStreamView);
    windowMatches.clear();

    // If another clause is more advanced than lead1 then advance lead1, it's important to take
    // advantage of large gaps in the postings lists of other clauses.
    int maxOtherDocID = -1;
    for (DocIdSetIterator other : others) {
      maxOtherDocID = Math.max(maxOtherDocID, other.docID());
    }
    if (lead.docID() < maxOtherDocID) {
      lead.advance(maxOtherDocID);
    }
  }

  @Override
  public long cost() {
    return lead.cost();
  }

  final class DocIdStreamView extends DocIdStream {

    int offset;

    @Override
    public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
      int offset = this.offset;
      long[] bitArray = windowMatches.getBits();
      for (int idx = 0; idx < bitArray.length; idx++) {
        long bits = bitArray[idx];
        while (bits != 0L) {
          int ntz = Long.numberOfTrailingZeros(bits);
          consumer.accept(offset + ((idx << 6) | ntz));
          bits ^= 1L << ntz;
        }
      }
    }

    @Override
    public int count() throws IOException {
      return windowMatches.cardinality();
    }
  }
}
