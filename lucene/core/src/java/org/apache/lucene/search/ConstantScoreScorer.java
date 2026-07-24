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
import java.util.Arrays;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A constant-scoring {@link Scorer}.
 *
 * @lucene.internal
 */
public final class ConstantScoreScorer extends Scorer {

  private class DocIdSetIteratorWrapper extends DocIdSetIterator {
    int doc = -1;
    DocIdSetIterator delegate;

    DocIdSetIteratorWrapper(DocIdSetIterator delegate) {
      this.delegate = delegate;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return doc = delegate.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return doc = delegate.advance(target);
    }

    @Override
    public long cost() {
      return delegate.cost();
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      if (doc != delegate.docID()) {
        // The delegate was swapped for an empty iterator (see setMinCompetitiveScore); the
        // default implementation terminates via nextDoc() without touching the stale delegate
        // position.
        super.intoBitSet(upTo, bitSet, offset);
        return;
      }
      delegate.intoBitSet(upTo, bitSet, offset);
      doc = delegate.docID();
    }
  }

  private final float score;
  private final ScoreMode scoreMode;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;
  // Whether the underlying iterator pays per-nextDoc() overhead that the bulk
  // #nextDocsAndScores path amortizes. Iterators that are cheap to advance one doc at a time
  // (single postings list, bit set) are better served by the plain doc-at-a-time loop.
  private final boolean bulkDrainWorthwhile;

  /**
   * Constructor based on a {@link DocIdSetIterator} which will be used to drive iteration. Two
   * phase iteration will not be supported.
   *
   * @param score the score to return on each document
   * @param scoreMode the score mode
   * @param disi the iterator that defines matching documents
   */
  public ConstantScoreScorer(float score, ScoreMode scoreMode, DocIdSetIterator disi) {
    this.score = score;
    this.scoreMode = scoreMode;
    // TODO: Only wrap when it is the top-level scoring clause? See
    // ScorerSupplier#setTopLevelScoringClause
    this.approximation =
        scoreMode == ScoreMode.TOP_SCORES ? new DocIdSetIteratorWrapper(disi) : disi;
    this.twoPhaseIterator = null;
    this.disi = this.approximation;
    this.bulkDrainWorthwhile = disi instanceof DisjunctionDISIApproximation;
  }

  /**
   * Constructor based on a {@link TwoPhaseIterator}. In that case the {@link Scorer} will support
   * two-phase iteration.
   *
   * @param score the score to return on each document
   * @param scoreMode the score mode
   * @param twoPhaseIterator the iterator that defines matching documents
   */
  public ConstantScoreScorer(float score, ScoreMode scoreMode, TwoPhaseIterator twoPhaseIterator) {
    this.score = score;
    this.scoreMode = scoreMode;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      // TODO: Only wrap when it is the top-level scoring clause? See
      // ScorerSupplier#setTopLevelScoringClause
      this.approximation = new DocIdSetIteratorWrapper(twoPhaseIterator.approximation());
      this.twoPhaseIterator =
          new TwoPhaseIterator(this.approximation) {
            @Override
            public boolean matches() throws IOException {
              return twoPhaseIterator.matches();
            }

            @Override
            public float matchCost() {
              return twoPhaseIterator.matchCost();
            }
          };
    } else {
      this.approximation = twoPhaseIterator.approximation();
      this.twoPhaseIterator = twoPhaseIterator;
    }
    this.disi = TwoPhaseIterator.asDocIdSetIterator(this.twoPhaseIterator);
    this.bulkDrainWorthwhile = false; // two-phase matches must be verified one by one
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return score;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (scoreMode == ScoreMode.TOP_SCORES && minScore > score) {
      ((DocIdSetIteratorWrapper) approximation).delegate = DocIdSetIterator.empty();
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return disi;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhaseIterator;
  }

  @Override
  public int docID() {
    return disi.docID();
  }

  @Override
  public float score() throws IOException {
    return score;
  }

  // Doc-ID window covered by one bulk #nextDocsAndScores fill. Matches
  // MaxScoreBulkScorer.INNER_WINDOW_SIZE and DenseConjunctionBulkScorer.WINDOW_SIZE, so a single
  // fill covers a full inner scoring window with a bit set that stays core-cache resident.
  private static final int BULK_WINDOW_SIZE = 4096;

  private FixedBitSet bulkWindowMatches; // lazily allocated

  @Override
  public void nextDocsAndScores(int upTo, Bits liveDocs, DocAndFloatFeatureBuffer buffer)
      throws IOException {
    if (bulkDrainWorthwhile == false) {
      // Either matches must be verified one by one (two-phase), or the iterator is cheap to
      // advance one doc at a time (single postings list, bit set) and the per-window fixed cost
      // of the bulk path (bit set clear/flatten) would not pay for itself. Only heap-based
      // composite iterators (disjunctions), which pay a priority-queue update per nextDoc(),
      // benefit from the bulk drain.
      int batchSize = 64;
      buffer.growNoCopy(batchSize);
      int size = 0;
      DocIdSetIterator iterator = iterator();
      for (int doc = iterator.docID(); doc < upTo && size < batchSize; doc = iterator.nextDoc()) {
        if (liveDocs == null || liveDocs.get(doc)) {
          buffer.docs[size] = doc;
          ++size;
        }
      }
      Arrays.fill(buffer.features, 0, size, score);
      buffer.size = size;
      return;
    }

    // Drain a window of matches in bulk via DocIdSetIterator#intoBitSet. Disjunctions implement
    // it with one bulk load per sub-iterator, which is much cheaper than paying a priority-queue
    // update per nextDoc() call. This matters for constant-score disjunction clauses under top-k
    // scoring (MaxScoreBulkScorer), which have no impact-based bulk path.
    buffer.size = 0;
    DocIdSetIterator iterator = iterator();
    int doc = iterator.docID();
    if (doc >= upTo) {
      return;
    }
    if (bulkWindowMatches == null) {
      bulkWindowMatches = new FixedBitSet(BULK_WINDOW_SIZE);
    } else {
      bulkWindowMatches.clear();
    }
    int windowMax = (int) Math.min(upTo, (long) doc + BULK_WINDOW_SIZE);
    iterator.intoBitSet(windowMax, bulkWindowMatches, doc);
    int cardinality = bulkWindowMatches.cardinality();
    if (cardinality == 0) {
      // No match in this window; the iterator already advanced to windowMax or beyond, the
      // caller re-invokes for the next window.
      return;
    }
    buffer.growNoCopy(cardinality);
    int size = bulkWindowMatches.intoArray(0, windowMax - doc, doc, buffer.docs);
    if (liveDocs != null) {
      int kept = 0;
      for (int i = 0; i < size; ++i) {
        int d = buffer.docs[i];
        if (liveDocs.get(d)) {
          buffer.docs[kept++] = d;
        }
      }
      size = kept;
    }
    Arrays.fill(buffer.features, 0, size, score);
    buffer.size = size;
  }
}
