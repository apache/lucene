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

    @Override
    public int intoArray(int upTo, int[] docs) throws IOException {
      if (doc != delegate.docID()) {
        // See #intoBitSet.
        return super.intoArray(upTo, docs);
      }
      int size = delegate.intoArray(upTo, docs);
      doc = delegate.docID();
      return size;
    }
  }

  private final float score;
  private final ScoreMode scoreMode;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;

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

  // Number of doc IDs that a single #nextDocsAndScores call loads at a time. It matches
  // MaxScoreBulkScorer#INNER_WINDOW_SIZE, so that iterators which load doc IDs in bulk, such as
  // disjunctions, can cover a full inner scoring window in a single call.
  private static final int BATCH_SIZE = 4096;

  @Override
  public void nextDocsAndScores(int upTo, Bits liveDocs, DocAndFloatFeatureBuffer buffer)
      throws IOException {
    DocIdSetIterator iterator = iterator();
    buffer.growNoCopy(BATCH_SIZE);
    for (; ; ) {
      buffer.size = iterator.intoArray(upTo, buffer.docs);
      Arrays.fill(buffer.features, 0, buffer.size, score);
      if (liveDocs == null || buffer.size == 0) {
        break;
      }
      buffer.apply(liveDocs);
      // An empty buffer indicates that there are no docs left before upTo. We may be unlucky, and
      // there are docs left, but all docs from the current batch happen to be marked as deleted.
      // So we need to iterate until we find a batch that has at least one non-deleted doc.
      if (buffer.size != 0) {
        break;
      }
    }
  }
}
