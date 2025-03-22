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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.Weight.DefaultBulkScorer;

/**
 * Specialization of {@link ScorerSupplier} for queries that produce constant scores.
 *
 * @lucene.internal
 */
public abstract class ConstantScoreScorerSupplier extends ScorerSupplier {

  /** Create a {@link ConstantScoreScorerSupplier} that matches all docs in [0, maxDoc). */
  public static ConstantScoreScorerSupplier matchAll(float score, ScoreMode scoreMode, int maxDoc) {
    return fromIterator(DocIdSetIterator.all(maxDoc), score, scoreMode, maxDoc);
  }

  /** Create a {@link ConstantScoreScorerSupplier} for the given iterator. */
  public static ConstantScoreScorerSupplier fromIterator(
      DocIdSetIterator iterator, float score, ScoreMode scoreMode, int maxDoc) {
    return new ConstantScoreScorerSupplier(score, scoreMode, maxDoc) {

      @Override
      public long cost() {
        return iterator.cost();
      }

      @Override
      public DocIdSetIterator iterator(long leadCost) throws IOException {
        return iterator;
      }
    };
  }

  private final ScoreMode scoreMode;
  private final float score;
  private final int maxDoc;

  /** Constructor, invoked by sub-classes. */
  protected ConstantScoreScorerSupplier(float score, ScoreMode scoreMode, int maxDoc) {
    this.scoreMode = scoreMode;
    this.score = score;
    this.maxDoc = maxDoc;
  }

  /** Return an iterator given the cost of the leading clause. */
  public abstract DocIdSetIterator iterator(long leadCost) throws IOException;

  @Override
  public final Scorer get(long leadCost) throws IOException {
    DocIdSetIterator iterator = iterator(leadCost);
    TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iterator);
    if (twoPhase == null) {
      return new ConstantScoreScorer(score, scoreMode, iterator);
    } else {
      return new ConstantScoreScorer(score, scoreMode, twoPhase);
    }
  }

  @Override
  public final BulkScorer bulkScorer() throws IOException {
    DocIdSetIterator iterator = iterator(Long.MAX_VALUE);
    if (maxDoc >= DenseConjunctionBulkScorer.WINDOW_SIZE / 2
        && iterator.cost() >= maxDoc / DenseConjunctionBulkScorer.DENSITY_THRESHOLD_INVERSE) {
      TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iterator);
      List<DocIdSetIterator> iterators;
      List<TwoPhaseIterator> twoPhases;
      if (twoPhase == null) {
        iterators = Collections.singletonList(iterator);
        twoPhases = Collections.emptyList();
      } else {
        iterators = Collections.emptyList();
        twoPhases = Collections.singletonList(twoPhase);
      }
      return new DenseConjunctionBulkScorer(iterators, twoPhases, maxDoc, score);
    } else {
      return new DefaultBulkScorer(new ConstantScoreScorer(score, scoreMode, iterator));
    }
  }
}
