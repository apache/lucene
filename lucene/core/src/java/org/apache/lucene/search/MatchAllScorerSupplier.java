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

/**
 * {@link ScorerSupplier} that matches all docs.
 *
 * @lucene.internal
 */
public final class MatchAllScorerSupplier extends ScorerSupplier {

  private final float score;
  private final ScoreMode scoreMode;
  private final int maxDoc;

  /** Sole constructor */
  public MatchAllScorerSupplier(float score, ScoreMode scoreMode, int maxDoc) {
    this.score = score;
    this.scoreMode = scoreMode;
    this.maxDoc = maxDoc;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    return new ConstantScoreScorer(score, scoreMode, DocIdSetIterator.all(maxDoc));
  }

  @Override
  public BulkScorer bulkScorer() throws IOException {
    if (maxDoc >= DenseConjunctionBulkScorer.WINDOW_SIZE / 2) {
      return new DenseConjunctionBulkScorer(Collections.emptyList(), maxDoc, score);
    } else {
      return super.bulkScorer();
    }
  }

  @Override
  public long cost() {
    return maxDoc;
  }
}
