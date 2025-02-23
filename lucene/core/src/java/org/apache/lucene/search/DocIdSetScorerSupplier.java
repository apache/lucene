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
import org.apache.lucene.util.Bits;

/** A scorer supplier optimized for {@link BulkScorer#scoreAllAsDocIdSet}. */
public abstract class DocIdSetScorerSupplier extends ScorerSupplier {

  private final float score;
  private final ScoreMode scoreMode;
  protected DocIdSet docIdSet;

  public DocIdSetScorerSupplier(float score, ScoreMode scoreMode) {
    this.score = score;
    this.scoreMode = scoreMode;
  }

  protected abstract DocIdSet computeDocIdSet() throws IOException;

  private DocIdSet docIdSet() throws IOException {
    if (docIdSet == null) {
      docIdSet = computeDocIdSet();
    }
    return docIdSet;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    return new ConstantScoreScorer(score, scoreMode, docIdSet().iterator());
  }

  @Override
  public BulkScorer bulkScorer() throws IOException {
    BulkScorer bulkScorer = super.bulkScorer();
    return new BulkScorer() {
      @Override
      public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
          throws IOException {
        return bulkScorer.score(collector, acceptDocs, min, max);
      }

      @Override
      public DocIdSet scoreAllAsDocIdSet(Bits acceptDocs, int maxDoc) throws IOException {
        return docIdSet();
      }

      @Override
      public long cost() {
        return bulkScorer.cost();
      }
    };
  }
}
