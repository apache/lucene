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
package org.apache.lucene.misc.search;

import java.io.IOException;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.similarities.Similarity;

/** ScorerSupplier wrapper that creates ImpactRangeBulkScorer. */
public class ImpactRangeScorerSupplier extends ScorerSupplier {

  private final ScorerSupplier in;
  private final int rangeSize;
  private final int minDoc;
  private final int maxDoc;
  private final Similarity.SimScorer simScorer;
  private final ScoreMode scoreMode;

  public ImpactRangeScorerSupplier(
      ScorerSupplier in,
      int rangeSize,
      int minDoc,
      int maxDoc,
      Similarity.SimScorer simScorer,
      ScoreMode scoreMode) {
    this.in = in;
    this.rangeSize = rangeSize;
    this.minDoc = minDoc;
    this.maxDoc = maxDoc;
    this.simScorer = simScorer;
    this.scoreMode = scoreMode;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    return in.get(leadCost);
  }

  @Override
  public BulkScorer bulkScorer() throws IOException {
    BulkScorer delegate = in.bulkScorer();
    if (delegate == null) {
      return null;
    }

    // Only try to get impacts when ScoreMode suggests they would be properly configured
    ImpactsEnum impactsEnum = null;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      Scorer scorer = in.get(Long.MAX_VALUE);
      if (scorer != null && scorer.iterator() instanceof ImpactsEnum) {
        impactsEnum = (ImpactsEnum) scorer.iterator();
      }
    }

    return new ImpactRangeBulkScorer(delegate, rangeSize, minDoc, maxDoc, simScorer, impactsEnum);
  }

  @Override
  public long cost() {
    return in.cost();
  }
}
