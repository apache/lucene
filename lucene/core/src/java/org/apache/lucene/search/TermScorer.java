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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 *
 * @lucene.internal
 */
public final class TermScorer extends Scorer {
  private final PostingsEnum postingsEnum;
  private final DocIdSetIterator iterator;
  private final SimScorer scorer;
  private final NumericDocValues norms;
  private final ImpactsDISI impactsDisi;
  private final MaxScoreCache maxScoreCache;

  /** Construct a {@link TermScorer} that will iterate all documents. */
  public TermScorer(PostingsEnum postingsEnum, SimScorer scorer, NumericDocValues norms) {
    iterator = this.postingsEnum = postingsEnum;
    ImpactsEnum impactsEnum = new SlowImpactsEnum(postingsEnum);
    maxScoreCache = new MaxScoreCache(impactsEnum, scorer);
    impactsDisi = null;
    this.scorer = scorer;
    this.norms = norms;
  }

  /**
   * Construct a {@link TermScorer} that will use impacts to skip blocks of non-competitive
   * documents.
   */
  public TermScorer(
      ImpactsEnum impactsEnum,
      SimScorer scorer,
      NumericDocValues norms,
      boolean topLevelScoringClause) {
    postingsEnum = impactsEnum;
    maxScoreCache = new MaxScoreCache(impactsEnum, scorer);
    if (topLevelScoringClause) {
      impactsDisi = new ImpactsDISI(impactsEnum, maxScoreCache);
      iterator = impactsDisi;
    } else {
      impactsDisi = null;
      iterator = impactsEnum;
    }
    this.scorer = scorer;
    this.norms = norms;
  }

  @Override
  public int docID() {
    return postingsEnum.docID();
  }

  /** Returns term frequency in the current document. */
  public final int freq() throws IOException {
    return postingsEnum.freq();
  }

  @Override
  public DocIdSetIterator iterator() {
    return iterator;
  }

  @Override
  public float score() throws IOException {
    var postingsEnum = this.postingsEnum;
    var norms = this.norms;

    long norm = 1L;
    if (norms != null && norms.advanceExact(postingsEnum.docID())) {
      norm = norms.longValue();
    }
    return scorer.score(postingsEnum.freq(), norm);
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    long norm = 1L;
    if (norms != null && norms.advanceExact(docId)) {
      norm = norms.longValue();
    }
    return scorer.score(0, norm);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return maxScoreCache.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return maxScoreCache.getMaxScore(upTo);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    if (impactsDisi != null) {
      impactsDisi.setMinCompetitiveScore(minScore);
    }
  }
}
