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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;

/**
 * Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 *
 * @lucene.internal
 */
public final class TermScorer extends Scorer {
  private final PostingsEnum postingsEnum;
  private final DocIdSetIterator iterator;
  private final LeafSimScorer docScorer;
  private final ImpactsDISI impactsDisi;
  private final MaxScoreCache maxScoreCache;

  /** Construct a {@link TermScorer} that will iterate all documents. */
  public TermScorer(Weight weight, PostingsEnum postingsEnum, LeafSimScorer docScorer) {
    super(weight);
    iterator = this.postingsEnum = postingsEnum;
    ImpactsEnum impactsEnum = new SlowImpactsEnum(postingsEnum);
    maxScoreCache = new MaxScoreCache(impactsEnum, docScorer.getSimScorer());
    impactsDisi = null;
    this.docScorer = docScorer;
  }

  /**
   * Construct a {@link TermScorer} that will use impacts to skip blocks of non-competitive
   * documents.
   */
  public TermScorer(
      Weight weight,
      ImpactsEnum impactsEnum,
      LeafSimScorer docScorer,
      boolean topLevelScoringClause) {
    super(weight);
    postingsEnum = impactsEnum;
    maxScoreCache = new MaxScoreCache(impactsEnum, docScorer.getSimScorer());
    if (topLevelScoringClause) {
      impactsDisi = new ImpactsDISI(impactsEnum, maxScoreCache);
      iterator = impactsDisi;
    } else {
      impactsDisi = null;
      iterator = impactsEnum;
    }
    this.docScorer = docScorer;
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
    assert docID() != DocIdSetIterator.NO_MORE_DOCS;
    return docScorer.score(postingsEnum.docID(), postingsEnum.freq());
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    return docScorer.score(docId, 0);
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

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() {
    return "scorer(" + weight + ")[" + super.toString() + "]";
  }
}
