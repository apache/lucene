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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongsRef;

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
  private DocAndFreqBuffer docAndFreqBuffer;
  private long[] normValues = LongsRef.EMPTY_LONGS;

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

  @Override
  public void nextDocsAndScores(int upTo, Bits liveDocs, DocAndScoreBuffer buffer)
      throws IOException {
    if (docAndFreqBuffer == null) {
      docAndFreqBuffer = new DocAndFreqBuffer();
    }

    for (; ; ) {
      if (impactsDisi != null) {
        impactsDisi.ensureCompetitive();
      }

      postingsEnum.nextPostings(upTo, docAndFreqBuffer);
      if (liveDocs != null && docAndFreqBuffer.size != 0) {
        // An empty return value indicates that there are no more docs before upTo. We may be
        // unlucky, and there are docs left, but all docs from the current batch happen to be marked
        // as deleted. So we need to iterate until we find a batch that has at least one non-deleted
        // doc.
        docAndFreqBuffer.apply(liveDocs);
        if (docAndFreqBuffer.size == 0) {
          continue;
        }
      }
      break;
    }

    int size = docAndFreqBuffer.size;
    if (normValues.length < size) {
      normValues = new long[ArrayUtil.oversize(size, Long.BYTES)];
      if (norms == null) {
        Arrays.fill(normValues, 1L);
      }
    }
    if (norms != null) {
      for (int i = 0; i < size; ++i) {
        if (norms.advanceExact(docAndFreqBuffer.docs[i])) {
          normValues[i] = norms.longValue();
        } else {
          normValues[i] = 1L;
        }
      }
    }

    buffer.growNoCopy(size);
    buffer.size = size;
    System.arraycopy(docAndFreqBuffer.docs, 0, buffer.docs, 0, size);
    for (int i = 0; i < size; ++i) {
      // Unless SimScorer#score is megamorphic, SimScorer#score should inline and (part of) score
      // computations should auto-vectorize.
      buffer.scores[i] = scorer.score(docAndFreqBuffer.freqs[i], normValues[i]);
    }
  }
}
