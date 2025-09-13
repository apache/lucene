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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.LongHeap;

/**
 * A {@link Collector} implementation that collects the top-scoring hits, returning them as a {@link
 * TopDocs}. This is used by {@link IndexSearcher} to implement {@link TopDocs}-based search. Hits
 * are sorted by score descending and then (when the scores are tied) docID ascending. When you
 * create an instance of this collector you should know in advance whether documents are going to be
 * collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and {@link Float#NEGATIVE_INFINITY} are not valid
 * scores. This collector will not properly collect hits with such scores.
 */
public class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  private final ScoreDoc after;
  private final LongHeap heap;
  final int totalHitsThreshold;
  final MaxScoreAccumulator minScoreAcc;

  // prevents instantiation
  TopScoreDocCollector(
      int numHits, ScoreDoc after, int totalHitsThreshold, MaxScoreAccumulator minScoreAcc) {
    super(null);
    this.heap = new LongHeap(numHits, DocScoreEncoder.LEAST_COMPETITIVE_CODE);
    this.after = after;
    this.totalHitsThreshold = totalHitsThreshold;
    this.minScoreAcc = minScoreAcc;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    return results == null
        ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
        : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  @Override
  public ScoreMode scoreMode() {
    return totalHitsThreshold == Integer.MAX_VALUE ? ScoreMode.COMPLETE : ScoreMode.TOP_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final int docBase = context.docBase;
    final ScoreDoc after = this.after;
    final float afterScore;
    final int afterDoc;
    if (after == null) {
      afterScore = Float.POSITIVE_INFINITY;
      afterDoc = DocIdSetIterator.NO_MORE_DOCS;
    } else {
      afterScore = after.score;
      afterDoc = after.doc - context.docBase;
    }

    return new LeafCollector() {

      private Scorable scorer;
      private long topCode = heap.top();
      private float topScore = DocScoreEncoder.toScore(topCode);
      private float minCompetitiveScore;

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
        if (minScoreAcc == null) {
          updateMinCompetitiveScore(scorer);
        } else {
          updateGlobalMinCompetitiveScore(scorer);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();

        int hitCountSoFar = ++totalHits;

        if (minScoreAcc != null && (hitCountSoFar & minScoreAcc.modInterval) == 0) {
          updateGlobalMinCompetitiveScore(scorer);
        }

        if (after != null && (score > afterScore || (score == afterScore && doc <= afterDoc))) {
          // hit was collected on a previous page
          if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
            // we just reached totalHitsThreshold, we can start setting the min
            // competitive score now
            updateMinCompetitiveScore(scorer);
          }
          return;
        }

        if (score <= topScore) {
          // Note: for queries that match lots of hits, this is the common case: most hits are not
          // competitive.
          if (hitCountSoFar == totalHitsThreshold + 1) {
            // we just exceeded totalHitsThreshold, we can start setting the min
            // competitive score now
            updateMinCompetitiveScore(scorer);
          }

          // Since docs are returned in-order (i.e., increasing doc Id), a document
          // with equal score to pqTop.score cannot compete since HitQueue favors
          // documents with lower doc Ids. Therefore reject those docs too.
        } else {
          collectCompetitiveHit(doc, score);
        }
      }

      private void collectCompetitiveHit(int doc, float score) throws IOException {
        final long code = DocScoreEncoder.encode(doc + docBase, score);
        topCode = heap.updateTop(code);
        topScore = DocScoreEncoder.toScore(topCode);
        updateMinCompetitiveScore(scorer);
      }

      private void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        long maxMinScore = minScoreAcc.getRaw();
        if (maxMinScore != Long.MIN_VALUE) {
          // since we tie-break on doc id and collect in doc id order we can require
          // the next float if the global minimum score is set on a document id that is
          // smaller than the ids in the current leaf
          float score = DocScoreEncoder.toScore(maxMinScore);
          score = docBase >= DocScoreEncoder.docId(maxMinScore) ? Math.nextUp(score) : score;
          if (score > minCompetitiveScore) {
            scorer.setMinCompetitiveScore(score);
            minCompetitiveScore = score;
            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
          }
        }
      }

      private void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (totalHits > totalHitsThreshold) {
          // since we tie-break on doc id and collect in doc id order, we can require the next float
          // pqTop is never null since TopScoreDocCollector fills the priority queue with sentinel
          // values if the top element is a sentinel value, its score will be -Infty and the below
          // logic is still valid
          float localMinScore = Math.nextUp(topScore);
          if (localMinScore > minCompetitiveScore) {
            scorer.setMinCompetitiveScore(localMinScore);
            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            minCompetitiveScore = localMinScore;
            if (minScoreAcc != null) {
              // we don't use the next float but we register the document id so that other leaves or
              // leaf partitions can require it if they are after the current maximum
              minScoreAcc.accumulate(topCode);
            }
          }
        }
      }
    };
  }

  @Override
  protected int topDocsSize() {
    int cnt = 0;
    for (int i = 1; i <= heap.size(); i++) {
      if (heap.get(i) != DocScoreEncoder.LEAST_COMPETITIVE_CODE) {
        cnt++;
      }
    }
    return cnt;
  }

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    for (int i = howMany - 1; i >= 0; i--) {
      long encode = heap.pop();
      results[i] = new ScoreDoc(DocScoreEncoder.docId(encode), DocScoreEncoder.toScore(encode));
    }
  }

  @Override
  protected void pruneLeastCompetitiveHitsTo(int keep) {
    for (int i = heap.size() - keep; i > 0; i--) {
      heap.pop();
    }
  }
}
