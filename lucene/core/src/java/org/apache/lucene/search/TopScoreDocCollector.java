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
import java.util.stream.IntStream;
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
    this.heap = new LongHeap(numHits);
    IntStream.range(0, numHits).forEach(_ -> heap.push(DocScoreEncoder.LEAST_COMPETITIVE_CODE));
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
    final int afterScore;
    final int afterDoc;
    if (after == null) {
      afterScore = Integer.MAX_VALUE;
      afterDoc = DocIdSetIterator.NO_MORE_DOCS;
    } else {
      afterScore = DocScoreEncoder.scoreToSortableInt(after.score);
      afterDoc = after.doc - context.docBase;
    }

    return new LeafCollector() {

      private Scorable scorer;
      // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
      // that at this point top() is already initialized.
      private long topCode = heap.top();
      private int topScore = DocScoreEncoder.toIntScore(topCode);
      ;
      private int minCompetitiveScore;

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
        final int score = DocScoreEncoder.scoreToSortableInt(scorer.score());

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

      private void collectCompetitiveHit(int doc, int score) throws IOException {
        final long code = DocScoreEncoder.encodeIntScore(doc + docBase, score);
        topCode = heap.updateTop(code);
        topScore = DocScoreEncoder.toIntScore(topCode);
        updateMinCompetitiveScore(scorer);
      }

      private void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        long maxMinScore = minScoreAcc.getRaw();
        if (maxMinScore != Long.MIN_VALUE) {
          // since we tie-break on doc id and collect in doc id order we can require
          // the next float if the global minimum score is set on a document id that is
          // smaller than the ids in the current leaf
          int score = DocScoreEncoder.toIntScore(maxMinScore);
          score =
              docBase >= DocScoreEncoder.docId(maxMinScore) ? DocScoreEncoder.nextUp(score) : score;
          if (score > minCompetitiveScore) {
            scorer.setMinCompetitiveScore(DocScoreEncoder.sortableIntToScore(score));
            minCompetitiveScore = score;
            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
          }
        }
      }

      private void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (totalHits > totalHitsThreshold) {
          if (topScore >= minCompetitiveScore) {
            minCompetitiveScore = DocScoreEncoder.nextUp(topScore);
            scorer.setMinCompetitiveScore(DocScoreEncoder.sortableIntToScore(minCompetitiveScore));
            totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
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
  public TopDocs topDocs(int start, int howMany) {
    // In case pq was populated with sentinel values, there might be less
    // results than pq.size(). Therefore return all results until either
    // pq.size() or totalHits.
    int size = topDocsSize();

    if (howMany < 0) {
      throw new IllegalArgumentException(
          "Number of hits requested must be greater than 0 but value was " + howMany);
    }

    if (start < 0) {
      throw new IllegalArgumentException(
          "Expected value of starting position is between 0 and " + size + ", got " + start);
    }

    if (start >= size || howMany == 0) {
      return newTopDocs(null, start);
    }

    // We know that start < pqsize, so just fix howMany.
    howMany = Math.min(size - start, howMany);
    ScoreDoc[] results = new ScoreDoc[howMany];

    // pq's pop() returns the 'least' element in the queue, therefore need
    // to discard the first ones, until we reach the requested range.
    // Note that this loop will usually not be executed, since the common usage
    // should be that the caller asks for the last howMany results. However it's
    // needed here for completeness.
    for (int i = heap.size() - start - howMany; i > 0; i--) {
      heap.pop();
    }

    // Get the requested results from pq.
    populateResults(results, howMany);

    return newTopDocs(results, start);
  }
}
