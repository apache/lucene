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
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.Weight.DefaultBulkScorer;
import org.apache.lucene.util.Bits;

/**
 * BulkScorer implementation of {@link BlockMaxConjunctionScorer} that focuses on top-level
 * conjunctions over clauses that do not have two-phase iterators. Use a {@link DefaultBulkScorer}
 * around a {@link BlockMaxConjunctionScorer} if you need two-phase support.
 */
final class BlockMaxConjunctionBulkScorer extends BulkScorer {

  private final Scorer[] scorers;
  private final DocIdSetIterator[] iterators;
  private final DocIdSetIterator lead;
  private final MaxScoreSumPropagator maxScorePropagator;
  private final DocAndScore scorable = new DocAndScore();

  BlockMaxConjunctionBulkScorer(List<Scorer> scorers) throws IOException {
    if (scorers.size() <= 1) {
      throw new IllegalArgumentException("Expected 2 or more scorers, got " + scorers.size());
    }
    this.scorers = scorers.toArray(Scorer[]::new);
    Arrays.sort(this.scorers, Comparator.comparingLong(scorer -> scorer.iterator().cost()));
    this.iterators =
        Arrays.stream(this.scorers).map(Scorer::iterator).toArray(DocIdSetIterator[]::new);
    lead = iterators[0];
    this.maxScorePropagator = new MaxScoreSumPropagator(Arrays.asList(this.scorers));
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorable);

    int windowMin = Math.max(lead.docID(), min);
    while (windowMin < max) {
      // Use impacts of the least costly scorer to compute windows
      // NOTE: windowMax is inclusive
      int windowMax = Math.min(scorers[0].advanceShallow(windowMin), max - 1);
      for (int i = 1; i < scorers.length; ++i) {
        scorers[i].advanceShallow(windowMin);
      }

      double maxWindowScore = 0;
      for (Scorer scorer : scorers) {
        maxWindowScore += scorer.getMaxScore(windowMax);
      }
      scoreWindow(collector, acceptDocs, windowMin, windowMax + 1, (float) maxWindowScore);
      windowMin = Math.max(lead.docID(), windowMax + 1);
    }

    return windowMin;
  }

  private void scoreWindow(
      LeafCollector collector, Bits acceptDocs, int min, int max, float maxWindowScore)
      throws IOException {
    if (maxWindowScore < scorable.minCompetitiveScore) {
      // no hits are competitive
      return;
    }

    if (lead.docID() < min) {
      lead.advance(min);
    }
    advanceHead:
    for (int doc = lead.docID(); doc < max; ) {
      for (int i = 1; i < iterators.length; ++i) {
        // NOTE: these iterators may already be on `doc` if we called `continue advanceHead` on the
        // previous loop iteration.
        if (iterators[i].docID() < doc) {
          int next = iterators[i].advance(doc);
          if (next != doc) {
            doc = lead.advance(next);
            continue advanceHead;
          }
        }
      }

      if (acceptDocs == null || acceptDocs.get(doc)) {
        double score = 0;
        for (Scorer scorer : scorers) {
          score += scorer.score();
        }
        scorable.doc = doc;
        scorable.score = (float) score;
        collector.collect(doc);
        // The collect() call may have updated the minimum competitive score.
        if (maxWindowScore < scorable.minCompetitiveScore) {
          // no more hits are competitive
          return;
        }
      }

      doc = lead.nextDoc();
    }
  }

  @Override
  public long cost() {
    return lead.cost();
  }

  private class DocAndScore extends Scorable {

    int doc;
    float score;
    float minCompetitiveScore;

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      this.minCompetitiveScore = minScore;
      maxScorePropagator.setMinCompetitiveScore(minScore);
    }
  }
}
