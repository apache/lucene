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

/**
 * A bulk scorer used when {@link ScoreMode#needsScores()} is true and {@link
 * Scorer#nextDocsAndScores} has optimizations to run faster than one-by-one iteration.
 */
class BatchScoreBulkScorer extends BulkScorer {

  private final SimpleScorable scorable = new SimpleScorable();
  private final DocAndScoreBuffer buffer = new DocAndScoreBuffer();
  private final Scorer scorer;

  BatchScoreBulkScorer(Scorer scorer) {
    this.scorer = scorer;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    if (collector.competitiveIterator() != null) {
      return new Weight.DefaultBulkScorer(scorer).score(collector, acceptDocs, min, max);
    }

    collector.setScorer(scorable);
    scorer.setMinCompetitiveScore(scorable.minCompetitiveScore);

    if (scorer.docID() < min) {
      scorer.iterator().advance(min);
    }

    for (scorer.nextDocsAndScores(max, acceptDocs, buffer);
        buffer.size > 0;
        scorer.nextDocsAndScores(max, acceptDocs, buffer)) {
      for (int i = 0, size = buffer.size; i < size; i++) {
        float score = scorable.score = buffer.scores[i];
        if (score >= scorable.minCompetitiveScore) {
          collector.collect(buffer.docs[i]);
        }
      }
      scorer.setMinCompetitiveScore(scorable.minCompetitiveScore);
    }

    return scorer.docID();
  }

  @Override
  public long cost() {
    return scorer.iterator().cost();
  }
}
