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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;

/**
 * Bulk scorer for no-score constant-score iterators that batches doc IDs via {@link
 * DocIdSetIterator#intoBitSet}.
 */
final class ConstantScoreBulkScorer extends BulkScorer {
  private final Scorer scorer;
  private final DocIdSetIterator iterator;
  private final FixedBitSet windowMatches = new FixedBitSet(DenseConjunctionBulkScorer.WINDOW_SIZE);

  ConstantScoreBulkScorer(float score, ScoreMode scoreMode, DocIdSetIterator iterator) {
    if (scoreMode.needsScores()) {
      throw new IllegalArgumentException("ScoreMode must not need scores: " + scoreMode);
    }
    if (TwoPhaseIterator.unwrap(iterator) != null) {
      throw new IllegalArgumentException("Iterator must not wrap a TwoPhaseIterator");
    }
    this.scorer = new ConstantScoreScorer(score, scoreMode, iterator);
    this.iterator = iterator;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    collector.setScorer(scorer);
    DocIdSetIterator competitiveIterator = collector.competitiveIterator();
    if (competitiveIterator != null) {
      scoreIterator(collector, acceptDocs, iterator, competitiveIterator, min, max);
    } else {
      scoreIteratorIntoBitSet(collector, acceptDocs, iterator, min, max);
    }
    return iterator.docID();
  }

  @Override
  public long cost() {
    return iterator.cost();
  }

  private static void scoreIterator(
      LeafCollector collector,
      Bits acceptDocs,
      DocIdSetIterator iterator,
      DocIdSetIterator competitiveIterator,
      int min,
      int max)
      throws IOException {
    if (competitiveIterator.docID() > min) {
      min = Math.min(competitiveIterator.docID(), max);
    }
    if (iterator.docID() < min) {
      if (iterator.docID() == min - 1) {
        iterator.nextDoc();
      } else {
        iterator.advance(min);
      }
    }
    for (int doc = iterator.docID(); doc < max; ) {
      assert competitiveIterator.docID() <= doc; // invariant
      if (competitiveIterator.docID() < doc) {
        int competitiveNext = competitiveIterator.advance(doc);
        if (competitiveNext != doc) {
          doc = iterator.advance(competitiveNext);
          continue;
        }
      }

      if (acceptDocs == null || acceptDocs.get(doc)) {
        collector.collect(doc);
      }

      doc = iterator.nextDoc();
    }
  }

  private void scoreIteratorIntoBitSet(
      LeafCollector collector, Bits acceptDocs, DocIdSetIterator iterator, int min, int max)
      throws IOException {
    if (iterator.docID() < min) {
      if (iterator.docID() == min - 1) {
        iterator.nextDoc();
      } else {
        iterator.advance(min);
      }
    }
    for (int doc = iterator.docID(); doc < max; ) {
      int windowBase = doc;
      int windowMax =
          MathUtil.unsignedMin(max, windowBase + DenseConjunctionBulkScorer.WINDOW_SIZE);

      assert windowMatches.scanIsEmpty();
      iterator.intoBitSet(windowMax, windowMatches, windowBase);

      if (acceptDocs != null) {
        acceptDocs.applyMask(windowMatches, windowBase);
      }

      collector.collect(new BitSetDocIdStream(windowMatches, windowBase));
      windowMatches.clear();

      doc = iterator.docID();
    }
  }
}
