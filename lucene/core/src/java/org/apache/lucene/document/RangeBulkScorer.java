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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

/**
 * A {@link BulkScorer} that restricts collection to the half-open doc ID interval {@code [minDocID,
 * maxDocID)}.
 *
 * <p>Typical use is a constant-score query backed by {@link DocIdSetIterator#range}, where
 * collecting the whole interval in one or few {@code collectRange} calls is cheaper than per-doc
 * {@link LeafCollector#collect}.
 */
final class RangeBulkScorer extends BulkScorer {
  private final int minDocID;
  private final int maxDocID;
  private final Scorer scorer;
  private final DocIdSetIterator iterator;

  /** Creates a bulk scorer that collects only within {@code [minDocID, maxDocID)}. */
  public RangeBulkScorer(Scorer scorer, int minDocID, int maxDocID) {
    if (minDocID >= maxDocID) {
      throw new IllegalArgumentException("minDocID must be less than maxDocID");
    }
    this.minDocID = minDocID;
    this.maxDocID = maxDocID;
    this.scorer = Objects.requireNonNull(scorer);
    this.iterator = scorer.iterator();
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    DocIdSetIterator competitiveIterator = collector.competitiveIterator();
    if (competitiveIterator != null) {
      if (competitiveIterator.docID() > min) {
        min = competitiveIterator.docID();
        // The competitive iterator may not match any docs in the range.
        min = Math.min(min, max);
      }
    }
    collector.setScorer(scorer);
    if (max <= minDocID) {
      iterator.advance(minDocID);
    } else if (min >= maxDocID) {
      iterator.advance(maxDocID);
    } else {
      int filteredMin = Math.max(min, minDocID);
      final int filteredMax = Math.min(max, maxDocID);
      iterator.advance(filteredMin);
      if (acceptDocs == null) {
        collector.collectRange(filteredMin, filteredMax);
      } else {
        int segmentStart = -1;
        for (int doc = filteredMin; doc < filteredMax; doc++) {
          if (acceptDocs.get(doc)) {
            if (segmentStart < 0) {
              segmentStart = doc;
            }
          } else if (segmentStart >= 0) {
            collector.collectRange(segmentStart, doc);
            segmentStart = -1;
          }
        }
        if (segmentStart >= 0) {
          collector.collectRange(segmentStart, filteredMax);
        }
      }
      iterator.advance(filteredMax);
    }
    return iterator.docID();
  }

  @Override
  public long cost() {
    return maxDocID - minDocID;
  }
}
