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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

/** A Weight wrapper that creates ImpactRangeBulkScorer for impact-based range prioritization. */
public class ImpactRangeWeight extends FilterWeight {

  private final int rangeSize;
  private final int minDoc;
  private final int maxDoc;
  private final IndexSearcher searcher;
  private final ScoreMode scoreMode;
  private final float boost;

  public ImpactRangeWeight(
      Query query,
      Weight weight,
      int rangeSize,
      int minDoc,
      int maxDoc,
      IndexSearcher searcher,
      ScoreMode scoreMode,
      float boost) {
    super(query, weight);
    this.rangeSize = rangeSize;
    this.minDoc = minDoc;
    this.maxDoc = maxDoc;
    this.searcher = searcher;
    this.scoreMode = scoreMode;
    this.boost = boost;
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    ScorerSupplier supplier = in.scorerSupplier(context);
    if (supplier == null) {
      return null;
    }

    // Try to create a SimScorer from the searcher's similarity
    Similarity.SimScorer simScorer = null;
    if (scoreMode.needsScores()) {
      try {
        Similarity similarity = searcher.getSimilarity();
        // Create basic collection and term stats for scoring
        // This is a simplified version - ideally we'd use actual stats from the wrapped weight
        CollectionStatistics collectionStats = new CollectionStatistics("field", 1, 1, 1, 1);
        TermStatistics termStats = new TermStatistics(new BytesRef(), 1, 1);
        simScorer = similarity.scorer(boost, collectionStats, termStats);
      } catch (Exception e) {
        // If we can't create a SimScorer, rethrow the exception
        throw new RuntimeException("Failed to create SimScorer", e);
      }
    }

    return new ImpactRangeScorerSupplier(supplier, rangeSize, minDoc, maxDoc, simScorer, scoreMode);
  }
}
