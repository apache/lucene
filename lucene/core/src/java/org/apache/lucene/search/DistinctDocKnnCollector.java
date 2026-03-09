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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.KnnVectorValues;

/**
 * A {@link KnnCollector} that collects the Top-D unique documents rather than K vectors. This is
 * useful for multi-vector search (e.g. Late Interaction, RAG with chunked documents) where a single
 * document may have multiple vectors indexed and the goal is to find the top source documents.
 *
 * <p>The collector tracks the maximum similarity score across all vectors (chunks) belonging to
 * each document. Only one entry per unique document is passed to the underlying collector's heap,
 * preventing duplicate documents from consuming heap slots. The {@link #topDocs()} method returns
 * results with accurate per-document max scores.
 */
public class DistinctDocKnnCollector extends KnnCollector.Decorator {

  private final KnnVectorValues vectorValues;
  private final Map<Integer, DocEntry> docEntries = new HashMap<>();
  private final int d;

  /** Tracks the best ordinal and maximum similarity score for a single document. */
  private static class DocEntry {
    float maxScore;
    int bestOrdinal;

    DocEntry(float score, int ordinal) {
      this.maxScore = score;
      this.bestOrdinal = ordinal;
    }
  }

  /**
   * Create a new DistinctDocKnnCollector.
   *
   * @param collector the underlying collector to wrap (its k is used as D, the number of desired
   *     unique documents)
   * @param vectorValues the vector values for the current segment, used for ordToDoc mapping
   */
  public DistinctDocKnnCollector(KnnCollector collector, KnnVectorValues vectorValues) {
    super(collector);
    this.vectorValues = vectorValues;
    this.d = collector.k();
  }

  @Override
  public boolean collect(int ordinal, float similarity) {
    int docId = vectorValues.ordToDoc(ordinal);

    DocEntry entry = docEntries.get(docId);
    if (entry != null) {
      // Document already tracked — update max score if this vector is better.
      if (similarity > entry.maxScore) {
        entry.maxScore = similarity;
        entry.bestOrdinal = ordinal;
      }
      // Don't pass duplicate documents to the underlying collector's heap.
      return true;
    }

    // New document encountered — add to tracking and pass to underlying collector.
    docEntries.put(docId, new DocEntry(similarity, ordinal));
    return super.collect(ordinal, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    if (docEntries.size() < d) {
      return Float.NEGATIVE_INFINITY;
    }
    // The underlying heap has at most one entry per unique document (the first-seen score).
    // This threshold is conservative (first-seen ≤ max), which is safe for recall.
    return super.minCompetitiveSimilarity();
  }

  @Override
  public TopDocs topDocs() {
    // Build results from our own document tracking with accurate max scores,
    // rather than from the underlying heap which only has first-seen scores.
    List<Map.Entry<Integer, DocEntry>> sorted = new ArrayList<>(docEntries.entrySet());
    sorted.sort((a, b) -> Float.compare(b.getValue().maxScore, a.getValue().maxScore));

    int resultSize = Math.min(d, sorted.size());
    ScoreDoc[] scoreDocs = new ScoreDoc[resultSize];
    for (int i = 0; i < resultSize; i++) {
      Map.Entry<Integer, DocEntry> e = sorted.get(i);
      scoreDocs[i] = new ScoreDoc(e.getValue().bestOrdinal, e.getValue().maxScore);
    }

    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
  }

  /** Returns the number of unique documents collected so far. */
  public int distinctDocCount() {
    return docEntries.size();
  }
}
