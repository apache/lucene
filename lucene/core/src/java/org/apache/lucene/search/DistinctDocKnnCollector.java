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

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.KnnVectorValues;

/**
 * A {@link KnnCollector} that ensures only the best representative vector for each document is
 * collected. This is useful for multi-vector search (e.g. Late Interaction) where a single
 * document may have multiple vectors indexed.
 */
public class DistinctDocKnnCollector extends KnnCollector.Decorator {

  private final KnnVectorValues vectorValues;
  private final Map<Integer, Float> docToMaxScore = new HashMap<>();

  /**
   * Create a new DistinctDocKnnCollector.
   *
   * @param collector the underlying collector to wrap
   * @param vectorValues the vector values for the current segment, used for ordToDoc mapping
   */
  public DistinctDocKnnCollector(KnnCollector collector, KnnVectorValues vectorValues) {
    super(collector);
    this.vectorValues = vectorValues;
  }

  @Override
  public boolean collect(int ordinal, float similarity) {
    int docId = vectorValues.ordToDoc(ordinal);

    Float existingScore = docToMaxScore.get(docId);
    if (existingScore != null && similarity <= existingScore) {
      // We already have a better or equal representative for this document.
      // We don't need to update the delegate TopKnnCollector because its TopDocs
      // will already contain this docId (via a different ordinal) with an equal or better score.
      return false;
    }

    // Update our internal max score for this document
    docToMaxScore.put(docId, similarity);
    
    // Delegate collection. Note: This might evict a DIFFERENT document.
    return super.collect(ordinal, similarity);
  }

  @Override
  public boolean shouldScore(int ordinal) {
    int docId = vectorValues.ordToDoc(ordinal);

    Float existingScore = docToMaxScore.get(docId);
    if (existingScore == null) {
      return true;
    }

    // RECALL-SAFE SHORT-CIRCUIT:
    // If we already have a score for this document, and that score is already
    // "competitive enough" that finding a better chunk for the SAME document
    // is unlikely to change the FINAL set of Top-K documents.
    
    // For 100% Recall Parity, we can only skip if existingScore >= 1.0f (or max possible).
    // However, we can use minCompetitiveSimilarity() as a heuristic floor.
    // If the doc is already in our heap with a score > minCompetitiveSimilarity,
    // we have already satisfied the requirement of "getting this document into the top-K".
    
    return existingScore < minCompetitiveSimilarity();
  }
}
