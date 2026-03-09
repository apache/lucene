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
 * A {@link KnnCollector} that manages a heap of Top-D unique documents rather than K vectors. This
 * is useful for multi-vector search (e.g. Late Interaction) where a single document may have
 * multiple vectors indexed and the goal is to find the top source documents.
 */
public class DistinctDocKnnCollector extends KnnCollector.Decorator {

  private final KnnVectorValues vectorValues;
  private final Map<Integer, Float> docToMaxScore = new HashMap<>();
  private final int d;

  /**
   * Create a new DistinctDocKnnCollector.
   *
   * @param collector the underlying collector to wrap
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

    Float existingScore = docToMaxScore.get(docId);
    if (existingScore != null) {
      if (similarity > existingScore) {
        // We found a better representative for a document already in our tracking map.
        // We update the map and then delegate to the underlying collector.
        // Note: The underlying TopKnnCollector will handle whether this ordinal
        // actually makes it into its internal vector heap.
        docToMaxScore.put(docId, similarity);
        return super.collect(ordinal, similarity);
      }
      // Current chunk is not better than what we already have for this doc.
      return true;
    }

    // New document encountered.
    docToMaxScore.put(docId, similarity);
    return super.collect(ordinal, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    // If we have fewer than D unique documents, we aren't competitive yet.
    if (docToMaxScore.size() < d) {
      return Float.NEGATIVE_INFINITY;
    }
    // Note: In a true Top-D Document Collector, we would return the score of the
    // D-th best document here to enable HNSW pruning.
    // For now, we delegate to the TopKnnCollector's vector-based floor.
    return super.minCompetitiveSimilarity();
  }

  @Override
  public boolean shouldExploreNeighbors(int ordinal) {
    // We've conceded that neighbor pruning is unsafe for 100% MaxSim recall.
    return true;
  }
}
