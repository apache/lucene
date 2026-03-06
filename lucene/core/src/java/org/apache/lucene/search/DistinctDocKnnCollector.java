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
 * collected. This is useful for multi-vector search (e.g. Late Interaction) where a single document
 * may have multiple vectors indexed.
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
      // We already have a better or equal representative for this document
      return false;
    }

    if (super.collect(ordinal, similarity)) {
      docToMaxScore.put(docId, similarity);
      return true;
    }
    return false;
  }

  @Override
  public boolean shouldScore(int ordinal) {
    int docId = vectorValues.ordToDoc(ordinal);

    Float existingScore = docToMaxScore.get(docId);
    if (existingScore != null && existingScore >= minCompetitiveSimilarity()) {
      // Document is already "satisfied" with a score better than the current threshold
      return false;
    }
    return true;
  }
}
