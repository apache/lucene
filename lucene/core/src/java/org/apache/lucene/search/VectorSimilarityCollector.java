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
import java.util.List;

/**
 * A collector that performs similarity-based vector searches. All vectors above a (lower) traversal
 * similarity are visited, and those above a (higher) result similarity are collected.
 *
 * @lucene.experimental
 */
public class VectorSimilarityCollector extends AbstractKnnCollector {
  private final float traversalSimilarity, resultSimilarity;
  private final List<ScoreDoc> scoreDocList;

  /**
   * Performs similarity-based vector searches.
   *
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param visitLimit limit of graph nodes to visit.
   */
  public VectorSimilarityCollector(
      float traversalSimilarity, float resultSimilarity, long visitLimit) {
    super(1, visitLimit);
    if (traversalSimilarity > resultSimilarity) {
      throw new IllegalArgumentException("traversalSimilarity should be <= resultSimilarity");
    }
    this.traversalSimilarity = traversalSimilarity;
    this.resultSimilarity = resultSimilarity;
    this.scoreDocList = new ArrayList<>();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    if (similarity >= resultSimilarity) {
      return scoreDocList.add(new ScoreDoc(docId, similarity));
    }
    return false;
  }

  @Override
  public float minCompetitiveSimilarity() {
    return traversalSimilarity;
  }

  @Override
  public TopDocs topDocs() {
    // This does not return results in a sorted order to prevent unnecessary calculations (because
    // we do not want to maintain the topK)
    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(
        new TotalHits(visitedCount(), relation), scoreDocList.toArray(ScoreDoc[]::new));
  }
}
