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
import org.apache.lucene.search.knn.HnswSearchStrategy;
import org.apache.lucene.search.knn.HnswSearchStrategyProvider;

/**
 * Perform a similarity-based graph search.
 *
 * @lucene.experimental
 */
class VectorSimilarityCollector extends AbstractKnnCollector implements HnswSearchStrategyProvider {
  private final float traversalSimilarity, resultSimilarity;
  private float maxSimilarity;
  private final List<ScoreDoc> scoreDocList;
  private final HnswSearchStrategy strategy;

  /**
   * Perform a similarity-based graph search. The graph is traversed till better scoring nodes are
   * available, or the best candidate is below {@link #traversalSimilarity}. All traversed nodes
   * above {@link #resultSimilarity} are collected.
   *
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param visitLimit limit on number of nodes to visit.
   */
  public VectorSimilarityCollector(
      float traversalSimilarity, float resultSimilarity, long visitLimit) {
    this(traversalSimilarity, resultSimilarity, visitLimit, HnswSearchStrategy.DEFAULT);
  }

  /**
   * Perform a similarity-based graph search. The graph is traversed till better scoring nodes are
   *
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param visitLimit limit on number of nodes to visit.
   * @param searchStrategy the HNSW search strategy to use, the underlying format is free to ignore
   *     this strategy hint.
   */
  public VectorSimilarityCollector(
      float traversalSimilarity,
      float resultSimilarity,
      long visitLimit,
      HnswSearchStrategy searchStrategy) {
    super(1, visitLimit);
    if (traversalSimilarity > resultSimilarity) {
      throw new IllegalArgumentException("traversalSimilarity should be <= resultSimilarity");
    }
    this.traversalSimilarity = traversalSimilarity;
    this.resultSimilarity = resultSimilarity;
    this.maxSimilarity = Float.NEGATIVE_INFINITY;
    this.scoreDocList = new ArrayList<>();
    this.strategy = searchStrategy;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    maxSimilarity = Math.max(maxSimilarity, similarity);
    if (similarity >= resultSimilarity) {
      scoreDocList.add(new ScoreDoc(docId, similarity));
    }
    return true;
  }

  @Override
  public float minCompetitiveSimilarity() {
    return Math.min(traversalSimilarity, maxSimilarity);
  }

  @Override
  public TopDocs topDocs() {
    // Results are not returned in a sorted order to prevent unnecessary calculations (because we do
    // not need to maintain the topK)
    TotalHits.Relation relation =
        earlyTerminated()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(
        new TotalHits(visitedCount(), relation), scoreDocList.toArray(ScoreDoc[]::new));
  }

  @Override
  public int numCollected() {
    return scoreDocList.size();
  }

  @Override
  public HnswSearchStrategy getHnswSearchStrategy() {
    return strategy;
  }
}
