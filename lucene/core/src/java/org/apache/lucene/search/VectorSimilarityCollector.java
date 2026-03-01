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
 * Perform a similarity-based graph search.
 *
 * @lucene.experimental
 */
class VectorSimilarityCollector extends AbstractKnnCollector {
  private float traversalSimilarity;
  private final float resultSimilarity;
  private final List<ScoreDoc> scoreDocList;

  /**
   * Perform a similarity-based graph search. The similarity for graph traversal is adaptive, and
   * exponentially decaying with lower-scoring traversed nodes.
   *
   * @param resultSimilarity similarity score for result collection.
   * @param visitLimit limit on number of nodes to visit.
   */
  public VectorSimilarityCollector(float resultSimilarity, long visitLimit) {
    // TODO: add search strategy support
    super(1, visitLimit, AbstractVectorSimilarityQuery.DEFAULT_STRATEGY);
    this.traversalSimilarity = -Float.MAX_VALUE;
    this.resultSimilarity = resultSimilarity;
    this.scoreDocList = new ArrayList<>();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    if (similarity >= resultSimilarity) {
      scoreDocList.add(new ScoreDoc(docId, similarity));
    } else if (similarity > traversalSimilarity) {
      traversalSimilarity = (float) (((double) traversalSimilarity + similarity) / 2);
    }
    return true;
  }

  @Override
  public float minCompetitiveSimilarity() {
    return traversalSimilarity;
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
}
