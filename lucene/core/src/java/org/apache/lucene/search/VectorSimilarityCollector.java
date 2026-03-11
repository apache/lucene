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
 * Perform a similarity-based graph search to find all (approximate) vectors above a similarity
 * threshold.
 *
 * <p>The buffer for graph traversal is adaptive: starts with a high value, and decays towards
 * scores of nodes traversed but not collected, with a provided factor. The decay factor should lie
 * in {@code [0, 1]}; with higher values producing better recall using more graph exploration.
 *
 * <p>Note: Some functions of this class deviate from {@link KnnCollector}, and should be used with
 * queries that are aware of the differences (like {@link ByteVectorSimilarityQuery} and {@link
 * FloatVectorSimilarityQuery}).
 *
 * <ul>
 *   <li>{@link #k()} does NOT provide a good estimate of the number of collected results.
 *   <li>{@link #topDocs()} does NOT return docs sorted in descending order of scores.
 *   <li>{@link #collect(int, float)} does NOT return true / false based on whether the document was
 *       collected.
 * </ul>
 *
 * @lucene.experimental
 */
class VectorSimilarityCollector extends AbstractKnnCollector {
  private final float resultSimilarity, decay;
  private final List<ScoreDoc> scoreDocList;
  private float minCompetitiveSimilarity;

  /**
   * Perform a similarity-based graph search.
   *
   * @param resultSimilarity similarity score for result collection.
   * @param decay decay factor for graph traversal buffer.
   * @param visitLimit limit on number of nodes to visit.
   */
  public VectorSimilarityCollector(float resultSimilarity, float decay, long visitLimit) {
    // TODO: add search strategy support
    super(1, visitLimit, AbstractVectorSimilarityQuery.DEFAULT_STRATEGY);

    if (decay < 0 || decay > 1) {
      throw new IllegalArgumentException("decay must lie in range [0,1]; got " + decay);
    }

    this.resultSimilarity = resultSimilarity;
    this.decay = decay;
    this.scoreDocList = new ArrayList<>();
    this.minCompetitiveSimilarity = Float.NEGATIVE_INFINITY;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    // Returns true / false based on whether minCompetitiveSimilarity has been updated
    if (similarity >= resultSimilarity) {
      scoreDocList.add(new ScoreDoc(docId, similarity));
      return false;
    }

    if (decay == 1) {
      // maximum exploration
      return false;

    } else if (minCompetitiveSimilarity == Float.NEGATIVE_INFINITY) {
      // start with a large buffer
      minCompetitiveSimilarity = -Float.MAX_VALUE;

    } else {
      // decay buffer towards score of current node
      minCompetitiveSimilarity =
          (float) (similarity + ((double) minCompetitiveSimilarity - similarity) * decay);
    }

    return true;
  }

  @Override
  public float minCompetitiveSimilarity() {
    return minCompetitiveSimilarity;
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
