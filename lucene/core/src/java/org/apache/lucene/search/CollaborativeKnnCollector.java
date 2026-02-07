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

import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * A {@link KnnCollector} that allows for collaborative search by sharing a global minimum
 * competitive similarity across multiple threads or segments.
 *
 * <p>This collector wraps a {@link TopKnnCollector} and a {@link LongAccumulator}. It uses {@link
 * DocScoreEncoder} logic to pack scores and document IDs into a single 64-bit value, ensuring that
 * tie-breaking rules (lower DocID wins) are respected across concurrent search processes.
 *
 * @lucene.experimental
 */
public class CollaborativeKnnCollector extends KnnCollector.Decorator {

  private final LongAccumulator minScoreAcc;
  private final int docBase;

  /**
   * Create a new CollaborativeKnnCollector
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param minScoreAcc shared accumulator for global pruning
   * @param docBase the starting document ID for the current segment
   */
  public CollaborativeKnnCollector(
      int k, int visitLimit, LongAccumulator minScoreAcc, int docBase) {
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, docBase);
  }

  /**
   * Create a new CollaborativeKnnCollector with a search strategy
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param searchStrategy search strategy to use
   * @param minScoreAcc shared accumulator for global pruning
   * @param docBase the starting document ID for the current segment
   */
  public CollaborativeKnnCollector(
      int k,
      int visitLimit,
      KnnSearchStrategy searchStrategy,
      LongAccumulator minScoreAcc,
      int docBase) {
    this(new TopKnnCollector(k, visitLimit, searchStrategy), minScoreAcc, docBase);
  }

  private CollaborativeKnnCollector(
      KnnCollector delegate, LongAccumulator minScoreAcc, int docBase) {
    super(delegate);
    this.minScoreAcc = minScoreAcc;
    this.docBase = docBase;
  }

  @Override
  public float minCompetitiveSimilarity() {
    float localMin = super.minCompetitiveSimilarity();
    long globalMinCode = minScoreAcc.get();
    if (globalMinCode == Long.MIN_VALUE) {
      return localMin;
    }

    float globalMinScore = DocScoreEncoder.toScore(globalMinCode);
    int globalMinDoc = DocScoreEncoder.docId(globalMinCode);

    // Lucene tie-breaking: lower DocID wins.
    // If the global minimum was found in a document with a smaller ID than our
    // current segment's base, then ANY document in our segment with the SAME
    // score is guaranteed to lose the tie-break. In this case, we return
    // the global score as-is.
    if (docBase > globalMinDoc) {
      return Math.max(localMin, globalMinScore);
    }

    // If our segment could contain a document with the same score that wins (smaller DocID),
    // we must allow it to be explored. We return localMin to ensure we only prune
    // when we are mathematically certain that no better match can be found in this segment.
    return localMin;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean collected = super.collect(docId, similarity);
    if (collected) {
      // Update the global accumulator with the new competitive hit.
      // We encode with the absolute docId (docId + docBase).
      minScoreAcc.accumulate(DocScoreEncoder.encode(docId + docBase, similarity));
    }
    return collected;
  }

  /**
   * Encode a score and docId into a long for the accumulator. Exposed for testing and orchestration
   * layers.
   */
  public static long encode(int docId, float score) {
    return DocScoreEncoder.encode(docId, score);
  }
}
