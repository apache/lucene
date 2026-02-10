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
import java.util.function.IntUnaryOperator;
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

  private static final IntUnaryOperator IDENTITY_MAPPER = docId -> docId;

  private final LongAccumulator minScoreAcc;
  private final int docBase;
  private final IntUnaryOperator docIdMapper;
  private float lastSharedScore = Float.NEGATIVE_INFINITY;

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
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, docBase, IDENTITY_MAPPER);
  }

  /**
   * Create a new CollaborativeKnnCollector with a docId mapper
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param minScoreAcc shared accumulator for global pruning
   * @param docBase the starting document ID for the current segment
   * @param docIdMapper maps absolute docIds (docBase + docId) to a globally comparable space
   */
  public CollaborativeKnnCollector(
      int k,
      int visitLimit,
      LongAccumulator minScoreAcc,
      int docBase,
      IntUnaryOperator docIdMapper) {
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, docBase, docIdMapper);
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
    this(new TopKnnCollector(k, visitLimit, searchStrategy), minScoreAcc, docBase, IDENTITY_MAPPER);
  }

  /**
   * Create a new CollaborativeKnnCollector with a search strategy and docId mapper
   *
   * @param k number of neighbors to collect
   * @param visitLimit maximum number of nodes to visit
   * @param searchStrategy search strategy to use
   * @param minScoreAcc shared accumulator for global pruning
   * @param docBase the starting document ID for the current segment
   * @param docIdMapper maps absolute docIds (docBase + docId) to a globally comparable space
   */
  public CollaborativeKnnCollector(
      int k,
      int visitLimit,
      KnnSearchStrategy searchStrategy,
      LongAccumulator minScoreAcc,
      int docBase,
      IntUnaryOperator docIdMapper) {
    this(
        new TopKnnCollector(k, visitLimit, searchStrategy),
        minScoreAcc,
        docBase,
        docIdMapper);
  }

  private CollaborativeKnnCollector(
      KnnCollector delegate,
      LongAccumulator minScoreAcc,
      int docBase,
      IntUnaryOperator docIdMapper) {
    super(delegate);
    this.minScoreAcc = minScoreAcc;
    this.docBase = docBase;
    this.docIdMapper = docIdMapper;
  }

  /**
   * Returns the minimum competitive similarity for this collector.
   *
   * <p>This method implements cross-segment pruning by consulting the shared {@link
   * LongAccumulator}.
   *
   * <p><b>Important for Distributed search:</b> This logic assumes that the {@code docIdMapper}
   * maps local document IDs to a globally consistent integer space where the ordering of IDs
   * reflects the desired tie-breaking priority across shards.
   */
  @Override
  public float minCompetitiveSimilarity() {
    float localMin = super.minCompetitiveSimilarity();

    // "Lagging Threshold" / Entry Point Protection:
    // Do not apply the global bar until we have explored a minimum number of nodes (100).
    // This prevents the search from terminating immediately at the entry point if the
    // global bar is high but the local entry point is poor (a "bridge" node).
    if (visitedCount() < 100) {
      return localMin;
    }

    long globalMinCode = minScoreAcc.get();
    if (globalMinCode == Long.MIN_VALUE) {
      return localMin;
    }

    float globalMinScore = DocScoreEncoder.toScore(globalMinCode);

    // Safety Slack: Use a 0.01 safety margin to allow shards to complete their 
    // local greedy climbs without being constantly interrupted by tiny 
    // threshold updates from other shards.
    return Math.max(localMin, globalMinScore - 0.01f);
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean collected = super.collect(docId, similarity);
    if (collected) {
      // Share the k-th best score (floor of the top-k queue). 
      float floorScore = super.minCompetitiveSimilarity();
      
      // Smart Accumulation: Only update the global bar if the improvement is significant (0.001).
      // This reduces atomic contention across threads and shards.
      if (floorScore > Float.NEGATIVE_INFINITY && floorScore > lastSharedScore + 0.001f) {
        int absoluteDocId = docId + docBase;
        int mappedDocId = docIdMapper.applyAsInt(absoluteDocId);
        minScoreAcc.accumulate(DocScoreEncoder.encode(mappedDocId, floorScore));
        lastSharedScore = floorScore;
      }
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