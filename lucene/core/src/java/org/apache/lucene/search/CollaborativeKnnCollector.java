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
  private final int mappedDocBase;
  private final IntUnaryOperator docIdMapper;

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
    this.mappedDocBase = docIdMapper.applyAsInt(docBase);
  }

  /**
   * Returns the minimum competitive similarity for this collector.
   *
   * <p>This method implements cross-segment pruning by consulting the shared {@link
   * LongAccumulator}. The global bar is only applied when this segment's mapped base docId is
   * strictly greater than the global minimum document ID, ensuring Lucene's tie-breaking semantics
   * (lower docId wins at equal scores) are preserved.
   *
   * <p><b>Design note:</b> Segment 0 (the segment with the lowest docBase) never benefits from
   * global pruning because its docBase is always {@code <= globalMinDoc}. This is intentional: if a
   * document in segment 0 ties with the global bar, it would win the tie-break, so we must not
   * prune it. In practice, exact float score ties are extremely rare for vector similarity, so this
   * conservative behavior has negligible impact on pruning effectiveness.
   *
   * <p><b>Important for Distributed search:</b> This logic assumes that the {@code docIdMapper}
   * maps local document IDs to a globally consistent integer space where the ordering of IDs reflects
   * the desired tie-breaking priority across shards.
   */
  @Override
  public float minCompetitiveSimilarity() {
    float localMin = super.minCompetitiveSimilarity();
    
    // "Lagging Threshold" / Entry Point Protection:
    // Do not apply the global bar until the local collector is full (has collected k results)
    // AND we have explored a minimum number of nodes.
    // This prevents the search from terminating immediately at the entry point if the 
    // global bar is high but the local entry point is poor (a "bridge" node).
    if (localMin == Float.NEGATIVE_INFINITY || visitedCount() < k() * 2) {
      return localMin;
    }

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
    // the global score as-is (with a small slack).
    if (mappedDocBase > globalMinDoc) {
      // Safety Slack: Use a slightly lower global bar to allow bridging through nodes
      // that are necessary to reach the target cluster.
      return Math.max(localMin, globalMinScore - 0.05f);
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
      // Share the k-th best score (floor of the top-k queue) rather than each collected
      // doc's score. The floor is Float.NEGATIVE_INFINITY until the queue is full, so we
      // only accumulate once we have a meaningful threshold. This gives a gentler global
      // bar that maintains high recall while still enabling cross-segment pruning.
      float floorScore = super.minCompetitiveSimilarity();
      if (floorScore > Float.NEGATIVE_INFINITY) {
        int absoluteDocId = docId + docBase;
        int mappedDocId = docIdMapper.applyAsInt(absoluteDocId);
        minScoreAcc.accumulate(DocScoreEncoder.encode(mappedDocId, floorScore));
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
