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
 * A {@link KnnCollector} that allows for collaborative search. PRUNING BASED ON GLOBAL FLOOR vs
 * LOCAL MAX.
 */
public class CollaborativeKnnCollector extends KnnCollector.Decorator {

  private static final IntUnaryOperator IDENTITY_MAPPER = docId -> docId;
  private static final int GLOBAL_BAR_MIN_VISITS = 100;
  private static final float GLOBAL_BAR_TERMINATION_SLACK = 0.0001f;

  private final LongAccumulator minScoreAcc;
  private final int docBase;
  private final IntUnaryOperator docIdMapper;

  private float localMaxScore = Float.NEGATIVE_INFINITY;
  private float lastSharedScore = Float.NEGATIVE_INFINITY;

  /** Convenience constructor for tests. */
  public CollaborativeKnnCollector(
      int k, int visitLimit, LongAccumulator minScoreAcc, int docBase) {
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, docBase, IDENTITY_MAPPER);
  }

  public CollaborativeKnnCollector(
      int k,
      int visitLimit,
      LongAccumulator minScoreAcc,
      int docBase,
      IntUnaryOperator docIdMapper) {
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, docBase, docIdMapper);
  }

  public CollaborativeKnnCollector(
      int k,
      int visitLimit,
      KnnSearchStrategy searchStrategy,
      LongAccumulator minScoreAcc,
      int docBase,
      IntUnaryOperator docIdMapper) {
    this(new TopKnnCollector(k, visitLimit, searchStrategy), minScoreAcc, docBase, docIdMapper);
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

  @Override
  public float minCompetitiveSimilarity() {
    // Pathfinding always uses local bar
    return super.minCompetitiveSimilarity();
  }

  @Override
  public boolean earlyTerminated() {
    if (super.earlyTerminated()) return true;
    if (visitedCount() < GLOBAL_BAR_MIN_VISITS) return false;

    long globalFloorCode = minScoreAcc.get();
    if (globalFloorCode == Long.MIN_VALUE) return false;

    float globalFloorScore = DocScoreEncoder.toScore(globalFloorCode);

    // CRITICAL: Only stop if our BEST hit is worse than the global floor.
    // If localMax < globalFloor, it's impossible for this shard to make the Top K.
    return localMaxScore > Float.NEGATIVE_INFINITY
        && localMaxScore < (globalFloorScore - GLOBAL_BAR_TERMINATION_SLACK);
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean collected = super.collect(docId, similarity);

    if (similarity > localMaxScore) {
      localMaxScore = similarity;
    }

    if (collected) {
      float floorScore = super.minCompetitiveSimilarity();
      if (floorScore > Float.NEGATIVE_INFINITY && floorScore > lastSharedScore + 0.0001f) {

        int absoluteDocId = docId + docBase;
        minScoreAcc.accumulate(
            DocScoreEncoder.encode(docIdMapper.applyAsInt(absoluteDocId), floorScore));
        lastSharedScore = floorScore;
      }
    }
    return collected;
  }

  public static float toScore(long value) {
    return DocScoreEncoder.toScore(value);
  }

  public static long encode(int docId, float score) {
    return DocScoreEncoder.encode(docId, score);
  }
}
