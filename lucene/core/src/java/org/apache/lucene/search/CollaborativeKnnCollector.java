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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.VectorUtil;

/**
 * A {@link KnnCollector} that allows for collaborative search.
 * PRUNING BASED ON GLOBAL FLOOR vs LOCAL MAX.
 */
public class CollaborativeKnnCollector extends KnnCollector.Decorator {

  private static final IntUnaryOperator IDENTITY_MAPPER = docId -> docId;
  private static final int GLOBAL_BAR_MIN_VISITS = 100;
  private static final float GLOBAL_BAR_TERMINATION_SLACK = 0.0001f;
  private static final int MAX_NEIGHBORHOOD_BIT_DIFF = 350;

  private final LongAccumulator minScoreAcc;
  private final AtomicReference<byte[]> globalHint;
  private final KnnVectorValues vectorValues;
  private final KnnVectorValues.DocIndexIterator vectorIterator;
  private final int docBase;
  private final IntUnaryOperator docIdMapper;
  
  private float localMaxScore = Float.NEGATIVE_INFINITY;
  private int localMaxDocId = -1;
  private float lastSharedScore = Float.NEGATIVE_INFINITY;

  public CollaborativeKnnCollector(
      int k, int visitLimit, LongAccumulator minScoreAcc, 
      AtomicReference<byte[]> globalHint, KnnVectorValues vectorValues, int docBase) {
    this(new TopKnnCollector(k, visitLimit), minScoreAcc, globalHint, vectorValues, docBase, IDENTITY_MAPPER);
  }

  public CollaborativeKnnCollector(
      int k, int visitLimit, KnnSearchStrategy searchStrategy, 
      LongAccumulator minScoreAcc, AtomicReference<byte[]> globalHint, 
      KnnVectorValues vectorValues, int docBase, IntUnaryOperator docIdMapper) {
    this(new TopKnnCollector(k, visitLimit, searchStrategy), minScoreAcc, globalHint, vectorValues, docBase, docIdMapper);
  }

  private CollaborativeKnnCollector(
      KnnCollector delegate,
      LongAccumulator minScoreAcc,
      AtomicReference<byte[]> globalHint,
      KnnVectorValues vectorValues,
      int docBase,
      IntUnaryOperator docIdMapper) {
    super(delegate);
    this.minScoreAcc = minScoreAcc;
    this.globalHint = globalHint;
    this.vectorValues = vectorValues;
    this.vectorIterator = (vectorValues != null) ? vectorValues.iterator() : null;
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

    // 1. Neighborhood Affinity (Immunity)
    // If we are topologically close to the current global winners, stay alive to find bridges.
    if (globalHint != null && globalHint.get() != null && localMaxDocId != -1 && 
        vectorIterator != null && vectorValues instanceof FloatVectorValues fvv) {
        try {
            if (vectorIterator.advance(localMaxDocId) == localMaxDocId) {
                byte[] localSig = computeSignature(fvv.vectorValue(vectorIterator.index()));
                if (VectorUtil.xorBitCount(globalHint.get(), localSig) <= MAX_NEIGHBORHOOD_BIT_DIFF) {
                    return false; // Immune from pruning
                }
            }
        } catch (IOException e) {
            // Ignore and fallback to score pruning
        }
    }

    // 2. Mathematically Safe Pruning
    // Only stop if our BEST hit is worse than the global 500th best hit.
    return localMaxScore > Float.NEGATIVE_INFINITY && 
           localMaxScore < (globalFloorScore - GLOBAL_BAR_TERMINATION_SLACK);
  }

  private byte[] computeSignature(float[] vector) {
    byte[] sig = new byte[128]; // 1024 bits
    for (int i = 0; i < Math.min(vector.length, 1024); i++) {
      if (vector[i] > 0) {
        sig[i >> 3] |= (1 << (i & 7));
      }
    }
    return sig;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean collected = super.collect(docId, similarity);
    
    // Track local maximum (best hit seen so far on this shard)
    if (similarity > localMaxScore) {
        localMaxScore = similarity;
        localMaxDocId = docId;
    }

    if (collected) {
      float floorScore = super.minCompetitiveSimilarity();
      if (floorScore > Float.NEGATIVE_INFINITY
          && floorScore > lastSharedScore + 0.0001f) {
        
        int absoluteDocId = docId + docBase;
        minScoreAcc.accumulate(DocScoreEncoder.encode(docIdMapper.applyAsInt(absoluteDocId), floorScore));
        lastSharedScore = floorScore;
      }
    }
    return collected;
  }

  public static float toScore(long value) { return DocScoreEncoder.toScore(value); }
  public static long encode(int docId, float score) { return DocScoreEncoder.encode(docId, score); }
}
