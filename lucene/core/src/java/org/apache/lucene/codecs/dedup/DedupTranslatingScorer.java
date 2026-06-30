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

package org.apache.lucene.codecs.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Bridges the dedup format's {@code docOrd → vecOrd} indirection to the underlying off-heap SIMD
 * scorer.
 *
 * <p>HNSW (build + search) calls {@link RandomVectorScorer#score(int)} with field-local doc-ords,
 * but the underlying flat data lives in a contiguous pool addressed by vec-ords. We build the SIMD
 * scorer over the pool view (so {@code Lucene99MemorySegmentFlatVectorsScorer} can use its {@code
 * MemorySegment} fast path), then translate each call's ord through a dense {@code int[]
 * docOrdToVecOrd} table before delegating.
 *
 * <p>Cost added per scoring call: one {@code int[]} read (a few ns); the dimension-sized score op
 * stays SIMD. The dense int[] is materialised once per field on the {@link
 * DedupFlatVectorsReader.FieldEntry} (a few MB at most for typical segments).
 */
final class DedupTranslatingScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer delegate;

  DedupTranslatingScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  /** Build a translating supplier from a pool-view supplier and the per-field dense map. */
  static RandomVectorScorerSupplier wrapSupplier(
      RandomVectorScorerSupplier poolSupplier, KnnVectorValues fieldView, int[] docOrdToVecOrd) {
    return new TranslatingSupplier(poolSupplier, fieldView, docOrdToVecOrd);
  }

  /** Build a translating one-shot scorer. */
  static RandomVectorScorer wrapScorer(
      RandomVectorScorer poolScorer, KnnVectorValues fieldView, int[] docOrdToVecOrd) {
    return new TranslatingScorer(poolScorer, fieldView, docOrdToVecOrd);
  }

  // -----------------------------------------------------------------------
  // FlatVectorsScorer impl — used when downstream re-routes via
  // FlatVectorsReader.getFlatVectorScorer(field). Receives the dedup field
  // values, downcasts to extract pool view + dense map, builds + wraps SIMD.
  // -----------------------------------------------------------------------

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction sim, KnnVectorValues vectorValues) throws IOException {
    Bridge bridge = bridgeOrNull(vectorValues);
    if (bridge == null) {
      return delegate.getRandomVectorScorerSupplier(sim, vectorValues);
    }
    RandomVectorScorerSupplier poolSup =
        delegate.getRandomVectorScorerSupplier(sim, bridge.poolView);
    return wrapSupplier(poolSup, vectorValues, bridge.docOrdToVecOrd);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction sim, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    Bridge bridge = bridgeOrNull(vectorValues);
    if (bridge == null || bridge.encoding != VectorEncoding.FLOAT32) {
      return delegate.getRandomVectorScorer(sim, vectorValues, target);
    }
    RandomVectorScorer poolScorer = delegate.getRandomVectorScorer(sim, bridge.poolView, target);
    return wrapScorer(poolScorer, vectorValues, bridge.docOrdToVecOrd);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction sim, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    Bridge bridge = bridgeOrNull(vectorValues);
    if (bridge == null || bridge.encoding != VectorEncoding.BYTE) {
      return delegate.getRandomVectorScorer(sim, vectorValues, target);
    }
    RandomVectorScorer poolScorer = delegate.getRandomVectorScorer(sim, bridge.poolView, target);
    return wrapScorer(poolScorer, vectorValues, bridge.docOrdToVecOrd);
  }

  private static Bridge bridgeOrNull(KnnVectorValues values) throws IOException {
    if (values instanceof OffHeapDedupFloatVectorValues fv) {
      return new Bridge(fv.poolView(), fv.docOrdToVecOrd, VectorEncoding.FLOAT32);
    }
    if (values instanceof OffHeapDedupByteVectorValues bv) {
      return new Bridge(bv.poolView(), bv.docOrdToVecOrd, VectorEncoding.BYTE);
    }
    return null;
  }

  private record Bridge(KnnVectorValues poolView, int[] docOrdToVecOrd, VectorEncoding encoding) {}

  // -----------------------------------------------------------------------
  // Wrappers
  // -----------------------------------------------------------------------

  private static final class TranslatingSupplier implements RandomVectorScorerSupplier {
    private final RandomVectorScorerSupplier delegate;
    private final KnnVectorValues fieldView;
    private final int[] docOrdToVecOrd;

    TranslatingSupplier(
        RandomVectorScorerSupplier delegate, KnnVectorValues fieldView, int[] docOrdToVecOrd) {
      this.delegate = delegate;
      this.fieldView = fieldView;
      this.docOrdToVecOrd = docOrdToVecOrd;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      UpdateableRandomVectorScorer inner = delegate.scorer();
      return new TranslatingUpdateableScorer(inner, fieldView, docOrdToVecOrd);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      // The dense int[] map and field-view metadata are immutable / thread-safe;
      // only the underlying SIMD supplier needs cloning (it clones its slice).
      return new TranslatingSupplier(delegate.copy(), fieldView, docOrdToVecOrd);
    }
  }

  private static final class TranslatingUpdateableScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final UpdateableRandomVectorScorer inner; // operates on vec-ord
    private final int[] docOrdToVecOrd;
    private int[] vecOrdScratch = new int[16];

    TranslatingUpdateableScorer(
        UpdateableRandomVectorScorer inner, KnnVectorValues fieldView, int[] docOrdToVecOrd) {
      super(fieldView);
      this.inner = inner;
      this.docOrdToVecOrd = docOrdToVecOrd;
    }

    @Override
    public void setScoringOrdinal(int docOrd) throws IOException {
      inner.setScoringOrdinal(docOrdToVecOrd[docOrd]);
    }

    @Override
    public float score(int docOrd) throws IOException {
      return inner.score(docOrdToVecOrd[docOrd]);
    }

    @Override
    public float bulkScore(int[] docOrds, float[] scores, int numNodes) throws IOException {
      int[] scratch = vecOrdScratch;
      if (scratch.length < numNodes) {
        scratch = new int[ArrayUtil.oversize(numNodes, Integer.BYTES)];
        vecOrdScratch = scratch;
      }
      final int[] map = docOrdToVecOrd;
      for (int i = 0; i < numNodes; i++) {
        scratch[i] = map[docOrds[i]];
      }
      return inner.bulkScore(scratch, scores, numNodes);
    }
  }

  private static final class TranslatingScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final RandomVectorScorer inner;
    private final int[] docOrdToVecOrd;
    private int[] vecOrdScratch = new int[16];

    TranslatingScorer(RandomVectorScorer inner, KnnVectorValues fieldView, int[] docOrdToVecOrd) {
      super(fieldView);
      this.inner = inner;
      this.docOrdToVecOrd = docOrdToVecOrd;
    }

    @Override
    public float score(int docOrd) throws IOException {
      return inner.score(docOrdToVecOrd[docOrd]);
    }

    @Override
    public float bulkScore(int[] docOrds, float[] scores, int numNodes) throws IOException {
      int[] scratch = vecOrdScratch;
      if (scratch.length < numNodes) {
        scratch = new int[ArrayUtil.oversize(numNodes, Integer.BYTES)];
        vecOrdScratch = scratch;
      }
      final int[] map = docOrdToVecOrd;
      for (int i = 0; i < numNodes; i++) {
        scratch[i] = map[docOrds[i]];
      }
      return inner.bulkScore(scratch, scores, numNodes);
    }
  }
}
