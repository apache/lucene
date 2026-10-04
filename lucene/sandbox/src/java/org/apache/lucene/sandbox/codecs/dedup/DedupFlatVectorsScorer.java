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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.SCRATCH_INITIAL_SIZE;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Scorer for de-duplicated vectors. Performs doc operations on the original vector values, but
 * delegates vector operations to the underlying {@link DedupVectorValues#getGroupView()}, mapping
 * document ordinals to group ordinals via {@link DedupVectorValues#getFieldOrdToGroupOrd()}.
 *
 * @lucene.experimental
 */
sealed class DedupFlatVectorsScorer implements FlatVectorsScorer
    permits DedupScalarQuantizedVectorsScorer {

  private static final FlatVectorsScorer FLAT_SCORER =
      FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

  private final FlatVectorsScorer delegate;

  DedupFlatVectorsScorer() {
    this(FLAT_SCORER);
  }

  protected DedupFlatVectorsScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  /** Resolves the values to score; subclasses may unwrap composite values. */
  protected KnnVectorValues unwrap(KnnVectorValues vectorValues) {
    return vectorValues;
  }

  /**
   * A {@link RandomVectorScorerSupplier} that operates purely in group-ordinal space. Unlike {@link
   * #getRandomVectorScorerSupplier(VectorSimilarityFunction, KnnVectorValues)}, where the scored
   * nodes are field (per-document) ordinals that are resolved to groups, here the scored nodes are
   * group ordinals themselves — so no translation is needed. Used by {@link DedupHnswVectorsWriter}
   * to build a single HNSW graph over a field's distinct vectors.
   */
  RandomVectorScorerSupplier getGroupRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, DedupVectorValues dedupValues)
      throws IOException {
    // Use the raw group view scored by the full-precision flat scorer (FLAT_SCORER), not the
    // (possibly quantized) delegate, so any scalar encoding can build a graph.
    return FLAT_SCORER.getRandomVectorScorerSupplier(
        similarityFunction, dedupValues.getGroupView());
  }

  /**
   * Returns a scorer that scores the query {@code target} against the distinct vectors in the
   * {@link DedupVectorValues#getGroupView() group view}. The scorer works in group-ordinal space:
   * {@code score(g)} scores the query against distinct vector {@code g}.
   *
   * <p>Used by {@link DedupHnswVectorsReader} to search the group graph. Because results come back
   * as group ordinals, the caller must expand each group back to the documents that reference it.
   */
  RandomVectorScorer getGroupRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, DedupVectorValues dedupValues, float[] target)
      throws IOException {
    DedupVectorValues scoringValues = scoringGroupValues(dedupValues);
    return delegate.getRandomVectorScorer(similarityFunction, scoringValues.getGroupView(), target);
  }

  /**
   * Byte-target variant of {@link #getGroupRandomVectorScorer(VectorSimilarityFunction,
   * DedupVectorValues, float[])}.
   */
  RandomVectorScorer getGroupRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, DedupVectorValues dedupValues, byte[] target)
      throws IOException {
    DedupVectorValues scoringValues = scoringGroupValues(dedupValues);
    return delegate.getRandomVectorScorer(similarityFunction, scoringValues.getGroupView(), target);
  }

  /**
   * Float16-target variant of {@link #getGroupRandomVectorScorer(VectorSimilarityFunction,
   * DedupVectorValues, float[])}.
   */
  RandomVectorScorer getGroupRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, DedupVectorValues dedupValues, short[] target)
      throws IOException {
    DedupVectorValues scoringValues = scoringGroupValues(dedupValues);
    return delegate.getRandomVectorScorer(similarityFunction, scoringValues.getGroupView(), target);
  }

  /**
   * Picks the {@link DedupVectorValues} to score against. For quantized fields this unwraps the
   * raw-and-quantized values to the quantized view, so scoring uses the quantized vectors (the same
   * view {@link #getRandomVectorScorer} uses per document).
   */
  private DedupVectorValues scoringGroupValues(DedupVectorValues dedupValues) {
    KnnVectorValues unwrapped = unwrap((KnnVectorValues) dedupValues);
    if (unwrapped instanceof DedupVectorValues unwrappedDedup) {
      return unwrappedDedup;
    }
    return dedupValues;
  }

  /**
   * Returns a node-vs-node {@link RandomVectorScorerSupplier} that accepts per-document (field)
   * ordinals, used by callers such as the HNSW graph builder to compare two stored vectors by their
   * ordinals.
   *
   * <p>For {@link DedupVectorValues} this wraps the scorer built on top of the unique vectors
   * ({@link DedupVectorValues#getGroupView() group view}), operating purely in group-ordinal space.
   * The field-ordinal to group-ordinal conversion for each scored node is done by {@link
   * RandomVectorScorerSupplierImpl} via {@link DedupVectorValues#getFieldOrdToGroupOrd()} before
   * delegating.
   *
   * <p>Non-deduplicated values are passed straight through to the delegate with no wrapping.
   */
  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    vectorValues = unwrap(vectorValues);
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorerSupplier groupView =
          delegate.getRandomVectorScorerSupplier(similarityFunction, dedupValues.getGroupView());
      return new RandomVectorScorerSupplierImpl(
          vectorValues, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return delegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    vectorValues = unwrap(vectorValues);
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer groupView =
          delegate.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(
          vectorValues, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    vectorValues = unwrap(vectorValues);
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer groupView =
          delegate.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(
          vectorValues, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, short[] target)
      throws IOException {
    vectorValues = unwrap(vectorValues);
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer groupView =
          delegate.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(
          vectorValues, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  /**
   * Supplies scorers whose scoring and target ordinals are both translated to group ordinals, with
   * doc operations on the original values.
   */
  record RandomVectorScorerSupplierImpl(
      KnnVectorValues values,
      RandomVectorScorerSupplier groupView,
      FieldOrdToGroupOrd fieldOrdToGroupOrd)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorerImpl(values, groupView.scorer(), fieldOrdToGroupOrd);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new RandomVectorScorerSupplierImpl(
          values.copy(), groupView.copy(), fieldOrdToGroupOrd.copy());
    }
  }

  private static class RandomVectorScorerImpl
      extends RandomVectorScorer.AbstractRandomVectorScorer {

    private final RandomVectorScorer groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    RandomVectorScorerImpl(
        KnnVectorValues values,
        RandomVectorScorer groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {

      super(values);
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[SCRATCH_INITIAL_SIZE];
    }

    @Override
    public float score(int node) throws IOException {
      return groupView.score(fieldOrdToGroupOrd.get(node));
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      if (scratch.length < nodes.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, nodes.length);
      }
      for (int i = 0; i < numNodes; i++) {
        scratch[i] = fieldOrdToGroupOrd.get(nodes[i]);
      }
      return groupView.bulkScore(scratch, scores, numNodes);
    }
  }

  private static final class UpdateableRandomVectorScorerImpl extends RandomVectorScorerImpl
      implements UpdateableRandomVectorScorer {

    private final UpdateableRandomVectorScorer groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;

    UpdateableRandomVectorScorerImpl(
        KnnVectorValues values,
        UpdateableRandomVectorScorer groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {

      super(values, groupView, fieldOrdToGroupOrd);
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      groupView.setScoringOrdinal(fieldOrdToGroupOrd.get(node));
    }
  }
}
