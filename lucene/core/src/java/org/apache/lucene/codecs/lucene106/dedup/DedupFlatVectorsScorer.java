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
package org.apache.lucene.codecs.lucene106.dedup;

import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.SCRATCH_SIZE;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.DedupVectorValues;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrd;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Scorer for de-duplicated vectors. Performs doc operations on the original vector values, but
 * delegates vector operations to the underlying {@link DedupVectorValues#getGroupView()}, mapped to
 * group ordinals via {@link DedupVectorValues#getOrdToVecOrd()}.
 *
 * @lucene.experimental
 */
final class DedupFlatVectorsScorer implements FlatVectorsScorer {
  private static final FlatVectorsScorer SCORER =
      FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorerSupplier delegate =
          SCORER.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
      RandomVectorScorerSupplier groupView =
          SCORER.getRandomVectorScorerSupplier(similarityFunction, dedupValues.getGroupView());
      return new RandomVectorScorerSupplierImpl(delegate, groupView, dedupValues.getOrdToVecOrd());
    }
    return SCORER.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer delegate =
          SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
      RandomVectorScorer groupView =
          SCORER.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(delegate, groupView, dedupValues.getOrdToVecOrd());
    }
    return SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer delegate =
          SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
      RandomVectorScorer groupView =
          SCORER.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(delegate, groupView, dedupValues.getOrdToVecOrd());
    }
    return SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  private record RandomVectorScorerSupplierImpl(
      RandomVectorScorerSupplier delegate,
      RandomVectorScorerSupplier groupView,
      OrdToVecOrd ordToVecOrd)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorerImpl(
          delegate.scorer(), groupView.scorer(), ordToVecOrd);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new RandomVectorScorerSupplierImpl(delegate.copy(), groupView.copy(), ordToVecOrd);
    }
  }

  private static class RandomVectorScorerImpl implements RandomVectorScorer {
    private final RandomVectorScorer delegate;
    private final RandomVectorScorer groupView;
    private final OrdToVecOrd ordToVecOrd;
    private int[] scratch;

    RandomVectorScorerImpl(
        RandomVectorScorer delegate, RandomVectorScorer groupView, OrdToVecOrd ordToVecOrd) {
      this.delegate = delegate;
      this.groupView = groupView;
      this.ordToVecOrd = ordToVecOrd;
      this.scratch = new int[SCRATCH_SIZE];
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return delegate.getAcceptOrds(acceptDocs);
    }

    @Override
    public float score(int node) throws IOException {
      return groupView.score(ordToVecOrd.get(node));
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      if (scratch.length < nodes.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, nodes.length);
      }
      for (int i = 0; i < numNodes; i++) {
        scratch[i] = ordToVecOrd.get(nodes[i]);
      }
      return groupView.bulkScore(scratch, scores, numNodes);
    }

    @Override
    public int maxOrd() {
      return delegate.maxOrd();
    }
  }

  private static final class UpdateableRandomVectorScorerImpl extends RandomVectorScorerImpl
      implements UpdateableRandomVectorScorer {
    private final UpdateableRandomVectorScorer groupView;
    private final OrdToVecOrd ordToVecOrd;

    UpdateableRandomVectorScorerImpl(
        UpdateableRandomVectorScorer delegate,
        UpdateableRandomVectorScorer groupView,
        OrdToVecOrd ordToVecOrd) {
      super(delegate, groupView, ordToVecOrd);
      this.groupView = groupView;
      this.ordToVecOrd = ordToVecOrd;
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      groupView.setScoringOrdinal(ordToVecOrd.get(node));
    }
  }
}
