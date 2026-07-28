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
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.DedupVectorValues;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.FieldOrdToGroupOrd;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
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
final class DedupFlatVectorsScorer implements FlatVectorsScorer {
  private static final FlatVectorsScorer SCORER =
      FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorerSupplier fieldView =
          SCORER.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
      RandomVectorScorerSupplier groupView =
          SCORER.getRandomVectorScorerSupplier(similarityFunction, dedupValues.getGroupView());
      return new RandomVectorScorerSupplierImpl(
          fieldView, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return SCORER.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer fieldView =
          SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
      RandomVectorScorer groupView =
          SCORER.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(fieldView, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer fieldView =
          SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
      RandomVectorScorer groupView =
          SCORER.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(fieldView, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, short[] target)
      throws IOException {
    if (vectorValues instanceof DedupVectorValues dedupValues) {
      RandomVectorScorer fieldView =
          SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
      RandomVectorScorer groupView =
          SCORER.getRandomVectorScorer(similarityFunction, dedupValues.getGroupView(), target);
      return new RandomVectorScorerImpl(fieldView, groupView, dedupValues.getFieldOrdToGroupOrd());
    }
    return SCORER.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  private record RandomVectorScorerSupplierImpl(
      RandomVectorScorerSupplier fieldView,
      RandomVectorScorerSupplier groupView,
      FieldOrdToGroupOrd fieldOrdToGroupOrd)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorerImpl(
          fieldView.scorer(), groupView.scorer(), fieldOrdToGroupOrd);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new RandomVectorScorerSupplierImpl(
          fieldView.copy(), groupView.copy(), fieldOrdToGroupOrd);
    }
  }

  private static class RandomVectorScorerImpl implements RandomVectorScorer {
    private final RandomVectorScorer fieldView;
    private final RandomVectorScorer groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    RandomVectorScorerImpl(
        RandomVectorScorer fieldView,
        RandomVectorScorer groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      this.fieldView = fieldView;
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[SCRATCH_INITIAL_SIZE];
    }

    @Override
    public int ordToDoc(int ord) {
      return fieldView.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return fieldView.getAcceptOrds(acceptDocs);
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

    @Override
    public int maxOrd() {
      return fieldView.maxOrd();
    }
  }

  private static final class UpdateableRandomVectorScorerImpl extends RandomVectorScorerImpl
      implements UpdateableRandomVectorScorer {
    private final UpdateableRandomVectorScorer groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;

    UpdateableRandomVectorScorerImpl(
        UpdateableRandomVectorScorer fieldView,
        UpdateableRandomVectorScorer groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      super(fieldView, groupView, fieldOrdToGroupOrd);
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      groupView.setScoringOrdinal(fieldOrdToGroupOrd.get(node));
    }
  }
}
