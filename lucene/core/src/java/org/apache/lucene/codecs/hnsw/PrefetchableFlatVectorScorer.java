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

package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * A {@link FlatVectorsScorer} wrapper that enables prefetching of vector data before scoring.
 *
 * <p>This implementation demonstrates how to use prefetch operations with KNN search to preload
 * vectors into memory before distance computations, potentially improving performance by reducing
 * memory access latency during bulk scoring operations.
 *
 * <p>The prefetching occurs in {@link PrefetchableRandomVectorScorer#bulkScore} before delegating
 * to the underlying scorer.
 *
 * @lucene.experimental
 */
public class PrefetchableFlatVectorScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer flatVectorsScorer;

  /**
   * Constructs a new prefetchable scorer wrapper.
   *
   * @param flatVectorsScorer the underlying scorer to delegate to
   */
  public PrefetchableFlatVectorScorer(FlatVectorsScorer flatVectorsScorer) {
    this.flatVectorsScorer = flatVectorsScorer;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    return new PrefetchableRandomVectorScorerSupplier(
        flatVectorsScorer.getRandomVectorScorerSupplier(similarityFunction, vectorValues),
        vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    return new PrefetchableRandomVectorScorer(
        (RandomVectorScorer.AbstractRandomVectorScorer)
            flatVectorsScorer.getRandomVectorScorer(similarityFunction, vectorValues, target));
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return new PrefetchableRandomVectorScorer(
        (RandomVectorScorer.AbstractRandomVectorScorer)
            flatVectorsScorer.getRandomVectorScorer(similarityFunction, vectorValues, target));
  }

  @Override
  public String toString() {
    return "PrefetchableFlatVectorScorer()";
  }

  /**
   * A supplier that wraps another {@link RandomVectorScorerSupplier} to provide prefetchable
   * scorers.
   */
  static class PrefetchableRandomVectorScorerSupplier implements RandomVectorScorerSupplier {

    private final RandomVectorScorerSupplier randomVectorScorerSupplier;
    private final KnnVectorValues vectorValues;

    /**
     * Constructs a new prefetchable scorer supplier.
     *
     * @param randomVectorScorerSupplier the underlying scorer supplier to delegate to
     * @param vectorValues the vector values for prefetching
     */
    public PrefetchableRandomVectorScorerSupplier(
        RandomVectorScorerSupplier randomVectorScorerSupplier, KnnVectorValues vectorValues) {
      this.randomVectorScorerSupplier = randomVectorScorerSupplier;
      this.vectorValues = vectorValues;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new PrefetchableUpdatableRandomVectorScorer(
          this.randomVectorScorerSupplier.scorer(), vectorValues);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new PrefetchableRandomVectorScorerSupplier(
          randomVectorScorerSupplier.copy(), vectorValues.copy());
    }
  }

  /**
   * A {@link RandomVectorScorer} that prefetches vector data before bulk scoring operations.
   *
   * <p>This scorer delegates all operations to an underlying scorer, but intercepts {@link
   * #bulkScore} to prefetch the required vectors before scoring.
   */
  static class PrefetchableRandomVectorScorer
      extends RandomVectorScorer.AbstractRandomVectorScorer {

    private final RandomVectorScorer.AbstractRandomVectorScorer delegate;

    /**
     * Constructs a new prefetchable random vector scorer.
     *
     * @param delegate the underlying scorer to delegate to
     */
    public PrefetchableRandomVectorScorer(
        final RandomVectorScorer.AbstractRandomVectorScorer delegate) {
      super(delegate.values());
      this.delegate = delegate;
    }

    @Override
    public float score(int node) throws IOException {
      return delegate.score(node);
    }

    /**
     * Scores multiple nodes with prefetching optimization.
     *
     * <p>Prefetches vector data before delegating to the underlying scorer for improved
     * performance.
     *
     * @param nodes array of node ordinals to score
     * @param scores output array for computed scores
     * @param numNodes number of nodes to score
     * @return the maximum score computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      values().prefetch(nodes, numNodes);
      return delegate.bulkScore(nodes, scores, numNodes);
    }

    @Override
    public int maxOrd() {
      return delegate.maxOrd();
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
    public KnnVectorValues values() {
      return delegate.values();
    }
  }

  /**
   * An updateable random vector scorer that prefetches vector data before bulk scoring.
   *
   * <p>This scorer wraps an {@link UpdateableRandomVectorScorer} and adds prefetching capability to
   * improve performance during bulk scoring operations.
   */
  static class PrefetchableUpdatableRandomVectorScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {

    private final UpdateableRandomVectorScorer delegate;

    /**
     * Constructs a new prefetchable updateable scorer.
     *
     * @param delegate the underlying scorer to delegate to
     * @param vectorValues the vector values for prefetching
     */
    PrefetchableUpdatableRandomVectorScorer(
        final UpdateableRandomVectorScorer delegate, final KnnVectorValues vectorValues) {
      super(vectorValues);
      this.delegate = delegate;
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      delegate.setScoringOrdinal(node);
    }

    @Override
    public float score(int node) throws IOException {
      return delegate.score(node);
    }

    /**
     * Scores multiple nodes with prefetching optimization.
     *
     * <p>Prefetches vector data before performing bulk scoring.
     *
     * @param nodes array of node ordinals to score
     * @param scores output array for computed scores
     * @param numNodes number of nodes to score
     * @return the maximum score computed
     * @throws IOException if an I/O error occurs
     */
    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      values().prefetch(nodes, numNodes);
      return super.bulkScore(nodes, scores, numNodes);
    }

    @Override
    public int maxOrd() {
      return delegate.maxOrd();
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
    public KnnVectorValues values() {
      return super.values();
    }
  }
}
