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
import java.util.Objects;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;

/**
 * This is a version of knn vector query that exits early when HNSW queue saturates over a {@code
 * #saturationThreshold} for more than {@code #patience} times.
 *
 * <p>See <a
 * href="https://cs.uwaterloo.ca/~jimmylin/publications/Teofili_Lin_ECIR2025.pdf">"Patience in
 * Proximity: A Simple Early Termination Strategy for HNSW Graph Traversal in Approximate k-Nearest
 * Neighbor Search"</a> (Teofili and Lin). In ECIR '25: Proceedings of the 47th European Conference
 * on Information Retrieval.
 *
 * @lucene.experimental
 */
public class PatienceKnnVectorQuery extends AbstractKnnVectorQuery {

  private static final double DEFAULT_SATURATION_THRESHOLD = 0.995d;

  private final int patience;
  private final double saturationThreshold;

  final AbstractKnnVectorQuery delegate;

  /**
   * Construct a new PatienceKnnVectorQuery instance for a float vector field
   *
   * @param knnQuery the knn query to be seeded
   * @param saturationThreshold the early exit saturation threshold
   * @param patience the patience parameter
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromFloatQuery(
      KnnFloatVectorQuery knnQuery, double saturationThreshold, int patience) {
    return new PatienceKnnVectorQuery(knnQuery, saturationThreshold, patience);
  }

  /**
   * Construct a new PatienceKnnVectorQuery instance for a float vector field
   *
   * @param knnQuery the knn query to be seeded
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromFloatQuery(KnnFloatVectorQuery knnQuery) {
    return new PatienceKnnVectorQuery(
        knnQuery, DEFAULT_SATURATION_THRESHOLD, defaultPatience(knnQuery));
  }

  /**
   * Construct a new PatienceKnnVectorQuery instance for a byte vector field
   *
   * @param knnQuery the knn query to be seeded
   * @param saturationThreshold the early exit saturation threshold
   * @param patience the patience parameter
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromByteQuery(
      KnnByteVectorQuery knnQuery, double saturationThreshold, int patience) {
    return new PatienceKnnVectorQuery(knnQuery, saturationThreshold, patience);
  }

  /**
   * Construct a new PatienceKnnVectorQuery instance for a byte vector field
   *
   * @param knnQuery the knn query to be seeded
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromByteQuery(KnnByteVectorQuery knnQuery) {
    return new PatienceKnnVectorQuery(
        knnQuery, DEFAULT_SATURATION_THRESHOLD, defaultPatience(knnQuery));
  }

  /**
   * Construct a new PatienceKnnVectorQuery instance for seeded vector field
   *
   * @param knnQuery the knn query to be seeded
   * @param saturationThreshold the early exit saturation threshold
   * @param patience the patience parameter
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromSeededQuery(
      SeededKnnVectorQuery knnQuery, double saturationThreshold, int patience) {
    return new PatienceKnnVectorQuery(knnQuery, saturationThreshold, patience);
  }

  /**
   * Construct a new PatienceKnnVectorQuery instance for seeded vector field
   *
   * @param knnQuery the knn query to be seeded
   * @return a new PatienceKnnVectorQuery instance
   * @lucene.experimental
   */
  public static PatienceKnnVectorQuery fromSeededQuery(SeededKnnVectorQuery knnQuery) {
    return new PatienceKnnVectorQuery(
        knnQuery, DEFAULT_SATURATION_THRESHOLD, defaultPatience(knnQuery));
  }

  PatienceKnnVectorQuery(
      AbstractKnnVectorQuery knnQuery, double saturationThreshold, int patience) {
    super(knnQuery.field, knnQuery.k, knnQuery.filter, knnQuery.searchStrategy);
    this.delegate = knnQuery;
    this.saturationThreshold = saturationThreshold;
    this.patience = patience;
  }

  private static int defaultPatience(AbstractKnnVectorQuery delegate) {
    return Math.max(7, (int) (delegate.k * 0.3));
  }

  @Override
  public String toString(String field) {
    return "PatienceKnnVectorQuery{"
        + "saturationThreshold="
        + saturationThreshold
        + ", patience="
        + patience
        + ", delegate="
        + delegate
        + '}';
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return delegate.getKnnCollectorManager(k, searcher);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    return delegate.approximateSearch(
        context, acceptDocs, visitedLimit, new PatienceCollectorManager(knnCollectorManager));
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    return delegate.exactSearch(context, acceptIterator, queryTimeout);
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return delegate.mergeLeafResults(perLeafResults);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    delegate.visit(visitor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    PatienceKnnVectorQuery that = (PatienceKnnVectorQuery) o;
    return saturationThreshold == that.saturationThreshold
        && patience == that.patience
        && Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), saturationThreshold, patience, delegate);
  }

  @Override
  public String getField() {
    return delegate.getField();
  }

  @Override
  public int getK() {
    return delegate.getK();
  }

  @Override
  public Query getFilter() {
    return delegate.getFilter();
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    return delegate.createVectorScorer(context, fi);
  }

  class PatienceCollectorManager implements KnnCollectorManager {
    final KnnCollectorManager knnCollectorManager;

    PatienceCollectorManager(KnnCollectorManager knnCollectorManager) {
      this.knnCollectorManager = knnCollectorManager;
    }

    @Override
    public KnnCollector newCollector(
        int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx)
        throws IOException {
      return new HnswQueueSaturationCollector(
          knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx),
          saturationThreshold,
          patience);
    }
  }
}
