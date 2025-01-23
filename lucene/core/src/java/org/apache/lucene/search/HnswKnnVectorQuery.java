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
import org.apache.lucene.search.knn.HnswSearchStrategy;
import org.apache.lucene.search.knn.HnswStrategyKnnCollectorManager;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

/**
 * Allows configuring a {@link HnswSearchStrategy} for a {@link AbstractKnnVectorQuery}.
 *
 * @lucene.experimental
 */
public final class HnswKnnVectorQuery extends AbstractKnnVectorQuery {

  private final HnswSearchStrategy searchStrategy;
  private final AbstractKnnVectorQuery query;

  public HnswKnnVectorQuery(HnswSearchStrategy searchStrategy, AbstractKnnVectorQuery query) {
    super(query.getField(), query.getK(), query.getFilter());
    this.searchStrategy = searchStrategy;
    this.query = query;
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + ":"
        + "query["
        + query
        + "]"
        + "strategy["
        + searchStrategy
        + "]";
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    return query.approximateSearch(
        context,
        acceptDocs,
        visitedLimit,
        new HnswStrategyKnnCollectorManager(searchStrategy, knnCollectorManager));
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    return query.createVectorScorer(context, fi);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    HnswKnnVectorQuery that = (HnswKnnVectorQuery) o;
    return Objects.equals(searchStrategy, that.searchStrategy) && Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), searchStrategy, query);
  }
}
