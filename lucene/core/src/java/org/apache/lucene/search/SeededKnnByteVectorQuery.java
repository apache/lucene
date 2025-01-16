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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.SeededKnnCollectorManager;

/**
 * This is a version of knn byte vector query that provides a query seed to initiate the vector
 * search. NOTE: The underlying format is free to ignore the provided seed
 *
 * <p>See <a href="https://dl.acm.org/doi/10.1145/3539618.3591715">"Lexically-Accelerated Dense
 * Retrieval"</a> (Kulkarni, Hrishikesh and MacAvaney, Sean and Goharian, Nazli and Frieder, Ophir).
 * In SIGIR '23: Proceedings of the 46th International ACM SIGIR Conference on Research and
 * Development in Information Retrieval Pages 152 - 162
 *
 * @lucene.experimental
 */
public class SeededKnnByteVectorQuery extends KnnByteVectorQuery {
  final Query seed;
  final Weight seedWeight;

  /**
   * Construct a new SeededKnnByteVectorQuery instance
   *
   * @param field knn byte vector field to query
   * @param target the query vector
   * @param k number of neighbors to return
   * @param filter a filter on the neighbors to return
   * @param seed a query seed to initiate the vector format search
   */
  public SeededKnnByteVectorQuery(String field, byte[] target, int k, Query filter, Query seed) {
    super(field, target, k, filter);
    this.seed = Objects.requireNonNull(seed);
    this.seedWeight = null;
  }

  SeededKnnByteVectorQuery(String field, byte[] target, int k, Query filter, Weight seedWeight) {
    super(field, target, k, filter);
    this.seed = null;
    this.seedWeight = Objects.requireNonNull(seedWeight);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (seedWeight != null) {
      return super.rewrite(indexSearcher);
    }
    BooleanQuery.Builder booleanSeedQueryBuilder =
        new BooleanQuery.Builder()
            .add(seed, BooleanClause.Occur.MUST)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER);
    if (filter != null) {
      booleanSeedQueryBuilder.add(filter, BooleanClause.Occur.FILTER);
    }
    Query seedRewritten = indexSearcher.rewrite(booleanSeedQueryBuilder.build());
    Weight seedWeight = indexSearcher.createWeight(seedRewritten, ScoreMode.TOP_SCORES, 1f);
    SeededKnnByteVectorQuery rewritten =
        new SeededKnnByteVectorQuery(field, target, k, filter, seedWeight);
    return rewritten.rewrite(indexSearcher);
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    if (seedWeight == null) {
      throw new UnsupportedOperationException("must be rewritten before constructing manager");
    }
    return new SeededKnnCollectorManager(
        super.getKnnCollectorManager(k, searcher),
        seedWeight,
        k,
        leaf -> {
          ByteVectorValues vv = leaf.getByteVectorValues(field);
          if (vv == null) {
            ByteVectorValues.checkField(leaf.getContext().reader(), field);
          }
          return vv;
        });
  }
}
