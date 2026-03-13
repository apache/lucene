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

package org.apache.lucene.search.knn;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollaborativeKnnCollector;
import org.apache.lucene.search.KnnCollector;

/**
 * A {@link KnnCollectorManager} that creates {@link CollaborativeKnnCollector} instances sharing a
 * single {@link LongAccumulator} for global pruning across segments, gated by topological hints.
 *
 * @lucene.experimental
 */
public class CollaborativeKnnCollectorManager implements KnnCollectorManager {

  private final int k;
  private final LongAccumulator minScoreAcc;
  private final IntUnaryOperator docIdMapper;

  /**
   * Create a new CollaborativeKnnCollectorManager
   *
   * @param k number of neighbors to collect
   * @param minScoreAcc shared accumulator for global pruning
   */
  public CollaborativeKnnCollectorManager(int k, LongAccumulator minScoreAcc) {
    this(k, minScoreAcc, docId -> docId);
  }

  /** Create a new CollaborativeKnnCollectorManager with a docId mapper */
  public CollaborativeKnnCollectorManager(
      int k, LongAccumulator minScoreAcc, IntUnaryOperator docIdMapper) {
    this.k = k;
    this.minScoreAcc = minScoreAcc;
    this.docIdMapper = docIdMapper;
  }

  @Override
  public KnnCollector newCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
      throws IOException {
    return new CollaborativeKnnCollector(
        k, visitedLimit, searchStrategy, minScoreAcc, context.docBase, docIdMapper);
  }
}
