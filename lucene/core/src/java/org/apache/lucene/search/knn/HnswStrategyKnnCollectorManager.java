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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnCollector;

/**
 * A {@link KnnCollectorManager} that uses a {@link HnswSearchStrategy} for a {@link KnnCollector}.
 *
 * @lucene.experimental
 */
public class HnswStrategyKnnCollectorManager implements KnnCollectorManager {
  private final HnswSearchStrategy searchStrategy;
  private final KnnCollectorManager delegate;

  /**
   * Creates a new {@link HnswStrategyKnnCollectorManager}.
   *
   * @param searchStrategy the HNSW search strategy
   * @param delegate the delegate {@link KnnCollectorManager}
   */
  public HnswStrategyKnnCollectorManager(
      HnswSearchStrategy searchStrategy, KnnCollectorManager delegate) {
    this.searchStrategy = searchStrategy;
    this.delegate = delegate;
  }

  @Override
  public KnnCollector newCollector(int visitedLimit, LeafReaderContext context) throws IOException {
    KnnCollector collector = delegate.newCollector(visitedLimit, context);
    if (collector == null) {
      return null;
    }
    return new HnswStrategyKnnCollector(collector, searchStrategy);
  }

  static class HnswStrategyKnnCollector extends KnnCollector.Decorator
      implements HnswSearchStrategyProvider {
    private final HnswSearchStrategy strategy;

    public HnswStrategyKnnCollector(KnnCollector collector, HnswSearchStrategy strategy) {
      super(collector);
      this.strategy = strategy;
    }

    @Override
    public HnswSearchStrategy getHnswSearchStrategy() {
      return strategy;
    }
  }
}
