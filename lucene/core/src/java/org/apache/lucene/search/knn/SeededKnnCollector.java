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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;

/**
 * A {@link KnnCollector} that provides seeded knn collection. See usage in {@link
 * SeededKnnCollectorManager}.
 *
 * @lucene.experimental
 */
class SeededKnnCollector extends KnnCollector.Decorator
    implements EntryPointProvider, HnswSearchStrategyProvider {
  private final DocIdSetIterator entryPoints;
  private final int numberOfEntryPoints;

  SeededKnnCollector(
      KnnCollector collector, DocIdSetIterator entryPoints, int numberOfEntryPoints) {
    super(collector);
    this.entryPoints = entryPoints;
    this.numberOfEntryPoints = numberOfEntryPoints;
  }

  @Override
  public DocIdSetIterator entryPoints() {
    return entryPoints;
  }

  @Override
  public int numberOfEntryPoints() {
    return numberOfEntryPoints;
  }

  @Override
  public HnswSearchStrategy getHnswSearchStrategy() {
    if (collector instanceof HnswSearchStrategyProvider provider) {
      return provider.getHnswSearchStrategy();
    }
    return HnswSearchStrategy.DEFAULT;
  }
}
