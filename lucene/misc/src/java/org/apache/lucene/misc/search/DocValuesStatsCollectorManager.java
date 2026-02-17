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
package org.apache.lucene.misc.search;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.lucene.search.CollectorManager;

/**
 * A {@link CollectorManager} implementation for {@link DocValuesStatsCollector}.
 *
 * @param <S> the type of {@link DocValuesStats}
 */
public class DocValuesStatsCollectorManager<S extends DocValuesStats<?>>
    implements CollectorManager<DocValuesStatsCollector, S> {

  private final Supplier<S> statsSupplier;

  /**
   * Creates a new DocValuesStatsCollectorManager.
   *
   * @param statsSupplier a supplier that creates new stats instances for each collector
   */
  public DocValuesStatsCollectorManager(Supplier<S> statsSupplier) {
    this.statsSupplier = statsSupplier;
  }

  @Override
  public DocValuesStatsCollector newCollector() throws IOException {
    return new DocValuesStatsCollector(statsSupplier.get());
  }

  @Override
  @SuppressWarnings("unchecked")
  public S reduce(Collection<DocValuesStatsCollector> collectors) throws IOException {
    if (collectors.isEmpty()) {
      return null;
    }

    S merged = statsSupplier.get();
    for (DocValuesStatsCollector collector : collectors) {
      S stats = (S) collector.getStats();
      merged.merge(stats);
    }
    return merged;
  }
}
