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
import java.util.Collection;

/**
 * Collector manager based on {@link TotalHitCountCollector} that allows users to parallelize
 * counting the number of hits, expected to be used mostly wrapped in {@link MultiCollectorManager}.
 * For cases when this is the only collector manager used, {@link IndexSearcher#count(Query)} should
 * be called instead of {@link IndexSearcher#search(Query, CollectorManager)} as the former is
 * faster whenever the count can be returned directly from the index statistics.
 */
public class TotalHitCountCollectorManager
    implements CollectorManager<TotalHitCountCollector, Integer> {
  @Override
  public TotalHitCountCollector newCollector() throws IOException {
    return new TotalHitCountCollector();
  }

  @Override
  public Integer reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
    int totalHits = 0;
    for (TotalHitCountCollector collector : collectors) {
      totalHits += collector.getTotalHits();
    }
    return totalHits;
  }
}
