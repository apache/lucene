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
package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.TopDocs;

/**
 * CollectorManager for {@link LargeNumHitsTopDocsCollector} that enables concurrent collection of
 * top docs across multiple segments.
 */
public class LargeNumHitsTopDocsCollectorManager
    implements CollectorManager<LargeNumHitsTopDocsCollector, TopDocs> {
  private final int numHits;

  /**
   * Creates a new {@link LargeNumHitsTopDocsCollectorManager} given the number of hits to collect.
   *
   * @param numHits the number of results to collect.
   */
  public LargeNumHitsTopDocsCollectorManager(int numHits) {
    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0, got " + numHits);
    }
    this.numHits = numHits;
  }

  @Override
  public LargeNumHitsTopDocsCollector newCollector() {
    return new LargeNumHitsTopDocsCollector(numHits);
  }

  @Override
  public TopDocs reduce(Collection<LargeNumHitsTopDocsCollector> collectors) throws IOException {
    final TopDocs[] topDocs = new TopDocs[collectors.size()];
    int i = 0;
    for (LargeNumHitsTopDocsCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(0, numHits, topDocs);
  }
}
