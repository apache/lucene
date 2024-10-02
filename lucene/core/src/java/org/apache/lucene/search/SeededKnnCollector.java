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

/** A {@link KnnCollector} that provides an initial set of seeds to initialize the search. */
class SeededKnnCollector implements KnnCollector {
  private final KnnCollector collector;
  private final DocIdSetIterator seedEntryPoints;

  public SeededKnnCollector(KnnCollector collector, DocIdSetIterator seedEntryPoints) {
    this.collector = collector;
    this.seedEntryPoints = seedEntryPoints;
  }

  @Override
  public boolean earlyTerminated() {
    return collector.earlyTerminated();
  }

  @Override
  public void incVisitedCount(int count) {
    collector.incVisitedCount(count);
  }

  @Override
  public long visitedCount() {
    return collector.visitedCount();
  }

  @Override
  public long visitLimit() {
    return collector.visitLimit();
  }

  @Override
  public int k() {
    return collector.k();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    return collector.collect(docId, similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return collector.minCompetitiveSimilarity();
  }

  @Override
  public TopDocs topDocs() {
    return collector.topDocs();
  }

  @Override
  public DocIdSetIterator getSeedEntryPoints() {
    return seedEntryPoints;
  }
}
