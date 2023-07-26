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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/** empty knnCollector when no nearest neighbors are found */
final class EmptyKnnCollector implements KnnCollector {
  private final long visitedCount;
  private final long visitLimit;
  private final int k;

  public EmptyKnnCollector(int k, long visitedCount, long visitLimit) {
    this.k = k;
    this.visitedCount = visitedCount;
    this.visitLimit = visitLimit;
  }

  @Override
  public boolean earlyTerminated() {
    return true;
  }

  @Override
  public void incVisitedCount(int count) {
    throw new IllegalArgumentException();
  }

  @Override
  public long visitedCount() {
    return visitedCount;
  }

  @Override
  public long visitLimit() {
    return visitLimit;
  }

  @Override
  public int k() {
    return k;
  }

  @Override
  public boolean collect(int vectorId, float similarity) {
    throw new IllegalArgumentException();
  }

  @Override
  public float minCompetitiveSimilarity() {
    return 0;
  }

  @Override
  public TopDocs topDocs() {
    TotalHits th = new TotalHits(visitedCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    return new TopDocs(th, new ScoreDoc[0]);
  }
}
