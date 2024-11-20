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

/**
 * AbstractKnnCollector is the default implementation for a knn collector used for gathering kNN
 * results and providing topDocs from the gathered neighbors
 */
public abstract class AbstractKnnCollector implements KnnCollector {

  protected long visitedCount;
  private final long visitLimit;
  private final int k;

  protected AbstractKnnCollector(int k, long visitLimit) {
    this.visitLimit = visitLimit;
    this.k = k;
  }

  @Override
  public final boolean earlyTerminated() {
    return visitedCount >= visitLimit;
  }

  @Override
  public final void incVisitedCount(int count) {
    assert count > 0;
    this.visitedCount += count;
  }

  @Override
  public final long visitedCount() {
    return visitedCount;
  }

  @Override
  public final long visitLimit() {
    return visitLimit;
  }

  @Override
  public final int k() {
    return k;
  }

  @Override
  public abstract boolean collect(int docId, float similarity);

  public abstract int numCollected();

  @Override
  public abstract float minCompetitiveSimilarity();

  @Override
  public abstract TopDocs topDocs();
}
