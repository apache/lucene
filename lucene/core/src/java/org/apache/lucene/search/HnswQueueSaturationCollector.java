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
 * A {@link HnswKnnCollector} that early exits when nearest neighbor queue keeps saturating beyond a
 * 'patience' parameter. This records the rate of collection of new nearest neighbors in the {@code
 * delegate} KnnCollector queue, at each HNSW node candidate visit. Once it saturates for a number
 * of consecutive node visits (e.g., the patience parameter), this early terminates.
 */
public class HnswQueueSaturationCollector implements HnswKnnCollector {

  private static final double DEFAULT_SATURATION_THRESHOLD = 0.995d;

  private final KnnCollector delegate;
  private final double saturationThreshold;
  private final int patience;
  private boolean patienceFinished;
  private int countSaturated;
  private int previousQueueSize;
  private int currentQueueSize;

  HnswQueueSaturationCollector(
      KnnCollector delegate, double saturationThreshold, int patience) {
    this.delegate = delegate;
    this.previousQueueSize = 0;
    this.currentQueueSize = 0;
    this.countSaturated = 0;
    this.patienceFinished = false;
    this.saturationThreshold = saturationThreshold;
    this.patience = patience;
  }

  public HnswQueueSaturationCollector(KnnCollector delegate) {
    this.delegate = delegate;
    this.previousQueueSize = 0;
    this.currentQueueSize = 0;
    this.countSaturated = 0;
    this.patienceFinished = false;
    this.saturationThreshold = DEFAULT_SATURATION_THRESHOLD;
    this.patience = defaultPatience();
  }

  private int defaultPatience() {
    return Math.max(7, (int) (k() * 0.3));
  }

  @Override
  public boolean earlyTerminated() {
    return delegate.earlyTerminated() || patienceFinished;
  }

  @Override
  public void incVisitedCount(int count) {
    delegate.incVisitedCount(count);
  }

  @Override
  public long visitedCount() {
    return delegate.visitedCount();
  }

  @Override
  public long visitLimit() {
    return delegate.visitLimit();
  }

  @Override
  public int k() {
    return delegate.k();
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean collect = delegate.collect(docId, similarity);
    if (collect) {
      currentQueueSize++;
    }
    return collect;
  }

  @Override
  public float minCompetitiveSimilarity() {
    return delegate.minCompetitiveSimilarity();
  }

  @Override
  public TopDocs topDocs() {
    TopDocs topDocs;
    if (patienceFinished && delegate.earlyTerminated() == false) {
      TopDocs delegateDocs = delegate.topDocs();
      TotalHits totalHits =
          new TotalHits(delegateDocs.totalHits.value(), TotalHits.Relation.EQUAL_TO);
      topDocs = new TopDocs(totalHits, delegateDocs.scoreDocs);
    } else {
      topDocs = delegate.topDocs();
    }
    return topDocs;
  }

  @Override
  public void nextCandidate() {
    double queueSaturation =
        (double) Math.min(currentQueueSize, previousQueueSize) / currentQueueSize;
    previousQueueSize = currentQueueSize;
    if (queueSaturation >= saturationThreshold) {
      countSaturated++;
    } else {
      countSaturated = 0;
    }
    if (countSaturated > patience) {
      patienceFinished = true;
    }
  }
}
