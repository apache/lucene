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

import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * A {@link KnnCollector.Decorator} that early exits when nearest neighbor queue keeps saturating
 * beyond a 'patience' parameter. This records the rate of collection of new nearest neighbors in
 * the {@code delegate} KnnCollector queue, at each HNSW node candidate visit. Once it saturates for
 * a number of consecutive node visits (e.g., the patience parameter), this early terminates.
 *
 * @lucene.experimental
 */
public class HnswQueueSaturationCollector extends KnnCollector.Decorator {

  private final KnnCollector delegate;
  private final double saturationThreshold;
  private final int patience;
  private boolean patienceFinished;
  private int countSaturated;
  private int previousQueueSize;
  private int currentQueueSize;

  public HnswQueueSaturationCollector(
      KnnCollector delegate, double saturationThreshold, int patience) {
    super(delegate);
    this.delegate = delegate;
    this.previousQueueSize = 0;
    this.currentQueueSize = 0;
    this.countSaturated = 0;
    this.patienceFinished = false;
    this.saturationThreshold = saturationThreshold;
    this.patience = patience;
  }

  @Override
  public boolean earlyTerminated() {
    return delegate.earlyTerminated() || patienceFinished;
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
  public TopDocs topDocs() {
    TopDocs topDocs;
    if (patienceFinished && delegate.earlyTerminated() == false) {
      // this avoids re-running exact search in the filtered scenario when patience is exhausted
      TopDocs delegateDocs = delegate.topDocs();
      TotalHits totalHits =
          new TotalHits(delegateDocs.totalHits.value(), TotalHits.Relation.EQUAL_TO);
      topDocs = new TopDocs(totalHits, delegateDocs.scoreDocs);
    } else {
      topDocs = delegate.topDocs();
    }
    return topDocs;
  }

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

  @Override
  public KnnSearchStrategy getSearchStrategy() {
    KnnSearchStrategy delegateStrategy = delegate.getSearchStrategy();
    if (delegateStrategy instanceof KnnSearchStrategy.Hnsw hnswStrategy) {
      return new KnnSearchStrategy.Patience(this, hnswStrategy.filteredSearchThreshold());
    } else if (delegateStrategy instanceof KnnSearchStrategy.Seeded seededStrategy) {
      if (seededStrategy.originalStrategy() instanceof KnnSearchStrategy.Hnsw hnswStrategy) {
        // rewrap the underlying HNSW strategy with patience
        // this way we still use the seeded entry points, filter threshold,
        // and can utilize patience thresholds
        KnnSearchStrategy.Patience patienceStrategy =
            new KnnSearchStrategy.Patience(this, hnswStrategy.filteredSearchThreshold());
        return new KnnSearchStrategy.Seeded(
            seededStrategy.entryPoints(), seededStrategy.numberOfEntryPoints(), patienceStrategy);
      }
    }
    return delegateStrategy;
  }
}
