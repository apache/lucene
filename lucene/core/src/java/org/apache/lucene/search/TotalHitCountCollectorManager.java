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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Collector manager based on {@link TotalHitCountCollector} that allows users to parallelize
 * counting the number of hits, expected to be used mostly wrapped in {@link MultiCollectorManager}.
 * For cases when this is the only collector manager used, {@link IndexSearcher#count(Query)} should
 * be called instead of {@link IndexSearcher#search(Query, CollectorManager)} as the former is
 * faster whenever the count can be returned directly from the index statistics.
 */
public class TotalHitCountCollectorManager
    implements CollectorManager<TotalHitCountCollector, Integer> {

  private final boolean leafPartitions;

  /**
   * Internal state shared across the different collectors that this collector manager creates. It
   * tracks leaves seen as an argument of {@link Collector#getLeafCollector(LeafReaderContext)}
   * calls, to ensure correctness: if the first partition of a segment early terminates, count has
   * been already retrieved for the entire segment hence subsequent partitions of the same segment
   * should also early terminate. If the first partition of a segment computes hit counts,
   * subsequent partitions of the same segment should do the same, to prevent their counts from
   * being retrieved from {@link LRUQueryCache} (which returns counts for the entire segment while
   * we need only that of the current leaf partition).
   */
  private final Map<LeafReaderContext, Boolean> seenContexts = new HashMap<>();

  public TotalHitCountCollectorManager(boolean leafPartitions) {
    this.leafPartitions = leafPartitions;
  }

  @Override
  public TotalHitCountCollector newCollector() throws IOException {
    if (leafPartitions) {
      return new LeafPartitionAwareTotalHitCountCollector(seenContexts);
    }
    return new TotalHitCountCollector();
  }

  @Override
  public Integer reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
    // TODO this makes the collector manager instance reusable across multiple searches. It isn't a
    // strict requirement
    // but it is currently supported as collector managers normally don't hold state, while
    // collectors do.
    // This currently works but would not allow to perform incremental reductions in the future. It
    // would be easy enough
    // to expose an additional method to the CollectorManager interface to explicitly clear state,
    // which index searcher
    // calls before starting a new search. That feels like overkill at the moment because there is
    // no real usecase for it.
    // An alternative is to document the requirement to not reuse collector managers across
    // searches, that would be a
    // breaking change. Perhaps not needed for now.
    seenContexts.clear();
    int totalHits = 0;
    for (TotalHitCountCollector collector : collectors) {
      totalHits += collector.getTotalHits();
    }
    return totalHits;
  }

  private static class LeafPartitionAwareTotalHitCountCollector extends TotalHitCountCollector {
    private final Map<LeafReaderContext, Boolean> seenContexts;

    LeafPartitionAwareTotalHitCountCollector(Map<LeafReaderContext, Boolean> seenContexts) {
      this.seenContexts = seenContexts;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      // TODO we could synchronize on context instead, but then the set would need to be made
      // thread-safe as well, probably overkill?
      Boolean earlyTerminated;
      synchronized (seenContexts) {
        earlyTerminated = seenContexts.get(context);
        // first time we see this leaf
        if (earlyTerminated == null) {
          try {
            LeafCollector leafCollector = super.getLeafCollector(context);
            seenContexts.put(context, false);
            return leafCollector;
          } catch (CollectionTerminatedException e) {
            seenContexts.put(context, true);
            throw e;
          }
        }
      }
      if (earlyTerminated) {
        // previous partition of the same leaf early terminated
        throw new CollectionTerminatedException();
      }
      // previous partition of the same leaf computed hit counts, do the same for subsequent ones
      return createLeafCollector();
    }
  }
}
