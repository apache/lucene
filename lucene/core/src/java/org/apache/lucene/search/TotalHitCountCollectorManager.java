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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Collector manager based on {@link TotalHitCountCollector} that allows users to parallelize
 * counting the number of hits, expected to be used mostly wrapped in {@link MultiCollectorManager}.
 * For cases when this is the only collector manager used, {@link IndexSearcher#count(Query)} should
 * be called instead of {@link IndexSearcher#search(Query, CollectorManager)} as the former is
 * faster whenever the count can be returned directly from the index statistics.
 */
public class TotalHitCountCollectorManager
    implements CollectorManager<TotalHitCountCollector, Integer> {

  private final boolean hasSegmentPartitions;

  /**
   * Creates a new total hit count collector manager, providing the array of leaf slices that search
   * targets, which can be retrieved via {@link IndexSearcher#getSlices()} for the searcher.
   *
   * @param leafSlices the slices that the searcher targets. Used to optimize the collection
   *     depending on whether segments have been partitioned into partitions or not.
   */
  public TotalHitCountCollectorManager(IndexSearcher.LeafSlice[] leafSlices) {
    this.hasSegmentPartitions = hasSegmentPartitions(leafSlices);
  }

  private static boolean hasSegmentPartitions(IndexSearcher.LeafSlice[] leafSlices) {
    for (IndexSearcher.LeafSlice leafSlice : leafSlices) {
      for (IndexSearcher.LeafReaderContextPartition leafPartition : leafSlice.partitions) {
        if (leafPartition.minDocId > 0
            || leafPartition.maxDocId < leafPartition.ctx.reader().maxDoc()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Internal state shared across the different collectors that this collector manager creates. This
   * is necessary to support intra-segment concurrency. We track leaves seen as an argument of
   * {@link Collector#getLeafCollector(LeafReaderContext)} calls, to ensure correctness: if the
   * first partition of a segment early terminates, count has been already retrieved for the entire
   * segment hence subsequent partitions of the same segment should also early terminate without
   * further incrementing hit count. If the first partition of a segment computes hit counts,
   * subsequent partitions of the same segment should do the same, to prevent their counts from
   * being retrieved from {@link LRUQueryCache} (which returns counts for the entire segment while
   * we'd need only that of the current leaf partition).
   */
  private final Map<Object, Future<Boolean>> earlyTerminatedMap = new ConcurrentHashMap<>();

  @Override
  public TotalHitCountCollector newCollector() throws IOException {
    if (hasSegmentPartitions) {
      return new LeafPartitionAwareTotalHitCountCollector(earlyTerminatedMap);
    }
    return new TotalHitCountCollector();
  }

  @Override
  public Integer reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
    // Make the same collector manager instance reusable across multiple searches. It isn't a strict
    // requirement but it is generally supported as collector managers normally don't hold state, as
    // opposed to collectors.
    assert hasSegmentPartitions || earlyTerminatedMap.isEmpty();
    if (hasSegmentPartitions) {
      earlyTerminatedMap.clear();
    }
    int totalHits = 0;
    for (TotalHitCountCollector collector : collectors) {
      totalHits += collector.getTotalHits();
    }
    return totalHits;
  }

  private static class LeafPartitionAwareTotalHitCountCollector extends TotalHitCountCollector {
    private final Map<Object, Future<Boolean>> earlyTerminatedMap;

    LeafPartitionAwareTotalHitCountCollector(Map<Object, Future<Boolean>> earlyTerminatedMap) {
      this.earlyTerminatedMap = earlyTerminatedMap;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      Future<Boolean> earlyTerminated = earlyTerminatedMap.get(context.id());
      if (earlyTerminated == null) {
        CompletableFuture<Boolean> firstEarlyTerminated = new CompletableFuture<>();
        Future<Boolean> previousEarlyTerminated =
            earlyTerminatedMap.putIfAbsent(context.id(), firstEarlyTerminated);
        if (previousEarlyTerminated == null) {
          // first thread for a given leaf gets to decide what the next threads targeting the same
          // leaf do
          try {
            LeafCollector leafCollector = super.getLeafCollector(context);
            firstEarlyTerminated.complete(false);
            return leafCollector;
          } catch (CollectionTerminatedException e) {
            firstEarlyTerminated.complete(true);
            throw e;
          }
        }
        earlyTerminated = previousEarlyTerminated;
      }

      try {
        if (earlyTerminated.get()) {
          // first partition of the same leaf early terminated, do the same for subsequent ones
          throw new CollectionTerminatedException();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }

      // first partition of the same leaf computed hit counts, do the same for subsequent ones
      return createLeafCollector();
    }
  }
}
