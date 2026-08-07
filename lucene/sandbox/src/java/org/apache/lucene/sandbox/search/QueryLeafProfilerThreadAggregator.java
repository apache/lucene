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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.sandbox.search.PartitionContext.PartitionKey;

/**
 * Aggregates leaf breakdowns into a nested slice → partition structure.
 *
 * <p>Leaf-level timings are keyed by the {@link PartitionKey partition} currently being searched by
 * the executing thread (as tracked by a shared {@link PartitionContext}). This attributes timings
 * to the specific doc-id-range partition that produced them, so that intra-segment partitions of
 * the same segment — which may run on the same or different threads — no longer collapse together.
 *
 * <p>At result-build time the partitions are grouped by their slice id into {@link
 * SliceProfilerResult}s: a slice is the unit of parallelism (one executor task), and it may search
 * several partitions. Each slice carries the thread that ran it plus a start/total time derived
 * from its partitions, so consumers can see how the search was parallelized and spot slices that
 * waited in the executor queue.
 *
 * <p>Non-partitioned search naturally produces a single slice with a single whole-segment
 * partition, so the output shape is the "concurrent search with a single slice" degenerate case.
 */
class QueryLeafProfilerThreadAggregator {
  private final PartitionContext partitionContext;
  private final ConcurrentMap<PartitionKey, PartitionBreakdown> partitionBreakdowns;
  private long queryStartTime = Long.MAX_VALUE;
  private long queryTotalTime = 0;

  public QueryLeafProfilerThreadAggregator(PartitionContext partitionContext) {
    this.partitionContext = partitionContext;
    partitionBreakdowns = new ConcurrentHashMap<>();
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }

  public long getQueryTotalTime() {
    return queryTotalTime;
  }

  private PartitionBreakdown getPartitionProfilerBreakdown() {
    final PartitionKey key = partitionContext.get();
    // See please https://bugs.openjdk.java.net/browse/JDK-8161372
    final PartitionBreakdown partitionBreakdown = partitionBreakdowns.get(key);

    if (partitionBreakdown != null) {
      return partitionBreakdown;
    }

    return partitionBreakdowns.computeIfAbsent(key, _ -> new PartitionBreakdown());
  }

  public QueryProfilerTimer getTimer(QueryProfilerTimingType timingType) {
    assert timingType.isLeafLevel();

    return getPartitionProfilerBreakdown().breakdown.getTimer(timingType);
  }

  /**
   * Builds the per-slice profiling results: partitions are grouped by slice id, and each slice's
   * start/total time is derived from its partitions. Also accumulates the query-level start and
   * total time across all partitions.
   */
  public List<SliceProfilerResult> getSliceProfilerResults() {
    // Group partitions by slice id, preserving first-seen order for stable output.
    final Map<Integer, SliceAccumulator> slices = new LinkedHashMap<>();
    for (PartitionKey key : partitionBreakdowns.keySet()) {
      final PartitionBreakdown partitionBreakdown = partitionBreakdowns.get(key);
      final AggregatedQueryLeafProfilerResult partitionResult =
          partitionBreakdown.breakdown.getLeafProfilerResult(
              key.segmentOrd(), key.minDocId(), key.maxDocId());
      queryStartTime = Math.min(queryStartTime, partitionResult.getStartTime());
      queryTotalTime += partitionResult.getTotalTime();

      final SliceAccumulator accumulator =
          slices.computeIfAbsent(
              key.sliceId(), id -> new SliceAccumulator(id, partitionBreakdown.thread));
      accumulator.add(partitionResult);
    }

    final List<SliceProfilerResult> results = new ArrayList<>(slices.size());
    for (SliceAccumulator accumulator : slices.values()) {
      results.add(accumulator.build());
    }
    return results;
  }

  /**
   * Pairs a partition's timing breakdown with the (last) thread that recorded into it. The thread
   * is captured lazily on first access; a single partition is searched start-to-finish by one
   * thread, so there is no contention on the breakdown itself.
   */
  private static class PartitionBreakdown {
    private final QueryLeafProfilerBreakdown breakdown = new QueryLeafProfilerBreakdown();
    private final Thread thread = Thread.currentThread();
  }

  /** Collects the partitions of one slice and derives its start/total time. */
  private static class SliceAccumulator {
    private final int sliceId;
    private final Thread thread;
    private final List<AggregatedQueryLeafProfilerResult> partitions = new ArrayList<>();
    private long sliceStartTime = Long.MAX_VALUE;
    private long sliceEndTime = Long.MIN_VALUE;

    SliceAccumulator(int sliceId, Thread thread) {
      this.sliceId = sliceId;
      this.thread = thread;
    }

    void add(AggregatedQueryLeafProfilerResult partition) {
      partitions.add(partition);
      sliceStartTime = Math.min(sliceStartTime, partition.getStartTime());
      sliceEndTime = Math.max(sliceEndTime, partition.getStartTime() + partition.getTotalTime());
    }

    SliceProfilerResult build() {
      return new SliceProfilerResult(
          sliceId,
          thread.threadId(),
          thread.getName(),
          sliceStartTime,
          sliceEndTime - sliceStartTime,
          partitions);
    }
  }
}
