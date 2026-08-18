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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Profiling result for a single {@link org.apache.lucene.search.IndexSearcher.LeafSlice slice} —
 * the unit of parallelism in concurrent search, executed as one task. A slice searches one or more
 * leaf {@link AggregatedQueryLeafProfilerResult partitions} (doc-id ranges of segments); those
 * partitions are nested within this result.
 *
 * <p>Slice-level start and total time are derived from the partitions searched within the slice, so
 * that consumers can see, per slice, when it actually began (revealing time spent waiting in the
 * executor queue) and how long it ran — the signals that matter for diagnosing concurrency skew.
 */
public class SliceProfilerResult {
  private final int sliceId;
  private final long threadId;
  private final String threadName;
  private final long startTime;
  private final long totalTime;
  private final List<AggregatedQueryLeafProfilerResult> partitions;

  public SliceProfilerResult(
      int sliceId,
      long threadId,
      String threadName,
      long startTime,
      long totalTime,
      List<AggregatedQueryLeafProfilerResult> partitions) {
    this.sliceId = sliceId;
    this.threadId = threadId;
    this.threadName = threadName;
    this.startTime = startTime;
    this.totalTime = totalTime;
    this.partitions =
        Collections.unmodifiableList(
            Objects.requireNonNull(partitions, "required partitions argument missing"));
  }

  /**
   * Returns the id of this slice, or {@code -1} if the timings were recorded outside of a slice
   * (e.g. a count satisfied from index statistics).
   *
   * @return the slice id, or {@code -1} if unknown
   */
  public int getSliceId() {
    return sliceId;
  }

  /**
   * Returns the {@link Thread#threadId() id} of the thread that executed this slice. Unlike the
   * thread name, this is always populated — including for virtual threads, which are typically
   * unnamed — and is safe to serialize.
   *
   * @return the executing thread's id
   */
  public long getThreadId() {
    return threadId;
  }

  /**
   * Returns the name of the thread that executed this slice. May be empty for virtual threads,
   * which are unnamed by default; use {@link #getThreadId()} for a value that is always present.
   *
   * @return the executing thread's name (possibly empty)
   */
  public String getThreadName() {
    return threadName;
  }

  /**
   * Returns the start time of this slice (the earliest partition start), useful for spotting slices
   * that waited in the executor queue.
   *
   * @return start time in nanoseconds
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the total wall-clock span of this slice (last partition end minus first partition
   * start).
   *
   * @return elapsed time in nanoseconds
   */
  public long getTotalTime() {
    return totalTime;
  }

  /**
   * Returns the per-partition profiling results for the partitions searched within this slice.
   *
   * @return the partitions of this slice
   */
  public List<AggregatedQueryLeafProfilerResult> getPartitions() {
    return partitions;
  }
}
