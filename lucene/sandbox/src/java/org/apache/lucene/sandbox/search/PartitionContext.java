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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks, per executing thread, which leaf partition (within which slice) is currently being
 * searched. This lets the profiler attribute leaf-level timings (scoring, iteration, etc.) to the
 * specific {@link org.apache.lucene.search.IndexSearcher.LeafReaderContextPartition partition} and
 * the {@link org.apache.lucene.search.IndexSearcher.LeafSlice slice} that produced them, rather
 * than only to the executing {@link Thread}.
 *
 * <p>Concurrent (including intra-segment) search runs one <i>slice</i> per executor task; each
 * slice searches one or more <i>partitions</i> (doc-id ranges of a segment). A single thread may
 * run several slices in sequence, and a segment may be split into partitions spread across slices,
 * so keying purely by {@link Thread} (as the profiler did originally) collapses distinct partitions
 * together. The identity below — {@code sliceId} plus {@code segmentOrd} plus the {@code [minDocId,
 * maxDocId)} range — is unique per partition and records which slice it belonged to.
 *
 * <p>The slice id is set at the per-slice {@code search(LeafReaderContextPartition[], ...)} seam
 * and the partition bounds at the {@code searchLeaf} seam nested within it — the two places these
 * are in scope.
 */
class PartitionContext {

  /**
   * Identity of a leaf partition: the slice it was searched as part of, the segment ordinal, and
   * the doc-id range that was searched.
   *
   * @param sliceId identifier of the {@link org.apache.lucene.search.IndexSearcher.LeafSlice slice}
   *     this partition was searched as part of
   * @param segmentOrd the {@link org.apache.lucene.index.LeafReaderContext#ord} of the segment
   * @param minDocId inclusive lower bound of the searched doc-id range
   * @param maxDocId exclusive upper bound of the searched doc-id range
   */
  record PartitionKey(int sliceId, int segmentOrd, int minDocId, int maxDocId) {}

  /**
   * Slice id used before any slice has been entered (e.g. leaf-level work outside a slice task).
   */
  static final int UNKNOWN_SLICE = -1;

  /**
   * Used when leaf-level timings are recorded outside of a partitioned {@code searchLeaf} call (for
   * example {@link org.apache.lucene.search.IndexSearcher#count(org.apache.lucene.search.Query)}
   * satisfied from index statistics).
   */
  static final PartitionKey UNKNOWN = new PartitionKey(UNKNOWN_SLICE, -1, -1, -1);

  /** Assigns a unique, stable id to each slice as it begins executing. */
  private final AtomicInteger sliceIdGenerator = new AtomicInteger();

  private final ThreadLocal<Integer> currentSliceId = ThreadLocal.withInitial(() -> UNKNOWN_SLICE);
  private final ThreadLocal<PartitionKey> current = ThreadLocal.withInitial(() -> UNKNOWN);

  /**
   * Allocates a fresh slice id and records it for the current thread, for the duration of a slice's
   * search. Returns the id so the caller can clear the exact value afterwards.
   */
  int enterSlice() {
    int sliceId = sliceIdGenerator.getAndIncrement();
    currentSliceId.set(sliceId);
    return sliceId;
  }

  /** Clears the current thread's slice once its slice task completes. */
  void exitSlice() {
    currentSliceId.set(UNKNOWN_SLICE);
  }

  /** The partition the current thread is searching, or {@link #UNKNOWN} if none has been set. */
  PartitionKey get() {
    return current.get();
  }

  /**
   * Records the partition the current thread is about to search, tagging it with the slice
   * currently being searched by this thread.
   */
  void set(int segmentOrd, int minDocId, int maxDocId) {
    current.set(new PartitionKey(currentSliceId.get(), segmentOrd, minDocId, maxDocId));
  }

  /** Clears the current thread's partition once its {@code searchLeaf} call completes. */
  void clear() {
    current.set(UNKNOWN);
  }
}
