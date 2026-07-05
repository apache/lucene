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

package org.apache.lucene.sandbox.search.knn;

import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.util.NumericUtils;

/**
 * Shared, per-query lower bound on the similarity of the k-th best result of a kNN search whose
 * hits are produced by multiple independent searchers (index segments searched concurrently, or
 * remote shards in separate JVMs).
 *
 * <p>The floor is the k-th highest similarity among all hits observed so far, tracked with a
 * bounded min-heap of the k best observed scores. Because the observed hits are always a subset of
 * the hits that will ever exist for this query, the k-th best of the observed subset can never
 * exceed the k-th best of the complete result set. The floor is therefore, at every moment, a valid
 * lower bound on the final top-k similarity cutoff: a candidate that cannot beat the current floor
 * can never enter the final merged top-k, and abandoning it loses nothing. This invariant requires
 * that every score offered or advertised belongs to a distinct document; feeding duplicate
 * documents (for example, hits for the same document from two replicas) may inflate the floor above
 * the true cutoff and callers must deduplicate before publishing.
 *
 * <p>The floor is monotonic: it starts at {@link Float#NEGATIVE_INFINITY}, becomes defined only
 * once k scores have been observed, and afterwards only rises. Monotonicity is what makes the floor
 * safe to share across threads and processes with no coordination beyond a max-reduction: updates
 * commute, duplicated or reordered deliveries are harmless, and a stale read can only under-prune
 * (costing visits), never over-prune (costing correctness).
 *
 * <p>There are two ways to feed the floor:
 *
 * <ul>
 *   <li>{@link #offer(float[], int)} publishes locally observed similarities into the shared heap.
 *       This is the in-process path used by {@link FloorAwareKnnCollector}, which batches its
 *       scores and publishes them at a fixed visit interval to keep this object off the search hot
 *       path.
 *   <li>{@link #advertise(float)} accepts an externally computed lower bound, such as the k-th best
 *       similarity found by a remote shard for the same query. It may be called from any thread at
 *       any time, typically a transport handler reacting to a message from another JVM. The caller
 *       is responsible for the value actually being a valid lower bound of the final k-th best
 *       similarity; the k-th best of any set of real, distinct hits for this query qualifies.
 * </ul>
 *
 * <p>An instance carries the state of a single query execution and must not be reused across
 * queries: a floor derived from one query's hits is meaningless, and unsafe, as a bound for another
 * query.
 *
 * @lucene.experimental
 */
public final class GlobalKnnFloor {

  private final int k;
  private final BlockingFloatHeap heap;

  /**
   * The current floor as a sortable-int-encoded float (see {@link
   * NumericUtils#floatToSortableInt(float)}), widened to a long. The sortable encoding preserves
   * float ordering under integer comparison, so a {@code Long::max} accumulator implements a
   * lock-free monotonic maximum over float values.
   */
  private final LongAccumulator floorBits;

  /**
   * Latched to true once the heap has received k scores. Written by whichever publishing thread
   * observes the heap reaching size k; the heap never shrinks, so the transition happens once and
   * the flag is stable afterwards. Until the latch is set, the heap's minimum is the minimum of
   * fewer than k scores, which is not a valid k-th-best bound and must not feed the floor.
   */
  private volatile boolean heapFull;

  /**
   * Create a floor for a query collecting {@code k} results.
   *
   * @param k the number of results the query collects; the floor becomes defined once k distinct
   *     scores have been observed
   */
  public GlobalKnnFloor(int k) {
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
    this.k = k;
    this.heap = new BlockingFloatHeap(k);
    this.floorBits = new LongAccumulator(Long::max, encode(Float.NEGATIVE_INFINITY));
  }

  /** Return the number of results the query collects, which is also the heap capacity. */
  public int k() {
    return k;
  }

  /**
   * Publish a batch of locally observed similarities and return the resulting floor.
   *
   * <p>Each published score must belong to a distinct document (across all calls for this query,
   * from all publishers). Scores that are not competitive are cheaply discarded by the bounded
   * heap; dropping scores never invalidates the floor, it only makes it less tight.
   *
   * @param scores similarities to publish, sorted in ascending order
   * @param len the number of leading entries of {@code scores} to publish, must be positive
   * @return the floor after this batch, {@link Float#NEGATIVE_INFINITY} if fewer than k scores have
   *     been observed so far
   */
  public float offer(float[] scores, int len) {
    if (len <= 0) {
      throw new IllegalArgumentException("len must be positive, got: " + len);
    }
    assert isSorted(scores, len) : "scores must be sorted in ascending order";
    float heapTop = heap.offer(scores, len);
    if (heapFull) {
      // The heap already held k scores before this call, so the value returned by offer() is the
      // minimum of a full heap: the k-th best of everything observed, a valid bound.
      floorBits.accumulate(encode(heapTop));
    } else if (heap.size() >= k) {
      heapFull = true;
      // The heap may have been filled by a concurrent publisher after our offer() returned, in
      // which case our heapTop is the minimum of fewer than k scores and could overshoot the
      // true k-th best. Re-read the top of the now-full heap instead of trusting heapTop.
      floorBits.accumulate(encode(heap.peek()));
    }
    return floor();
  }

  /**
   * Raise the floor to an externally computed lower bound of the final k-th best similarity, if it
   * exceeds the current floor. Values at or below the current floor are ignored, so duplicated or
   * reordered deliveries from remote feeders need no special handling.
   *
   * <p>Contract: {@code kthBestLowerBound} must not exceed the k-th best similarity of the query's
   * final, merged result set. The k-th best similarity among any k or more real, distinct hits for
   * this query (for example, a remote shard's converged top-k) always satisfies this. A value that
   * violates the contract makes over-pruning, and therefore missing results, possible.
   *
   * @param kthBestLowerBound the bound to advertise; must not be NaN
   */
  public void advertise(float kthBestLowerBound) {
    if (Float.isNaN(kthBestLowerBound)) {
      throw new IllegalArgumentException("advertised bound must not be NaN");
    }
    floorBits.accumulate(encode(kthBestLowerBound));
  }

  /**
   * Return the current floor: a lower bound of the final top-k similarity cutoff, or {@link
   * Float#NEGATIVE_INFINITY} while fewer than k scores have been observed and nothing has been
   * advertised. The returned value never decreases over the lifetime of this object.
   */
  public float floor() {
    return decode(floorBits.get());
  }

  private static long encode(float value) {
    return NumericUtils.floatToSortableInt(value);
  }

  private static float decode(long bits) {
    return NumericUtils.sortableIntToFloat((int) bits);
  }

  private static boolean isSorted(float[] scores, int len) {
    for (int i = 1; i < len; i++) {
      if (scores[i - 1] > scores[i]) {
        return false;
      }
    }
    return true;
  }
}
