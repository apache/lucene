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
package org.apache.lucene.store;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.internal.hppc.BitMixer;

/**
 * Decides whether {@link MemorySegmentIndexInput#prefetch} should check the page cache before
 * calling madvise. One instance is shared by all clones and slices of an input.
 *
 * <p>We probe on every call until we have seen {@link #N} consecutive hits, then double the gap
 * between probes as hits keep coming, but never past {@link #MAX_SKIP}. The cap matters: pages can
 * be evicted at any time, and while we are skipping probes we have no feedback on cache quality.
 */
final class PrefetchBackoff {

  // A probe (mincore) costs about a microsecond. A page cache miss costs tens of microseconds on a
  // fast local NVMe and a few hundred on network storage like EBS, more under contention; see
  // https://github.com/apache/lucene/pull/16145 for measurements. Skipping a probe pays off once
  // the miss rate is below the ratio of the two costs, and we size for the slow end, roughly
  // 1/300, since an extra probe is cheap and a missed prefetch is not. After n hits without a
  // miss, the miss rate is very likely below 3/n (the "rule of three": (1-p)^n drops under 5% once
  // p > 3/n), so it takes on the order of a thousand hits to be confident enough to skip.
  static final int N = 1024;
  private static final int LOG2_N = Integer.numberOfTrailingZeros(N);

  // Gap once we start skipping, and the largest gap we allow. Every skipped call is a read we may
  // fail to prefetch, so MAX_SKIP bounds how many of those an eviction can cost us.
  static final int MIN_SKIP = 16;
  static final int MAX_SKIP = 256;

  // The hit count at which skip() reaches MAX_SKIP; counting past it changes nothing.
  static final int HITS_AT_MAX_SKIP = N * (MAX_SKIP / MIN_SKIP);

  private final AtomicInteger consecutiveHits = new AtomicInteger();

  /** {@code localCount} is per clone; it is mixed so probes don't line up with callers' loops. */
  boolean shouldProbe(int localCount) {
    final int hits = consecutiveHits.get();
    return hits < N || (BitMixer.mix(localCount) & (skip(hits) - 1)) == 0;
  }

  /** Gap between probes after {@code hits >= N} consecutive hits. */
  static int skip(int hits) {
    final int doublings = 31 - Integer.numberOfLeadingZeros(hits >>> LOG2_N);
    return Math.min(MAX_SKIP, MIN_SKIP << doublings);
  }

  // Both updates are racy on purpose. A lost increment or a repeated reset is harmless, and the
  // guards keep the hot path from dirtying a cache line that every core reads.

  void onHit() {
    if (consecutiveHits.get() < HITS_AT_MAX_SKIP) {
      consecutiveHits.incrementAndGet();
    }
  }

  void onMiss() {
    if (consecutiveHits.get() != 0) {
      consecutiveHits.set(0);
    }
  }
}
