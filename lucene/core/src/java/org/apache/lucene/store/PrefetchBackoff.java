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

/**
 * Decides whether {@link MemorySegmentIndexInput#prefetch} should check the page cache before
 * calling madvise. One instance is shared by all clones and slices of an input.
 *
 * <p>Probe on every call until {@link #N} consecutive hits, then every {@link #SKIP}th call; a miss
 * goes back to probing on every call. Files start cold unless preloaded: a wrong warm guess costs
 * up to SKIP unprefetched reads with no feedback, a wrong cold guess costs N cheap probes. While
 * sampling, an eviction goes unnoticed for at most SKIP - 1 reads.
 *
 * <p>Each input counts its own calls, seeded from a sequence number taken when it is created, so
 * consecutive clones sample at different offsets and short-lived clones still sample collectively,
 * though possibly at a lower frequency if clones sample fewer than {@link #SKIP} times per
 * instance.
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

  // Every SKIPth call probes when the file looks warm, so an eviction costs at most SKIP - 1
  // unprefetched reads before a sample notices it. On fully cached files, this is assumed to be
  // noise. Must be a power of two.
  static final int SKIP = 64;

  private final AtomicInteger consecutiveHits;
  private final AtomicInteger inputs;

  PrefetchBackoff() {
    this(false);
  }

  /** A preloaded file was touched page by page at open, so it starts in sampling mode. */
  PrefetchBackoff(boolean preloaded) {
    consecutiveHits = new AtomicInteger(preloaded ? N : 0);
    inputs = new AtomicInteger();
  }

  /** Initial value for a new input's call counter, see {@link #shouldProbe}. Once per input. */
  int nextSeed() {
    return inputs.incrementAndGet();
  }

  /** {@code calls} is the input's own call count, starting from {@link #nextSeed()}. */
  boolean shouldProbe(int calls) {
    return consecutiveHits.get() < N || (calls & (SKIP - 1)) == 0;
  }

  // Both updates are racy on purpose. A lost increment or a repeated reset is harmless, and the
  // guards keep the hot path from dirtying a cache line that every core reads.

  void onHit() {
    if (consecutiveHits.get() < N) {
      consecutiveHits.incrementAndGet();
    }
  }

  void onMiss() {
    if (consecutiveHits.get() != 0) {
      consecutiveHits.set(0);
    }
  }
}
