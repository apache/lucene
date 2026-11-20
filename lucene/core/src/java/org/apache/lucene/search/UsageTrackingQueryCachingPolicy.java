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
import java.util.Arrays;

/**
 * A {@link QueryCachingPolicy} that tracks usage statistics of recently-used filters in order to
 * decide on which filters are worth caching.
 *
 * @lucene.experimental
 */
public class UsageTrackingQueryCachingPolicy implements QueryCachingPolicy {

  // the hash code that we use as a sentinel in the ring buffer.
  private static final int SENTINEL = Integer.MIN_VALUE;

  private static boolean isPointQuery(Query query) {
    // we need to check for super classes because we occasionally use anonymous
    // sub classes of eg. PointRangeQuery
    for (Class<?> clazz = query.getClass(); clazz != Query.class; clazz = clazz.getSuperclass()) {
      final String simpleName = clazz.getSimpleName();
      if (simpleName.startsWith("Point") && simpleName.endsWith("Query")) {
        return true;
      }
    }
    return false;
  }

  static boolean isCostly(Query query) {
    // This does not measure the cost of iterating over the filter (for this we
    // already have the DocIdSetIterator#cost API) but the cost to build the
    // DocIdSet in the first place
    return query instanceof MultiTermQuery
        || query instanceof MultiTermQueryConstantScoreBlendedWrapper
        || query instanceof MultiTermQueryConstantScoreWrapper
        || query instanceof TermInSetQuery
        || isPointQuery(query);
  }

  private static boolean shouldNeverCache(Query query) {
    if (query instanceof TermQuery) {
      // We do not bother caching term queries since they are already plenty fast.
      return true;
    }

    if (query instanceof FieldExistsQuery) {
      // We do not bother caching FieldExistsQuery queries since they are already plenty fast.
      return true;
    }

    if (query instanceof MatchAllDocsQuery) {
      // MatchAllDocsQuery has an iterator that is faster than what a bit set could do.
      return true;
    }

    // For the below queries, it's cheap to notice they cannot match any docs so
    // we do not bother caching them.
    if (query instanceof MatchNoDocsQuery) {
      return true;
    }

    if (query instanceof BooleanQuery bq) {
      if (bq.clauses().isEmpty()) {
        return true;
      }
    }

    if (query instanceof DisjunctionMaxQuery dmq) {
      if (dmq.getDisjuncts().isEmpty()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Maximum number of stripes the usage history is split into. Stripes are sized so that they hold
   * between {@link #MIN_HISTORY_PER_STRIPE} and twice as many entries, which keeps the linear scan
   * in {@link Stripe#frequency} short for any history size up to {@code MAX_STRIPES * 32}.
   */
  private static final int MAX_STRIPES = 1024;

  /**
   * Minimum number of entries per stripe. A query can never appear more often in its stripe than
   * the stripe has entries, so this must stay comfortably above {@link #minFrequencyToCache}.
   */
  private static final int MIN_HISTORY_PER_STRIPE = 16;

  /**
   * The usage history is split into independent stripes, each guarded by its own monitor, so that
   * concurrent searches on different queries do not contend on a single lock. A query always maps
   * to the same stripe, so its frequency within its stripe is its frequency in the history.
   */
  private final Stripe[] stripes;

  private final int stripeShift;

  /**
   * Expert: Create a new instance with a configurable history size. Beware of passing too large
   * values for the size of the history, either {@link #minFrequencyToCache} returns low values and
   * this means some filters that are rarely used will be cached, which would hurt performance. Or
   * {@link #minFrequencyToCache} returns high values that are function of the size of the history
   * but then filters will be slow to make it to the cache.
   *
   * <p>Internally the history is split into stripes of 16 to 31 entries (a single stripe is used
   * for histories smaller than 32), and a query only competes for history slots with the queries of
   * its own stripe. The default history of 256 is split into 16 stripes.
   *
   * @param historySize the number of recently used filters to track
   */
  public UsageTrackingQueryCachingPolicy(int historySize) {
    if (historySize < 2) {
      throw new IllegalArgumentException("historySize must be at least 2, got: " + historySize);
    }
    int numStripes = 1;
    while (numStripes < MAX_STRIPES && historySize / (numStripes << 1) >= MIN_HISTORY_PER_STRIPE) {
      numStripes <<= 1;
    }
    this.stripes = new Stripe[numStripes];
    for (int i = 0; i < numStripes; ++i) {
      // spread the remainder so that the total history size is preserved
      int size = historySize / numStripes + (i < historySize % numStripes ? 1 : 0);
      stripes[i] = new Stripe(size);
    }
    // the stripe is picked from the high bits of a mixed hash, numStripes == 1 maps to shift 32
    this.stripeShift = 32 - Integer.numberOfTrailingZeros(numStripes);
  }

  /**
   * Create a new instance with an history size of 256. This should be a good default for most
   * cases.
   */
  public UsageTrackingQueryCachingPolicy() {
    this(256);
  }

  /**
   * For a given filter, return how many times it should appear in the history before being cached.
   * The default implementation returns 2 for filters that need to evaluate against the entire index
   * to build a {@link DocIdSetIterator}, like {@link MultiTermQuery}, point-based queries or {@link
   * TermInSetQuery}, and 5 for other filters.
   */
  protected int minFrequencyToCache(Query query) {
    if (isCostly(query)) {
      return 2;
    } else {
      // default: cache after the filter has been seen 5 times
      int minFrequency = 5;
      if (query instanceof BooleanQuery || query instanceof DisjunctionMaxQuery) {
        // Say you keep reusing a boolean query that looks like "A OR B" and
        // never use the A and B queries out of that context. 5 times after it
        // has been used, we would cache both A, B and A OR B, which is
        // wasteful. So instead we cache compound queries a bit earlier so that
        // we would only cache "A OR B" in that case.
        minFrequency--;
      }
      return minFrequency;
    }
  }

  @Override
  public void onUse(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    if (shouldNeverCache(query)) {
      return;
    }

    // we only track hash codes to avoid holding references to possible
    // large queries; this may cause rare false positives, but at worse
    // this just means we cache a query that was not in fact used enough:
    final int hashCode = query.hashCode();
    stripeFor(hashCode).add(hashCode);
  }

  int frequency(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    final int hashCode = query.hashCode();
    return stripeFor(hashCode).frequency(hashCode);
  }

  private Stripe stripeFor(int hashCode) {
    if (stripes.length == 1) {
      return stripes[0];
    }
    // Fibonacci hashing: use the high bits so the stripe choice is independent of the low bits
    return stripes[(hashCode * 0x9E3779B9) >>> stripeShift];
  }

  @Override
  public boolean shouldCache(Query query) throws IOException {
    if (shouldNeverCache(query)) {
      return false;
    }
    final int frequency = frequency(query);
    final int minFrequency = minFrequencyToCache(query);
    return frequency >= minFrequency;
  }

  /**
   * A ring buffer of recently used hash codes. Stripes are small (typically 16 entries), so the
   * frequency of a key is computed by scanning the buffer, which is faster than maintaining a
   * frequency hash table on every add and keeps {@link #add} constant-time.
   */
  private static final class Stripe {
    private final int[] buffer;
    private int position;

    Stripe(int size) {
      buffer = new int[size];
      Arrays.fill(buffer, SENTINEL);
    }

    synchronized void add(int hashCode) {
      final int[] buffer = this.buffer;
      int position = this.position;
      buffer[position] = hashCode;
      if (++position == buffer.length) {
        position = 0;
      }
      this.position = position;
    }

    /**
     * Counts occurrences of {@code hashCode} in this stripe. This intentionally reads the buffer
     * without holding the lock: int reads cannot tear, and a concurrent {@link #add} can at worst
     * make the count off by one, which is harmless for a caching heuristic and keeps the read path
     * (called on every cache miss) free of lock traffic.
     */
    int frequency(int hashCode) {
      int frequency = 0;
      for (int value : buffer) {
        // branch-free so that the JIT can unroll and vectorize the loop
        frequency += value == hashCode ? 1 : 0;
      }
      return frequency;
    }
  }
}
