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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link CollectorManager} that wraps a delegate {@link CollectorManager} and caches all
 * collected documents (and optionally scores) per slice, so they can be replayed to a second-pass
 * {@link CollectorManager} without re-running the query.
 *
 * <p>One {@link CachingCollector} is created per slice. During {@link #replay}, each cached slice
 * is replayed into a fresh second-pass collector, and all second-pass collectors are reduced
 * together. This works correctly with both sequential and concurrent search.
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 * CachingCollectorManager&lt;C1, R1&gt; caching = new CachingCollectorManager&lt;&gt;(
 *     firstPassManager, cacheScores, maxRAMMB, null);
 * R1 firstResult = searcher.search(query, caching);
 *
 * if (caching.isCached()) {
 *   R2 secondResult = caching.replay(secondPassManager);
 * } else {
 *   // cache overflowed — re-run the query
 *   R2 secondResult = searcher.search(query, secondPassManager);
 * }
 * </pre>
 *
 * @lucene.experimental
 */
public class CachingCollectorManager<C extends Collector, R>
    implements CollectorManager<CachingCollector, R> {

  private final CollectorManager<C, R> delegate;
  private final boolean cacheScores;
  private final Double maxRAMMB;
  private final Integer maxDocsToCache;

  // One CachingCollector per slice
  private final List<CachingCollector> cachingCollectors = new ArrayList<>();

  /**
   * @param delegate the first-pass {@link CollectorManager}
   * @param cacheScores whether to cache scores in addition to document IDs
   * @param maxRAMMB the maximum RAM in MB to use per slice cache, or null if using maxDocsToCache
   * @param maxDocsToCache the maximum number of documents to cache per slice, or null if using
   *     maxRAMMB
   */
  public CachingCollectorManager(
      CollectorManager<C, R> delegate,
      boolean cacheScores,
      Double maxRAMMB,
      Integer maxDocsToCache) {
    if (maxRAMMB == null && maxDocsToCache == null || maxRAMMB != null && maxDocsToCache != null) {
      throw new IllegalArgumentException("Exactly one of maxRAMMB or maxDocsToCache must be set");
    }
    this.delegate = delegate;
    this.cacheScores = cacheScores;
    this.maxRAMMB = maxRAMMB;
    this.maxDocsToCache = maxDocsToCache;
  }

  @Override
  public CachingCollector newCollector() throws IOException {
    C collector = delegate.newCollector();
    CachingCollector cache =
        maxDocsToCache != null
            ? CachingCollector.create(collector, cacheScores, maxDocsToCache)
            : CachingCollector.create(collector, cacheScores, maxRAMMB);
    cachingCollectors.add(cache);
    return cache;
  }

  @Override
  @SuppressWarnings("unchecked")
  public R reduce(Collection<CachingCollector> collectors) throws IOException {
    List<C> originals = new ArrayList<>(collectors.size());
    for (CachingCollector cache : collectors) {
      originals.add((C) cache.in);
    }
    return delegate.reduce(originals);
  }

  /**
   * Returns {@code true} if the search has been run and all per-slice caches are intact (none
   * overflowed their RAM/doc budget). Returns {@code false} if the search has not yet been run or
   * any cache overflowed.
   */
  public boolean isCached() {
    return !cachingCollectors.isEmpty()
        && cachingCollectors.stream().allMatch(CachingCollector::isCached);
  }

  /**
   * Replays each per-slice cache into a fresh second-pass collector, then reduces all results.
   *
   * @throws IllegalStateException if {@link #isCached()} returns {@code false}
   */
  public <C2 extends Collector, R2> R2 replay(CollectorManager<C2, R2> secondPassManager)
      throws IOException {
    if (!isCached()) {
      throw new IllegalStateException("cache is not available; re-run the query instead");
    }
    List<C2> secondCollectors = new ArrayList<>(cachingCollectors.size());
    for (CachingCollector cache : cachingCollectors) {
      C2 secondCollector = secondPassManager.newCollector();
      cache.replay(secondCollector);
      secondCollectors.add(secondCollector);
    }
    return secondPassManager.reduce(secondCollectors);
  }
}
