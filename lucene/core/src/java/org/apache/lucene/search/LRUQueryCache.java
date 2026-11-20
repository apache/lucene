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

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * A {@link QueryCache} that evicts queries using a LRU (least-recently-used) eviction policy in
 * order to remain under a given maximum size and number of bytes used.
 *
 * <p>This class is thread-safe.
 *
 * <p>Internally, the cache is split into multiple partitions to reduce lock contention. Each cache
 * entry is keyed by a composite of (segment, query), and entries are routed to a partition by the
 * query alone, so all entries for a given query reside in the same partition. Each partition
 * enforces its share of the maximum size and RAM budget and evicts its own least-recently-used
 * entries.
 *
 * <p>Each cache entry holds the matching documents of one segment, so this cache works best with
 * {@link QueryCachingPolicy caching policies} that only cache on "large" segments, and it is
 * advised to not share this cache across too many indices.
 *
 * <p>{@link IndexSearcher} does not use a query cache by default. Enabling one is typically done
 * like this:
 *
 * <pre><code class="language-java">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </code></pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count}, {@link
 * #getMissCount() miss count}, {@link #getCacheSize() number of cache entries}, {@link
 * #getCacheCount() total number of DocIdSets that have ever been cached}, {@link
 * #getEvictionCount() number of evicted entries}). In case you would like to have more fine-grained
 * statistics, such as per-index or per-query-class statistics, it is possible to override various
 * callbacks: {@link #onHit}, {@link #onMiss}, {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}. {@link #onQueryCache}
 * and {@link #onQueryEviction} fire once per distinct query, while {@link #onDocIdSetCache} and
 * {@link #onDocIdSetEviction} fire once per (segment, query) entry. It is better to not perform
 * heavy computations in these methods though since they are called synchronously and under a lock.
 *
 * <p>Stale cache entries (from closed segments or cleared queries) are not removed inline during
 * cache operations. Instead, cleanup is performed either automatically by a background thread or
 * manually by the caller:
 *
 * <ul>
 *   <li>To enable automatic background cleanup, pass a {@link CacheCleanUpParameters} with a {@link
 *       java.util.concurrent.ScheduledThreadPoolExecutor} to the constructor. The cache will
 *       schedule periodic cleanup at the configured interval. The cache implements {@link
 *       java.io.Closeable} and must be closed to shut down the background scheduler.
 *   <li>If no {@link CacheCleanUpParameters} is provided (or it is {@code null}), the caller is
 *       responsible for invoking {@link #cleanUp()} periodically to remove stale entries.
 * </ul>
 *
 * @see QueryCachingPolicy
 * @lucene.experimental
 */
public class LRUQueryCache implements QueryCache, Accountable, Closeable {

  private final long maxRamBytesUsed;
  private final Predicate<LeafReaderContext> leavesToCache;
  private volatile float skipCacheFactor;
  private final LongAdder hitCount;
  private final LongAdder missCount;
  private final int numberOfPartitions;
  private final LRUQueryCachePartition[] lruQueryCachePartition;

  private final ConcurrentMap<IndexReader.CacheKey, Boolean> registeredClosedListeners =
      new ConcurrentHashMap<>();

  private final Set<IndexReader.CacheKey> keysToClean =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<Query> queriesToClean = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ScheduledThreadPoolExecutor scheduler;

  private static final int DEFAULT_NUMBER_OF_PARTITIONS = 16;

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code> cache entries (each
   * entry being a segment-query pair) with at most <code>maxRamBytesUsed</code> bytes of memory,
   * only on leaves that satisfy {@code leavesToCache}. The cache is split into 16 partitions, or
   * into the largest power of two that is at most {@code maxSize} if {@code maxSize} is smaller.
   *
   * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
   * top-level query will not be cached in order to not slow down queries too much.
   */
  public LRUQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor) {
    this(
        maxSize,
        maxRamBytesUsed,
        leavesToCache,
        skipCacheFactor,
        defaultNumberOfPartitions(maxSize),
        null);
  }

  // The default number of partitions, capped so that every partition can hold at least one entry.
  private static int defaultNumberOfPartitions(int maxSize) {
    return Integer.highestOneBit(Math.clamp(maxSize, 1, DEFAULT_NUMBER_OF_PARTITIONS));
  }

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code> cache entries (each
   * entry being a segment-query pair) with at most <code>maxRamBytesUsed</code> bytes of memory,
   * only on leaves that satisfy {@code leavesToCache}. The cache is internally split into {@code
   * numberOfPartitions} partitions to reduce lock contention. {@code numberOfPartitions} must be a
   * power of two and no greater than {@code maxSize}, and {@code maxSize} and {@code
   * maxRamBytesUsed} are divided evenly across partitions.
   *
   * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
   * top-level query will not be cached in order to not slow down queries too much.
   *
   * <p>If {@code cacheCleanUpParameters} is provided with a {@link
   * java.util.concurrent.ScheduledThreadPoolExecutor}, stale cache entries (from closed segments or
   * cleared queries) will be removed automatically by a background thread at the configured
   * interval. When using background cleanup, this cache must be {@link #close() closed} to shut
   * down the scheduler. If {@code cacheCleanUpParameters} is {@code null}, the caller must invoke
   * {@link #cleanUp()} manually to remove stale entries.
   */
  public LRUQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor,
      int numberOfPartitions,
      CacheCleanUpParameters cacheCleanUpParameters) {
    if (numberOfPartitions <= 0 || Integer.bitCount(numberOfPartitions) != 1) {
      throw new IllegalArgumentException(
          "numberOfPartitions must be a positive power of two, got " + numberOfPartitions);
    }
    if (maxSize < numberOfPartitions) {
      throw new IllegalArgumentException(
          "maxSize must be at least numberOfPartitions so that every partition can hold an entry,"
              + " got maxSize="
              + maxSize
              + ", numberOfPartitions="
              + numberOfPartitions);
    }
    this.maxRamBytesUsed = maxRamBytesUsed;
    this.leavesToCache = leavesToCache;
    if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
      throw new IllegalArgumentException(
          "skipCacheFactor must be no less than 1, get " + skipCacheFactor);
    }
    this.skipCacheFactor = skipCacheFactor;

    hitCount = new LongAdder();
    missCount = new LongAdder();
    this.numberOfPartitions = numberOfPartitions;
    this.lruQueryCachePartition = new LRUQueryCachePartition[numberOfPartitions];
    int maxSizePerPartition = maxSize / this.numberOfPartitions;
    long maxRamBytesUsedPerPartition = maxRamBytesUsed / this.numberOfPartitions;
    for (int i = 0; i < numberOfPartitions; i++) {
      lruQueryCachePartition[i] =
          new LRUQueryCachePartition(maxSizePerPartition, maxRamBytesUsedPerPartition);
    }
    if (cacheCleanUpParameters != null
        && cacheCleanUpParameters.getScheduledThreadPoolExecutor().isPresent()) {
      this.scheduler = cacheCleanUpParameters.getScheduledThreadPoolExecutor().get();
      this.scheduler.scheduleWithFixedDelay(
          this::cleanUp,
          cacheCleanUpParameters.getScheduleDelayMs(),
          cacheCleanUpParameters.getScheduleDelayMs(),
          TimeUnit.MILLISECONDS);
    } else {
      this.scheduler = null; // The caller prefer to manually clean up cache
    }
  }

  public LRUQueryCachePartition[] getLruQueryCacheSegments() {
    return this.lruQueryCachePartition;
  }

  Iterable<QueryCacheKey> keys() {
    List<Iterable<QueryCacheKey>> queryCacheKeyList = new ArrayList<>();
    for (int i = 0; i < this.numberOfPartitions; i++) {
      queryCacheKeyList.add(this.lruQueryCachePartition[i].keys());
    }
    return queryCacheKeyList.stream()
        .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
        .collect(Collectors.toList());
  }

  /**
   * Get the skip cache factor
   *
   * @return #setSkipCacheFactor
   */
  public float getSkipCacheFactor() {
    return skipCacheFactor;
  }

  /**
   * This setter enables the skipCacheFactor to be updated dynamically.
   *
   * @param skipCacheFactor clauses whose cost is {@code skipCacheFactor} times more than the cost
   *     of the top-level query will not be cached in order to not slow down queries too much.
   */
  public void setSkipCacheFactor(float skipCacheFactor) {
    this.skipCacheFactor = skipCacheFactor;
  }

  /**
   * Create a new instance that will cache at most <code>maxSize</code> cache entries (each entry
   * being a segment-query pair) with at most <code>maxRamBytesUsed</code> bytes of memory. Items
   * will only be cached on leaves that have more than 10k documents and have more than half of the
   * average documents per leave of the index. This should guarantee that all leaves from the upper
   * {@link TieredMergePolicy tier} will be cached. Only clauses whose cost is at most 100x the cost
   * of the top-level query will be cached in order to not hurt latency too much because of caching.
   */
  public LRUQueryCache(int maxSize, long maxRamBytesUsed) {
    this(maxSize, maxRamBytesUsed, new MinSegmentSizePredicate(10000), 10);
  }

  // pkg-private for testing
  Map<QueryCacheKey, QueryMetadata> getUniqueQueries() {
    Map<QueryCacheKey, QueryMetadata> map = new HashMap<>();
    for (int i = 0; i < this.numberOfPartitions; i++) {
      map.putAll(this.lruQueryCachePartition[i].getUniqueCacheKeys());
    }
    return map;
  }

  /**
   * Shuts down the background cleanup scheduler, if one was configured via {@link
   * CacheCleanUpParameters}. This method should be called when the cache is no longer needed to
   * prevent the background thread from running indefinitely. If no background scheduler was
   * configured, this method is a no-op.
   */
  @Override
  public void close() throws IOException {
    if (this.scheduler != null) {
      this.scheduler.shutdown();
    }
  }

  // pkg-private for testing
  static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
    private final int minSize;

    MinSegmentSizePredicate(int minSize) {
      this.minSize = minSize;
    }

    @Override
    public boolean test(LeafReaderContext context) {
      final int maxDoc = context.reader().maxDoc();
      if (maxDoc < minSize) {
        return false;
      }
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      final int averageTotalDocs =
          topLevelContext.reader().maxDoc() / topLevelContext.leaves().size();
      return maxDoc * 2 > averageTotalDocs;
    }
  }

  /**
   * Expert: callback when there is a cache hit on a given query. Implementing this method is
   * typically useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onMiss
   * @lucene.experimental
   */
  protected void onHit(Object readerCoreKey, Query query) {
    hitCount.add(1);
  }

  /**
   * Expert: callback when there is a cache miss on a given query.
   *
   * @see #onHit
   * @lucene.experimental
   */
  protected void onMiss(Object readerCoreKey, Query query) {
    assert query != null;
    missCount.add(1);
  }

  /**
   * Expert: callback when a query is first cached. Fires once per distinct query (on its first
   * {@code (segment, query)} entry), with the RAM used by the query itself, so subclasses can track
   * unique cached queries without their own deduplication.
   *
   * <p>This method is purely a notification hook for subclasses. Internal bookkeeping (RAM
   * accounting, size tracking) is handled by the cache partitions.
   *
   * @see #onQueryEviction
   * @lucene.experimental
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {}

  /**
   * Expert: callback when a query's last cache entry is evicted. Fires once per distinct query
   * (when its final {@code (segment, query)} entry goes), with the RAM reclaimed for the query
   * itself.
   *
   * <p>This method is purely a notification hook for subclasses. Internal bookkeeping (RAM
   * accounting, size tracking) is handled by the cache partitions.
   *
   * @see #onQueryCache
   * @lucene.experimental
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {}

  /**
   * Expert: callback when a {@link DocIdSet} is cached for a {@code (segment, query)} entry. Fires
   * once per entry (i.e. once per segment for a given query), with the RAM used by the doc-id set
   * plus this entry's overhead.
   *
   * @see #onDocIdSetEviction
   * @lucene.experimental
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {}

  /**
   * Expert: callback when a {@link DocIdSet} is removed from a {@code (segment, query)} entry.
   * Fires once per entry evicted.
   *
   * @see #onDocIdSetCache
   * @lucene.experimental
   */
  protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {}

  /**
   * Expert: callback when the cache is completely cleared.
   *
   * @lucene.experimental
   */
  protected void onClear() {}

  public CacheAndCount get(Query key, IndexReader.CacheHelper cacheHelper) {
    assert key instanceof BoostQuery == false;
    assert key instanceof ConstantScoreQuery == false;
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    final int queryHash = key.hashCode();
    QueryCacheKey queryCacheKey = new QueryCacheKey(readerKey, key, queryHash);
    int partitionNumber = queryHash & (this.numberOfPartitions - 1);
    return this.lruQueryCachePartition[partitionNumber].get(queryCacheKey, key, cacheHelper);
  }

  public void putIfAbsent(Query query, CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    final IndexReader.CacheKey key = cacheHelper.getKey();
    // This saves computing the hash twice for the cache lookup and the routing.
    final int queryHash = query.hashCode();
    QueryCacheKey queryCacheKey = new QueryCacheKey(key, query, queryHash);
    int partitionNumber = queryHash & (this.numberOfPartitions - 1);
    this.lruQueryCachePartition[partitionNumber].putIfAbsent(
        queryCacheKey, query, queryHash, cacheHelper, cached);
  }

  /** Remove all cache entries for the given core cache key. */
  public void clearCoreCacheKey(IndexReader.CacheKey coreKey) {
    this.registeredClosedListeners.remove(coreKey);
    synchronized (keysToClean) {
      keysToClean.add(coreKey);
    }
  }

  /** Remove all cache entries for the given query. */
  public void clearQuery(Query query) {
    for (int i = 0; i < numberOfPartitions; i++) {
      this.lruQueryCachePartition[i].clearQuery(query);
    }
  }

  /** Clear the content of this cache. */
  public void clear() {
    for (int i = 0; i < this.numberOfPartitions; i++) {
      this.lruQueryCachePartition[i].clear();
    }
    onClear();
  }

  // Deep RAM cost of the query object itself. This is accounted once per distinct query in a
  // partition (see LRUQueryCachePartition#uniqueQueries), not once per (segment, query) cache
  // entry, because a query cached across N segments is a single shared Query instance on the heap.
  private static long queryRamBytesUsed(Query query) {
    // Here 32 represents a rough shallow size for a query object
    return RamUsageEstimator.sizeOf(query, 32);
  }

  // pkg-private for testing
  void assertConsistent() {
    long recomputedCacheSize = 0;
    for (int i = 0; i < this.numberOfPartitions; i++) {
      this.lruQueryCachePartition[i].writeLock.lock();
      try {
        if (this.lruQueryCachePartition[i].requiresEviction()) {
          throw new AssertionError(
              "requires evictions: size="
                  + this.lruQueryCachePartition[i].mostRecentlyUsedCacheKeys.size()
                  + ", maxSize="
                  + this.lruQueryCachePartition[i].maxSize
                  + ", ramBytesUsed="
                  + ramBytesUsed()
                  + ", maxRamBytesUsed="
                  + this.lruQueryCachePartition[i].maxRamBytesUsed);
        }
        Set<QueryCacheKey> keys = Collections.newSetFromMap(new IdentityHashMap<>());
        for (QueryCacheKey queryCacheKey : this.lruQueryCachePartition[i].keys()) {
          keys.add(queryCacheKey);
        }
        keys.removeAll(this.lruQueryCachePartition[i].mostRecentlyUsedCacheKeys);
        if (!keys.isEmpty()) {
          throw new AssertionError(
              "The key set entry for the cache does not match with mostRecentlyUsedQueries set for partition: "
                  + i);
        }
        long recomputedRamBytesUsed = 0;
        recomputedRamBytesUsed =
            HASHTABLE_RAM_BYTES_PER_ENTRY * this.lruQueryCachePartition[i].cache.size();
        for (QueryCacheKey queryCacheKey :
            this.lruQueryCachePartition[i].mostRecentlyUsedCacheKeys) {
          recomputedRamBytesUsed += queryCacheKey.ramBytesUsed();
        }
        for (CacheAndCount cached : this.lruQueryCachePartition[i].cache.values()) {
          recomputedRamBytesUsed += cached.ramBytesUsed();
        }
        // Query objects are accounted once per distinct query, not per entry.
        for (LRUQueryCachePartition.QueryRef ref :
            this.lruQueryCachePartition[i].uniqueQueries.values()) {
          recomputedRamBytesUsed += ref.queryBytes;
        }
        if (recomputedRamBytesUsed != this.lruQueryCachePartition[i].ramBytesUsed) {
          throw new AssertionError(
              "ramBytesUsed mismatch : "
                  + this.lruQueryCachePartition[i].ramBytesUsed
                  + " != "
                  + recomputedRamBytesUsed
                  + " for partition: "
                  + i);
        }

        recomputedCacheSize += this.lruQueryCachePartition[i].cache.size();
      } finally {
        this.lruQueryCachePartition[i].writeLock.unlock();
      }
    }

    if (recomputedCacheSize != getCacheSize()) {
      throw new AssertionError(
          "cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
    }
  }

  // package-private for testing only; returns the list of unique
  // cached queries in LRU order.
  List<Query> cachedQueries() {
    Set<Query> dedup = new HashSet<>();
    List<Query> list = new ArrayList<>();
    for (int i = 0; i < this.numberOfPartitions; i++) {
      List<QueryCacheKey> segmentQueryList = lruQueryCachePartition[i].cachedQueries();
      for (QueryCacheKey queryCacheKey : segmentQueryList) {
        if (dedup.add(queryCacheKey.query)) {
          list.add(queryCacheKey.query);
        }
      }
    }
    return list;
  }

  @Override
  public Weight doCache(Weight weight, QueryCachingPolicy policy) {
    while (weight instanceof CachingWrapperWeight cww) {
      weight = cww.in;
    }

    return new CachingWrapperWeight(weight, policy);
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = 0;
    for (int i = 0; i < this.numberOfPartitions; i++) {
      ramBytesUsed += this.lruQueryCachePartition[i].ramBytesUsed;
    }
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> accountables = new ArrayList<>();
    for (int i = 0; i < this.numberOfPartitions; i++) {
      accountables.addAll(lruQueryCachePartition[i].getChildResources());
    }
    return accountables;
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet} for sets that have a density &lt; 1%
   * and a {@link BitDocIdSet} over a {@link FixedBitSet} otherwise.
   */
  protected CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
    if (scorer.cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      return cacheIntoBitSet(scorer, maxDoc);
    } else {
      return cacheIntoRoaringDocIdSet(scorer, maxDoc);
    }
  }

  /**
   * Populates the cache for the given scorer and leaf reader context.
   *
   * <p>Subclasses can override this method to implement custom caching strategies, such as
   * deferring cache population to a background thread or skipping it when other threads are already
   * populating the cache. This can be useful for intra-segment searches.
   *
   * @return the cached result if available; otherwise {@code null}
   */
  protected CacheAndCount tryPopulateCache(
      IndexReader.CacheHelper cacheKey,
      Weight weight,
      ScorerSupplier scorerSupplier,
      LeafReaderContext context)
      throws IOException {
    final CacheAndCount cached = cacheImpl(scorerSupplier.bulkScorer(), context.reader().maxDoc());
    putIfAbsent(weight.getQuery(), cached, cacheKey);
    return cached;
  }

  private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    scorer.score(
        new LeafCollector() {

          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            bitSet.set(doc);
          }

          @Override
          public void collectRange(int min, int max) {
            bitSet.set(min, max);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            stream.intoBitSet(DocIdSetIterator.NO_MORE_DOCS, bitSet, 0);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    int count = bitSet.cardinality();
    return new CacheAndCount(new BitDocIdSet(bitSet, count), count);
  }

  private static CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc)
      throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    scorer.score(
        new LeafCollector() {

          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            builder.add(doc);
          }

          @Override
          public void collectRange(int min, int max) {
            builder.add(min, max);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            stream.forEach(builder::add);
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    RoaringDocIdSet cache = builder.build();
    return new CacheAndCount(cache, cache.cardinality());
  }

  /**
   * Return the total number of times that a {@link Query} has been looked up in this {@link
   * QueryCache}. Note that this number is incremented once per segment so running a cached query
   * only once will increment this counter by the number of segments that are wrapped by the
   * searcher. Note that by definition, {@link #getTotalCount()} is the sum of {@link
   * #getHitCount()} and {@link #getMissCount()}.
   *
   * @see #getHitCount()
   * @see #getMissCount()
   */
  public final long getTotalCount() {
    return getHitCount() + getMissCount();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
   * how many times a cached {@link DocIdSet} has been found and returned.
   *
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount.sum();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
   * how many times this query was not contained in the cache.
   *
   * @see #getTotalCount()
   * @see #getHitCount()
   */
  public final long getMissCount() {
    return missCount.sum();
  }

  /**
   * Return the total number of cache entries (segment-query pairs) currently stored in the cache,
   * across all partitions.
   *
   * @see #getCacheCount()
   * @see #getEvictionCount()
   */
  public final long getCacheSize() {
    long cacheSize = 0;
    for (int i = 0; i < this.numberOfPartitions; i++) {
      cacheSize += this.lruQueryCachePartition[i].cacheSize;
    }
    return cacheSize;
  }

  /**
   * Return the total number of cache entries that have been generated and put in the cache. It is
   * highly desirable to have a {@link #getHitCount() hit count} that is much higher than the {@link
   * #getCacheCount() cache count} as the opposite would indicate that the query cache makes efforts
   * in order to cache queries but then they do not get reused.
   *
   * @see #getCacheSize()
   * @see #getEvictionCount()
   */
  public final long getCacheCount() {
    long cacheCount = 0;
    for (int i = 0; i < this.numberOfPartitions; i++) {
      cacheCount += this.lruQueryCachePartition[i].cacheCount;
    }
    return cacheCount;
  }

  /**
   * Return the number of cache entries that have been removed from the cache either in order to
   * stay under the maximum configured size/ram usage, or because a segment has been closed. High
   * numbers of evictions might mean that queries are not reused or that the {@link
   * QueryCachingPolicy caching policy} caches too aggressively on NRT segments which get merged
   * early.
   *
   * @see #getCacheCount()
   * @see #getCacheSize()
   */
  public final long getEvictionCount() {
    return getCacheCount() - getCacheSize();
  }

  // pkg-private for testing
  record QueryMetadata(Query query, long queryRamBytesUsed) {}

  private class CachingWrapperWeight extends ConstantScoreWeight {

    private final Weight in;
    private final QueryCachingPolicy policy;
    // we use an AtomicBoolean because Weight.scorer may be called from multiple
    // threads when IndexSearcher is created with threads
    private final AtomicBoolean used;

    CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
      super(in.getQuery(), 1f);
      this.in = in;
      this.policy = policy;
      used = new AtomicBoolean(false);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return in.matches(context, doc);
    }

    private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
      // The worst-case (dense) is a bit set which needs one bit per document
      final long worstCaseRamUsage = maxDoc / 8;
      // Imagine the worst-case that a cache entry is large than the size of
      // the cache: not only will this entry be trashed immediately but it
      // will also evict all current entries from the cache. For this reason
      // we only cache on an IndexReader if we have available room for
      // 5 different filters on this reader to avoid excessive trashing
      return worstCaseRamUsage * 5 < (maxRamBytesUsed / numberOfPartitions);
    }

    /** Check whether this segment is eligible for caching, regardless of the query. */
    private boolean shouldCache(LeafReaderContext context) throws IOException {
      return cacheEntryHasReasonableWorstCaseSize(
              ReaderUtil.getTopLevelContext(context).reader().maxDoc())
          && leavesToCache.test(context);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }

      if (in.isCacheable(context) == false) {
        // this segment is not suitable for caching
        return in.scorerSupplier(context);
      }

      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      if (shouldCache(context) == false) {
        return in.scorerSupplier(context);
      }

      final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
      if (cacheHelper == null) {
        // this reader has no cache helper
        return in.scorerSupplier(context);
      }

      CacheAndCount cached;
      cached = get(in.getQuery(), cacheHelper);

      int maxDoc = context.reader().maxDoc();
      if (cached == null) {
        if (policy.shouldCache(in.getQuery())) {
          final ScorerSupplier supplier = in.scorerSupplier(context);
          if (supplier == null) {
            putIfAbsent(in.getQuery(), CacheAndCount.EMPTY, cacheHelper);
            return null;
          }

          final long cost = supplier.cost();
          return new ConstantScoreScorerSupplier(0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc) {
            @Override
            public DocIdSetIterator iterator(long leadCost) throws IOException {
              // skip cache operation which would slow query down too much
              if (cost / skipCacheFactor > leadCost) {
                return supplier.get(leadCost).iterator();
              }
              CacheAndCount cached = tryPopulateCache(cacheHelper, in, supplier, context);
              // cache is not available, use uncached iterator
              if (cached == null) {
                return supplier.get(leadCost).iterator();
              }
              DocIdSetIterator disi = cached.iterator();
              if (disi == null) {
                // docIdSet.iterator() is allowed to return null when empty but we want a non-null
                // iterator here
                disi = DocIdSetIterator.empty();
              }

              return disi;
            }

            @Override
            public long cost() {
              return cost;
            }
          };
        } else {
          return in.scorerSupplier(context);
        }
      }

      assert cached != null;
      if (cached == CacheAndCount.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = cached.iterator();
      if (disi == null) {
        return null;
      }

      return ConstantScoreScorerSupplier.fromIterator(
          disi, 0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc);
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
      // Our cache won't have an accurate count if there are deletions
      if (context.reader().hasDeletions()) {
        return in.count(context);
      }

      // Otherwise check if the count is in the cache
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }

      if (in.isCacheable(context) == false) {
        // this segment is not suitable for caching
        return in.count(context);
      }

      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      if (shouldCache(context) == false) {
        return in.count(context);
      }

      final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
      if (cacheHelper == null) {
        // this reader has no cacheHelper
        return in.count(context);
      }

      CacheAndCount cached;
      cached = get(in.getQuery(), cacheHelper);
      if (cached != null) {
        // cached
        return cached.count();
      }
      // Not cached, check if the wrapped weight can count quickly then use that
      return in.count(context);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return in.isCacheable(ctx);
    }
  }

  /** Partition within a cache. */
  public class LRUQueryCachePartition {
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    public final Set<QueryCacheKey> mostRecentlyUsedCacheKeys;
    private final Map<QueryCacheKey, QueryMetadata> uniqueCacheKeys;
    private int maxSize;
    private final long maxRamBytesUsed;
    protected volatile long ramBytesUsed;
    private volatile long cacheCount;
    private volatile long cacheSize;
    private final Map<QueryCacheKey, LRUQueryCache.CacheAndCount> cache;

    // Distinct queries held by this partition, keyed by value. Because routing is by query alone,
    // every (segment, query) entry for a given query lands in this partition, so this map sees all
    // of them. It canonicalizes queries to a single shared instance and reference-counts how many
    // cache entries use each one, so the query object is accounted exactly once. Any mutation
    // operations happen under the write lock.
    private final Map<Query, QueryRef> uniqueQueries;

    LRUQueryCachePartition(int maxSize, long maxRamBytesUsed) {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      writeLock = lock.writeLock();
      readLock = lock.readLock();
      uniqueCacheKeys = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
      mostRecentlyUsedCacheKeys = uniqueCacheKeys.keySet();
      this.maxSize = maxSize;
      this.maxRamBytesUsed = maxRamBytesUsed;
      cache = new HashMap<>();
      uniqueQueries = new HashMap<>();
      this.ramBytesUsed = 0;
    }

    /**
     * Canonical instance of a query held by this partition, plus a reference count of the cache
     * entries (one per segment) that use it and the RAM cost of the query object.
     */
    private static final class QueryRef {
      final Query canonical;
      final long queryBytes;
      int refCount;

      QueryRef(Query canonical, long queryBytes) {
        this.canonical = canonical;
        this.queryBytes = queryBytes;
      }
    }

    /**
     * Resolve {@code query} to this partition's canonical instance for that value, incrementing its
     * reference count (creating the entry on the first reference). Does not touch {@link
     * #ramBytesUsed}; callers add the query bytes to it on the first reference (when the returned
     * ref's count is 1). Must be called under the write lock. Returns the {@link QueryRef}.
     */
    private QueryRef acquireQuery(Query query) {
      assert writeLock.isHeldByCurrentThread();
      QueryRef ref = uniqueQueries.get(query);
      if (ref == null) {
        ref = new QueryRef(query, queryRamBytesUsed(query));
        uniqueQueries.put(query, ref);
      }
      ref.refCount++;
      return ref;
    }

    /**
     * Drop one reference to {@code query}'s canonical instance. On the last reference (1 -&gt; 0)
     * the query is removed and its query bytes are returned so the caller can subtract them from
     * {@link #ramBytesUsed}; otherwise returns 0. Must be called under the write lock.
     */
    private long releaseQuery(Query query) {
      assert writeLock.isHeldByCurrentThread();
      QueryRef ref = uniqueQueries.get(query);
      if (ref == null) {
        return 0;
      }
      if (--ref.refCount == 0) {
        uniqueQueries.remove(query);
        return ref.queryBytes;
      }
      return 0;
    }

    void setMaxSize(int maxSize) {
      this.maxSize = maxSize;
    }

    // Package private for testing
    Set<QueryCacheKey> keys() {
      readLock.lock();
      try {
        return new HashSet<>(cache.keySet());
      } finally {
        readLock.unlock();
      }
    }

    Map<QueryCacheKey, QueryMetadata> getUniqueCacheKeys() {
      return uniqueCacheKeys;
    }

    protected void onQueryCache(QueryCacheKey queryCacheKey, long ramBytesUsed) {
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onQueryCache(queryCacheKey.query, ramBytesUsed);
    }

    protected void onQueryEviction(QueryCacheKey queryCacheKey, long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      LRUQueryCache.this.onQueryEviction(queryCacheKey.query, ramBytesUsed);
    }

    protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
      cacheSize += 1;
      cacheCount += 1;
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onDocIdSetCache(readerCoreKey, ramBytesUsed);
    }

    protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
      this.ramBytesUsed -= sumRamBytesUsed;
      cacheSize -= numEntries;
      LRUQueryCache.this.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
    }

    LRUQueryCache.CacheAndCount get(
        QueryCacheKey queryCacheKey, Query key, IndexReader.CacheHelper cacheHelper) {
      assert key instanceof BoostQuery == false;
      assert key instanceof ConstantScoreQuery == false;
      readLock.lock();
      try {
        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        final LRUQueryCache.CacheAndCount cached = cache.get(queryCacheKey);
        // this get call moves the query to the most-recently-used position
        final QueryMetadata record = uniqueCacheKeys.get(queryCacheKey);
        if (record == null || record.query == null) {
          onMiss(readerKey, key);
          return null;
        }
        if (cached == null) {
          onMiss(readerKey, record.query);
        } else {
          onHit(readerKey, record.query);
        }
        return cached;
      } finally {
        readLock.unlock();
      }
    }

    void putIfAbsent(
        QueryCacheKey queryCacheKey,
        Query query,
        int queryHash,
        IndexReader.CacheHelper cacheHelper,
        LRUQueryCache.CacheAndCount cached) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;

      // Pre-calculate values outside lock. perEntryBytes is the query-independent per-entry
      // overhead. The query object is accounted separately, once per distinct query.
      long perEntryBytes = queryCacheKey.ramBytesUsed();
      IndexReader.CacheKey cacheKey = cacheHelper.getKey();
      long cachedValueBytesUsed = HASHTABLE_RAM_BYTES_PER_ENTRY + cached.ramBytesUsed();

      writeLock.lock();
      try {
        // Entry already cached: skip canonicalization and the reference count.
        if (cache.containsKey(queryCacheKey)) {
          return;
        }

        // Absent: canonicalize the query to this partition's shared instance and store the entry
        // under it, so every segment's entry for this query references a single Query object. We
        // only acquire a reference on this path, so there is nothing to undo.
        QueryRef ref = acquireQuery(query);
        boolean firstReference = ref.refCount == 1;
        Query canonical = ref.canonical;
        QueryCacheKey key =
            canonical == queryCacheKey.query
                ? queryCacheKey
                : new QueryCacheKey(cacheKey, canonical, queryHash);

        cache.put(key, cached);

        // The query is accounted once per distinct query (on its first reference). The per-entry
        // overhead and the cached doc-id set are accounted per (segment, query) entry.
        long entryBytes = perEntryBytes + cachedValueBytesUsed;
        QueryMetadata metadata = new QueryMetadata(canonical, perEntryBytes);
        uniqueCacheKeys.putIfAbsent(key, metadata);
        if (firstReference) {
          onQueryCache(key, ref.queryBytes);
        }
        onDocIdSetCache(cacheKey, entryBytes);
        evictIfNecessary();
      } finally {
        writeLock.unlock();
      }

      if (registeredClosedListeners.putIfAbsent(cacheKey, Boolean.TRUE) == null) {
        // we just created a new leaf cache, need to register a close listener
        cacheHelper.addClosedListener(LRUQueryCache.this::clearCoreCacheKey);
      }
    }

    protected void onClear() {
      assert writeLock.isHeldByCurrentThread();
      this.ramBytesUsed = 0;
      this.cacheSize = 0;
    }

    void remove(QueryCacheKey queryCacheKey) {
      remove(queryCacheKey, -1);
    }

    /**
     * Remove a cache entry. {@code knownPerEntryBytes} &gt;= 0 means the caller already removed the
     * {@code uniqueCacheKeys} entry (e.g. eviction) and passes its per-entry overhead; otherwise it
     * is removed here. The query's reference count is dropped, and the query bytes are subtracted
     * from {@link #ramBytesUsed} when its last entry goes.
     */
    private void remove(QueryCacheKey queryCacheKey, long knownPerEntryBytes) {
      writeLock.lock();
      try {
        CacheAndCount cacheAndCount = cache.remove(queryCacheKey);
        long perEntryBytes;
        boolean tracked;
        if (knownPerEntryBytes >= 0) {
          perEntryBytes = knownPerEntryBytes;
          tracked = true;
        } else {
          QueryMetadata queryMetadata = uniqueCacheKeys.remove(queryCacheKey);
          tracked = queryMetadata != null && queryMetadata.query != null;
          perEntryBytes = tracked ? queryMetadata.queryRamBytesUsed : 0;
        }

        // Per (segment, query) entry: the cached doc-id set plus this entry's overhead.
        long entryBytes = 0;
        if (cacheAndCount != null) {
          entryBytes = perEntryBytes + HASHTABLE_RAM_BYTES_PER_ENTRY + cacheAndCount.ramBytesUsed();
          onDocIdSetEviction(queryCacheKey.cacheKey, 1, entryBytes);
        }

        // Once per distinct query: reclaim the query bytes when its last entry goes.
        long releasedQueryBytes = tracked ? releaseQuery(queryCacheKey.query) : 0;
        if (releasedQueryBytes > 0) {
          onQueryEviction(queryCacheKey, releasedQueryBytes);
        }
      } finally {
        writeLock.unlock();
      }
    }

    public void clear() {
      writeLock.lock();
      try {
        cache.clear();
        // Note that this also clears the uniqueCacheKeys map since mostRecentlyUsedCacheKeys is the
        // uniqueCacheKeys.keySet view:
        mostRecentlyUsedCacheKeys.clear();
        uniqueQueries.clear();
        onClear();
      } finally {
        writeLock.unlock();
      }
    }

    Collection<Accountable> getChildResources() {
      writeLock.lock();
      try {
        return Accountables.namedAccountables("segment", cache);
      } finally {
        writeLock.unlock();
      }
    }

    boolean requiresEviction() {
      final int size = mostRecentlyUsedCacheKeys.size();
      if (size == 0) {
        return false;
      } else {
        return size > maxSize || this.ramBytesUsed > maxRamBytesUsed;
      }
    }

    private void evictIfNecessary() {
      assert writeLock.isHeldByCurrentThread();
      // under a lock to make sure that mostRecentlyUsedCacheKeys and cache keep sync'ed
      if (requiresEviction()) {
        Iterator<Map.Entry<QueryCacheKey, QueryMetadata>> iterator =
            uniqueCacheKeys.entrySet().iterator();
        do {
          final Map.Entry<QueryCacheKey, QueryMetadata> entry = iterator.next();
          final int size = uniqueCacheKeys.size();
          iterator.remove();
          if (size == uniqueCacheKeys.size()) {
            // size did not decrease, because the hash of the query changed since it has been
            // put into the cache
            throw new ConcurrentModificationException(
                "Removal from the cache failed! This "
                    + "is probably due to a query which has been modified after having been put into "
                    + " the cache or a badly implemented clone(). Query class: ["
                    + entry.getKey().getClass()
                    + "], query: ["
                    + entry.getKey()
                    + "]");
          }
          remove(entry.getKey(), entry.getValue().queryRamBytesUsed);
        } while (iterator.hasNext() && requiresEviction());
      }
    }

    public void clearQuery(Query query) {
      writeLock.lock();
      try {
        queriesToClean.add(query);
      } finally {
        writeLock.unlock();
      }
    }

    List<QueryCacheKey> cachedQueries() {
      readLock.lock();
      try {
        return new ArrayList<>(mostRecentlyUsedCacheKeys);
      } finally {
        readLock.unlock();
      }
    }
  }

  public int getPartitionNumber(QueryCacheKey queryCacheKey) {
    return getPartitionNumber(queryCacheKey.query);
  }

  // Route to a partition by the query alone (not the segment), so that every (segment, query) entry
  // for a given query lands in the same partition. This lets each partition maintain a complete,
  // cache-wide view of the queries it holds (see uniqueQueries), which is what makes per-partition
  // query canonicalization and count-once RAM accounting correct.
  private int getPartitionNumber(Query query) {
    return query.hashCode() & (this.numberOfPartitions - 1);
  }

  /** Cache of doc ids with a count. */
  public static class CacheAndCount implements Accountable {
    protected static final CacheAndCount EMPTY = new CacheAndCount(DocIdSet.EMPTY, 0);

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(CacheAndCount.class);
    private final DocIdSet cache;
    private final int count;

    public CacheAndCount(DocIdSet cache, int count) {
      this.cache = cache;
      this.count = count;
    }

    public DocIdSetIterator iterator() throws IOException {
      return cache.iterator();
    }

    public int count() {
      return count;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
    }
  }

  static class QueryCacheKey implements Accountable {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(QueryCacheKey.class);

    IndexReader.CacheKey cacheKey;
    Query query;
    private final int hash;

    QueryCacheKey(IndexReader.CacheKey cacheKey, Query query) {
      this(cacheKey, query, query.hashCode());
    }

    // Variant that reuses an already-computed query.hashCode() to avoid recomputing it (query
    // hashing, e.g. murmur over term bytes, is a hot cost on get/putIfAbsent).
    QueryCacheKey(IndexReader.CacheKey cacheKey, Query query, int queryHash) {
      this.cacheKey = cacheKey;
      this.query = query;
      this.hash = 31 * System.identityHashCode(cacheKey) + queryHash;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueryCacheKey queryCacheKey = (QueryCacheKey) o;
      if (!Objects.equals(cacheKey, queryCacheKey.cacheKey)) return false;
      if (!Objects.equals(query, queryCacheKey.query)) return false;
      return true;
    }

    @Override
    public long ramBytesUsed() {
      // Per-entry overhead only: the QueryCacheKey object plus its slot in the uniqueCacheKeys
      // LinkedHashMap. The query object is accounted separately, once per distinct query.
      return BASE_RAM_BYTES_USED + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
    }
  }

  /**
   * Configuration for automatic background cache cleanup. When provided to the {@link
   * LRUQueryCache} constructor, a background thread will periodically invoke {@link #cleanUp()} to
   * remove stale entries from closed segments or cleared queries.
   *
   * <p>The {@code scheduleDelayMs} parameter controls the fixed delay in milliseconds between
   * successive cleanup runs. The {@code scheduledThreadPoolExecutor} is the executor used to
   * schedule cleanup tasks. If the executor is {@code null}, no background cleanup is performed and
   * the caller must invoke {@link #cleanUp()} manually.
   */
  public static class CacheCleanUpParameters {
    private long scheduleDelayMs = 60 * 1000; // 1 minute
    private final Optional<ScheduledThreadPoolExecutor> scheduledThreadPoolExecutor;

    public CacheCleanUpParameters(
        long scheduleDelayMs, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
      this.scheduledThreadPoolExecutor = Optional.ofNullable(scheduledThreadPoolExecutor);
      this.scheduleDelayMs = scheduleDelayMs;
    }

    public long getScheduleDelayMs() {
      return scheduleDelayMs;
    }

    public Optional<ScheduledThreadPoolExecutor> getScheduledThreadPoolExecutor() {
      return scheduledThreadPoolExecutor;
    }
  }

  /**
   * Removes stale cache entries that have been marked for cleanup, such as entries belonging to
   * closed segments or explicitly cleared queries. This method is safe to call concurrently.
   *
   * <p>If a {@link CacheCleanUpParameters} with a {@link
   * java.util.concurrent.ScheduledThreadPoolExecutor} was provided at construction time, this
   * method is invoked automatically at the configured interval and does not need to be called
   * manually. Otherwise, the caller should invoke this method periodically to prevent stale entries
   * from accumulating and consuming memory.
   */
  public synchronized void cleanUp() {
    Set<IndexReader.CacheKey> keysToCleanCopy = Set.copyOf(keysToClean);
    Set<Query> queriesToCleanCopy = Set.copyOf(queriesToClean);

    // Break early if there are no stale keys to be cleaned up.
    if (keysToCleanCopy.isEmpty() && queriesToCleanCopy.isEmpty()) {
      return;
    }

    keysToClean.removeAll(keysToCleanCopy);
    queriesToClean.removeAll(queriesToCleanCopy);

    for (QueryCacheKey queryCacheKey : keys()) {
      boolean shouldEvict =
          (queryCacheKey.cacheKey != null && keysToCleanCopy.contains(queryCacheKey.cacheKey))
              || (queryCacheKey.query != null && queriesToCleanCopy.contains(queryCacheKey.query));
      if (shouldEvict) {
        int partitionNumber = getPartitionNumber(queryCacheKey);
        lruQueryCachePartition[partitionNumber].remove(queryCacheKey);
      }
    }
  }
}
