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
 * <p>Note that query eviction runs in linear time with the total number of segments that have cache
 * entries so this cache works best with {@link QueryCachingPolicy caching policies} that only cache
 * on "large" segments, and it is advised to not share this cache across too many indices.
 *
 * <p>A default query cache and policy instance is used in IndexSearcher. If you want to replace
 * those defaults it is typically done like this:
 *
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count}, {@link
 * #getMissCount() miss count}, {@link #getCacheSize() number of cache entries}, {@link
 * #getCacheCount() total number of DocIdSets that have ever been cached}, {@link
 * #getEvictionCount() number of evicted entries}). In case you would like to have more fine-grained
 * statistics, such as per-index or per-query-class statistics, it is possible to override various
 * callbacks: {@link #onHit}, {@link #onMiss}, {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}. It is better to not
 * perform heavy computations in these methods though since they are called synchronously and under
 * a lock.
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

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code> queries with at most
   * <code>maxRamBytesUsed</code> bytes of memory, only on leaves that satisfy {@code
   * leavesToCache}.
   *
   * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
   * top-level query will not be cached in order to not slow down queries too much.
   */
  public LRUQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor) {
    this(maxSize, maxRamBytesUsed, leavesToCache, skipCacheFactor, 16, null);
  }

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code> queries with at most
   * <code>maxRamBytesUsed</code> bytes of memory, only on leaves that satisfy {@code
   * leavesToCache}.
   *
   * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
   * top-level query will not be cached in order to not slow down queries too much.
   */
  public LRUQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor,
      int numberOfPartitions,
      CacheCleanUpParameters cacheCleanUpParameters) {
    this.maxRamBytesUsed = maxRamBytesUsed;
    this.leavesToCache = leavesToCache;
    if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
      throw new IllegalArgumentException(
          "skipCacheFactor must be no less than 1, get " + skipCacheFactor);
    }
    this.skipCacheFactor = skipCacheFactor;

    // Note that reads on this LinkedHashMap trigger modifications on the linked list under the
    // hood, so reading from multiple threads is not thread-safe. This is why it is wrapped in a
    // Collections#synchronizedMap.
    hitCount = new LongAdder();
    missCount = new LongAdder();
    this.numberOfPartitions = numberOfPartitions;
    this.lruQueryCachePartition = new LRUQueryCachePartition[numberOfPartitions];
    int maxSizePerSegment = maxSize / this.numberOfPartitions;
    long maxSizeInBytesPerSegment = (maxRamBytesUsed / this.numberOfPartitions);
    for (int i = 0; i < numberOfPartitions; i++) {
      lruQueryCachePartition[i] =
          new LRUQueryCachePartition(maxSizePerSegment, maxSizeInBytesPerSegment);
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
   * Create a new instance that will cache at most <code>maxSize</code> queries with at most <code>
   * maxRamBytesUsed</code> bytes of memory. Queries will only be cached on leaves that have more
   * than 10k documents and have more than half of the average documents per leave of the index.
   * This should guarantee that all leaves from the upper {@link TieredMergePolicy tier} will be
   * cached. Only clauses whose cost is at most 100x the cost of the top-level query will be cached
   * in order to not hurt latency too much because of caching.
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
   * Expert: callback when a query is added to this cache. Implementing this method is typically
   * useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onQueryEviction
   * @lucene.experimental
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {}

  /**
   * Expert: callback when a query is evicted from this cache.
   *
   * @see #onQueryCache
   * @lucene.experimental
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {}

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache. Implementing this method is
   * typically useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onDocIdSetEviction
   * @lucene.experimental
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {}

  /**
   * Expert: callback when one or more {@link DocIdSet}s are removed from this cache.
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
    QueryCacheKey queryCacheKey = new QueryCacheKey(readerKey, key);
    int partitionNumber = getPartitionNumber(queryCacheKey);
    return this.lruQueryCachePartition[partitionNumber].get(queryCacheKey, key, cacheHelper);
  }

  public void putIfAbsent(Query query, CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    final IndexReader.CacheKey key = cacheHelper.getKey();
    QueryCacheKey queryCacheKey = new QueryCacheKey(key, query);
    int partitionNumber = getPartitionNumber(queryCacheKey);
    this.lruQueryCachePartition[partitionNumber].putIfAbsent(
        queryCacheKey, query, cacheHelper, cached);
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

  private static long getRamBytesUsed(Query query) {
    // Here 32 represents a rough shallow size for a query object
    long queryRamBytesUsed = RamUsageEstimator.sizeOf(query, 32);
    return LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + queryRamBytesUsed;
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

  // pkg-private for testing
  // return the list of cached queries in LRU order
  List<Query> cachedQueries() {
    List<Query> list = new ArrayList<Query>();
    for (int i = 0; i < this.numberOfPartitions; i++) {
      List<QueryCacheKey> segmentQueryList = lruQueryCachePartition[i].cachedQueries();
      for (QueryCacheKey queryCacheKey : segmentQueryList) {
        list.add(queryCacheKey.query);
      }
    }
    return list;
  }

  @Override
  public Weight doCache(Weight weight, QueryCachingPolicy policy) {
    while (weight instanceof CachingWrapperWeight) {
      weight = ((CachingWrapperWeight) weight).in;
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

  private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    int[] count = new int[1];
    scorer.score(
        new LeafCollector() {

          private int[] buffer;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            count[0]++;
            bitSet.set(doc);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              for (int i = 0; i < c; ++i) {
                bitSet.set(buffer[i]);
              }
              count[0] += c;
            }
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    return new CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
  }

  private static CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc)
      throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    scorer.score(
        new LeafCollector() {

          private int[] buffer = null;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            builder.add(doc);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              for (int i = 0; i < c; ++i) {
                builder.add(buffer[i]);
              }
            }
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
   * Return the total number of {@link DocIdSet}s which are currently stored in the cache.
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

              CacheAndCount cached = cacheImpl(supplier.bulkScorer(), maxDoc);
              putIfAbsent(in.getQuery(), cached, cacheHelper);
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

    LRUQueryCachePartition(int maxSize, long maxRamBytesUsed) {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      writeLock = lock.writeLock();
      readLock = lock.readLock();
      uniqueCacheKeys = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
      mostRecentlyUsedCacheKeys = uniqueCacheKeys.keySet();
      this.maxSize = maxSize;
      this.maxRamBytesUsed = maxRamBytesUsed;
      cache = new HashMap<>();
      this.ramBytesUsed = 0;
    }

    void setMaxSize(int maxSize) {
      this.maxSize = maxSize;
    }

    Iterable<QueryCacheKey> keys() {
      return cache.keySet();
    }

    Map<QueryCacheKey, QueryMetadata> getUniqueCacheKeys() {
      return uniqueCacheKeys;
    }

    protected void onQueryCache(QueryCacheKey queryCacheKey, long ramBytesUsed) {
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onQueryCache(queryCacheKey.query, ramBytesUsed);
    }

    /**
     * Expert: callback when a query is evicted from this cache.
     *
     * @see #onQueryCache
     * @lucene.experimental
     */
    protected void onQueryEviction(QueryCacheKey queryCacheKey, long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      LRUQueryCache.this.onQueryEviction(queryCacheKey.query, ramBytesUsed);
    }

    /**
     * Expert: callback when a {@link DocIdSet} is added to this cache. Implementing this method is
     * typically useful in order to compute more fine-grained statistics about the query cache.
     *
     * @see #onDocIdSetEviction
     * @lucene.experimental
     */
    protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
      cacheSize += 1;
      cacheCount += 1;
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onDocIdSetCache(readerCoreKey, ramBytesUsed);
    }

    /**
     * Expert: callback when one or more {@link DocIdSet}s are removed from this cache.
     *
     * @see #onDocIdSetCache
     * @lucene.experimental
     */
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
        IndexReader.CacheHelper cacheHelper,
        LRUQueryCache.CacheAndCount cached) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;

      // Pre-calculate values outside lock
      long ramBytes = queryCacheKey.ramBytesUsed();
      IndexReader.CacheKey cacheKey = cacheHelper.getKey();
      long cachedValueBytesUsed = HASHTABLE_RAM_BYTES_PER_ENTRY + cached.ramBytesUsed();

      writeLock.lock();
      try {
        // Use putIfAbsent return value to determine if insertion actually happened
        LRUQueryCache.CacheAndCount existing = cache.putIfAbsent(queryCacheKey, cached);
        if (existing != null) {
          // Entry already exists, no work needed
          return;
        }

        // Only execute side effects when insertion actually occurred
        QueryMetadata metadata = new QueryMetadata(queryCacheKey.query, ramBytes);
        uniqueCacheKeys.putIfAbsent(queryCacheKey, metadata);
        onQueryCache(queryCacheKey, ramBytes);
        onDocIdSetCache(cacheKey, cachedValueBytesUsed);
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
      writeLock.lock();
      try {
        CacheAndCount cacheAndCount = cache.remove(queryCacheKey);
        if (cacheAndCount != null) {
          onDocIdSetEviction(
              queryCacheKey.cacheKey,
              1,
              HASHTABLE_RAM_BYTES_PER_ENTRY + cacheAndCount.ramBytesUsed());
        }
        QueryMetadata queryMetadata = uniqueCacheKeys.remove(queryCacheKey);
        if (queryMetadata != null && queryMetadata.query != null) {
          onQueryEviction(queryCacheKey, queryMetadata.queryRamBytesUsed);
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
          onQueryEviction(entry.getKey(), entry.getValue().queryRamBytesUsed);
          remove(entry.getKey());
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
    return queryCacheKey.hashCode() & (this.numberOfPartitions - 1);
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

    QueryCacheKey(IndexReader.CacheKey cacheKey, Query query) {
      this.cacheKey = cacheKey;
      this.query = query;
    }

    @Override
    public int hashCode() {
      int res = System.identityHashCode(cacheKey);
      res = 31 * res + System.identityHashCode(query);
      return res;
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
      return BASE_RAM_BYTES_USED + getRamBytesUsed(query);
    }
  }

  public static class CacheCleanUpParameters {
    private long scheduleDelayMs = 60 * 1000; // 1 minute
    private Optional<ScheduledThreadPoolExecutor> scheduledThreadPoolExecutor;

    CacheCleanUpParameters() {}

    public CacheCleanUpParameters(
        long scheduleDelayMs, ScheduledThreadPoolExecutor ScheduledThreadPoolExecutor) {
      this.scheduledThreadPoolExecutor = Optional.ofNullable(ScheduledThreadPoolExecutor);
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
   * Performs cleanup of cache stale entries. This can be either be done manually or via background
   * scheduler passed to this cache.
   */
  public synchronized void cleanUp() {
    Set<IndexReader.CacheKey> keysToCleanCopy = Set.copyOf(keysToClean);
    Set<Query> queriesToCleanCopy = Set.copyOf(queriesToClean);

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
