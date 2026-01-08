package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Group)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 5,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class LRUQueryCacheBenchmark {
  private static final int MAX_SIZE = 50000;
  private static final int MAX_SIZE_IN_BYTES = 10048576; // 10MB

  private static final int SEGMENTS = 50;

  private Query[] queries;
  private IndexReader.CacheHelper[] cacheHelpers;

  private LRUQueryCache.CacheAndCount sampleCacheAndCount;
  private LRUQueryCache queryCache;
  private int[] zipfKeys;
  private int[] zipfKeysForInvalidation;
  private AtomicBoolean[] invalidated;
  private final AtomicLong lastInvalidateNs = new AtomicLong(0);
  private static final long INVALIDATE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(2000);

  @Setup
  public void setup() {
    ZipfDistribution zipf = new ZipfDistribution(MAX_SIZE, 1.2);
    zipfKeys = new int[100_000];
    for (int i = 0; i < zipfKeys.length; i++) {
      zipfKeys[i] = zipf.sample() - 1;
    }
    ZipfDistribution zipfForInvalidation = new ZipfDistribution(MAX_SIZE / 100, 1.2);
    zipfKeysForInvalidation = new int[100_000];
    for (int i = 0; i < zipfKeys.length; i++) {
      zipfKeysForInvalidation[i] = zipfForInvalidation.sample() - 1;
    }
    invalidated = new AtomicBoolean[MAX_SIZE];
    for (int i = 0; i < MAX_SIZE; i++) {
      invalidated[i] = new AtomicBoolean(false);
    }
    // Run the background cache clean thread every 50ms for this test
    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
        new ScheduledThreadPoolExecutor(1, new DefaultCleanUpThreadFactory());
    //    LRUQueryCache.CacheCleanUpParameters cacheCleanUpParameters =
    //        new LRUQueryCache.CacheCleanUpParameters(100, scheduledThreadPoolExecutor);
    queryCache =
        new LRUQueryCache(
            MAX_SIZE * SEGMENTS * 16, MAX_SIZE_IN_BYTES * 16, leafReaderContext -> true, 100000);

    this.queries = new Query[MAX_SIZE];
    for (int i = 0; i < MAX_SIZE; i++) {
      queries[i] = new TermQuery(new Term("field" + i, UUID.randomUUID().toString()));
    }
    this.cacheHelpers = new IndexReader.CacheHelper[SEGMENTS];
    for (int i = 0; i < SEGMENTS; i++) {
      this.cacheHelpers[i] =
          new IndexReader.CacheHelper() {
            private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();

            @Override
            public IndexReader.CacheKey getKey() {
              return cacheKey;
            }

            @Override
            public void addClosedListener(IndexReader.ClosedListener listener) {}
          };
    }

    FixedBitSet bitSet = new FixedBitSet(10000);
    int cost = 0;
    int start = 10;
    final int interval = 12; // to keep it simple
    for (int i = 10; i < bitSet.length(); i += interval) {
      cost++;
      bitSet.set(i);
    }

    this.sampleCacheAndCount =
        new LRUQueryCache.CacheAndCount(
            new BitDocIdSet(bitSet, cost), cost); // Using same value for all cache entries.
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws IOException {
    assert queryCache.getCacheSize() >= 0;
    assert queryCache.getCacheCount() > 0;
    assert queryCache.ramBytesUsed() >= 0;
    queryCache.clear();
    assert queryCache.getCacheSize() == 0;
    assert queryCache.ramBytesUsed() == 0;
  }


  @Benchmark
  @Group("concurrent_putIfAbsent")
  @GroupThreads(8)
  public void testConcurrentPutIfAbsent() {
    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
    queryCache.putIfAbsent(
        queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
  }

  @Benchmark
  @Group("concurrentGetAndPuts_uniform")
  @GroupThreads(6)
  public LRUQueryCache.CacheAndCount concurrentGetAndPuts_get() {
    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
    return queryCache.get(queries[random], cacheHelpers[random & (SEGMENTS - 1)]);
  }

  @Benchmark
  @Group("concurrentGetAndPuts_uniform")
  @GroupThreads(4)
  public void concurrentGetAndPuts_putIfAbsent() {
    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
    queryCache.putIfAbsent(
        queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
  }

  @Benchmark
  @Group("concurrentGetAndPuts_zipf")
  @GroupThreads(6)
  public LRUQueryCache.CacheAndCount concurrentGetAndPuts_Zipf_get() {
    int key = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    return queryCache.get(queries[key], cacheHelpers[key & (SEGMENTS - 1)]);
  }

  @Benchmark
  @Group("concurrentGetAndPuts_zipf")
  @GroupThreads(4)
  public void concurrentGetAndPuts_Zipf_putIfAbsent() {
    int key = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    queryCache.putIfAbsent(
        queries[key], this.sampleCacheAndCount, cacheHelpers[key & (SEGMENTS - 1)]);
  }

  @Benchmark
  @Group("concurrentGetAndPutsWithInvalidate_zipf")
  @GroupThreads(6)
  public LRUQueryCache.CacheAndCount concurrentGetAndPutsWithInvalidate_get() {
    int k = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    if (!invalidated[k].get()) {
      return queryCache.get(queries[k], cacheHelpers[k & (SEGMENTS - 1)]);
    } else {
      return null;
    }
  }

  @Benchmark
  @Group("concurrentGetAndPutsWithInvalidate_zipf")
  @GroupThreads(3)
  public void concurrentGetAndPutsWithInvalidate_putIfAbsent() {
    int k = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    if (!invalidated[k].get()) {
      queryCache.putIfAbsent(queries[k], sampleCacheAndCount, cacheHelpers[k & (SEGMENTS - 1)]);
    }
  }

  @Benchmark
  @Group("concurrentGetAndPutsWithInvalidate_zipf")
  @GroupThreads(1)
  public void concurrentGetAndPutsWithInvalidate_invalidate() {
    long now = System.nanoTime();
    long last = lastInvalidateNs.get();

    if (now - last < INVALIDATE_INTERVAL_NS) {
      return; // skip
    }

    if (!lastInvalidateNs.compareAndSet(last, now)) {
      return;
    }
    int k =
        zipfKeysForInvalidation[
            ThreadLocalRandom.current().nextInt(zipfKeysForInvalidation.length)];
    if (!invalidated[k].compareAndSet(false, true)) {
      return; // Skip as already invalidated
    }
    queryCache.clearCoreCacheKey(cacheHelpers[k & (SEGMENTS - 1)].getKey());
    queryCache.clearQuery(queries[k]);
  }

  public static class DefaultCleanUpThreadFactory implements ThreadFactory {
    private final String namePrefix;

    public DefaultCleanUpThreadFactory() {
      this.namePrefix = "query-cache-cleanup-" + UUID.randomUUID();
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r, namePrefix);
      thread.setDaemon(true);
      thread.setPriority(Thread.NORM_PRIORITY);
      thread.setUncaughtExceptionHandler(
          (t, e) -> {
            System.err.println(
                "Uncaught exception in thread " + t.getName() + ": " + e.getMessage());
            e.printStackTrace();
          });
      return thread;
    }
  }
}
