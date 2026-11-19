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
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestUsageTrackingFilterCachingPolicy extends LuceneTestCase {

  public void testCostlyFilter() {
    assertTrue(
        UsageTrackingQueryCachingPolicy.isCostly(new PrefixQuery(new Term("field", "prefix"))));
    assertTrue(
        UsageTrackingQueryCachingPolicy.isCostly(IntPoint.newRangeQuery("intField", 1, 1000)));
    assertFalse(
        UsageTrackingQueryCachingPolicy.isCostly(new TermQuery(new Term("field", "value"))));
  }

  public void testNeverCacheMatchAll() throws Exception {
    Query q = MatchAllDocsQuery.INSTANCE;
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    for (int i = 0; i < 1000; ++i) {
      policy.onUse(q);
    }
    assertFalse(policy.shouldCache(q));
  }

  public void testNeverCacheTermFilter() throws IOException {
    Query q = new TermQuery(new Term("foo", "bar"));
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    for (int i = 0; i < 1000; ++i) {
      policy.onUse(q);
    }
    assertFalse(policy.shouldCache(q));
  }

  public void testNeverCacheDocValuesFieldExistsFilter() throws IOException {
    Query q = new FieldExistsQuery("foo");
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    for (int i = 0; i < 1000; ++i) {
      policy.onUse(q);
    }
    assertFalse(policy.shouldCache(q));
  }

  public void testBooleanQueries() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader reader = w.getReader();
    w.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    LRUQueryCache cache =
        new LRUQueryCache(10, Long.MAX_VALUE, _ -> true, Float.POSITIVE_INFINITY, 1, null);
    searcher.setQueryCache(cache);
    searcher.setQueryCachingPolicy(policy);

    DummyQuery q1 = new DummyQuery(1);
    DummyQuery q2 = new DummyQuery(2);
    BooleanQuery bq =
        new BooleanQuery.Builder().add(q1, Occur.SHOULD).add(q2, Occur.SHOULD).build();

    for (int i = 0; i < 3; ++i) {
      searcher.count(bq);
    }
    assertEquals(0, cache.getCacheSize()); // nothing cached yet, too early

    searcher.count(bq);
    assertEquals(1, cache.getCacheSize()); // the bq got cached, but not q1 and q2

    for (int i = 0; i < 10; ++i) {
      searcher.count(bq);
    }
    assertEquals(
        1, cache.getCacheSize()); // q1 and q2 still not cached since we do not pull scorers on them

    searcher.count(q1);
    assertEquals(2, cache.getCacheSize()); // q1 used on its own -> cached

    reader.close();
    dir.close();
  }

  public void testInvalidHistorySize() {
    expectThrows(IllegalArgumentException.class, () -> new UsageTrackingQueryCachingPolicy(1));
    expectThrows(IllegalArgumentException.class, () -> new UsageTrackingQueryCachingPolicy(0));
  }

  public void testConsecutiveUsesAreCounted() throws IOException {
    // Whatever the history size, a query that is used minFrequencyToCache times in a row must be
    // cached. Small histories must not end up with stripes too small to ever reach the threshold.
    for (int historySize :
        new int[] {5, 8, 31, 32, 63, 64, 100, 256, 1000, TestUtil.nextInt(random(), 5, 5000)}) {
      UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy(historySize);
      DummyQuery q = new DummyQuery(random().nextInt());
      int minFrequency = policy.minFrequencyToCache(q);
      for (int i = 0; i < minFrequency; ++i) {
        assertFalse("historySize=" + historySize, policy.shouldCache(q));
        assertEquals(i, policy.frequency(q));
        policy.onUse(q);
      }
      assertTrue("historySize=" + historySize, policy.shouldCache(q));
    }
  }

  public void testHistoryEvictsOldEntries() {
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy(256);
    DummyQuery q = new DummyQuery(42);
    for (int i = 0; i < 10; ++i) {
      policy.onUse(q);
    }
    assertEquals(10, policy.frequency(q));
    // flooding with other queries eventually pushes q out of its stripe
    for (int i = 0; i < 100_000; ++i) {
      policy.onUse(new DummyQuery(1000 + i));
    }
    assertEquals(0, policy.frequency(q));
  }

  public void testFrequencyNeverExceedsUses() {
    UsageTrackingQueryCachingPolicy policy =
        new UsageTrackingQueryCachingPolicy(TestUtil.nextInt(random(), 2, 2000));
    int numQueries = TestUtil.nextInt(random(), 1, 100);
    int[] uses = new int[numQueries];
    for (int i = 0; i < 10_000; ++i) {
      int id = random().nextInt(numQueries);
      policy.onUse(new DummyQuery(id));
      uses[id]++;
      assertTrue(policy.frequency(new DummyQuery(id)) <= uses[id]);
    }
  }

  public void testConcurrentUse() throws Exception {
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    int numThreads = TestUtil.nextInt(random(), 2, 8);
    Thread[] threads = new Thread[numThreads];
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < numThreads; ++t) {
      final int seed = random().nextInt();
      threads[t] =
          new Thread(
              () -> {
                java.util.Random r = new java.util.Random(seed);
                try {
                  start.await();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                for (int i = 0; i < 10_000; ++i) {
                  DummyQuery q = new DummyQuery(r.nextInt(64));
                  policy.onUse(q);
                  int freq = policy.frequency(q);
                  assertTrue(freq >= 0 && freq <= 256);
                }
              });
      threads[t].start();
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private static class DummyQuery extends Query {

    private final int id;

    DummyQuery(int id) {
      this.id = id;
    }

    @Override
    public String toString(String field) {
      return "dummy";
    }

    @Override
    public boolean equals(Object obj) {
      return sameClassAs(obj) && ((DummyQuery) obj).id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(DummyQuery.this, boost) {
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          final var scorer = new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(1));
          return new DefaultScorerSupplier(scorer);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {}
  }
}
