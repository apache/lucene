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

import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Tests how concurrent slices react to the failure of one of them. */
public class TestIndexSearcherSliceFailure extends LuceneTestCase {
  private static final int NUM_SEGMENTS = 4;
  private static final int DOCS_PER_SEGMENT = 10 * TimeLimitingBulkScorer.INTERVAL;

  private static Directory dir;
  private static IndexReader reader;
  private static ExecutorService executor;

  @BeforeClass
  public static void beforeClass() throws IOException {
    dir = newDirectory();
    try (IndexWriter w =
        new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      for (int s = 0; s < NUM_SEGMENTS; s++) {
        for (int d = 0; d < DOCS_PER_SEGMENT; d++) {
          w.addDocument(new Document());
        }
        w.flush();
      }
    }
    reader = DirectoryReader.open(dir);
    assertEquals(NUM_SEGMENTS, reader.leaves().size());
    executor =
        Executors.newFixedThreadPool(
            2 * NUM_SEGMENTS,
            new NamedThreadFactory(TestIndexSearcherSliceFailure.class.getName()));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    reader.close();
    dir.close();
    executor = null;
    reader = null;
    dir = null;
  }

  /**
   * Returns a searcher with one slice per segment, or a single slice if {@code concurrent=false}.
   */
  private static IndexSearcher newSearcher(boolean concurrent) {
    IndexSearcher searcher =
        new IndexSearcher(reader, executor) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            return concurrent
                ? slices(leaves, 1, 1, false)
                : slices(leaves, Integer.MAX_VALUE, Integer.MAX_VALUE, false);
          }
        };
    searcher.setQueryCache(null);
    assertEquals(concurrent ? NUM_SEGMENTS : 1, searcher.getSlices().length);
    return searcher;
  }

  public void testBulkScorerIsNotWrappedWithoutTimeoutOrSiblings() throws IOException {
    assertBulkScorerCalls(newSearcher(false), false);
  }

  public void testBulkScorerIsWrappedWithTimeout() throws IOException {
    IndexSearcher searcher = newSearcher(false);
    searcher.setTimeout(() -> false);
    assertBulkScorerCalls(searcher, true);
  }

  public void testBulkScorerIsWrappedWithSiblings() throws IOException {
    assertBulkScorerCalls(newSearcher(true), true);
  }

  private static void assertBulkScorerCalls(IndexSearcher searcher, boolean wrapped)
      throws IOException {
    AtomicIntegerArray scoreCalls = new AtomicIntegerArray(NUM_SEGMENTS);
    Query query =
        new HookedQuery() {
          @Override
          BulkScorer bulkScorer(LeafReaderContext context, BulkScorer in) {
            return new BulkScorer() {
              @Override
              public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
                  throws IOException {
                scoreCalls.incrementAndGet(context.ord);
                return in.score(collector, acceptDocs, min, max);
              }

              @Override
              public long cost() {
                return in.cost();
              }
            };
          }
        };
    assertEquals(reader.maxDoc(), (long) searcher.search(query, new HookedCollectorManager()));
    assertFalse(searcher.timedOut());
    for (int ord = 0; ord < NUM_SEGMENTS; ord++) {
      if (wrapped) {
        // scored in chunks so that the exit condition is checked in-between
        assertTrue(scoreCalls.get(ord) > 1);
      } else {
        assertEquals(1, scoreCalls.get(ord));
      }
    }
  }

  public void testTimeoutWithSiblingsReturnsPartialResults() throws IOException {
    IndexSearcher searcher = newSearcher(true);
    searcher.setTimeout(() -> true);
    assertEquals(
        0L, (long) searcher.search(MatchAllDocsQuery.INSTANCE, new HookedCollectorManager()));
    assertTrue(searcher.timedOut());
  }

  public void testRunningSlicesStopWhenSiblingFailsInCollect() {
    doTestRunningSlicesStopWhenSiblingFails(false, null);
  }

  public void testRunningSlicesStopWhenSiblingFailsInCollectWithTimeout() {
    doTestRunningSlicesStopWhenSiblingFails(false, () -> false);
  }

  public void testRunningSlicesStopWhenSiblingFailsInGetLeafCollector() {
    doTestRunningSlicesStopWhenSiblingFails(true, null);
  }

  public void testRunningSlicesStopWhenSiblingFailsInGetLeafCollectorWithTimeout() {
    doTestRunningSlicesStopWhenSiblingFails(true, () -> false);
  }

  private void doTestRunningSlicesStopWhenSiblingFails(
      boolean failInGetLeafCollector, QueryTimeout timeout) {
    IndexSearcher searcher = newSearcher(true);
    searcher.setTimeout(timeout);
    CountDownLatch siblingsCollecting = new CountDownLatch(NUM_SEGMENTS - 1);
    HookedCollectorManager manager =
        new HookedCollectorManager() {
          @Override
          void onLeaf(LeafReaderContext context) throws IOException {
            if (failInGetLeafCollector && context.ord == 0) {
              failAfter(siblingsCollecting);
            }
          }

          @Override
          void onFirstDoc(LeafReaderContext context) throws IOException {
            if (context.ord == 0) {
              failAfter(siblingsCollecting);
            }
            siblingsCollecting.countDown();
            awaitTaskGroupFailure();
          }
        };

    IOException e =
        expectThrows(IOException.class, () -> searcher.search(MatchAllDocsQuery.INSTANCE, manager));
    assertEquals("boom", e.getMessage());
    assertEquals(0, e.getSuppressed().length);
    assertFalse("a failure is not a timeout", searcher.timedOut());
    assertSame(timeout, searcher.getTimeout());
    for (HookedCollector collector : manager.collectors) {
      // siblings exit at the first check after the failure instead of collecting the segment
      assertTrue(collector.count <= TimeLimitingBulkScorer.INTERVAL);
    }
  }

  public void testRemainingLeavesOfSliceAreSkippedWhenSiblingFails() {
    IndexSearcher searcher =
        new IndexSearcher(reader, executor) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            List<LeafReaderContextPartition> others = new ArrayList<>();
            for (LeafReaderContext leaf : leaves.subList(1, leaves.size())) {
              others.add(LeafReaderContextPartition.createForEntireSegment(leaf));
            }
            return new LeafSlice[] {
              new LeafSlice(
                  List.of(LeafReaderContextPartition.createForEntireSegment(leaves.getFirst()))),
              new LeafSlice(others)
            };
          }
        };
    searcher.setQueryCache(null);
    CountDownLatch siblingStarted = new CountDownLatch(1);
    AtomicIntegerArray scorerSupplierCalls = new AtomicIntegerArray(NUM_SEGMENTS);
    Query query =
        new HookedQuery() {
          @Override
          ScorerSupplier scorerSupplier(LeafReaderContext context, ScorerSupplier in)
              throws IOException {
            scorerSupplierCalls.incrementAndGet(context.ord);
            if (context.ord == 0) {
              failAfter(siblingStarted);
            } else if (context.ord == 1) {
              siblingStarted.countDown();
              awaitTaskGroupFailure();
              // no matches in this leaf, so the next check happens before the next leaf
              return null;
            }
            return in;
          }
        };

    expectThrows(IOException.class, () -> searcher.search(query, new HookedCollectorManager()));
    assertEquals(1, scorerSupplierCalls.get(1));
    for (int ord = 2; ord < NUM_SEGMENTS; ord++) {
      assertEquals("leaf " + ord + " should be skipped", 0, scorerSupplierCalls.get(ord));
    }
  }

  public void testSiblingFailureDoesNotAffectSubsequentSearches() throws IOException {
    IndexSearcher searcher = newSearcher(true);
    expectThrows(
        IOException.class, () -> searcher.search(new FailingQuery(), new HookedCollectorManager()));
    assertEquals(
        reader.maxDoc(),
        (long) searcher.search(MatchAllDocsQuery.INSTANCE, new HookedCollectorManager()));
  }

  public void testSiblingFailureDoesNotAffectConcurrentSearches() throws Exception {
    IndexSearcher searcher = newSearcher(true);
    CountDownLatch healthyCollecting = new CountDownLatch(NUM_SEGMENTS);
    CountDownLatch failingSearchDone = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread failingSearch =
        new Thread(
            () -> {
              try {
                searcher.search(
                    new HookedQuery() {
                      @Override
                      ScorerSupplier scorerSupplier(LeafReaderContext context, ScorerSupplier in)
                          throws IOException {
                        if (context.ord == 0) {
                          failAfter(healthyCollecting);
                        }
                        return in;
                      }
                    },
                    new HookedCollectorManager());
              } catch (Throwable t) {
                failure.set(t);
              } finally {
                failingSearchDone.countDown();
              }
            });
    failingSearch.start();

    HookedCollectorManager healthy =
        new HookedCollectorManager() {
          @Override
          void onFirstDoc(LeafReaderContext context) {
            healthyCollecting.countDown();
            try {
              assertTrue(failingSearchDone.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }
        };
    assertEquals(reader.maxDoc(), (long) searcher.search(MatchAllDocsQuery.INSTANCE, healthy));
    failingSearch.join();

    assertThat(failure.get(), instanceOf(IOException.class));
    assertFalse(searcher.timedOut());
  }

  private static void failAfter(CountDownLatch waitFor) throws IOException {
    try {
      assertTrue(waitFor.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
    throw new IOException("boom");
  }

  private static void awaitTaskGroupFailure() {
    TestTaskExecutor.awaitTaskGroupFailure(TaskExecutor.currentTaskGroup());
  }

  /** Counts collected docs and calls hooks when it starts collecting a leaf and its first doc. */
  private static class HookedCollectorManager implements CollectorManager<HookedCollector, Long> {
    final List<HookedCollector> collectors = Collections.synchronizedList(new ArrayList<>());

    void onLeaf(LeafReaderContext context) throws IOException {}

    void onFirstDoc(LeafReaderContext context) throws IOException {}

    @Override
    public HookedCollector newCollector() {
      HookedCollector collector = new HookedCollector(this);
      collectors.add(collector);
      return collector;
    }

    @Override
    public Long reduce(Collection<HookedCollector> collectors) {
      return collectors.stream().mapToLong(c -> c.count).sum();
    }
  }

  private static final class HookedCollector extends SimpleCollector {
    private final HookedCollectorManager manager;
    private LeafReaderContext context;
    private boolean firstDoc;
    long count;

    HookedCollector(HookedCollectorManager manager) {
      this.manager = manager;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.context = context;
      this.firstDoc = true;
      manager.onLeaf(context);
    }

    @Override
    public void collect(int doc) throws IOException {
      if (firstDoc) {
        firstDoc = false;
        manager.onFirstDoc(context);
      }
      count++;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  /** Matches all docs and lets tests intercept the per-leaf scorer supplier and bulk scorer. */
  private static class HookedQuery extends Query {

    ScorerSupplier scorerSupplier(LeafReaderContext context, ScorerSupplier in) throws IOException {
      return in;
    }

    BulkScorer bulkScorer(LeafReaderContext context, BulkScorer in) {
      return in;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      Weight in = MatchAllDocsQuery.INSTANCE.createWeight(searcher, scoreMode, boost);
      return new FilterWeight(this, in) {
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier supplier =
              HookedQuery.this.scorerSupplier(context, in.scorerSupplier(context));
          if (supplier == null) {
            return null;
          }
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return supplier.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
              return HookedQuery.this.bulkScorer(context, supplier.bulkScorer());
            }

            @Override
            public long cost() throws IOException {
              return supplier.cost();
            }

            @Override
            public void setTopLevelScoringClause() {
              supplier.setTopLevelScoringClause();
            }
          };
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  /** Fails on the first segment. */
  private static final class FailingQuery extends HookedQuery {
    @Override
    ScorerSupplier scorerSupplier(LeafReaderContext context, ScorerSupplier in) throws IOException {
      if (context.ord == 0) {
        throw new IOException("boom");
      }
      return in;
    }
  }
}
