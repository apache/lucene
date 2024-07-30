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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.hamcrest.Matchers;

public class TestIndexSearcher extends LuceneTestCase {
  Directory dir;
  IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Random random = random();
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
      doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
      doc.add(new SortedDocValuesField("field2", newBytesRef(Boolean.toString(i % 2 == 0))));
      iw.addDocument(doc);

      if (random.nextBoolean()) {
        iw.commit();
      }
    }
    reader = iw.getReader();
    iw.close();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(reader, dir);
  }

  // should not throw exception
  public void testHugeN() throws Exception {
    ExecutorService service =
        new ThreadPoolExecutor(
            4,
            4,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("TestIndexSearcher"));

    IndexSearcher[] searchers =
        new IndexSearcher[] {new IndexSearcher(reader), new IndexSearcher(reader, service)};
    Query[] queries = new Query[] {new MatchAllDocsQuery(), new TermQuery(new Term("field", "1"))};
    Sort[] sorts = new Sort[] {null, new Sort(new SortField("field2", SortField.Type.STRING))};
    ScoreDoc[] afters =
        new ScoreDoc[] {null, new FieldDoc(0, 0f, new Object[] {newBytesRef("boo!")})};

    for (IndexSearcher searcher : searchers) {
      for (ScoreDoc after : afters) {
        for (Query query : queries) {
          for (Sort sort : sorts) {
            searcher.search(query, Integer.MAX_VALUE);
            searcher.searchAfter(after, query, Integer.MAX_VALUE);
            if (sort != null) {
              searcher.search(query, Integer.MAX_VALUE, sort);
              searcher.search(query, Integer.MAX_VALUE, sort, true);
              searcher.search(query, Integer.MAX_VALUE, sort, false);
              searcher.searchAfter(after, query, Integer.MAX_VALUE, sort);
              searcher.searchAfter(after, query, Integer.MAX_VALUE, sort, true);
              searcher.searchAfter(after, query, Integer.MAX_VALUE, sort, false);
            }
          }
        }
      }
    }

    TestUtil.shutdownExecutorService(service);
  }

  public void testSearchAfterPassedMaxDoc() throws Exception {
    // LUCENE-5128: ensure we get a meaningful message if searchAfter exceeds maxDoc
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          s.searchAfter(new ScoreDoc(r.maxDoc(), 0.54f), new MatchAllDocsQuery(), 10);
        });

    IOUtils.close(r, dir);
  }

  public void testCount() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (random().nextBoolean()) {
        doc.add(new StringField("foo", "bar", Store.NO));
      }
      if (random().nextBoolean()) {
        doc.add(new StringField("foo", "baz", Store.NO));
      }
      if (rarely()) {
        doc.add(new StringField("delete", "yes", Store.NO));
      }
      w.addDocument(doc);
    }
    for (boolean delete : new boolean[] {false, true}) {
      if (delete) {
        w.deleteDocuments(new Term("delete", "yes"));
      }
      final IndexReader reader = w.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      // Test multiple queries, some of them are optimized by IndexSearcher.count()
      for (Query query :
          Arrays.asList(
              new MatchAllDocsQuery(),
              new MatchNoDocsQuery(),
              new TermQuery(new Term("foo", "bar")),
              new ConstantScoreQuery(new TermQuery(new Term("foo", "baz"))),
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                  .build())) {
        assertEquals(searcher.count(query), searcher.search(query, 1).totalHits.value);
      }
      reader.close();
    }
    IOUtils.close(w, dir);
  }

  public void testGetQueryCache() throws IOException {
    IndexSearcher searcher = new IndexSearcher(new MultiReader());
    assertEquals(IndexSearcher.getDefaultQueryCache(), searcher.getQueryCache());
    QueryCache dummyCache =
        new QueryCache() {
          @Override
          public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            return weight;
          }
        };
    searcher.setQueryCache(dummyCache);
    assertEquals(dummyCache, searcher.getQueryCache());

    IndexSearcher.setDefaultQueryCache(dummyCache);
    searcher = new IndexSearcher(new MultiReader());
    assertEquals(dummyCache, searcher.getQueryCache());

    searcher.setQueryCache(null);
    assertNull(searcher.getQueryCache());

    IndexSearcher.setDefaultQueryCache(null);
    searcher = new IndexSearcher(new MultiReader());
    assertNull(searcher.getQueryCache());
  }

  public void testGetQueryCachingPolicy() throws IOException {
    IndexSearcher searcher = new IndexSearcher(new MultiReader());
    assertEquals(IndexSearcher.getDefaultQueryCachingPolicy(), searcher.getQueryCachingPolicy());
    QueryCachingPolicy dummyPolicy =
        new QueryCachingPolicy() {
          @Override
          public boolean shouldCache(Query query) throws IOException {
            return false;
          }

          @Override
          public void onUse(Query query) {}
        };
    searcher.setQueryCachingPolicy(dummyPolicy);
    assertEquals(dummyPolicy, searcher.getQueryCachingPolicy());

    IndexSearcher.setDefaultQueryCachingPolicy(dummyPolicy);
    searcher = new IndexSearcher(new MultiReader());
    assertEquals(dummyPolicy, searcher.getQueryCachingPolicy());
  }

  public void testGetSlicesNoLeavesNoExecutor() throws IOException {
    IndexSearcher.LeafSlice[] slices = new IndexSearcher(new MultiReader()).getSlices();
    assertEquals(0, slices.length);
  }

  public void testGetSlicesNoLeavesWithExecutor() throws IOException {
    IndexSearcher.LeafSlice[] slices =
        new IndexSearcher(new MultiReader(), Runnable::run).getSlices();
    assertEquals(0, slices.length);
  }

  public void testGetSlices() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 10; i++) {
      w.addDocument(new Document());
      // manually flush, so we get to create multiple segments almost all the times, as well as
      // multiple slices
      w.flush();
    }
    IndexReader r = w.getReader();
    w.close();

    {
      // without executor
      IndexSearcher.LeafSlice[] slices = new IndexSearcher(r).getSlices();
      assertEquals(1, slices.length);
      assertEquals(r.leaves().size(), slices[0].leaves.length);
    }
    {
      // force creation of multiple slices, and provide an executor
      IndexSearcher searcher =
          new IndexSearcher(r, Runnable::run) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              return slices(leaves, 1, 1);
            }
          };
      IndexSearcher.LeafSlice[] slices = searcher.getSlices();
      for (IndexSearcher.LeafSlice slice : slices) {
        assertEquals(1, slice.leaves.length);
      }
      assertEquals(r.leaves().size(), slices.length);
    }
    IOUtils.close(r, dir);
  }

  public void testSlicesOffloadedToTheExecutor() throws IOException {
    List<LeafReaderContext> leaves = reader.leaves();
    AtomicInteger numExecutions = new AtomicInteger(0);
    IndexSearcher searcher =
        new IndexSearcher(
            reader,
            task -> {
              numExecutions.incrementAndGet();
              task.run();
            }) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            ArrayList<LeafSlice> slices = new ArrayList<>();
            for (LeafReaderContext ctx : leaves) {
              slices.add(new LeafSlice(Arrays.asList(ctx)));
            }
            return slices.toArray(new LeafSlice[0]);
          }
        };
    searcher.search(new MatchAllDocsQuery(), 10);
    assertEquals(leaves.size() - 1, numExecutions.get());
  }

  public void testNullExecutorNonNullTaskExecutor() {
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    assertNotNull(indexSearcher.getTaskExecutor());
  }

  /*
   * The goal of this test is to ensure that when multiple concurrent slices are
   * being searched, and one of the concurrent tasks throws an Exception, the other
   * tasks become aware of it (via the ExceptionBasedQueryTimeout in IndexSearcher)
   * and exit immediately rather than completing their search actions.
   *
   * To test this:
   * - a concurrent Executor is used to ensure concurrent tasks are running
   * - the MatchAllOrThrowExceptionQuery is used to ensure that one of the search
   *   tasks throws an Exception
   * - a testing ExceptionBasedTimeoutWrapper is used to track the number of times
   *   and early exit happens
   * - a CountDownLatch is used to synchronize the task that is going to throw an Exception
   *   with another task that is in the ExceptionBasedQueryTimeout.shouldExit method,
   *   ensuring the Exception is thrown while at least one other task is still running
   * - a second CountDownLatch is used to synchronize between the
   *   ExceptionBasedQueryTimeout.notifyExceptionThrown call (coming from the task thread
   *   where the exception is thrown) and the ExceptionBasedQueryTimeout.shouldExit method
   *   to ensure that at least one task has shouldExit return true (for an early exit)
   * - an atomic earlyExitCounter tracks how many tasks exited early due to
   *   TimeLimitingBulkScorer.TimeExceededException in the TimeLimitingBulkScorer
   */
  public void testMultipleSegmentsWithExceptionCausesEarlyTerminationOfRunningTasks() {
    // skip this test when only one leaf, since one leaf means one task
    // and the TimeLimitingBulkScorer will NOT be added in IndexSearcher
    if (reader.leaves().size() <= 1) {
      return;
    }
    // tracks how many tasks exited early due to Exception being thrown in another task
    AtomicInteger earlyExitCounter = new AtomicInteger(0);
    // task that throws an Exception waits on this latch to ensure at least one task is checking the
    // QueryTimeout.shouldExit method before it throws the Exception
    CountDownLatch shouldExitLatch = new CountDownLatch(1);
    // latch used by the ExceptionBasedQueryTimeoutWrapper to ensure that the shouldExit method
    // of at least one task waits until the ExceptionBasedQueryTimeout.notifyExceptionThrown method
    // is called before checking getting the shouldExit method
    CountDownLatch exceptionThrownLatch = new CountDownLatch(1);
    ExecutorService executorService =
        Executors.newFixedThreadPool(7, new NamedThreadFactory("concurrentSlicesTest"));
    try {
      IndexSearcher searcher =
          new IndexSearcher(reader, executorService) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              return slices(leaves, 1, 1);
            }

            @Override
            BulkScorer createTimeLimitingBulkScorer(BulkScorer scorer, QueryTimeout queryTimeout) {
              return new TimeLimitingBulkScorerWrapper(earlyExitCounter, scorer, queryTimeout);
            }

            @Override
            void addExceptionBasedQueryTimeout(QueryTimeout delegate) {
              setTimeout(
                  new ExceptionBasedQueryTimeoutWrapper(
                      shouldExitLatch, exceptionThrownLatch, delegate));
            }
          };

      MatchAllOrThrowExceptionQuery query = new MatchAllOrThrowExceptionQuery(shouldExitLatch);
      RuntimeException exc = expectThrows(RuntimeException.class, () -> searcher.search(query, 10));
      assertThat(
          exc.getMessage(), Matchers.containsString("MatchAllOrThrowExceptionQuery Exception"));
      assertThat(earlyExitCounter.get(), Matchers.greaterThan(0));

    } finally {
      executorService.shutdown();
    }
  }

  private static class ExceptionBasedQueryTimeoutWrapper
      extends IndexSearcher.ExceptionBasedQueryTimeout {
    private final CountDownLatch shouldExitLatch;
    private final CountDownLatch exceptionThrownLatch;

    public ExceptionBasedQueryTimeoutWrapper(
        CountDownLatch shouldExitLatch,
        CountDownLatch exceptionThrownLatch,
        QueryTimeout delegate) {
      super(delegate);
      this.shouldExitLatch = shouldExitLatch;
      this.exceptionThrownLatch = exceptionThrownLatch;
    }

    // called on the thread of the task that threw an Exception
    @Override
    public void notifyExceptionThrown() {
      super.notifyExceptionThrown();
      exceptionThrownLatch.countDown();
    }

    @Override
    public boolean shouldExit() {
      // notifies other tasks that this task is in the shouldExit method
      shouldExitLatch.countDown();
      try {
        // wait until at least one task has been notified that an exception was thrown
        exceptionThrownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Unexpected timeout of latch await", e);
      }
      return super.shouldExit();
    }
  }

  private static class MatchAllOrThrowExceptionQuery extends Query {

    private final CountDownLatch shouldExitLatch;
    private final AtomicInteger numExceptionsToThrow;
    private final Query delegate;

    /**
     * Throws an Exception out of the {@code scorer} method the first time it is called. Otherwise,
     * it delegates all calls to the MatchAllDocsQuery.
     */
    public MatchAllOrThrowExceptionQuery(CountDownLatch shouldExitLatch) {
      this.numExceptionsToThrow = new AtomicInteger(1);
      this.shouldExitLatch = shouldExitLatch;
      this.delegate = new MatchAllDocsQuery();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      Weight matchAllWeight = delegate.createWeight(searcher, scoreMode, boost);

      return new Weight(delegate) {
        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return matchAllWeight.isCacheable(ctx);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          return matchAllWeight.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          // only throw exception when the counter hits 1 (so only one exception gets thrown)
          if (numExceptionsToThrow.getAndDecrement() == 1) {
            try {
              // wait until we know another task is in the QueryTimeout.shouldExit method
              // before throwing an Exception to ensure at least one shouldExit method
              // returns true causing another task to exit early
              shouldExitLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            throw new RuntimeException("MatchAllOrThrowExceptionQuery Exception");
          }
          return matchAllWeight.scorer(context);
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
      delegate.visit(visitor);
    }

    @Override
    public String toString(String field) {
      return "MatchAllOrThrowExceptionQuery";
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return other == this;
    }
  }

  static class TimeLimitingBulkScorerWrapper extends BulkScorer {
    private final TimeLimitingBulkScorer delegate;
    private final AtomicInteger earlyExitCounter;

    /**
     * Wraps a {@link TimeLimitingBulkScorer}, recording the counts of how many times {@link
     * TimeLimitingBulkScorer.TimeExceededException} is thrown from the {@code score} method.
     *
     * @param earlyExitCounter counter to increment when {@link
     *     TimeLimitingBulkScorer.TimeExceededException} is caught
     * @param scorer to pass to {@link TimeLimitingBulkScorer} constructor
     * @param queryTimeout to pass to {@link TimeLimitingBulkScorer} constructor
     */
    public TimeLimitingBulkScorerWrapper(
        AtomicInteger earlyExitCounter, BulkScorer scorer, QueryTimeout queryTimeout) {
      this.earlyExitCounter = earlyExitCounter;
      this.delegate = new TimeLimitingBulkScorer(scorer, queryTimeout);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      try {
        return delegate.score(collector, acceptDocs, min, max);
      } catch (TimeLimitingBulkScorer.TimeExceededException tee) {
        earlyExitCounter.incrementAndGet();
        throw tee;
      }
    }

    @Override
    public long cost() {
      return delegate.cost();
    }
  }
}
