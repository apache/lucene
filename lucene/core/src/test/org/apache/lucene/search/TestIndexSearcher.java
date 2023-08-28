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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.hamcrest.Matchers;
import org.junit.Test;

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

  @Test
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

  public void testGetSlices() throws Exception {
    assertNull(new IndexSearcher(new MultiReader()).getSlices());

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader r = w.getReader();
    w.close();

    ExecutorService service =
        new ThreadPoolExecutor(
            4,
            4,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("TestIndexSearcher"));
    IndexSearcher s = new IndexSearcher(r, service);
    IndexSearcher.LeafSlice[] slices = s.getSlices();
    assertNotNull(slices);
    assertEquals(1, slices.length);
    assertEquals(1, slices[0].leaves.length);
    assertTrue(slices[0].leaves[0] == r.leaves().get(0));
    service.shutdown();
    IOUtils.close(r, dir);
  }

  public void testSlicesAllOffloadedToTheExecutor() throws IOException {
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
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
    assertTrue(topDocs.totalHits.value > 0);
    if (leaves.size() <= 1) {
      assertEquals(0, numExecutions.get());
    } else {
      assertEquals(leaves.size(), numExecutions.get());
    }
  }

  /**
   * Tests that when IndexerSearcher runs concurrent searches on multiple slices if any Exception is
   * thrown by one of the slice tasks, it will not return until all tasks have completed.
   *
   * <p>Without a larger refactoring of the Lucene IndexSearcher and/or TaskExecutor there isn't a
   * clean deterministic way to test this. This test is probabilistic using short timeouts in the
   * tasks that do not throw an Exception.
   */
  public void testMultipleSegmentsOnTheExecutorWithException() {
    List<LeafReaderContext> leaves = reader.leaves();
    int fixedThreads = leaves.size() == 1 ? 1 : leaves.size() / 2;

    ExecutorService fixedThreadPoolExecutor =
        Executors.newFixedThreadPool(fixedThreads, new NamedThreadFactory("concurrent-slices"));

    IndexSearcher searcher =
        new IndexSearcher(reader, fixedThreadPoolExecutor) {
          @Override
          protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
            ArrayList<LeafSlice> slices = new ArrayList<>();
            for (LeafReaderContext ctx : leaves) {
              slices.add(new LeafSlice(Arrays.asList(ctx)));
            }
            return slices.toArray(new LeafSlice[0]);
          }
        };

    try {
      AtomicInteger callsToScorer = new AtomicInteger(0);
      int numExceptions = leaves.size() == 1 ? 1 : RandomizedTest.randomIntBetween(1, 2);
      MatchAllOrThrowExceptionQuery query =
          new MatchAllOrThrowExceptionQuery(numExceptions, callsToScorer);
      RuntimeException exc = expectThrows(RuntimeException.class, () -> searcher.search(query, 10));
      // if the TaskExecutor didn't wait for all tasks to finish, this assert would frequently fail
      assertEquals(leaves.size(), callsToScorer.get());
      assertThat(
          exc.getMessage(), Matchers.containsString("MatchAllOrThrowExceptionQuery Exception"));
    } finally {
      TestUtil.shutdownExecutorService(fixedThreadPoolExecutor);
    }
  }

  private static class MatchAllOrThrowExceptionQuery extends Query {

    private final AtomicInteger numExceptionsToThrow;
    private final Query delegate;
    private final AtomicInteger callsToScorer;

    /**
     * Throws an Exception out of the {@code scorer} method the first {@code numExceptions} times it
     * is called. Otherwise, it delegates all calls to the MatchAllDocsQuery.
     *
     * @param numExceptions number of exceptions to throw from scorer method
     * @param callsToScorer where to record the number of times the {@code scorer}
     *                      method has been called
     */
    public MatchAllOrThrowExceptionQuery(int numExceptions, AtomicInteger callsToScorer) {
      this.numExceptionsToThrow = new AtomicInteger(numExceptions);
      this.callsToScorer = callsToScorer;
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
          if (numExceptionsToThrow.getAndDecrement() > 0) {
            callsToScorer.getAndIncrement();
            throw new RuntimeException("MatchAllOrThrowExceptionQuery Exception");
          } else {
            // A small sleep before incrementing the callsToScorer counter allows
            // the task with the Exception to be thrown and if TaskExecutor.invokeAll
            // does not wait until all tasks have finished, then the callsToScorer
            // counter will not match the total number of tasks (or rather usually will
            // not match, since there is a race condition that makes it probabilistic).
            RandomizedTest.sleep(25);
            callsToScorer.getAndIncrement();
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
}
