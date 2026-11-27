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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunnable;
import org.apache.lucene.util.automaton.CompiledAutomaton;

public class TestFuzzyAutomatonCache extends LuceneTestCase {

  private static FuzzyAutomatonCache newCache() {
    return new FuzzyAutomatonCache(1000, Long.MAX_VALUE);
  }

  private static FuzzyQuery newQuery(
      String field, String text, int maxEdits, int prefixLength, FuzzyAutomatonCache cache) {
    return new FuzzyQuery(
        new Term(field, text),
        maxEdits,
        prefixLength,
        FuzzyQuery.defaultMaxExpansions,
        FuzzyQuery.defaultTranspositions,
        FuzzyQuery.defaultRewriteMethod(FuzzyQuery.defaultMaxExpansions),
        cache);
  }

  public void testSameKeyReturnsSameAutomata() {
    FuzzyAutomatonCache cache = newCache();
    FuzzyAutomatonCache.Automata first = cache.getOrBuild("lucene", 2, 0, true);
    assertEquals(3, first.automata().length);
    assertEquals(6, first.termLength());
    assertEquals(1, cache.getMissCount());
    assertEquals(0, cache.getHitCount());

    assertSame(first, cache.getOrBuild("lucene", 2, 0, true));
    assertEquals(1, cache.getMissCount());
    assertEquals(1, cache.getHitCount());
    assertEquals(1, cache.size());

    // every parameter that changes the automata is part of the key
    assertNotSame(first, cache.getOrBuild("lucene", 1, 0, true));
    assertNotSame(first, cache.getOrBuild("lucene", 2, 1, true));
    assertNotSame(first, cache.getOrBuild("lucene", 2, 0, false));
    assertNotSame(first, cache.getOrBuild("lucenf", 2, 0, true));
    assertEquals(5, cache.getMissCount());
    assertEquals(5, cache.size());
  }

  public void testPrefixLongerThanTermSharesEntry() {
    FuzzyAutomatonCache cache = newCache();
    FuzzyAutomatonCache.Automata first = cache.getOrBuild("abc", 1, 3, true);
    assertSame(first, cache.getOrBuild("abc", 1, 10, true));
    assertEquals(1, cache.size());
  }

  public void testEvictsLeastRecentlyUsed() {
    FuzzyAutomatonCache cache = new FuzzyAutomatonCache(2, Long.MAX_VALUE);
    FuzzyAutomatonCache.Automata a = cache.getOrBuild("aaaa", 1, 0, true);
    FuzzyAutomatonCache.Automata b = cache.getOrBuild("bbbb", 1, 0, true);
    assertSame(a, cache.getOrBuild("aaaa", 1, 0, true)); // "bbbb" is now the eldest
    cache.getOrBuild("cccc", 1, 0, true);
    assertEquals(2, cache.size());
    assertEquals(1, cache.getEvictionCount());

    assertSame(a, cache.getOrBuild("aaaa", 1, 0, true));
    assertNotSame(b, cache.getOrBuild("bbbb", 1, 0, true));
  }

  public void testRamBound() {
    FuzzyAutomatonCache unbounded = newCache();
    long emptyBytes = unbounded.ramBytesUsed();
    unbounded.getOrBuild("aaaa", 2, 0, true);
    long oneEntryBytes = unbounded.ramBytesUsed() - emptyBytes;
    assertTrue(oneEntryBytes > 0);
    unbounded.clear();
    assertEquals(0, unbounded.size());
    assertEquals(emptyBytes, unbounded.ramBytesUsed());

    // room for one entry of this shape, but not for two
    FuzzyAutomatonCache bounded = new FuzzyAutomatonCache(1000, oneEntryBytes + oneEntryBytes / 2);
    bounded.getOrBuild("aaaa", 2, 0, true);
    bounded.getOrBuild("bbbb", 2, 0, true);
    assertEquals(1, bounded.size());
    assertEquals(1, bounded.getEvictionCount());
    assertTrue(bounded.ramBytesUsed() <= emptyBytes + oneEntryBytes + oneEntryBytes / 2);

    // an entry that can never fit is returned but not kept
    for (FuzzyAutomatonCache tiny :
        new FuzzyAutomatonCache[] {
          new FuzzyAutomatonCache(1000, 0), new FuzzyAutomatonCache(0, Long.MAX_VALUE)
        }) {
      assertNotNull(tiny.getOrBuild("aaaa", 2, 0, true));
      assertEquals(0, tiny.size());
      assertEquals(0, tiny.getEvictionCount());
      assertEquals(emptyBytes, tiny.ramBytesUsed());
    }
  }

  public void testFailuresAreNotCached() {
    FuzzyAutomatonCache cache = newCache();
    expectThrows(IllegalArgumentException.class, () -> cache.getOrBuild("abc", 3, 0, true));
    expectThrows(IllegalArgumentException.class, () -> cache.getOrBuild("abc", 1, -1, true));
    assertEquals(0, cache.size());
    expectThrows(IllegalArgumentException.class, () -> new FuzzyAutomatonCache(-1, 1));
    expectThrows(IllegalArgumentException.class, () -> new FuzzyAutomatonCache(1, -1));
  }

  public void testCacheIsNotPartOfQueryIdentity() {
    FuzzyQuery without = newQuery("field", "lucene", 2, 0, null);
    FuzzyQuery with = newQuery("field", "lucene", 2, 0, newCache());
    assertNull(without.getAutomatonCache());
    assertNotNull(with.getAutomatonCache());
    assertEquals(without, with);
    assertEquals(without.hashCode(), with.hashCode());
  }

  public void testGetAutomataAndVisitorUseCache() {
    FuzzyAutomatonCache cache = newCache();
    FuzzyQuery query = newQuery("field", "blob", 2, 0, cache);
    CompiledAutomaton automaton = query.getAutomata();
    assertSame(automaton, query.getAutomata());
    assertEquals(newQuery("field", "blob", 2, 0, null).getAutomata(), automaton);

    AtomicBoolean visited = new AtomicBoolean();
    query.visit(
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            visited.set(true);
            ByteRunnable a = automaton.get();
            assertTrue(run(a, "blob"));
            assertTrue(run(a, "bolb"));
            assertTrue(run(a, "blobby"));
            assertFalse(run(a, "bolbby"));
          }
        });
    assertTrue(visited.get());
    assertEquals(1, cache.getMissCount());
  }

  private static boolean run(ByteRunnable automaton, String text) {
    BytesRef b = newBytesRef(text);
    return automaton.run(b.bytes, b.offset, b.length);
  }

  public void testSharedAcrossFieldsAndSearches() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      for (String value : new String[] {"lucene", "lucent", "lucerne", "search", "luke"}) {
        Document doc = new Document();
        doc.add(new StringField("title", value, Field.Store.NO));
        doc.add(new StringField("body", value, Field.Store.NO));
        w.addDocument(doc);
        if (random().nextBoolean()) {
          w.commit(); // several segments
        }
      }
      try (DirectoryReader reader = w.getReader()) {
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        FuzzyAutomatonCache cache = newCache();

        assertEquals(3, searcher.count(newQuery("title", "lucene", 2, 0, cache)));
        assertEquals(1, cache.getMissCount());
        assertEquals(1, cache.size());

        long hitsBefore = cache.getHitCount();
        assertEquals(3, searcher.count(newQuery("body", "lucene", 2, 0, cache)));
        assertEquals(3, searcher.count(newQuery("title", "lucene", 2, 0, cache)));
        assertEquals("no new automata for another field or search", 1, cache.getMissCount());
        assertTrue(cache.getHitCount() >= hitsBefore + 2);
        assertEquals(1, cache.size());
      }
    }
  }

  public void testRandomSameResultsAsUncached() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      int numDocs = atLeast(200);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("f1", randomTerm(), Field.Store.NO));
        doc.add(new StringField("f2", randomTerm(), Field.Store.NO));
        w.addDocument(doc);
      }
      try (DirectoryReader reader = w.getReader()) {
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        // small enough that entries are evicted and rebuilt during the test
        FuzzyAutomatonCache cache =
            new FuzzyAutomatonCache(TestUtil.nextInt(random(), 1, 20), 1L << 30);

        int iters = atLeast(200);
        for (int iter = 0; iter < iters; iter++) {
          String field = random().nextBoolean() ? "f1" : "f2";
          String text = randomTerm();
          int maxEdits = random().nextInt(3);
          int prefixLength = random().nextInt(text.length() + 2);
          TopDocs expected =
              searcher.search(newQuery(field, text, maxEdits, prefixLength, null), 20);
          TopDocs actual =
              searcher.search(newQuery(field, text, maxEdits, prefixLength, cache), 20);
          assertTopDocsEquals(expected, actual);
        }
        assertTrue(cache.getEvictionCount() > 0);
      }
    }
  }

  public void testConcurrentSearches() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      int numDocs = atLeast(200);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("f1", randomTerm(), Field.Store.NO));
        w.addDocument(doc);
      }
      try (DirectoryReader reader = w.getReader()) {
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        FuzzyAutomatonCache cache = new FuzzyAutomatonCache(8, 1L << 30);

        // few distinct queries, so that threads keep sharing the same automata, including the
        // CompiledAutomaton#floor calls made when the fuzzy enum lowers its edit distance
        int numQueries = 16;
        String[] texts = new String[numQueries];
        TopDocs[] expected = new TopDocs[numQueries];
        for (int i = 0; i < numQueries; i++) {
          texts[i] = randomTerm();
          expected[i] = searcher.search(new FuzzyQuery(new Term("f1", texts[i]), 2, 0, 5, true), 5);
        }

        int numThreads = TestUtil.nextInt(random(), 2, 6);
        int itersPerThread = atLeast(100);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
          int seed = random().nextInt();
          Thread thread =
              new Thread(
                  () -> {
                    try {
                      start.await();
                      Random r = new Random(seed);
                      for (int i = 0; i < itersPerThread && failure.get() == null; i++) {
                        int q = r.nextInt(numQueries);
                        // maxExpansions=5 makes the enum drop to smaller edit distances
                        FuzzyQuery query =
                            new FuzzyQuery(
                                new Term("f1", texts[q]),
                                2,
                                0,
                                5,
                                true,
                                FuzzyQuery.defaultRewriteMethod(5),
                                cache);
                        assertTopDocsEquals(expected[q], searcher.search(query, 5));
                      }
                    } catch (Throwable e) {
                      failure.compareAndSet(null, e);
                    }
                  });
          thread.start();
          threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
          thread.join();
        }
        if (failure.get() != null) {
          throw new AssertionError(failure.get());
        }
        assertTrue(cache.size() <= 8);
        assertTrue(cache.getHitCount() > 0);
      }
    }
  }

  private static String randomTerm() {
    // small alphabet and short terms, so that fuzzy queries have plenty of matches
    int length = TestUtil.nextInt(random(), 3, 6);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + random().nextInt(4)));
    }
    return sb.toString();
  }

  private static void assertTopDocsEquals(TopDocs expected, TopDocs actual) {
    assertEquals(expected.totalHits.value(), actual.totalHits.value());
    assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
    for (int i = 0; i < expected.scoreDocs.length; i++) {
      assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
      assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0f);
    }
  }
}
