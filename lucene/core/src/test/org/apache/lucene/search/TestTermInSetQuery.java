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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

public class TestTermInSetQuery extends LuceneTestCase {

  public void testAllDocsInFieldTerm() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    String field = "f";

    BytesRef denseTerm = new BytesRef(TestUtil.randomAnalysisString(random(), 10, true));

    Set<BytesRef> randomTerms = new HashSet<>();
    while (randomTerms.size() < TermInSetQuery.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
      randomTerms.add(new BytesRef(TestUtil.randomAnalysisString(random(), 10, true)));
    }
    assert randomTerms.size() == TermInSetQuery.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD;
    BytesRef[] otherTerms = new BytesRef[randomTerms.size()];
    int idx = 0;
    for (BytesRef term : randomTerms) {
      otherTerms[idx++] = term;
    }

    // Every doc with a value for `field` will contain `denseTerm`:
    int numDocs = 10 * otherTerms.length;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField(field, denseTerm, Store.NO));
      BytesRef sparseTerm = otherTerms[i % otherTerms.length];
      doc.add(new StringField(field, sparseTerm, Store.NO));
      iw.addDocument(doc);
    }

    // Make sure there are some docs in the index that don't contain a value for the field at all:
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
    }

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    List<BytesRef> queryTerms = Arrays.stream(otherTerms).collect(Collectors.toList());
    queryTerms.add(denseTerm);

    TermInSetQuery query = new TermInSetQuery(field, queryTerms);
    TopDocs topDocs = searcher.search(query, numDocs);
    assertEquals(numDocs, topDocs.totalHits.value);

    reader.close();
    dir.close();
  }

  public void testRewriteApproaches() throws IOException {
    final List<BytesRef> denseTerms = new ArrayList<>();
    for (int i = 0; i < 25; ++i) {
      final String value = TestUtil.randomSimpleString(random(), 20, 40);
      denseTerms.add(newBytesRef(value));
    }
    final List<BytesRef> sparseTerms = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      final String value = TestUtil.randomSimpleString(random(), 20, 40);
      sparseTerms.add(newBytesRef(value));
    }

    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    // Almost all 100,000 docs have the same "lead" term. Each doc has a random "dense" term chosen
    // from 25, so each of the 25 terms should cover 4,000 docs. Each doc also has a "sparse"
    // term chosen from 1,000, so each of the 1,000 terms should cover 100 docs:
    for (int i = 0; i < 100000; i++) {
      Document doc = new Document();
      // "common" covers all but 20 docs (which are covered by "rare"):
      if (i % 5000 != 0) {
        doc.add(new StringField("lead", new BytesRef("common"), Store.NO));
      } else {
        doc.add(new StringField("lead", new BytesRef("rare"), Store.NO));
      }
      BytesRef term = denseTerms.get(i % denseTerms.size());
      doc.add(new StringField("t", term, Store.NO));
      doc.add(new SortedSetDocValuesField("t", term));
      term = sparseTerms.get(i % sparseTerms.size());
      doc.add(new StringField("t", term, Store.NO));
      doc.add(new SortedSetDocValuesField("t", term));
      iw.addDocument(doc);
    }
    // Force merge to ensure our relative cost setup holds:
    iw.forceMerge(1);

    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    final float boost = random().nextFloat() * 10;

    // Query with 20 of the random dense terms, meaning our term-in-set terms should cover 80,000
    // docs in total. First do this with the "common" lead term to ensure we run a postings-based
    // approach:
    List<BytesRef> queryTerms = denseTerms.subList(0, 20);
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    for (BytesRef term : queryTerms) {
      bq.add(new TermQuery(new Term("t", term)), Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(1);
    Query q1 = new ConstantScoreQuery(bq.build());

    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    bq.add(new TermInSetQuery("t", queryTerms), Occur.MUST);
    Query q2 = new ConstantScoreQuery(bq.build());

    assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

    // Query with 20 of the random sparse terms, meaning our term-in-set terms should cover 2,000
    // docs in total. Each of the terms should only have 100 docs, forcing our term-in-set
    // implementation to pre-process all the postings into a bitset up-front:
    queryTerms = sparseTerms.subList(0, 20);
    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    for (BytesRef term : queryTerms) {
      bq.add(new TermQuery(new Term("t", term)), Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(1);
    q1 = new ConstantScoreQuery(bq.build());

    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    bq.add(new TermInSetQuery("t", queryTerms), Occur.MUST);
    q2 = new ConstantScoreQuery(bq.build());

    assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

    // Query with a mix of sparse and dense terms. The 20 sparse terms should cover 2,000 docs
    // with each covering 100. These should all be pre-processed into a bitset. We then include
    // 5 dense terms (each covering 4,000), which should not be pre-processed up-front. The total
    // cost should be 22,000, which should still be low enough relative to the lead cost to force
    // a postings-based approach:
    queryTerms = new ArrayList<>(sparseTerms.subList(0, 20));
    queryTerms.addAll(denseTerms.subList(0, 5));
    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    for (BytesRef term : queryTerms) {
      bq.add(new TermQuery(new Term("t", term)), Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(1);
    q1 = new ConstantScoreQuery(bq.build());

    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "common")), Occur.MUST);
    bq.add(new TermInSetQuery("t", queryTerms), Occur.MUST);
    q2 = new ConstantScoreQuery(bq.build());

    assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

    // Now use a "rare" lead term with 50 sparse terms. Because there are only 20 "rare" docs, we
    // should use a dv-approach based on the number of terms alone:
    queryTerms = sparseTerms.subList(0, 50);
    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "rare")), Occur.MUST);
    for (BytesRef term : queryTerms) {
      bq.add(new TermQuery(new Term("t", term)), Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(1);
    q1 = new ConstantScoreQuery(bq.build());

    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "rare")), Occur.MUST);
    bq.add(new TermInSetQuery("t", queryTerms), Occur.MUST);
    q2 = new ConstantScoreQuery(bq.build());

    assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

    // Finally, run a similar query but with < 20 sparse terms to trigger a dv-approach based on
    // visiting some terms to evaluate an expected cost:
    queryTerms = denseTerms.subList(0, 18);
    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "rare")), Occur.MUST);
    for (BytesRef term : queryTerms) {
      bq.add(new TermQuery(new Term("t", term)), Occur.SHOULD);
    }
    bq.setMinimumNumberShouldMatch(1);
    q1 = new ConstantScoreQuery(bq.build());

    bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("lead", "rare")), Occur.MUST);
    bq.add(new TermInSetQuery("t", queryTerms), Occur.MUST);
    q2 = new ConstantScoreQuery(bq.build());

    assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

    reader.close();
    dir.close();
  }

  public void testDuel() throws IOException {
    final int iters = atLeast(2);
    final String leadField = "l";
    final String field = "f";
    final BytesRef[] leadTerms = {new BytesRef("a"), new BytesRef("b"), new BytesRef("c")};
    for (int iter = 0; iter < iters; ++iter) {
      final boolean indexDocValues = random().nextBoolean();
      final List<BytesRef> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(newBytesRef(value));
      }
      final List<BytesRef> denseTerms = new ArrayList<>();
      for (int i = 0; i < 5; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        denseTerms.add(newBytesRef(value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        int r = random().nextInt(10);
        BytesRef leadTerm;
        if (r < 1) { // 10% chance
          leadTerm = leadTerms[0];
        } else if (r < 4) { // 30% chance
          leadTerm = leadTerms[1];
        } else { // 60% chance
          leadTerm = leadTerms[2];
        }
        doc.add(new StringField(leadField, leadTerm, Store.NO));
        final BytesRef term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(field, term, Store.NO));
        final BytesRef denseTerm = denseTerms.get(random().nextInt(denseTerms.size()));
        doc.add(new StringField(field, denseTerm, Store.NO));
        if (indexDocValues) {
          doc.add(new SortedSetDocValuesField(field, term));
          doc.add(new SortedSetDocValuesField(field, denseTerm));
        }
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term(field, allTerms.get(0))));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      if (reader.numDocs() == 0) {
        // may occasionally happen if all documents got the same term
        IOUtils.close(reader, dir);
        continue;
      }

      for (int i = 0; i < 100; ++i) {
        final float boost = random().nextFloat() * 10;
        final int numQueryTerms =
            TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        List<BytesRef> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          List<BytesRef> candidateTerms;
          if (random().nextInt(10) < 3) {
            candidateTerms = denseTerms;
          } else {
            candidateTerms = allTerms;
          }
          queryTerms.add(candidateTerms.get(random().nextInt(candidateTerms.size())));
        }
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef t : queryTerms) {
          bq.add(new TermQuery(new Term(field, t)), Occur.SHOULD);
        }
        final Query q1 = new ConstantScoreQuery(bq.build());
        final Query q2 = new TermInSetQuery(field, queryTerms);
        assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);

        for (BytesRef lead : leadTerms) {
          bq = new BooleanQuery.Builder();
          bq.add(new TermQuery(new Term(leadField, lead)), Occur.MUST);
          bq.add(q1, Occur.MUST);
          final Query q3 = new ConstantScoreQuery(bq.build());

          bq = new BooleanQuery.Builder();
          bq.add(new TermQuery(new Term(leadField, lead)), Occur.MUST);
          bq.add(q2, Occur.MUST);
          final Query q4 = new ConstantScoreQuery(bq.build());

          assertSameMatches(searcher, new BoostQuery(q3, boost), new BoostQuery(q4, boost), true);
        }
      }

      reader.close();
      dir.close();
    }
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores)
      throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits.value, td2.totalHits.value);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    List<BytesRef> terms = new ArrayList<>();
    Set<BytesRef> uniqueTerms = new HashSet<>();
    for (int i = 0; i < num; i++) {
      String string = TestUtil.randomRealisticUnicodeString(random());
      terms.add(newBytesRef(string));
      uniqueTerms.add(newBytesRef(string));
      TermInSetQuery left = new TermInSetQuery("field", uniqueTerms);
      Collections.shuffle(terms, random());
      TermInSetQuery right = new TermInSetQuery("field", terms);
      assertEquals(right, left);
      assertEquals(right.hashCode(), left.hashCode());
      if (uniqueTerms.size() > 1) {
        List<BytesRef> asList = new ArrayList<>(uniqueTerms);
        asList.remove(0);
        TermInSetQuery notEqual = new TermInSetQuery("field", asList);
        assertFalse(left.equals(notEqual));
        assertFalse(right.equals(notEqual));
      }
    }

    TermInSetQuery tq1 = new TermInSetQuery("thing", newBytesRef("apple"));
    TermInSetQuery tq2 = new TermInSetQuery("thing", newBytesRef("orange"));
    assertFalse(tq1.hashCode() == tq2.hashCode());

    // different fields with the same term should have differing hashcodes
    tq1 = new TermInSetQuery("thing", newBytesRef("apple"));
    tq2 = new TermInSetQuery("thing2", newBytesRef("apple"));
    assertFalse(tq1.hashCode() == tq2.hashCode());
  }

  public void testSimpleEquals() {
    // Two terms with the same hash code
    assertEquals("AaAaBB".hashCode(), "BBBBBB".hashCode());
    TermInSetQuery left = new TermInSetQuery("id", newBytesRef("AaAaAa"), newBytesRef("AaAaBB"));
    TermInSetQuery right = new TermInSetQuery("id", newBytesRef("AaAaAa"), newBytesRef("BBBBBB"));
    assertFalse(left.equals(right));
  }

  public void testToString() {
    TermInSetQuery termsQuery =
        new TermInSetQuery("field1", newBytesRef("a"), newBytesRef("b"), newBytesRef("c"));
    assertEquals("field1:(a b c)", termsQuery.toString());
  }

  public void testDedup() {
    Query query1 = new TermInSetQuery("foo", newBytesRef("bar"));
    Query query2 = new TermInSetQuery("foo", newBytesRef("bar"), newBytesRef("bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testOrderDoesNotMatter() {
    // order of terms if different
    Query query1 = new TermInSetQuery("foo", newBytesRef("bar"), newBytesRef("baz"));
    Query query2 = new TermInSetQuery("foo", newBytesRef("baz"), newBytesRef("bar"));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testRamBytesUsed() {
    List<BytesRef> terms = new ArrayList<>();
    final int numTerms = 10000 + random().nextInt(1000);
    for (int i = 0; i < numTerms; ++i) {
      terms.add(newBytesRef(RandomStrings.randomUnicodeOfLength(random(), 10)));
    }
    TermInSetQuery query = new TermInSetQuery("f", terms);
    final long actualRamBytesUsed = RamUsageTester.ramUsed(query);
    final long expectedRamBytesUsed = query.ramBytesUsed();
    // error margin within 5%
    assertEquals(
        (double) expectedRamBytesUsed, (double) actualRamBytesUsed, actualRamBytesUsed / 20.d);
  }

  private static class TermsCountingDirectoryReaderWrapper extends FilterDirectoryReader {

    private final AtomicInteger counter;

    public TermsCountingDirectoryReaderWrapper(DirectoryReader in, AtomicInteger counter)
        throws IOException {
      super(in, new TermsCountingSubReaderWrapper(counter));
      this.counter = counter;
    }

    private static class TermsCountingSubReaderWrapper extends SubReaderWrapper {
      private final AtomicInteger counter;

      public TermsCountingSubReaderWrapper(AtomicInteger counter) {
        this.counter = counter;
      }

      @Override
      public LeafReader wrap(LeafReader reader) {
        return new TermsCountingLeafReaderWrapper(reader, counter);
      }
    }

    private static class TermsCountingLeafReaderWrapper extends FilterLeafReader {

      private final AtomicInteger counter;

      public TermsCountingLeafReaderWrapper(LeafReader in, AtomicInteger counter) {
        super(in);
        this.counter = counter;
      }

      @Override
      public Terms terms(String field) throws IOException {
        Terms terms = super.terms(field);
        if (terms == null) {
          return null;
        }
        return new FilterTerms(terms) {
          @Override
          public TermsEnum iterator() throws IOException {
            counter.incrementAndGet();
            return super.iterator();
          }
        };
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new TermsCountingDirectoryReaderWrapper(in, counter);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  public void testPullOneTermsEnum() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "1", Store.NO));
    w.addDocument(doc);
    DirectoryReader reader = w.getReader();
    w.close();
    final AtomicInteger counter = new AtomicInteger();
    DirectoryReader wrapped = new TermsCountingDirectoryReaderWrapper(reader, counter);

    final List<BytesRef> terms = new ArrayList<>();
    // enough terms to avoid the rewrite
    final int numTerms =
        TestUtil.nextInt(random(), TermInSetQuery.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD + 1, 100);
    for (int i = 0; i < numTerms; ++i) {
      final BytesRef term = newBytesRef(RandomStrings.randomUnicodeOfCodepointLength(random(), 10));
      terms.add(term);
    }

    assertEquals(0, new IndexSearcher(wrapped).count(new TermInSetQuery("bar", terms)));
    assertEquals(0, counter.get()); // missing field
    new IndexSearcher(wrapped).count(new TermInSetQuery("foo", terms));
    assertEquals(1, counter.get());
    wrapped.close();
    dir.close();
  }

  public void testBinaryToString() {
    TermInSetQuery query =
        new TermInSetQuery("field", newBytesRef(new byte[] {(byte) 0xff, (byte) 0xfe}));
    assertEquals("field:([ff fe])", query.toString());
  }

  public void testIsConsideredCostlyByQueryCache() throws IOException {
    TermInSetQuery query = new TermInSetQuery("foo", newBytesRef("bar"), newBytesRef("baz"));
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    assertFalse(policy.shouldCache(query));
    policy.onUse(query);
    policy.onUse(query);
    // cached after two uses
    assertTrue(policy.shouldCache(query));
  }

  public void testVisitor() {
    // singleton reports back to consumeTerms()
    TermInSetQuery singleton = new TermInSetQuery("field", newBytesRef("term1"));
    singleton.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            assertEquals(1, terms.length);
            assertEquals(new Term("field", newBytesRef("term1")), terms[0]);
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            fail("Singleton TermInSetQuery should not try to build ByteRunAutomaton");
          }
        });

    // multiple values built into automaton
    List<BytesRef> terms = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      terms.add(newBytesRef("term" + i));
    }
    TermInSetQuery t = new TermInSetQuery("field", terms);
    t.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            fail("TermInSetQuery with multiple terms should build automaton");
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            ByteRunAutomaton a = automaton.get();
            BytesRef test = newBytesRef("nonmatching");
            assertFalse(a.run(test.bytes, test.offset, test.length));
            for (BytesRef term : terms) {
              assertTrue(a.run(term.bytes, term.offset, term.length));
            }
          }
        });
  }
}
