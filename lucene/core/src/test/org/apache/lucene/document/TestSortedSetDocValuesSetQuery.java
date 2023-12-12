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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestSortedSetDocValuesSetQuery extends LuceneTestCase {

  public void testMissingTerms() throws Exception {
    String fieldName = "field1";
    Directory rd = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), rd);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int term = i * 10; // terms are units of 10;
      doc.add(newStringField(fieldName, "" + term, Field.Store.YES));
      doc.add(new SortedDocValuesField(fieldName, new BytesRef("" + term)));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();

    IndexSearcher searcher = newSearcher(reader);
    int numDocs = reader.numDocs();
    ScoreDoc[] results;

    List<BytesRef> terms = new ArrayList<>();
    terms.add(new BytesRef("5"));
    results =
        searcher.search(SortedDocValuesField.newSlowSetQuery(fieldName, terms), numDocs).scoreDocs;
    assertEquals("Must match nothing", 0, results.length);

    terms = new ArrayList<>();
    terms.add(new BytesRef("10"));
    results =
        searcher.search(SortedDocValuesField.newSlowSetQuery(fieldName, terms), numDocs).scoreDocs;
    assertEquals("Must match 1", 1, results.length);

    terms = new ArrayList<>();
    terms.add(new BytesRef("10"));
    terms.add(new BytesRef("20"));
    results =
        searcher.search(SortedDocValuesField.newSlowSetQuery(fieldName, terms), numDocs).scoreDocs;
    assertEquals("Must match 2", 2, results.length);

    reader.close();
    rd.close();
  }

  public void testEquals() {
    List<BytesRef> bar = new ArrayList<>();
    bar.add(new BytesRef("bar"));

    List<BytesRef> barbar = new ArrayList<>();
    barbar.add(new BytesRef("bar"));
    barbar.add(new BytesRef("bar"));

    List<BytesRef> barbaz = new ArrayList<>();
    barbaz.add(new BytesRef("bar"));
    barbaz.add(new BytesRef("baz"));

    List<BytesRef> bazbar = new ArrayList<>();
    bazbar.add(new BytesRef("baz"));
    bazbar.add(new BytesRef("bar"));

    List<BytesRef> baz = new ArrayList<>();
    baz.add(new BytesRef("baz"));

    assertEquals(
        SortedDocValuesField.newSlowSetQuery("foo", bar),
        SortedDocValuesField.newSlowSetQuery("foo", bar));
    assertEquals(
        SortedDocValuesField.newSlowSetQuery("foo", bar),
        SortedDocValuesField.newSlowSetQuery("foo", barbar));
    assertEquals(
        SortedDocValuesField.newSlowSetQuery("foo", barbaz),
        SortedDocValuesField.newSlowSetQuery("foo", bazbar));
    assertNotEquals(
        SortedDocValuesField.newSlowSetQuery("foo", bar),
        SortedDocValuesField.newSlowSetQuery("foo2", bar));
    assertNotEquals(
        SortedDocValuesField.newSlowSetQuery("foo", bar),
        SortedDocValuesField.newSlowSetQuery("foo", baz));
  }

  public void testDuelTermsQuery() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Term> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new Term("f", value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Term term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(term.field(), term.text(), Field.Store.NO));
        doc.add(new SortedDocValuesField(term.field(), new BytesRef(term.text())));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(allTerms.get(0)));
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
        List<Term> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Term term : queryTerms) {
          bq.add(new TermQuery(term), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);
        List<BytesRef> bytesTerms = new ArrayList<>();
        for (Term term : queryTerms) {
          bytesTerms.add(term.bytes());
        }
        final Query q2 =
            new BoostQuery(SortedDocValuesField.newSlowSetQuery("f", bytesTerms), boost);
        assertSameMatches(searcher, q1, q2, true);
      }

      reader.close();
      dir.close();
    }
  }

  public void testApproximation() throws IOException {
    final int iters = atLeast(2);
    for (int iter = 0; iter < iters; ++iter) {
      final List<Term> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(new Term("f", value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final Term term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(term.field(), term.text(), Field.Store.NO));
        doc.add(new SortedDocValuesField(term.field(), new BytesRef(term.text())));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(allTerms.get(0)));
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
        List<Term> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (Term term : queryTerms) {
          bq.add(new TermQuery(term), Occur.SHOULD);
        }
        Query q1 = new BoostQuery(new ConstantScoreQuery(bq.build()), boost);
        List<BytesRef> bytesTerms = new ArrayList<>();
        for (Term term : queryTerms) {
          bytesTerms.add(term.bytes());
        }
        final Query q2 =
            new BoostQuery(SortedDocValuesField.newSlowSetQuery("f", bytesTerms), boost);

        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(q1, Occur.MUST);
        bq1.add(new TermQuery(allTerms.get(0)), Occur.FILTER);

        BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        bq2.add(q2, Occur.MUST);
        bq2.add(new TermQuery(allTerms.get(0)), Occur.FILTER);

        assertSameMatches(searcher, bq1.build(), bq2.build(), true);
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
}
