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
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NumericUtils;
import org.hamcrest.MatcherAssert;

@LuceneTestCase.SuppressCodecs(value = "SimpleText")
public class TestIndexSortSortedNumericDocValuesRangeQuery extends LuceneTestCase {

  public void testSameHitsAsPointRangeQuery() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      boolean enableMissingValue = random().nextBoolean();
      Long missingValue = null;
      if (enableMissingValue) {
        missingValue =
            random().nextBoolean()
                ? TestUtil.nextLong(random(), -100, 10000)
                : (random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE);
      }
      SortField sortField =
          new SortedNumericSortField(
              "dv", SortField.Type.LONG, reverse, SortedNumericSelector.Type.MIN, missingValue);
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final long value = TestUtil.nextLong(random(), -100, 10000);
          doc.add(new SortedNumericDocValuesField("dv", value));
          doc.add(new LongPoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(LongPoint.newRangeQuery("idx", 0L, 10L));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final long min =
            random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final long max =
            random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final Query q1 = LongPoint.newRangeQuery("idx", min, max);
        final Query q2 = createQuery("dv", min, max);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  public void testSameHitsAsPointRangeQueryIntSort() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      boolean enableMissingValue = random().nextBoolean();
      Integer missingValue = null;
      if (enableMissingValue) {
        missingValue =
            random().nextBoolean()
                ? TestUtil.nextInt(random(), -100, 10000)
                : (random().nextBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE);
      }
      SortField sortField =
          new SortedNumericSortField(
              "dv", SortField.Type.INT, reverse, SortedNumericSelector.Type.MIN, missingValue);
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final int value = TestUtil.nextInt(random(), -100, 10000);
          doc.add(new SortedNumericDocValuesField("dv", value));
          doc.add(new IntPoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(IntPoint.newRangeQuery("idx", 0, 10));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final int min =
            random().nextBoolean() ? Integer.MIN_VALUE : TestUtil.nextInt(random(), -100, 10000);
        final int max =
            random().nextBoolean() ? Integer.MAX_VALUE : TestUtil.nextInt(random(), -100, 10000);
        final Query q1 = IntPoint.newRangeQuery("idx", min, max);
        final Query q2 = createQuery("dv", min, max);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  private static void assertSameHits(IndexSearcher searcher, Query q1, Query q2, boolean scores)
      throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits.value(), td2.totalHits.value());
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  public void testEquals() {
    Query q1 = createQuery("foo", 3, 5);
    QueryUtils.checkEqual(q1, createQuery("foo", 3, 5));
    QueryUtils.checkUnequal(q1, createQuery("foo", 3, 6));
    QueryUtils.checkUnequal(q1, createQuery("foo", 4, 5));
    QueryUtils.checkUnequal(q1, createQuery("bar", 3, 5));
  }

  public void testToString() {
    Query q1 = createQuery("foo", 3, 5);
    assertEquals("foo:[3 TO 5]", q1.toString());
    assertEquals("[3 TO 5]", q1.toString("foo"));
    assertEquals("foo:[3 TO 5]", q1.toString("bar"));
  }

  public void testIndexSortDocValuesWithEvenLength() throws Exception {
    for (SortField.Type type : new SortField.Type[] {SortField.Type.INT, SortField.Type.LONG}) {
      testIndexSortDocValuesWithEvenLength(true, type);
      testIndexSortDocValuesWithEvenLength(false, type);
    }
  }

  public void testIndexSortDocValuesWithEvenLength(boolean reverse, SortField.Type type)
      throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", type, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertNumberOfHits(searcher, createQuery("field", -80, -80), 1);
    assertNumberOfHits(searcher, createQuery("field", -5, -5), 1);
    assertNumberOfHits(searcher, createQuery("field", 0, 0), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 30), 1);
    assertNumberOfHits(searcher, createQuery("field", 35, 35), 1);

    assertNumberOfHits(searcher, createQuery("field", -90, -90), 0);
    assertNumberOfHits(searcher, createQuery("field", 5, 5), 0);
    assertNumberOfHits(searcher, createQuery("field", 40, 40), 0);

    // Test the lower end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", -90, -4), 2);
    assertNumberOfHits(searcher, createQuery("field", -80, -4), 2);
    assertNumberOfHits(searcher, createQuery("field", -70, -4), 1);
    assertNumberOfHits(searcher, createQuery("field", -80, -5), 2);

    // Test the upper end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", 25, 34), 1);
    assertNumberOfHits(searcher, createQuery("field", 25, 35), 2);
    assertNumberOfHits(searcher, createQuery("field", 25, 36), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 35), 2);

    // Test multiple occurrences of the same value.
    assertNumberOfHits(searcher, createQuery("field", -4, 4), 2);
    assertNumberOfHits(searcher, createQuery("field", -4, 0), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 4), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 30), 3);

    // Test ranges that span all documents.
    assertNumberOfHits(searcher, createQuery("field", -80, 35), 6);
    assertNumberOfHits(searcher, createQuery("field", -90, 40), 6);

    writer.close();
    reader.close();
    dir.close();
  }

  private static void assertNumberOfHits(IndexSearcher searcher, Query query, int numberOfHits)
      throws IOException {
    assertEquals(
        numberOfHits,
        searcher.search(query, DummyTotalHitCountCollector.createManager()).intValue());
    assertEquals(numberOfHits, searcher.count(query));
  }

  public void testIndexSortDocValuesWithOddLength() throws Exception {
    testIndexSortDocValuesWithOddLength(false);
    testIndexSortDocValuesWithOddLength(true);
  }

  public void testIndexSortDocValuesWithOddLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 5));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertNumberOfHits(searcher, createQuery("field", -80, -80), 1);
    assertNumberOfHits(searcher, createQuery("field", -5, -5), 1);
    assertNumberOfHits(searcher, createQuery("field", 0, 0), 2);
    assertNumberOfHits(searcher, createQuery("field", 5, 5), 1);
    assertNumberOfHits(searcher, createQuery("field", 30, 30), 1);
    assertNumberOfHits(searcher, createQuery("field", 35, 35), 1);

    assertNumberOfHits(searcher, createQuery("field", -90, -90), 0);
    assertNumberOfHits(searcher, createQuery("field", 6, 6), 0);
    assertNumberOfHits(searcher, createQuery("field", 40, 40), 0);

    // Test the lower end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", -90, -4), 2);
    assertNumberOfHits(searcher, createQuery("field", -80, -4), 2);
    assertNumberOfHits(searcher, createQuery("field", -70, -4), 1);
    assertNumberOfHits(searcher, createQuery("field", -80, -5), 2);

    // Test the upper end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", 25, 34), 1);
    assertNumberOfHits(searcher, createQuery("field", 25, 35), 2);
    assertNumberOfHits(searcher, createQuery("field", 25, 36), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 35), 2);

    // Test multiple occurrences of the same value.
    assertNumberOfHits(searcher, createQuery("field", -4, 4), 2);
    assertNumberOfHits(searcher, createQuery("field", -4, 0), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 4), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 30), 4);

    // Test ranges that span all documents.
    assertNumberOfHits(searcher, createQuery("field", -80, 35), 7);
    assertNumberOfHits(searcher, createQuery("field", -90, 40), 7);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortDocValuesWithSingleValue() throws Exception {
    testIndexSortDocValuesWithSingleValue(false);
    testIndexSortDocValuesWithSingleValue(true);
  }

  private void testIndexSortDocValuesWithSingleValue(boolean reverse) throws IOException {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", 42));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertNumberOfHits(searcher, createQuery("field", 42, 43), 1);
    assertNumberOfHits(searcher, createQuery("field", 42, 42), 1);
    assertNumberOfHits(searcher, createQuery("field", 41, 41), 0);
    assertNumberOfHits(searcher, createQuery("field", 43, 43), 0);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortMissingValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField =
        new SortedNumericSortField(
            "field",
            SortField.Type.LONG,
            false,
            SortedNumericSelector.Type.MIN,
            random().nextLong());
    iwc.setIndexSort(new Sort(sortField));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 35));

    writer.addDocument(createDocument("other-field", 0));
    writer.addDocument(createDocument("other-field", 10));
    writer.addDocument(createDocument("other-field", 20));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertNumberOfHits(searcher, createQuery("field", -70, 0), 2);
    assertNumberOfHits(searcher, createQuery("field", -2, 35), 2);

    assertNumberOfHits(searcher, createQuery("field", -80, 35), 4);
    assertNumberOfHits(searcher, createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE), 4);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testNoDocuments() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Query query = createQuery("foo", 2, 4);
    Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
    assertNull(w.scorer(searcher.getIndexReader().leaves().get(0)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteExhaustiveRange() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    Query query = createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE);
    Query rewrittenQuery = query.rewrite(newSearcher(reader));
    assertEquals(new FieldExistsQuery("field"), rewrittenQuery);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteFallbackToMatchNone() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    // An empty BooleanQuery rewrites to a MatchNoDocsQuery; the wrapper matches the same docs as
    // its
    // fallback, so it collapses to that MatchNoDocsQuery instead of re-wrapping it.
    Query fallbackQuery = new BooleanQuery.Builder().build();
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field", 1, 42, fallbackQuery);
    assertEquals(new MatchNoDocsQuery(), query.rewrite(newSearcher(reader)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteFallbackReWrapped() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    // A fallback that rewrites to a non-terminal query (BoostQuery with boost 1 unwraps to its
    // inner query) keeps the index-sort wrapper, now wrapping the rewritten fallback.
    TermQuery inner = new TermQuery(new Term("field", "x"));
    Query fallbackQuery = new BoostQuery(inner, 1.0f);
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field", 1, 42, fallbackQuery);

    Query rewrittenQuery = query.rewrite(newSearcher(reader));
    MatcherAssert.assertThat(
        rewrittenQuery, instanceOf(IndexSortSortedNumericDocValuesRangeQuery.class));
    assertEquals(
        inner, ((IndexSortSortedNumericDocValuesRangeQuery) rewrittenQuery).getFallbackQuery());

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteFallbackToFieldExists() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    // Indexed (skip-indexed) sorted-numeric field on all docs but one, so the field is sparse and
    // its values span [100, 108].
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      if (i != 5) {
        doc.add(SortedNumericDocValuesField.indexedField("field", 100 + i));
      }
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();

    // A range covering every value of the sparse field: the fallback rewrites to a
    // FieldExistsQuery,
    // which the index-sort query must return directly rather than re-wrapping.
    Query fallbackQuery = SortedNumericDocValuesField.newSlowRangeQuery("field", 0, 250);
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field", 0, 250, fallbackQuery);
    assertEquals(new FieldExistsQuery("field"), query.rewrite(newSearcher(reader)));

    writer.close();
    reader.close();
    dir.close();
  }

  /** Test that the index sort optimization not activated if there is no index sort. */
  public void testNoIndexSort() throws Exception {
    Directory dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(createDocument("field", 0));

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  /** Test that the index sort optimization is not activated when the sort is on the wrong field. */
  public void testIndexSortOnWrongField() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("other-field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    writer.addDocument(createDocument("field", 0));

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  public void testSameHitsAsPointRangeQueryFloatSort() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      boolean enableMissingValue = random().nextBoolean();
      Float missingValue = null;
      if (enableMissingValue) {
        missingValue =
            random().nextBoolean()
                ? (float) TestUtil.nextInt(random(), -100, 10000)
                : (random().nextBoolean() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
      }
      SortField sortField =
          new SortedNumericSortField(
              "dv", SortField.Type.FLOAT, reverse, SortedNumericSelector.Type.MIN, missingValue);
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final float value = TestUtil.nextInt(random(), -100, 10000) + 0.5f;
          doc.add(new SortedNumericDocValuesField("dv", NumericUtils.floatToSortableInt(value)));
          doc.add(new FloatPoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(FloatPoint.newRangeQuery("idx", 0f, 10f));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final float min =
            random().nextBoolean()
                ? Float.NEGATIVE_INFINITY
                : TestUtil.nextInt(random(), -100, 10000) + 0.5f;
        final float max =
            random().nextBoolean()
                ? Float.POSITIVE_INFINITY
                : TestUtil.nextInt(random(), -100, 10000) + 0.5f;
        final Query q1 = FloatPoint.newRangeQuery("idx", min, max);
        final Query q2 = createFloatQuery("dv", min, max);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  public void testSameHitsAsPointRangeQueryDoubleSort() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      boolean enableMissingValue = random().nextBoolean();
      Double missingValue = null;
      if (enableMissingValue) {
        missingValue =
            random().nextBoolean()
                ? (double) TestUtil.nextLong(random(), -100, 10000)
                : (random().nextBoolean() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
      }
      SortField sortField =
          new SortedNumericSortField(
              "dv", SortField.Type.DOUBLE, reverse, SortedNumericSelector.Type.MIN, missingValue);
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final double value = TestUtil.nextLong(random(), -100, 10000) + 0.5d;
          doc.add(new SortedNumericDocValuesField("dv", NumericUtils.doubleToSortableLong(value)));
          doc.add(new DoublePoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(DoublePoint.newRangeQuery("idx", 0d, 10d));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final double min =
            random().nextBoolean()
                ? Double.NEGATIVE_INFINITY
                : TestUtil.nextLong(random(), -100, 10000) + 0.5d;
        final double max =
            random().nextBoolean()
                ? Double.POSITIVE_INFINITY
                : TestUtil.nextLong(random(), -100, 10000) + 0.5d;
        final Query q1 = DoublePoint.newRangeQuery("idx", min, max);
        final Query q2 = createDoubleQuery("dv", min, max);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  /**
   * Verifies that float and double index sorts, indexed with points on the same field (as {@link
   * FloatField}/{@link DoubleField} do), activate the optimization end to end and produce the same
   * hits and counts as the equivalent point range query.
   */
  public void testFloatAndDoubleFieldSameHits() throws IOException {
    for (boolean isFloat : new boolean[] {true, false}) {
      for (boolean reverse : new boolean[] {false, true}) {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        SortField sortField =
            isFloat
                ? FloatField.newSortField("field", reverse, SortedNumericSelector.Type.MIN)
                : DoubleField.newSortField("field", reverse, SortedNumericSelector.Type.MIN);
        iwc.setIndexSort(new Sort(sortField));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        final int numDocs = atLeast(100);
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          double value = TestUtil.nextInt(random(), -100, 10000) + 0.25d;
          doc.add(
              isFloat
                  ? new FloatField("field", (float) value, Field.Store.NO)
                  : new DoubleField("field", value, Field.Store.NO));
          iw.addDocument(doc);
        }
        iw.forceMerge(1);
        IndexReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        for (int i = 0; i < 100; ++i) {
          double a = TestUtil.nextInt(random(), -110, 10010) + 0.25d;
          double b = TestUtil.nextInt(random(), -110, 10010) + 0.25d;
          double min = Math.min(a, b);
          double max = Math.max(a, b);
          Query q1;
          Query q2;
          if (isFloat) {
            q1 = FloatPoint.newRangeQuery("field", (float) min, (float) max);
            q2 = FloatField.newRangeQuery("field", (float) min, (float) max);
          } else {
            q1 = DoublePoint.newRangeQuery("field", min, max);
            q2 = DoubleField.newRangeQuery("field", min, max);
          }
          assertEquals(searcher.count(q1), searcher.count(q2));
          assertSameHits(searcher, q1, q2, false);
        }

        reader.close();
        dir.close();
      }
    }
  }

  /**
   * Verifies support for single-valued {@link org.apache.lucene.index.NumericDocValues} float and
   * double fields, which store raw IEEE-754 bits (unlike {@code SortedNumericDocValues} fields,
   * which store the sortable encoding). Bounds are passed in that same raw-bits space. A parallel
   * points field ("pt") acts as both the oracle and a correct fallback; optionally points are also
   * indexed on the doc-values field itself to exercise the BKD path.
   */
  public void testNumericDocValuesFloatAndDouble() throws IOException {
    for (boolean isFloat : new boolean[] {true, false}) {
      for (boolean pointsOnDvField : new boolean[] {false, true}) {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        SortField.Type type = isFloat ? SortField.Type.FLOAT : SortField.Type.DOUBLE;
        // Plain SortField (not SortedNumeric) over single-valued NumericDocValues (raw bits).
        iwc.setIndexSort(new Sort(new SortField("dv", type, random().nextBoolean())));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        final int numDocs = atLeast(100);
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          double value = TestUtil.nextInt(random(), -100, 10000) + 0.5d;
          if (isFloat) {
            doc.add(new FloatDocValuesField("dv", (float) value));
            doc.add(new FloatPoint("pt", (float) value));
            if (pointsOnDvField) doc.add(new FloatPoint("dv", (float) value));
          } else {
            doc.add(new DoubleDocValuesField("dv", value));
            doc.add(new DoublePoint("pt", value));
            if (pointsOnDvField) doc.add(new DoublePoint("dv", value));
          }
          iw.addDocument(doc);
        }
        iw.forceMerge(1);
        IndexReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        for (int i = 0; i < 100; ++i) {
          double a = TestUtil.nextInt(random(), -110, 10010) + 0.5d;
          double b = TestUtil.nextInt(random(), -110, 10010) + 0.5d;
          double min = Math.min(a, b);
          double max = Math.max(a, b);
          Query oracle; // range over the parallel "pt" points field
          Query q; // uses the float/double constructor, which converts to sortable longs
          if (isFloat) {
            oracle = FloatPoint.newRangeQuery("pt", (float) min, (float) max);
            q =
                new IndexSortSortedNumericDocValuesRangeQuery(
                    "dv", (float) min, (float) max, oracle);
          } else {
            oracle = DoublePoint.newRangeQuery("pt", min, max);
            q = new IndexSortSortedNumericDocValuesRangeQuery("dv", min, max, oracle);
          }
          assertSameHits(searcher, oracle, q, false);
          assertEquals(
              "count pointsOnDvField=" + pointsOnDvField,
              searcher.count(oracle),
              searcher.count(q));
        }

        reader.close();
        dir.close();
      }
    }
  }

  /**
   * Verifies that the scorer path (not just {@code count()}) actually engages the optimization for
   * float and double sorts and returns the right documents. A live optimization produces a plain
   * iterator with no two-phase iterator, whereas the {@code SortedNumericDocValuesField} fallback
   * always exposes a two-phase iterator, so the two are distinguishable. Both the binary-search
   * scorer path (no points) and the BKD scorer path (points on the field) are covered.
   */
  public void testFloatDoubleScorerUsesOptimization() throws Exception {
    for (boolean isFloat : new boolean[] {true, false}) {
      for (boolean withPoints : new boolean[] {false, true}) {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        SortField.Type type = isFloat ? SortField.Type.FLOAT : SortField.Type.DOUBLE;
        iwc.setIndexSort(new Sort(new SortedNumericSortField("dv", type)));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        double[] vals = {-80.5, -5.25, 0.5, 2.5, 30.5, 35.5};
        for (double v : vals) {
          Document doc = new Document();
          if (isFloat) {
            doc.add(
                new SortedNumericDocValuesField("dv", NumericUtils.floatToSortableInt((float) v)));
            if (withPoints) doc.add(new FloatPoint("dv", (float) v));
          } else {
            doc.add(new SortedNumericDocValuesField("dv", NumericUtils.doubleToSortableLong(v)));
            if (withPoints) doc.add(new DoublePoint("dv", v));
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        DirectoryReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);
        writer.close();

        // Range [0, 31] matches 0.5, 2.5 and 30.5 -> 3 docs.
        Query q = isFloat ? createFloatQuery("dv", 0f, 31f) : createDoubleQuery("dv", 0d, 31d);
        Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        int totalHits = 0;
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
          Scorer scorer = weight.scorer(ctx);
          if (scorer == null) {
            continue;
          }
          assertNull(
              "optimization should be active in the scorer (isFloat="
                  + isFloat
                  + ", withPoints="
                  + withPoints
                  + ")",
              scorer.twoPhaseIterator());
          DocIdSetIterator it = scorer.iterator();
          for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
            totalHits++;
          }
        }
        assertEquals(3, totalHits);

        reader.close();
        dir.close();
      }
    }
  }

  /**
   * Test that the index sort optimization is not activated when some documents have multiple
   * values.
   */
  public void testMultiDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 0));
    doc.add(new SortedNumericDocValuesField("field", 10));
    writer.addDocument(doc);

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  public void testIndexSortOptimizationDeactivated(RandomIndexWriter writer) throws IOException {
    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query query = createQuery("field", 0, 0);
    Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);

    // Check that the two-phase iterator is not null, indicating that we've fallen
    // back to SortedNumericDocValuesField.newSlowRangeQuery.
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(context);
      assertNotNull(scorer.twoPhaseIterator());
    }

    reader.close();
  }

  public void testFallbackCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 10));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // we use an unrealistic query that exposes its own Weight#count
    Query fallbackQuery = MatchNoDocsQuery.INSTANCE;
    // the index is not sorted on this field, the fallback query is used
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("another", 1, 42, fallbackQuery);
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(0, weight.count(context));
    }

    writer.close();
    reader.close();
    dir.close();
  }

  public void testCompareCount() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean enableMissingValue = random().nextBoolean();
      Long missingValue = null;
      if (enableMissingValue) {
        missingValue =
            random().nextBoolean()
                ? TestUtil.nextLong(random(), -100, 10000)
                : (random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE);
      }
      SortField sortField =
          new SortedNumericSortField(
              "field", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN, missingValue);
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final long value = TestUtil.nextLong(random(), -100, 10000);
          doc = createSNDVAndPointDocument("field", value);
        }
        writer.addDocument(doc);
      }

      if (random().nextBoolean()) {
        writer.deleteDocuments(LongPoint.newRangeQuery("field", 0L, 10L));
      }

      final IndexReader reader = writer.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      writer.close();

      for (int i = 0; i < 100; ++i) {
        final long min =
            random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final long max =
            random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final Query q1 = LongPoint.newRangeQuery("field", min, max);

        final Query fallbackQuery = LongPoint.newRangeQuery("field", min, max);
        final Query q2 =
            new IndexSortSortedNumericDocValuesRangeQuery("field", min, max, fallbackQuery);
        final Weight weight1 = q1.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        final Weight weight2 = q2.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        assertSameCount(weight1, weight2, searcher);
      }

      reader.close();
      dir.close();
    }
  }

  private void assertSameCount(Weight weight1, Weight weight2, IndexSearcher searcher)
      throws IOException {
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(weight1.count(context), weight2.count(context));
    }
  }

  public void testCountBoundary() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    boolean useLower = random().nextBoolean();
    long lowerValue = 1;
    long upperValue = 100;
    SortField sortField =
        new SortedNumericSortField(
            "field",
            SortField.Type.LONG,
            false,
            SortedNumericSelector.Type.MIN,
            useLower ? lowerValue : upperValue);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(
        createSNDVAndPointDocument("field", random().nextLong(lowerValue, upperValue)));
    writer.addDocument(
        createSNDVAndPointDocument("field", random().nextLong(lowerValue, upperValue)));
    // missingValue
    writer.addDocument(createMissingValueDocument());

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query fallbackQuery = LongPoint.newRangeQuery("field", lowerValue, upperValue);
    Query query =
        new IndexSortSortedNumericDocValuesRangeQuery(
            "field", lowerValue, upperValue, fallbackQuery);
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    int count = 0;
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      count += weight.count(context);
    }
    assertEquals(2, count);

    writer.close();
    reader.close();
    dir.close();
  }

  private Document createMissingValueDocument() {
    Document doc = new Document();
    doc.add(new StringField("foo", "fox", Field.Store.YES));
    return doc;
  }

  private Document createSNDVAndPointDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    doc.add(new LongPoint(field, value));
    return doc;
  }

  private Document createDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    return doc;
  }

  private Query createQuery(String field, long lowerValue, long upperValue) {
    Query fallbackQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  private Query createFloatQuery(String field, float lowerValue, float upperValue) {
    Query fallbackQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(
            field,
            NumericUtils.floatToSortableInt(lowerValue),
            NumericUtils.floatToSortableInt(upperValue));
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  private Query createDoubleQuery(String field, double lowerValue, double upperValue) {
    Query fallbackQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(
            field,
            NumericUtils.doubleToSortableLong(lowerValue),
            NumericUtils.doubleToSortableLong(upperValue));
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  public void testCountWithBkdAsc() throws Exception {
    doTestCountWithBkd(false);
  }

  public void testCountWithBkdDesc() throws Exception {
    doTestCountWithBkd(true);
  }

  public void doTestCountWithBkd(boolean reverse) throws Exception {
    String filedName = "field";
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField(filedName, SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    addDocWithBkd(writer, filedName, 7, 500);
    addDocWithBkd(writer, filedName, 5, 600);
    addDocWithBkd(writer, filedName, 11, 700);
    addDocWithBkd(writer, filedName, 13, 800);
    addDocWithBkd(writer, filedName, 9, 900);
    writer.flush();
    writer.forceMerge(1);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Both bounds exist in the dataset
    Query fallbackQuery = LongPoint.newRangeQuery(filedName, 7, 9);
    Query query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 7, 9, fallbackQuery);
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Both bounds do not exist in the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 6, 10);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 6, 10, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound exists in the dataset, not the max
    fallbackQuery = LongPoint.newRangeQuery(filedName, 7, 10);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 7, 10, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound doesn't exist in the dataset, max does
    fallbackQuery = LongPoint.newRangeQuery(filedName, 6, 9);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 6, 9, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound is the min value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 5, 8);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 5, 8, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1100, weight.count(context));
    }

    // Min bound is less than min value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 4, 8);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 4, 8, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1100, weight.count(context));
    }

    // Max bound is the max value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 10, 13);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 10, 13, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1500, weight.count(context));
    }

    // Max bound is greater than max value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 10, 14);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 10, 14, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1500, weight.count(context));
    }

    // Everything matches
    fallbackQuery = LongPoint.newRangeQuery(filedName, 2, 14);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 2, 14, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(3500, weight.count(context));
    }

    // Bounds equal to min/max values of the dataset, everything matches
    fallbackQuery = LongPoint.newRangeQuery(filedName, 2, 14);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 2, 14, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(3500, weight.count(context));
    }

    // Bounds are less than the min value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 2, 3);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 2, 3, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(0, weight.count(context));
    }

    // Bounds are greater than the max value of the dataset
    fallbackQuery = LongPoint.newRangeQuery(filedName, 14, 15);
    query = new IndexSortSortedNumericDocValuesRangeQuery(filedName, 14, 15, fallbackQuery);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(0, weight.count(context));
    }

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRandomCountWithBkdAsc() throws Exception {
    doTestRandomCountWithBkd(false);
  }

  public void testRandomCountWithBkdDesc() throws Exception {
    doTestRandomCountWithBkd(true);
  }

  private void doTestRandomCountWithBkd(boolean reverse) throws Exception {
    String filedName = "field";
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField(filedName, SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Random random = random();
    for (int i = 0; i < 100; i++) {
      addDocWithBkd(writer, filedName, random.nextInt(1000), random.nextInt(1000));
    }
    writer.flush();
    writer.forceMerge(1);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    for (int i = 0; i < 100; i++) {
      int random1 = random.nextInt(1100);
      int random2 = random.nextInt(1100);
      int low = Math.min(random1, random2);
      int upper = Math.max(random1, random2);
      Query rangeQuery = LongPoint.newRangeQuery(filedName, low, upper);
      Query indexSortRangeQuery =
          new IndexSortSortedNumericDocValuesRangeQuery(filedName, low, upper, rangeQuery);
      Weight indexSortRangeQueryWeight =
          indexSortRangeQuery.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
      Weight rangeQueryWeight = rangeQuery.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
      for (LeafReaderContext context : searcher.getLeafContexts()) {
        assertEquals(rangeQueryWeight.count(context), indexSortRangeQueryWeight.count(context));
      }
    }

    writer.close();
    reader.close();
    dir.close();
  }

  /**
   * Verifies that the binary-search optimization activates when the primary index sort field has a
   * single constant value (detected as a no-op via its skip index), promoting the secondary field
   * to effective primary and enabling direct doc-ID range iteration without a two-phase iterator.
   */
  public void testNoOpPrimarySort() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort =
        new Sort(
            new SortField("field1", SortField.Type.LONG), // constant → no-op via skipper
            new SortField("field2", SortField.Type.LONG)); // varying → effective primary
    iwc.setIndexSort(indexSort);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field1", 42)); // constant; skip index present
      doc.add(NumericDocValuesField.indexedField("field2", i)); // varying
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    DirectoryReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // field1 has a single distinct value, so getPrimarySortField skips it and returns field2.
    // The binary-search optimization should activate for the query on field2, producing a plain
    // range iterator with no two-phase iterator.
    Query fallback = SortedNumericDocValuesField.newSlowRangeQuery("field2", 3, 7);
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field2", 3, 7, fallback);
    Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(context);
      assertNotNull(scorer);
      assertNull(
          "binary-search optimization should be active when primary sort is a no-op",
          scorer.twoPhaseIterator());
    }

    reader.close();
    dir.close();
  }

  /**
   * Verifies that the binary-search optimization activates when the primary index sort field has no
   * values in the segment at all (detected as a no-op via FieldInfos), promoting the secondary
   * field to effective primary.
   */
  public void testMissingPrimarySort() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort =
        new Sort(
            new SortField("field1", SortField.Type.LONG), // absent → no-op via FieldInfos check
            new SortField("field2", SortField.Type.LONG)); // varying → effective primary
    iwc.setIndexSort(indexSort);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      // field1 intentionally absent: no FieldInfo in segment → detected as no-op
      doc.add(NumericDocValuesField.indexedField("field2", i));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    DirectoryReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // field1 has no FieldInfo, so getPrimarySortField skips it and returns field2.
    // The binary-search optimization should activate for the query on field2.
    Query fallback = SortedNumericDocValuesField.newSlowRangeQuery("field2", 3, 7);
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field2", 3, 7, fallback);
    Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(context);
      assertNotNull(scorer);
      assertNull(
          "binary-search optimization should be active when primary sort field has no values",
          scorer.twoPhaseIterator());
    }

    reader.close();
    dir.close();
  }

  private void addDocWithBkd(RandomIndexWriter indexWriter, String field, long value, int repeat)
      throws IOException {
    for (int i = 0; i < repeat; i++) {
      Document doc = new Document();
      doc.add(new SortedNumericDocValuesField(field, value));
      doc.add(new LongPoint(field, value));
      indexWriter.addDocument(doc);
    }
  }
}
