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

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

@LuceneTestCase.SuppressCodecs(value = "SimpleText")
public class TestIndexSortSortedNumericDocValuesRangeQuery extends LuceneTestCase {

  public void testSameHitsAsPointRangeQuery() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      SortField sortField = new SortedNumericSortField("dv", SortField.Type.LONG, reverse);
      boolean enableMissingValue = random().nextBoolean();
      if (enableMissingValue) {
        long missingValue =
            random().nextBoolean()
                ? TestUtil.nextLong(random(), -100, 10000)
                : (random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE);
        sortField.setMissingValue(missingValue);
      }
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

  private static void assertSameHits(IndexSearcher searcher, Query q1, Query q2, boolean scores)
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
    SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
    sortField.setMissingValue(random().nextLong());
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
    Query rewrittenQuery = query.rewrite(reader);
    assertEquals(new FieldExistsQuery("field"), rewrittenQuery);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteFallbackQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    // Create an (unrealistic) fallback query that is sure to be rewritten.
    Query fallbackQuery = new BooleanQuery.Builder().build();
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field", 1, 42, fallbackQuery);

    Query rewrittenQuery = query.rewrite(reader);
    assertNotEquals(query, rewrittenQuery);
    assertThat(rewrittenQuery, instanceOf(IndexSortSortedNumericDocValuesRangeQuery.class));

    IndexSortSortedNumericDocValuesRangeQuery rangeQuery =
        (IndexSortSortedNumericDocValuesRangeQuery) rewrittenQuery;
    assertEquals(new MatchNoDocsQuery(), rangeQuery.getFallbackQuery());

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

  public void testOtherSortTypes() throws Exception {
    for (SortField.Type type : new SortField.Type[] {SortField.Type.FLOAT, SortField.Type.DOUBLE}) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      Sort indexSort = new Sort(new SortedNumericSortField("field", type));
      iwc.setIndexSort(indexSort);

      RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
      writer.addDocument(createDocument("field", 0));

      testIndexSortOptimizationDeactivated(writer);

      writer.close();
      dir.close();
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
    Query fallbackQuery = new MatchNoDocsQuery();
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
      SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
      boolean enableMissingValue = random().nextBoolean();
      if (enableMissingValue) {
        long missingValue =
            random().nextBoolean()
                ? TestUtil.nextLong(random(), -100, 10000)
                : (random().nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE);
        sortField.setMissingValue(missingValue);
      }
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
    SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
    boolean useLower = random().nextBoolean();
    long lowerValue = 1;
    long upperValue = 100;
    sortField.setMissingValue(useLower ? lowerValue : upperValue);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(
        createSNDVAndPointDocument("field", TestUtil.nextLong(random(), lowerValue, upperValue)));
    writer.addDocument(
        createSNDVAndPointDocument("field", TestUtil.nextLong(random(), lowerValue, upperValue)));
    // missingValue
    writer.addDocument(createMissingValueDocument());

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query fallbackQuery = LongPoint.newRangeQuery("field", lowerValue, upperValue);
    Query query =
        new IndexSortSortedNumericDocValuesRangeQuery(
            "field", lowerValue, upperValue, fallbackQuery);
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(2, weight.count(context));
    }

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
