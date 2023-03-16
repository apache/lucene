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
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
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
import org.apache.lucene.util.NumericUtils;

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
        final Query q2 = createQuery("dv", min, max, SortField.Type.LONG);
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
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      Query q1 = createQuery("foo", 3, 5, type);
      QueryUtils.checkEqual(q1, createQuery("foo", 3, 5, type));
      QueryUtils.checkUnequal(q1, createQuery("foo", 3, 6, type));
      QueryUtils.checkUnequal(q1, createQuery("foo", 4, 5, type));
      QueryUtils.checkUnequal(q1, createQuery("bar", 3, 5, type));
    }
  }

  public void testToString() {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      Query q1 = createQuery("foo", 3, 5, type);
      if (type == SortField.Type.LONG || type == SortField.Type.INT) {
        assertEquals("foo:[3 TO 5]", q1.toString());
        assertEquals("[3 TO 5]", q1.toString("foo"));
        assertEquals("foo:[3 TO 5]", q1.toString("bar"));
      } else if (type == SortField.Type.FLOAT) {
        assertEquals("foo:[3.0 TO 5.0]", q1.toString());
        assertEquals("[3.0 TO 5.0]", q1.toString("foo"));
        assertEquals("foo:[3.0 TO 5.0]", q1.toString("bar"));
      } else {
        // assume DOUBLE for the other case
        assertEquals("foo:[3.0 TO 5.0]", q1.toString());
        assertEquals("[3.0 TO 5.0]", q1.toString("foo"));
        assertEquals("foo:[3.0 TO 5.0]", q1.toString("bar"));
      }
    }
  }

  public void testIndexSortDocValuesWithEvenLength() throws Exception {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      testIndexSortDocValuesWithEvenLength(true, type);
      testIndexSortDocValuesWithEvenLength(false, type);
    }
  }

  public void testIndexSortDocValuesWithEvenLength(boolean reverse, SortField.Type type)
      throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = createIndexSort("field", type, reverse);
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80, type));
    writer.addDocument(createDocument("field", -5, type));
    writer.addDocument(createDocument("field", 0, type));
    writer.addDocument(createDocument("field", 0, type));
    writer.addDocument(createDocument("field", 30, type));
    writer.addDocument(createDocument("field", 35, type));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertNumberOfHits(searcher, createQuery("field", -80, -80, type), 1);
    assertNumberOfHits(searcher, createQuery("field", -5, -5, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 0, 0, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 30, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 35, 35, type), 1);

    assertNumberOfHits(searcher, createQuery("field", -90, -90, type), 0);
    assertNumberOfHits(searcher, createQuery("field", 5, 5, type), 0);
    assertNumberOfHits(searcher, createQuery("field", 40, 40, type), 0);

    // Test the lower end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", -90, -4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -80, -4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -70, -4, type), 1);
    assertNumberOfHits(searcher, createQuery("field", -80, -5, type), 2);

    // Test the upper end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", 25, 34, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 25, 35, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 25, 36, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 35, type), 2);

    // Test multiple occurrences of the same value.
    assertNumberOfHits(searcher, createQuery("field", -4, 4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -4, 0, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 30, type), 3);

    // Test ranges that span all documents.
    assertNumberOfHits(searcher, createQuery("field", -80, 35, type), 6);
    assertNumberOfHits(searcher, createQuery("field", -90, 40, type), 6);

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
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      testIndexSortDocValuesWithOddLength(false, type);
      testIndexSortDocValuesWithOddLength(true, type);
    }
  }

  public void testIndexSortDocValuesWithOddLength(boolean reverse, SortField.Type type)
      throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80, type));
    writer.addDocument(createDocument("field", -5, type));
    writer.addDocument(createDocument("field", 0, type));
    writer.addDocument(createDocument("field", 0, type));
    writer.addDocument(createDocument("field", 5, type));
    writer.addDocument(createDocument("field", 30, type));
    writer.addDocument(createDocument("field", 35, type));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertNumberOfHits(searcher, createQuery("field", -80, -80, type), 1);
    assertNumberOfHits(searcher, createQuery("field", -5, -5, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 0, 0, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 5, 5, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 30, 30, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 35, 35, type), 1);

    assertNumberOfHits(searcher, createQuery("field", -90, -90, type), 0);
    assertNumberOfHits(searcher, createQuery("field", 6, 6, type), 0);
    assertNumberOfHits(searcher, createQuery("field", 40, 40, type), 0);

    // Test the lower end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", -90, -4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -80, -4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -70, -4, type), 1);
    assertNumberOfHits(searcher, createQuery("field", -80, -5, type), 2);

    // Test the upper end of the document value range.
    assertNumberOfHits(searcher, createQuery("field", 25, 34, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 25, 35, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 25, 36, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 30, 35, type), 2);

    // Test multiple occurrences of the same value.
    assertNumberOfHits(searcher, createQuery("field", -4, 4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -4, 0, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 4, type), 2);
    assertNumberOfHits(searcher, createQuery("field", 0, 30, type), 4);

    // Test ranges that span all documents.
    assertNumberOfHits(searcher, createQuery("field", -80, 35, type), 7);
    assertNumberOfHits(searcher, createQuery("field", -90, 40, type), 7);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortDocValuesWithSingleValue() throws Exception {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      testIndexSortDocValuesWithSingleValue(false, type);
      testIndexSortDocValuesWithSingleValue(true, type);
    }
  }

  private void testIndexSortDocValuesWithSingleValue(boolean reverse, SortField.Type type)
      throws IOException {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", 42, type));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertNumberOfHits(searcher, createQuery("field", 42, 43, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 42, 42, type), 1);
    assertNumberOfHits(searcher, createQuery("field", 41, 41, type), 0);
    assertNumberOfHits(searcher, createQuery("field", 43, 43, type), 0);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortMissingValues() throws Exception {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      testIndexSortMissingValues(type);
    }
  }

  public void testIndexSortMissingValues(SortField.Type type) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
    createIndexSort("field", type, false);
    sortField.setMissingValue(random().nextLong());
    iwc.setIndexSort(new Sort(sortField));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80, type));
    writer.addDocument(createDocument("field", -5, type));
    writer.addDocument(createDocument("field", 0, type));
    writer.addDocument(createDocument("field", 35, type));

    writer.addDocument(createDocument("other-field", 0, type));
    writer.addDocument(createDocument("other-field", 10, type));
    writer.addDocument(createDocument("other-field", 20, type));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertNumberOfHits(searcher, createQuery("field", -70, 0, type), 2);
    assertNumberOfHits(searcher, createQuery("field", -2, 35, type), 2);

    assertNumberOfHits(searcher, createQuery("field", -80, 35, type), 4);
    assertNumberOfHits(searcher, createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE, type), 4);

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
    Query query = createQuery("foo", 2, 4, SortField.Type.LONG);
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

    Query query = createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE, SortField.Type.LONG);
    Query rewrittenQuery = query.rewrite(newSearcher(reader));
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

    Query rewrittenQuery = query.rewrite(newSearcher(reader));
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
    writer.addDocument(createDocument("field", 0, SortField.Type.LONG));

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
    writer.addDocument(createDocument("field", 0, SortField.Type.LONG));

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
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

    Query query = createQuery("field", 0, 0, SortField.Type.LONG);
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

  private Document createDocument(String field, long value, SortField.Type type) {
    Document doc = new Document();
    if (type == SortField.Type.LONG || type == SortField.Type.INT) {
      doc.add(new SortedNumericDocValuesField(field, value));
    } else if (type == SortField.Type.FLOAT) {
      final float floatValue = Float.parseFloat(String.valueOf(value));
      doc.add(new SortedNumericDocValuesField(field, NumericUtils.floatToSortableInt(floatValue)));
    } else {
      // assume DOUBLE for the other case
      final double doubleValue = Double.parseDouble(String.valueOf(value));
      doc.add(
          new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(doubleValue)));
    }
    return doc;
  }

  private Query createQuery(String field, long lowerValue, long upperValue, SortField.Type type) {
    Query fallbackQuery;
    Query query;
    if (type == SortField.Type.LONG || type == SortField.Type.INT) {
      fallbackQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              field, lowerValue, upperValue, fallbackQuery);
    } else if (type == SortField.Type.FLOAT) {
      final float floatLower = Float.parseFloat(String.valueOf(lowerValue));
      final float floatUpper = Float.parseFloat(String.valueOf(upperValue));
      fallbackQuery =
          SortedNumericDocValuesField.newSlowRangeQuery(
              field,
              NumericUtils.floatToSortableInt(floatLower),
              NumericUtils.floatToSortableInt(floatUpper));
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              field, floatLower, floatUpper, fallbackQuery);
      // assume DOUBLE for the other case
    } else {
      final double doubleLower = Double.parseDouble(String.valueOf(lowerValue));
      final double doubleUpper = Double.parseDouble(String.valueOf(upperValue));
      fallbackQuery =
          SortedNumericDocValuesField.newSlowRangeQuery(
              field,
              NumericUtils.doubleToSortableLong(doubleLower),
              NumericUtils.doubleToSortableLong(doubleUpper));
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              field, doubleLower, doubleUpper, fallbackQuery);
    }
    return query;
  }

  public void testCountWithBkdAsc() throws Exception {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      doTestCountWithBkd(false, type);
    }
  }

  public void testCountWithBkdDesc() throws Exception {
    for (SortField.Type type :
        new SortField.Type[] {
          SortField.Type.INT, SortField.Type.LONG, SortField.Type.FLOAT, SortField.Type.DOUBLE
        }) {
      doTestCountWithBkd(true, type);
    }
  }

  private Sort createIndexSort(String filedName, SortField.Type type, boolean reverse) {
    if (type == SortField.Type.LONG || type == SortField.Type.INT) {
      return new Sort(new SortedNumericSortField(filedName, SortField.Type.LONG, reverse));
    } else if (type == SortField.Type.FLOAT) {
      return new Sort(new SortedNumericSortField(filedName, SortField.Type.FLOAT, reverse));
    } else {
      return new Sort(new SortedNumericSortField(filedName, SortField.Type.DOUBLE, reverse));
    }
  }

  public void doTestCountWithBkd(boolean reverse, SortField.Type type) throws Exception {
    String filedName = "field";
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = createIndexSort(filedName, type, reverse);
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    addDocWithBkd(writer, filedName, 7, 500, type);
    addDocWithBkd(writer, filedName, 5, 600, type);
    addDocWithBkd(writer, filedName, 11, 700, type);
    addDocWithBkd(writer, filedName, 13, 800, type);
    addDocWithBkd(writer, filedName, 9, 900, type);
    writer.flush();
    writer.forceMerge(1);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Both bounds exist in the dataset
    Query query = createRangeQuery(filedName, 7, 9, type);
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Both bounds do not exist in the dataset
    query = createRangeQuery(filedName, 6, 10, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound exists in the dataset, not the max
    query = createRangeQuery(filedName, 7, 10, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound doesn't exist in the dataset, max does
    query = createRangeQuery(filedName, 6, 9, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1400, weight.count(context));
    }

    // Min bound is the min value of the dataset
    query = createRangeQuery(filedName, 5, 8, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1100, weight.count(context));
    }

    // Min bound is less than min value of the dataset
    query = createRangeQuery(filedName, 4, 8, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1100, weight.count(context));
    }

    // Max bound is the max value of the dataset
    query = createRangeQuery(filedName, 10, 13, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1500, weight.count(context));
    }

    // Max bound is greater than max value of the dataset
    query = createRangeQuery(filedName, 10, 14, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(1500, weight.count(context));
    }

    // Everything matches
    query = createRangeQuery(filedName, 2, 14, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(3500, weight.count(context));
    }

    // Bounds equal to min/max values of the dataset, everything matches
    query = createRangeQuery(filedName, 2, 14, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(3500, weight.count(context));
    }

    // Bounds are less than the min value of the dataset
    query = createRangeQuery(filedName, 2, 3, type);
    weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    for (LeafReaderContext context : searcher.getLeafContexts()) {
      assertEquals(0, weight.count(context));
    }

    // Bounds are greater than the max value of the dataset
    query = createRangeQuery(filedName, 14, 15, type);
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
      addDocWithBkd(
          writer, filedName, random.nextInt(1000), random.nextInt(1000), SortField.Type.LONG);
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

  private void addDocWithBkd(
      RandomIndexWriter indexWriter, String field, long value, int repeat, SortField.Type type)
      throws IOException {
    for (int i = 0; i < repeat; i++) {
      Document doc = new Document();
      if (type == SortField.Type.LONG || type == SortField.Type.INT) {
        doc.add(new LongPoint(field, value));
        doc.add(new SortedNumericDocValuesField(field, value));
      } else if (type == SortField.Type.FLOAT) {
        final float floatValue = Float.parseFloat(String.valueOf(value));
        doc.add(new FloatPoint(field, floatValue));
        doc.add(
            new SortedNumericDocValuesField(field, NumericUtils.floatToSortableInt(floatValue)));
      } else {
        // assume DOUBLE for the other case
        final double doubleValue = Double.parseDouble(String.valueOf(value));
        doc.add(new DoublePoint(field, doubleValue));
        doc.add(
            new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(doubleValue)));
      }
      indexWriter.addDocument(doc);
    }
  }

  private Query createRangeQuery(
      String filedName, long lowerValue, long upperValue, SortField.Type type) {
    Query fallbackQuery;
    Query query;
    if (type == SortField.Type.LONG || type == SortField.Type.INT) {
      fallbackQuery = LongPoint.newRangeQuery(filedName, lowerValue, upperValue);
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              filedName, lowerValue, upperValue, fallbackQuery);
    } else if (type == SortField.Type.FLOAT) {
      fallbackQuery =
          FloatPoint.newRangeQuery(
              filedName,
              Float.parseFloat(String.valueOf(lowerValue)),
              Float.parseFloat(String.valueOf(upperValue)));
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              filedName,
              Float.parseFloat(String.valueOf(lowerValue)),
              Float.parseFloat(String.valueOf(upperValue)),
              fallbackQuery);
    } else {
      // assume DOUBLE for the other case
      fallbackQuery =
          DoublePoint.newRangeQuery(
              filedName,
              Double.parseDouble(String.valueOf(lowerValue)),
              Double.parseDouble(String.valueOf(upperValue)));
      query =
          new IndexSortSortedNumericDocValuesRangeQuery(
              filedName,
              Double.parseDouble(String.valueOf(lowerValue)),
              Double.parseDouble(String.valueOf(upperValue)),
              fallbackQuery);
    }
    return query;
  }
}
