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
package org.apache.lucene.sandbox.facet.plain.histograms;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NumericUtils;

public class TestHistogramCollectorManager extends LuceneTestCase {

  public void testSingleValuedNoSkipIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("f", 3));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("f", 4));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("f", 6));
    w.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    LongIntHashMap actualCounts =
        searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    expectedCounts.put(0, 1);
    expectedCounts.put(1, 2);
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () ->
            searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4, 1)));

    reader.close();
    dir.close();
  }

  public void testMultiValuedNoSkipIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("f", 3));
    doc.add(new SortedNumericDocValuesField("f", 8));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("f", 4));
    doc.add(new SortedNumericDocValuesField("f", 6));
    doc.add(new SortedNumericDocValuesField("f", 8));
    w.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    LongIntHashMap actualCounts =
        searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    expectedCounts.put(0, 1);
    expectedCounts.put(1, 1);
    expectedCounts.put(2, 2);
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () ->
            searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4, 1)));

    reader.close();
    dir.close();
  }

  public void testSkipIndex() throws IOException {
    doTestSkipIndex(newIndexWriterConfig());
  }

  public void testSkipIndexWithSort() throws IOException {
    doTestSkipIndex(
        newIndexWriterConfig().setIndexSort(new Sort(new SortField("f", SortField.Type.LONG))));
  }

  public void testSkipIndexWithSortAndLowInterval() throws IOException {
    doTestSkipIndex(
        newIndexWriterConfig()
            .setIndexSort(new Sort(new SortField("f", SortField.Type.LONG)))
            .setCodec(TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(3))));
  }

  public void testMultiRangePointTreeCollector() throws IOException {
    Directory dir = newDirectory();
    long[] values = generateRandomData(5000);
    DirectoryReader reader = indexNumericData(dir, values, true);
    IndexSearcher searcher = newSearcher(reader);

    // Validate the MATCH_ALL case
    LongIntHashMap actualCounts =
        searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 1000));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    for (long value : values) {
      expectedCounts.addTo(Math.floorDiv(value, 1000), 1);
    }
    assertEquals(expectedCounts, actualCounts);

    // Validate the Point Range Query case
    int lowerBound = random().nextInt(0, 1500);
    int upperBound = random().nextInt(3500, 5000);

    byte[] lowerPoint = new byte[Long.BYTES];
    byte[] upperPoint = new byte[Long.BYTES];
    NumericUtils.longToSortableBytes(lowerBound, lowerPoint, 0);
    NumericUtils.longToSortableBytes(upperBound, upperPoint, 0);
    final PointRangeQuery prq =
        new PointRangeQuery("f", lowerPoint, upperPoint, 1) {
          @Override
          protected String toString(int dimension, byte[] value) {
            return Long.toString(NumericUtils.sortableBytesToLong(value, 0));
          }
        };

    actualCounts = searcher.search(prq, new HistogramCollectorManager("f", 1000));
    expectedCounts = new LongIntHashMap();
    for (long value : values) {
      if (value >= lowerBound && value <= upperBound) {
        expectedCounts.addTo(Math.floorDiv(value, 1000), 1);
      }
    }
    assertEquals(expectedCounts, actualCounts);

    // Validate the BoostQuery case
    actualCounts =
        searcher.search(new BoostQuery(prq, 1.5f), new HistogramCollectorManager("f", 1000));
    // Don't need to compute expectedCounts again as underlying point range
    // query is not changing
    assertEquals(expectedCounts, actualCounts);

    // Validate the ConstantScoreQuery case
    actualCounts =
        searcher.search(new ConstantScoreQuery(prq), new HistogramCollectorManager("f", 1000));
    // Don't need to compute expectedCounts again as underlying point range query is not changing
    assertEquals(expectedCounts, actualCounts);

    // Validate the FunctionScoreQuery case
    actualCounts =
        searcher.search(
            new FunctionScoreQuery(prq, DoubleValuesSource.SCORES),
            new HistogramCollectorManager("f", 1000));
    // Don't need to compute expectedCounts again as underlying point range query is not changing
    assertEquals(expectedCounts, actualCounts);

    // Validate the IndexOrDocValuesQuery case
    actualCounts =
        searcher.search(
            new IndexOrDocValuesQuery(prq, prq), new HistogramCollectorManager("f", 1000));
    // Don't need to compute expectedCounts again as underlying point range query is not changing
    assertEquals(expectedCounts, actualCounts);

    // Validate the recursive wrapping case
    actualCounts =
        searcher.search(
            new ConstantScoreQuery(
                new BoostQuery(
                    new FunctionScoreQuery(
                        new IndexOrDocValuesQuery(prq, prq), DoubleValuesSource.SCORES),
                    1.5f)),
            new HistogramCollectorManager("f", 1000));
    // Don't need to compute expectedCounts again as underlying point range query is not changing
    assertEquals(expectedCounts, actualCounts);

    reader.close();
    dir.close();
  }

  public void testHistogramCollectorExceptionWithoutDocValues() throws IOException {
    Directory dir = newDirectory();
    long[] values = generateRandomData(5000);
    DirectoryReader reader = indexNumericData(dir, values, false);
    IndexSearcher searcher = newSearcher(reader);

    // Validate that exception is thrown when doc values is disabled on numeric field
    expectThrows(
        IllegalStateException.class,
        () ->
            searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 1000)));

    reader.close();
    dir.close();
  }

  private long[] generateRandomData(int bound) {
    long[] values = new long[bound];
    for (int i = 0; i < values.length; i++) {
      values[i] = random().nextInt(0, bound); // Generates a random integer
    }
    return values;
  }

  private DirectoryReader indexNumericData(Directory dir, long[] values, boolean docValueEnabled)
      throws IOException {
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());

    for (long value : values) {
      Document doc = new Document();
      // Adding indexed point field to verify multi range collector
      doc.add(new LongPoint("f", value));
      if (docValueEnabled) {
        // Doc values need to be enabled for histogram collection
        doc.add(new NumericDocValuesField("f", value));
      }
      w.addDocument(doc);
    }

    DirectoryReader reader = DirectoryReader.open(w);
    w.close();

    return reader;
  }

  private void doTestSkipIndex(IndexWriterConfig cfg) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, cfg);
    long[] values = new long[] {3, 6, 0, 4, 6, 12, 8, 8, 7, 8, 0, 4, 3, 6, 11};
    for (long value : values) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("f", value));
      w.addDocument(doc);
    }

    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    LongIntHashMap actualCounts =
        searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    for (long value : values) {
      expectedCounts.addTo(Math.floorDiv(value, 4), 1);
    }
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () ->
            searcher.search(MatchAllDocsQuery.INSTANCE, new HistogramCollectorManager("f", 4, 1)));

    // Create a query so that bucket "1" (values from 4 to 8), which is in the middle of the range,
    // doesn't match any docs. HistogramCollector should not add an entry with a count of 0 in this
    // case.
    Query query =
        new BooleanQuery.Builder()
            .add(NumericDocValuesField.newSlowRangeQuery("f", Long.MIN_VALUE, 2), Occur.SHOULD)
            .add(NumericDocValuesField.newSlowRangeQuery("f", 10, Long.MAX_VALUE), Occur.SHOULD)
            .build();
    actualCounts = searcher.search(query, new HistogramCollectorManager("f", 4, 3));
    expectedCounts = new LongIntHashMap();
    for (long value : values) {
      if (value <= 2 || value >= 10) {
        expectedCounts.addTo(Math.floorDiv(value, 4), 1);
      }
    }
    assertEquals(expectedCounts, actualCounts);

    reader.close();
    dir.close();
  }
}
