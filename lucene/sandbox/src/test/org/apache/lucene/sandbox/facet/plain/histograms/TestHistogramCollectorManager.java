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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

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
        searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    expectedCounts.put(0, 1);
    expectedCounts.put(1, 2);
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4, 1)));

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
        searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    expectedCounts.put(0, 1);
    expectedCounts.put(1, 1);
    expectedCounts.put(2, 2);
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4, 1)));

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
        searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4));
    LongIntHashMap expectedCounts = new LongIntHashMap();
    for (long value : values) {
      expectedCounts.addTo(Math.floorDiv(value, 4), 1);
    }
    assertEquals(expectedCounts, actualCounts);

    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), new HistogramCollectorManager("f", 4, 1)));

    reader.close();
    dir.close();
  }
}
