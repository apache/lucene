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
package org.apache.lucene.sandbox.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexSortSortedNumericDocValuesRangeQueryByType extends LuceneTestCase {
  public void testIndexSortDocValues() throws Exception {
    testIndexSortDocValuesWithEvenLength(true);
    testIndexSortDocValuesWithEvenLength(false);
  }

  public void testUnsupportedIndexSortDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final SortField.Type type =
        RandomPicks.randomFrom(
            random(), new SortField.Type[] {SortField.Type.FLOAT, SortField.Type.DOUBLE});
    Sort indexSort = new Sort(new SortedNumericSortField("field", type, false));
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

    assertThrows(
        IllegalArgumentException.class, () -> searcher.count(createQuery("field", -80, -80)));

    writer.close();
    reader.close();
    dir.close();
  }

  private void testIndexSortDocValuesWithEvenLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final SortField.Type type =
        RandomPicks.randomFrom(
            random(), new SortField.Type[] {SortField.Type.INT, SortField.Type.LONG});
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
    assertEquals(1, searcher.count(createQuery("field", -80, -80)));
    assertEquals(1, searcher.count(createQuery("field", -5, -5)));
    assertEquals(2, searcher.count(createQuery("field", 0, 0)));
    assertEquals(1, searcher.count(createQuery("field", 30, 30)));
    assertEquals(1, searcher.count(createQuery("field", 35, 35)));

    assertEquals(0, searcher.count(createQuery("field", -90, -90)));
    assertEquals(0, searcher.count(createQuery("field", 5, 5)));
    assertEquals(0, searcher.count(createQuery("field", 40, 40)));

    // Test the lower end of the document value range.
    assertEquals(2, searcher.count(createQuery("field", -90, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -4)));
    assertEquals(1, searcher.count(createQuery("field", -70, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -5)));

    // Test the upper end of the document value range.
    assertEquals(1, searcher.count(createQuery("field", 25, 34)));
    assertEquals(2, searcher.count(createQuery("field", 25, 35)));
    assertEquals(2, searcher.count(createQuery("field", 25, 36)));
    assertEquals(2, searcher.count(createQuery("field", 30, 35)));

    // Test multiple occurrences of the same value.
    assertEquals(2, searcher.count(createQuery("field", -4, 4)));
    assertEquals(2, searcher.count(createQuery("field", -4, 0)));
    assertEquals(2, searcher.count(createQuery("field", 0, 4)));
    assertEquals(3, searcher.count(createQuery("field", 0, 30)));

    // Test ranges that span all documents.
    assertEquals(6, searcher.count(createQuery("field", -80, 35)));
    assertEquals(6, searcher.count(createQuery("field", -90, 40)));

    writer.close();
    reader.close();
    dir.close();
  }

  private Query createQuery(String field, long lowerValue, long upperValue) {
    Query fallbackQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  private Document createDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    return doc;
  }
}
