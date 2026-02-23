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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.NumericFieldStats.MinMax;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestNumericFieldStats extends LuceneTestCase {

  public void testGlobalMinMaxWithLongField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(new LongField("field", value, Field.Store.NO));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(10L, minMax.min());
        assertEquals(30L, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxWithIntField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int value : new int[] {-5, 0, 42}) {
        final Document doc = new Document();
        doc.add(new IntField("field", value, Field.Store.NO));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(-5L, minMax.min());
        assertEquals(42L, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxFromDocValuesSkipper() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {100L, 200L, 300L}) {
        final Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField("field", value));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(100L, minMax.min());
        assertEquals(300L, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxEmptyIndex() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNull(minMax);
        assertEquals(0, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testGlobalMinMaxNonexistentField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new LongField("other_field", 42L, Field.Store.NO));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNull(minMax);
        assertEquals(0, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testGlobalMinMaxDocValuesWithoutSkipIndex() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("field", value));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNull(minMax);
      }
    }
  }

  public void testGlobalDocCountWithLongField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(new LongField("field", value, Field.Store.NO));
        w.addDocument(doc);
      }
      w.addDocument(new Document());
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        assertEquals(3, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testGlobalDocCountFromDocValuesSkipper() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField("field", value));
        w.addDocument(doc);
      }
      w.addDocument(new Document());
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        assertEquals(3, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testGlobalMinMaxWithExtremeLongValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc1 = new Document();
      doc1.add(new LongField("field", Long.MIN_VALUE, Field.Store.NO));
      w.addDocument(doc1);
      final Document doc2 = new Document();
      doc2.add(new LongField("field", Long.MAX_VALUE, Field.Store.NO));
      w.addDocument(doc2);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(Long.MIN_VALUE, minMax.min());
        assertEquals(Long.MAX_VALUE, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxWithExtremeIntValues() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc1 = new Document();
      doc1.add(new IntField("field", Integer.MIN_VALUE, Field.Store.NO));
      w.addDocument(doc1);
      final Document doc2 = new Document();
      doc2.add(new IntField("field", Integer.MAX_VALUE, Field.Store.NO));
      w.addDocument(doc2);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(Integer.MIN_VALUE, minMax.min());
        assertEquals(Integer.MAX_VALUE, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxMultiValuedField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc1 = new Document();
      doc1.add(SortedNumericDocValuesField.indexedField("field", 5L));
      doc1.add(SortedNumericDocValuesField.indexedField("field", 50L));
      w.addDocument(doc1);
      final Document doc2 = new Document();
      doc2.add(SortedNumericDocValuesField.indexedField("field", 25L));
      w.addDocument(doc2);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(5L, minMax.min());
        assertEquals(50L, minMax.max());
      }
    }
  }

  public void testGlobalMinMaxMultipleSegments() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc1 = new Document();
      doc1.add(new LongField("field", 100L, Field.Store.NO));
      w.addDocument(doc1);
      w.commit();

      final Document doc2 = new Document();
      doc2.add(new LongField("field", 50L, Field.Store.NO));
      w.addDocument(doc2);
      w.commit();

      final Document doc3 = new Document();
      doc3.add(new LongField("field", 200L, Field.Store.NO));
      w.addDocument(doc3);
      w.commit();

      try (IndexReader reader = DirectoryReader.open(w)) {
        assertTrue(reader.leaves().size() > 1);
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(50L, minMax.min());
        assertEquals(200L, minMax.max());
        assertEquals(3, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testGlobalMinMaxWithSegmentsWithAndWithoutField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new LongField("field", 100L, Field.Store.NO));
      w.addDocument(doc);
      w.commit();

      w.addDocument(new Document());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final MinMax minMax = NumericFieldStats.globalMinMax(searcher, "field");
        assertNotNull(minMax);
        assertEquals(100L, minMax.min());
        assertEquals(100L, minMax.max());
        assertEquals(1, NumericFieldStats.globalDocCount(searcher, "field"));
      }
    }
  }

  public void testRewriteWorksWithPointsButNoSkipIndex() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int i = 0; i < 100; i++) {
        final Document doc = new Document();
        doc.add(new LongField("field", 100 + i, Field.Store.NO));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        // Query range [0, 50] is entirely below field range [100, 199]
        Query query = SortedNumericDocValuesField.newSlowRangeQuery("field", 0, 50);
        Query rewritten = searcher.rewrite(query);
        assertThat(rewritten, instanceOf(MatchNoDocsQuery.class));

        // Query range [0, 250] covers entire field range [100, 199]
        // and all docs have a value
        query = SortedNumericDocValuesField.newSlowRangeQuery("field", 0, 250);
        rewritten = searcher.rewrite(query);
        assertThat(rewritten, instanceOf(MatchAllDocsQuery.class));
      }
    }
  }
}
