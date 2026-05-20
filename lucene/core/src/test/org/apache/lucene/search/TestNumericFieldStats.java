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

import java.io.IOException;
import java.net.InetAddress;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.NumericFieldStats.Stats;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestNumericFieldStats extends LuceneTestCase {

  private static Field newCustomWidthPoint(String name, int numBytes, long value) {
    FieldType type = new FieldType();
    type.setDimensions(1, numBytes);
    type.freeze();
    byte[] packed = new byte[numBytes];
    for (int i = numBytes - 1; i >= 0; i--) {
      packed[i] = (byte) (value & 0xFF);
      value >>= 8;
    }
    packed[0] ^= (byte) 0x80;
    return new Field(name, new BytesRef(packed), type);
  }

  private static long minValueForWidth(int numBytes) {
    return -(1L << (numBytes * 8 - 1));
  }

  private static long maxValueForWidth(int numBytes) {
    return (1L << (numBytes * 8 - 1)) - 1;
  }

  public void testGetStatsWithAllByteWidths() throws IOException {
    for (int numBytes = 1; numBytes <= Long.BYTES; numBytes++) {
      long min = minValueForWidth(numBytes);
      long max = maxValueForWidth(numBytes);
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (long value : new long[] {min, 0, max}) {
          final Document doc = new Document();
          doc.add(newCustomWidthPoint("field", numBytes, value));
          w.addDocument(doc);
        }
        w.commit();
        try (IndexReader reader = DirectoryReader.open(w)) {
          final Stats stats = NumericFieldStats.getStats(reader, "field");
          assertNotNull("numBytes=" + numBytes, stats);
          assertEquals("numBytes=" + numBytes, min, stats.min());
          assertEquals("numBytes=" + numBytes, max, stats.max());
          assertEquals("numBytes=" + numBytes, 3, stats.docCount());
        }
      }
    }
  }

  public void testGetStatsWithLongField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(new LongField("field", value, Field.Store.NO));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(10L, stats.min());
        assertEquals(30L, stats.max());
      }
    }
  }

  public void testGetStatsWithIntField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (int value : new int[] {-5, 0, 42}) {
        final Document doc = new Document();
        doc.add(new IntField("field", value, Field.Store.NO));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(-5L, stats.min());
        assertEquals(42L, stats.max());
      }
    }
  }

  public void testGetStatsFromDocValuesSkipper() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {100L, 200L, 300L}) {
        final Document doc = new Document();
        doc.add(SortedNumericDocValuesField.indexedField("field", value));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(100L, stats.min());
        assertEquals(300L, stats.max());
      }
    }
  }

  public void testGetStatsEmptyIndex() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        assertNull(NumericFieldStats.getStats(reader, "field"));
      }
    }
  }

  public void testGetStatsNonexistentField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new LongField("other_field", 42L, Field.Store.NO));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        assertNull(NumericFieldStats.getStats(reader, "field"));
      }
    }
  }

  public void testGetStatsDocValuesWithoutSkipIndex() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      for (long value : new long[] {10L, 20L, 30L}) {
        final Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("field", value));
        w.addDocument(doc);
      }
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNull(stats);
      }
    }
  }

  public void testDocCountWithLongField() throws IOException {
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
        assertEquals(3, NumericFieldStats.getStats(reader, "field").docCount());
      }
    }
  }

  public void testDocCountFromDocValuesSkipper() throws IOException {
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
        assertEquals(3, NumericFieldStats.getStats(reader, "field").docCount());
      }
    }
  }

  public void testGetStatsWithExtremeLongValues() throws IOException {
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
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(Long.MIN_VALUE, stats.min());
        assertEquals(Long.MAX_VALUE, stats.max());
      }
    }
  }

  public void testGetStatsWithExtremeIntValues() throws IOException {
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
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(Integer.MIN_VALUE, stats.min());
        assertEquals(Integer.MAX_VALUE, stats.max());
      }
    }
  }

  public void testGetStatsMultiValuedField() throws IOException {
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
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(5L, stats.min());
        assertEquals(50L, stats.max());
      }
    }
  }

  public void testGetStatsMultipleSegments() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config = newIndexWriterConfig();
      config.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, config)) {
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
          final Stats stats = NumericFieldStats.getStats(reader, "field");
          assertNotNull(stats);
          assertEquals(50L, stats.min());
          assertEquals(200L, stats.max());
          assertEquals(3, stats.docCount());
        }
      }
    }
  }

  public void testGetStatsWithSegmentsWithAndWithoutField() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new LongField("field", 100L, Field.Store.NO));
      w.addDocument(doc);
      w.commit();

      w.addDocument(new Document());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(w)) {
        final Stats stats = NumericFieldStats.getStats(reader, "field");
        assertNotNull(stats);
        assertEquals(100L, stats.min());
        assertEquals(100L, stats.max());
        assertEquals(1, stats.docCount());
      }
    }
  }

  public void testGetStatsReturnsNullForWidePointValues() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new InetAddressPoint("field", InetAddress.getByName("192.168.0.1")));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        assertNull(NumericFieldStats.getStats(reader, "field"));
      }
    }
  }
}
